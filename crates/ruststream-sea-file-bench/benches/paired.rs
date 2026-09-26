// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a transport fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `sea-streamer-file` client it wraps, and what the framework's
//! runtime costs on top of that.
//!
//! Every scenario runs three times over, as three loops that differ in one thing each: what
//! carries the messages.
//!
//! - **raw** - `sea-streamer-file` driven directly: its streamer, its consumer, its producer.
//! - **adapter** - this crate's own types, hand-driven: [`FileBroker`], [`FileStream`], the
//!   subscription stream it yields, the delivery's payload and its settlement, and
//!   [`FilePublisher`]. No handler, no app, no dispatch.
//! - **framework** - the service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! `adapter` against `raw` is what this crate's consumer and publisher cost over the library they
//! wrap. `framework` against `adapter` is what the runtime costs on top, over this transport.
//!
//! Everything else is held equal: the same stream file, the same consumer options, the same decode
//! into the same type, the same payload bytes, the same tokio runtime and the same binary. The
//! procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! Every run owns a stream file of its own, under [`DIR_VAR`], and removes it when it is done, so
//! a run never reads what the one before it left behind.
//!
//! [`Scenario::Replay`] writes the whole file first and then reads it back from the beginning.
//! Nothing paces that read: the bytes are already there, which is what makes this the one place
//! where the cost of dispatch is visible in full rather than hidden behind a wait. The recorded
//! region holds [`TAIL_SLACK`] bodies more than the window measures, so no loop ever reaches the
//! end of the file.
//!
//! [`Scenario::Tail`] attaches the subscription first and appends to the file while it reads. Each
//! loop appends through its own layer, and an append is durable before the next one starts - what
//! [`FilePublisher`] does per publish - so this row is paced by the filesystem.
//!
//! The window runs from the first delivery to the end of the last handler call. Opening the file,
//! creating the consumer and writing the recorded region are startup cost and sit outside it.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on,
//! up to the file-size ceiling in [`MAX_MESSAGES`].
//!
//! Rounds are interleaved - raw, adapter, framework, raw, adapter, framework - and each loop
//! reports its best, median and worst round. The best is the headline: noise only ever slows a
//! run down, so the fastest round is the closest to the undisturbed cost. The distance between
//! the best and the worst is the noise a difference has to clear. Blocking one loop and then the
//! next would charge every drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! They are a property of the filesystem under [`DIR_VAR`] as much as of this code. Every loop
//! reads a file written moments earlier on a machine with room to spare for it, so the numbers are
//! taken with the file in the page cache; a replay served from the device measures the device.
//!
//! Nothing is acknowledged here. The transport keeps no consumer positions, so a settlement
//! reports `AckError::Unsupported` instead of pretending, and the ack position the procedure asks
//! about is the same in all three loops.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use ruststream::runtime::RunningApp;
use ruststream::{
    AckError, ConnectedBroker as _, IncomingMessage as _, OutgoingMessage, Publisher as _,
    Subscriber as _,
};
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::{ConnectedFileBroker, FileSubscriber};
use sea_streamer_file::{
    AutoStreamReset, FileConnectOptions, FileConsumer, FileConsumerOptions, FileId, FileProducer,
    FileProducerOptions, FileStreamer,
};
use sea_streamer_types::{
    Buffer as _, Consumer as _, ConsumerMode, ConsumerOptions as _, Message as _, Producer as _,
    StreamKey, Streamer as _,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw and every handler call runs inside a task-local scope, so
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: `ruststream-sea-file`'s dev-dependencies enable that feature through
// the conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Names the directory every run's stream file is created in.
///
/// It decides what is being measured, so there is no default: a directory on a tmpfs measures
/// memory, and one on a network mount measures the network.
const DIR_VAR: &str = "RUSTSTREAM_BENCH_DIR";

/// Deliveries the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 100_000;
/// Durable appends the round-trip probe times, after a tenth of that as warm-up.
const PROBE_APPENDS: usize = 50_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count.
///
/// Every message is a body on disk here, so a count is a file size: at [`BODY_BYTES`] this is
/// about a gibibyte per run, written and read back thirty-six times over. A machine fast enough
/// to want more would be measuring its device rather than this crate.
const MAX_MESSAGES: usize = 2_000_000;
/// Bodies the recorded region of a replay holds beyond what the window measures.
///
/// The client's replay ends at the end of the file and drops what it had read but not handed on,
/// so a window that reached the last body would be racing that. The slack keeps every loop far
/// from it.
const TAIL_SLACK: usize = 8_192;
/// Rounds run. Each loop reports its best, median and worst round.
const ROUNDS: usize = 3;
/// Worker threads every loop is driven on.
const WORKERS: usize = 4;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(60);

/// How far the writer may run ahead of a tailing subscription, in messages.
///
/// The client's send queue is unbounded, so without a ceiling the writer would turn a paced
/// scenario into a replay with extra steps - and hold the whole run in memory while it did.
const IN_FLIGHT: usize = 32_768;
/// How often the writer checks that ceiling.
const CHECK_EVERY: usize = 512;
/// How many appends the recorded region of a replay puts between flushes: enough to keep the
/// client's send queue bounded, rare enough not to make writing the file the slow part.
const RECORD_FLUSH_EVERY: usize = 8_192;
/// The internal stream key this crate primes a fresh file with, spelled out here so the raw loop
/// opens its live consumer on a file in the same state. No subscription of a run matches it.
const PRIME_STREAM: &str = "ruststream-internal";

/// The body size every loop publishes and decodes, to the byte: the scenario is published under
/// this number, so the bytes in the file have to be it.
const BODY_BYTES: usize = 512;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What every loop publishes.
///
/// The padding is what makes the body the size the scenario is published under. It is a field of
/// the type rather than bytes glued to the outside, so the framework's publish path encodes
/// exactly the bytes the other two loops send as they are.
#[derive(Debug, Outgoing, Serialize)]
struct Body {
    id: u64,
    quantity: u32,
    pad: String,
}

impl Clone for Body {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            quantity: self.quantity,
            pad: self.pad.clone(),
        }
    }
}

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and the padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// The one body every loop sends, in both the forms the three loops need.
struct Payload {
    value: Body,
    bytes: Vec<u8>,
}

/// Builds that body at exactly `size` bytes.
///
/// The assertion holds the promise the published scenario name makes.
fn payload(size: usize) -> Payload {
    let bare = serde_json::to_vec(&Body {
        id: ID,
        quantity: QUANTITY,
        pad: String::new(),
    })
    .expect("the body serializes");
    let value = Body {
        id: ID,
        quantity: QUANTITY,
        pad: repeat_n('x', size - bare.len()).collect(),
    };
    let bytes = serde_json::to_vec(&value).expect("the body serializes");
    assert_eq!(
        bytes.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    Payload { value, bytes }
}

/// The names one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Names {
    path: String,
    stream: String,
}

impl Names {
    fn fresh(dir: &Path) -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            path: dir
                .join(format!("bench-{stamp}.ss"))
                .to_string_lossy()
                .into_owned(),
            stream: format!("bench-{stamp}"),
        }
    }

    fn key(&self) -> StreamKey {
        StreamKey::new(&self.stream).expect("a timestamped stream key is valid")
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run reads.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// All three loops call the same methods, so all three pay for the signal. A delivery pays one
/// relaxed increment and two comparisons; the waiter is a single future for the whole run, woken
/// once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last handler call.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, which: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{which}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What one measured loop produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// The stream file this run read, once it held everything it was going to hold.
    file_bytes: u64,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

// ---------------------------------------------------------------------------------------------
// The file, shared by every loop
// ---------------------------------------------------------------------------------------------

/// Opens a connection to the stream file, creating it when it is not there.
///
/// The options are [`FileBroker`]'s own, so the raw loop opens the file the way this crate does.
async fn open(path: &str) -> FileStreamer {
    let mut options = FileConnectOptions::default();
    options.set_create_if_not_exists(true);
    let uri = FileId::new(path.to_owned())
        .to_streamer_uri()
        .expect("a path is a streamer uri");
    FileStreamer::connect(uri, options)
        .await
        .expect("the stream file opens")
}

async fn open_producer(streamer: &FileStreamer) -> FileProducer {
    streamer
        .create_generic_producer(FileProducerOptions::default())
        .await
        .expect("the producer is created")
}

/// The consumer options this crate opens a subscription with, spelled out here so the raw loop
/// asks the client for the same thing.
fn consumer_options(replay: bool) -> FileConsumerOptions {
    let mut options = FileConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(if replay {
        AutoStreamReset::Earliest
    } else {
        AutoStreamReset::Latest
    });
    options.set_live_streaming(!replay);
    options
}

/// One durable append: the body is in the file before the call returns.
///
/// This is what this crate's publisher does per publish, spelled out on the client so the raw loop
/// appends the way the other two do.
async fn append_raw(producer: &FileProducer, key: &StreamKey, body: &[u8]) {
    producer
        .send_to(key, body)
        .expect("the body is queued")
        .await
        .expect("the body reaches the file");
    producer
        .clone()
        .flush()
        .await
        .expect("the append reaches the file");
}

/// Writes the recorded region of a replay.
///
/// It is startup work - complete and flushed before any subscription opens - so it batches its
/// flushes instead of paying for one per body.
async fn record(producer: &FileProducer, key: &StreamKey, messages: usize, body: &[u8]) {
    let mut flusher = producer.clone();
    for sent in 1..=messages {
        // The receipt is dropped rather than awaited: the client's queue is what orders the
        // appends, and a round trip per body would make writing the file the slow part.
        let _receipt = producer.send_to(key, body).expect("the body is queued");
        if sent.is_multiple_of(RECORD_FLUSH_EVERY) {
            flusher.flush().await.expect("the region reaches the file");
        }
    }
    flusher.flush().await.expect("the region reaches the file");
}

/// Holds a tailing writer to [`IN_FLIGHT`] messages ahead of the subscription.
async fn hold_back(sent: usize, run: &Run) {
    if sent.is_multiple_of(CHECK_EVERY) {
        while sent.saturating_sub(run.handled()) > IN_FLIGHT {
            sleep(Duration::from_micros(200)).await;
        }
    }
}

fn file_bytes(path: &str) -> u64 {
    fs::metadata(path).map_or(0, |meta| meta.len())
}

fn remove(path: &str) {
    let _ = fs::remove_file(path);
}

/// What the filesystem charges for one durable append, measured outside every loop.
///
/// This is the round trip of a transport with no server: what a delivery waits on when it waits on
/// anything. A row is marked `broker_bound` against this figure rather than against a guess about
/// who outran whom.
async fn round_trip(dir: &Path, body: &[u8]) -> Duration {
    let names = Names::fresh(dir);
    let streamer = open(&names.path).await;
    let producer = open_producer(&streamer).await;
    let key = names.key();
    for _ in 0..PROBE_APPENDS / 10 {
        append_raw(&producer, &key, body).await;
    }
    let start = Instant::now();
    for _ in 0..PROBE_APPENDS {
        append_raw(&producer, &key, body).await;
    }
    let each = start.elapsed() / PROBE_APPENDS as u32;
    let _ = streamer.disconnect().await;
    remove(&names.path);
    each
}

// ---------------------------------------------------------------------------------------------
// The raw loop: the client, driven directly
// ---------------------------------------------------------------------------------------------

/// Reads until the run is over, decoding every body into [`Order`] and touching a field.
///
/// This is the loop a service writes when it reads the file itself: one consumer, one decode, no
/// crate in between. It settles nothing, because the client has nothing to settle.
async fn consume_raw(consumer: &FileConsumer, run: &Run) {
    loop {
        let message = consumer.next().await.unwrap_or_else(|e| {
            panic!(
                "raw: the subscription ended after {} of {} deliveries: {e}",
                run.handled(),
                run.0.total
            )
        });
        let order: Order =
            serde_json::from_slice(message.message().as_bytes()).expect("the body decodes");
        black_box((order.id, order.quantity));
        if run.arrived() {
            return;
        }
    }
}

async fn raw_replay(names: &Names, messages: usize, body: &Payload) -> Sample {
    let writer = open(&names.path).await;
    let producer = open_producer(&writer).await;
    record(&producer, &names.key(), messages + TAIL_SLACK, &body.bytes).await;

    let reader = open(&names.path).await;
    // The connected broker the other two loops open keeps a producer of its own beside the
    // subscription; this one is here so the reading connection is the same shape.
    let _idle = open_producer(&reader).await;
    let consumer = reader
        .create_consumer(&[names.key()], consumer_options(true))
        .await
        .expect("the consumer is created");

    let run = Run::new(messages);
    consume_raw(&consumer, &run).await;

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    drop(consumer);
    let _ = reader.disconnect().await;
    let _ = writer.disconnect().await;
    remove(&names.path);
    sample
}

async fn raw_tail(names: &Names, messages: usize, body: &Payload) -> Sample {
    let streamer = open(&names.path).await;
    let producer = open_producer(&streamer).await;
    // A live consumer cannot be created on a file with no content, which is why this crate primes
    // a fresh file on connect. The raw loop writes the same marker on the same internal key.
    let prime = StreamKey::new(PRIME_STREAM).expect("the internal key is valid");
    append_raw(&producer, &prime, b"1").await;
    let consumer = streamer
        .create_consumer(&[names.key()], consumer_options(false))
        .await
        .expect("the consumer is created");

    let run = Run::new(messages);
    let writing = tokio::spawn({
        let (producer, key, bytes, run) = (producer, names.key(), body.bytes.clone(), run.clone());
        async move {
            for sent in 1..=messages {
                hold_back(sent, &run).await;
                append_raw(&producer, &key, &bytes).await;
            }
        }
    });
    consume_raw(&consumer, &run).await;
    writing.await.expect("the writing task ends");

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    drop(consumer);
    let _ = streamer.disconnect().await;
    remove(&names.path);
    sample
}

// ---------------------------------------------------------------------------------------------
// The adapter loop: this crate's own consumer and publisher, hand-driven
// ---------------------------------------------------------------------------------------------

/// Pulls from the subscription this crate yields, decodes, reads a field and settles.
///
/// The window closes where the framework closes it: after the field is read and before the
/// settlement, which on this transport reports `AckError::Unsupported` rather than pretending.
async fn consume_adapter(subscriber: &mut FileSubscriber, run: &Run) {
    let mut deliveries = subscriber.stream();
    while let Some(delivery) = deliveries.next().await {
        let message = delivery.unwrap_or_else(|e| {
            panic!(
                "adapter: the subscription failed after {} of {} deliveries: {e}",
                run.handled(),
                run.0.total
            )
        });
        let order: Order = serde_json::from_slice(message.payload()).expect("the body decodes");
        black_box((order.id, order.quantity));
        let done = run.arrived();
        if let Err(e) = message.ack().await {
            assert!(
                matches!(e, AckError::Unsupported),
                "adapter: the settlement failed: {e}"
            );
        }
        if done {
            return;
        }
    }
    panic!(
        "adapter: the subscription ended after {} of {} deliveries",
        run.handled(),
        run.0.total
    );
}

async fn connect(path: &str) -> ConnectedFileBroker {
    FileBroker::new(path.to_owned())
        .connect()
        .await
        .expect("the broker connects")
}

async fn adapter_replay(names: &Names, messages: usize, body: &Payload) -> Sample {
    let writer = open(&names.path).await;
    let producer = open_producer(&writer).await;
    record(&producer, &names.key(), messages + TAIL_SLACK, &body.bytes).await;

    let connected = connect(&names.path).await;
    let mut subscriber = connected
        .subscribe_stream(FileStream::new(&names.stream).replay())
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    consume_adapter(&mut subscriber, &run).await;

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    drop(subscriber);
    connected.shutdown().await.expect("the broker shuts down");
    let _ = writer.disconnect().await;
    remove(&names.path);
    sample
}

async fn adapter_tail(names: &Names, messages: usize, body: &Payload) -> Sample {
    // `connect` creates the file and primes it, which is what lets a live subscription open.
    let connected = connect(&names.path).await;
    let mut subscriber = connected
        .subscribe_stream(FileStream::new(&names.stream))
        .await
        .expect("the subscription opens");
    let publisher = connected.publisher();

    let run = Run::new(messages);
    let writing = tokio::spawn({
        let (publisher, stream, bytes, run) = (
            publisher,
            names.stream.clone(),
            body.bytes.clone(),
            run.clone(),
        );
        async move {
            for sent in 1..=messages {
                hold_back(sent, &run).await;
                publisher
                    .publish(OutgoingMessage::new(&stream, &bytes), None)
                    .await
                    .expect("the publish reaches the file");
            }
        }
    });
    consume_adapter(&mut subscriber, &run).await;
    writing.await.expect("the writing task ends");

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    drop(subscriber);
    connected.shutdown().await.expect("the broker shuts down");
    remove(&names.path);
    sample
}

// ---------------------------------------------------------------------------------------------
// The framework loop: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber(FileStream::new(installed().stream).replay())]
async fn replay_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(FileStream::new(installed().stream))]
async fn tail_consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start_replay(broker: FileBroker, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("sea-file-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(broker, |b| {
            b.include(replay_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn start_tail(broker: FileBroker, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("sea-file-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(broker, |b| {
            b.include(tail_consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn framework_replay(names: &Names, messages: usize, body: &Payload) -> Sample {
    let writer = open(&names.path).await;
    let producer = open_producer(&writer).await;
    record(&producer, &names.key(), messages + TAIL_SLACK, &body.bytes).await;

    let run = Run::new(messages);
    install(names);
    let app = start_replay(FileBroker::new(names.path.clone()), run.clone()).await;
    drain(&run, "framework replay").await;

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    app.shutdown().await.expect("the service stops");
    let _ = writer.disconnect().await;
    remove(&names.path);
    sample
}

async fn framework_tail(names: &Names, messages: usize, body: &Payload) -> Sample {
    let broker = FileBroker::new(names.path.clone());
    // The publisher a service hands out before the app starts: it resolves against the connection
    // the runtime opens, and it publishes through the builder, so the codec is in the number.
    let publisher = broker.publisher();

    let run = Run::new(messages);
    install(names);
    let app = start_tail(broker, run.clone()).await;
    let writing = tokio::spawn({
        let (publisher, stream, value, run) = (
            publisher,
            names.stream.clone(),
            body.value.clone(),
            run.clone(),
        );
        async move {
            for sent in 1..=messages {
                hold_back(sent, &run).await;
                publisher
                    .message(&value)
                    .to(stream.as_str())
                    .publish()
                    .await
                    .expect("the publish reaches the file");
            }
        }
    });
    drain(&run, "framework tail").await;
    writing.await.expect("the writing task ends");

    let sample = Sample {
        window: run.window(),
        file_bytes: file_bytes(&names.path),
    };
    app.shutdown().await.expect("the service stops");
    remove(&names.path);
    sample
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Replay,
    Tail,
}

/// Which of the three loops a run is.
#[derive(Clone, Copy, Debug)]
enum Layer {
    Raw,
    Adapter,
    Framework,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Replay => "replay of a recorded stream file, 512 B JSON",
            Self::Tail => "live tail of a file being appended to, 512 B JSON",
        }
    }

    /// Durable appends the filesystem charges for one delivery.
    ///
    /// A replay reads a file that is already complete, so nothing inside its window waits on
    /// storage. A tail pays for one append per delivery, which is what paces it.
    fn appends_per_delivery(self) -> f64 {
        match self {
            Self::Replay => 0.0,
            Self::Tail => 1.0,
        }
    }

    async fn run(self, layer: Layer, names: &Names, messages: usize, body: &Payload) -> Sample {
        match (self, layer) {
            (Self::Replay, Layer::Raw) => raw_replay(names, messages, body).await,
            (Self::Replay, Layer::Adapter) => adapter_replay(names, messages, body).await,
            (Self::Replay, Layer::Framework) => framework_replay(names, messages, body).await,
            (Self::Tail, Layer::Raw) => raw_tail(names, messages, body).await,
            (Self::Tail, Layer::Adapter) => adapter_tail(names, messages, body).await,
            (Self::Tail, Layer::Framework) => framework_tail(names, messages, body).await,
        }
    }
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    verdict: &'static str,
    broker_bound: bool,
    file_bytes: u64,
}

async fn measure(
    scenario: Scenario,
    dir: &Path,
    rounds: usize,
    seconds: f64,
    body: &Payload,
    trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`, within the file-size ceiling.
    let probe = scenario
        .run(Layer::Raw, &Names::fresh(dir), PROBE_MESSAGES, body)
        .await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    let mut file_bytes = 0;
    for round in 1..=rounds {
        let raw = scenario
            .run(Layer::Raw, &Names::fresh(dir), messages, body)
            .await;
        let adapter = scenario
            .run(Layer::Adapter, &Names::fresh(dir), messages, body)
            .await;
        let framework = scenario
            .run(Layer::Framework, &Names::fresh(dir), messages, body)
            .await;
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages)
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
        file_bytes = file_bytes
            .max(raw.file_bytes)
            .max(adapter.file_bytes)
            .max(framework.file_bytes);
    }

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    let difference = (raw.best - framework.best).abs();
    let adapter_difference = (raw.best - adapter.best).abs();
    // What the transport charges for a message, against what a message cost. Half or more of it
    // and the row is a lower bound on the cost of dispatch rather than a measurement of it.
    let storage = scenario.appends_per_delivery() * trip.as_secs_f64();
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.best - framework.best) / raw.best * 100.0,
        adapter_overhead_percent: (raw.best - adapter.best) / raw.best * 100.0,
        adapter_verdict: if adapter_difference < raw.spread().max(adapter.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        verdict: if difference < raw.spread().max(framework.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: storage >= 0.5 / raw.best,
        file_bytes,
    }
}

fn document(measured: &[Measured], dir: &Path, trip: Duration) -> String {
    let mut out = String::from("{\n  \"scenarios\": [\n");
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            overhead = row.overhead_percent,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ],\n");
    // The storage the run was taken on. The numbers are a property of it as much as of the code,
    // and the script that publishes them cannot work out from a machine what directory this run
    // was pointed at.
    write!(
        out,
        concat!(
            "  \"storage\": {{\n",
            "    \"directory\": \"{directory}\",\n",
            "    \"largest_file_bytes\": {bytes},\n",
            "    \"round_trip_nanos\": {trip}\n",
            "  }}\n",
            "}}\n",
        ),
        directory = dir.display(),
        bytes = measured.iter().map(|row| row.file_bytes).max().unwrap_or(0),
        trip = trip.as_nanos(),
    )
    .expect("writing to a String");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a round count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let dir = PathBuf::from(env::var(DIR_VAR).unwrap_or_else(|_| {
        panic!("{DIR_VAR} names the directory the stream files go in; `just bench` sets it")
    }));
    fs::create_dir_all(&dir).expect("the stream file directory is writable");
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let body = payload(BODY_BYTES);
    let runtime = runtime();
    let trip = runtime.block_on(round_trip(&dir, &body.bytes));
    println!("one durable append: {:.1} us", trip.as_secs_f64() * 1e6);

    let measured: Vec<Measured> = [Scenario::Replay, Scenario::Tail]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &dir, rounds, seconds, &body, trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%, {}{})",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", storage-bound"
            } else {
                ""
            }
        );
    }

    fs::write(&out, document(&measured, &dir, trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
