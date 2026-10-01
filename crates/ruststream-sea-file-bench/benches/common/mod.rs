//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! stream file, the service setup, the latch a handler counts deliveries down on, and the
//! measurement configuration. The method is the core's, described in its `benches/common` and on
//! the [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes, on the broker a user writes it with: the app,
//! started through [`RustStream::start`], on [`FileBroker`] over a stream file of its own in the
//! target directory. The handler subscribes with the production `FileStream` descriptor and
//! starts at the beginning of the file, so it reads every message the fill appends.
//!
//! # Filling the file
//!
//! The deliveries are appended after the service has started and before the drain begins, by the
//! `sea-streamer-file` client on a thread of its own with its own runtime, and the fill returns
//! once a flush has put every one of them in the file. The service's runtime is single-threaded
//! and runs only inside a measured region, so nothing is consumed while the file is filled, and a
//! stream file keeps every message until the subscription reads it.
//!
//! The filling thread opens the file first. The client runs the writer of a file as one task per
//! process, on the runtime that opened the first producer for it, so that task lives on the
//! filling thread: the fill never touches the service's thread, and the appends a reply makes are
//! written there too. A file the service opened with its messages already in it would not do: the
//! client scans an existing file to its end when it opens a producer or a consumer on it, which
//! would put a cost that grows with the file into the start.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: the connect, the subscription and the first delivery.
//!
//! What a body measures is the start and the drain, in two regions, with the fill between them
//! and never counted.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything on the service's thread inside the region is counted: the dispatcher, the
//! codec, this crate's code, and the part of `sea-streamer-file` that runs on the service's
//! runtime - the tasks that read and decode the file. The writer task runs on the filling thread,
//! the reads and writes themselves on tokio's blocking threads and the file watcher on a thread of
//! its own, and none of those is counted. [`measure`] is the only frame that carries its
//! name, because a toggle on a name that also appears inside closure types switches collection off
//! again one frame deeper. DHAT is pointed at the same frame; the number read is `Total blocks`,
//! allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_sea_file::FileBroker;
use sea_streamer_file::{FileConnectOptions, FileId, FileProducerOptions, FileStreamer};
use sea_streamer_types::{Producer as _, StreamKey, Streamer as _};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw, so a number taken with it on is not the production path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The stream key every scenario delivers on, named by each handler's `FileStream` descriptor.
pub const INPUT: &str = "orders";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within seconds of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run fails when
/// the path allocates more than it does today. Both are floors the code is held to, so a number
/// that goes down is lowered here in the same change. The instruction limit is relative, at
/// [`INSTRUCTION_LIMIT`] percent: `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come a whole number per delivery: `steady`
/// blocks per `per` deliveries.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, INSTRUCTION_LIMIT)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// How far a run's instruction total may rise over the run it is compared with, in percent.
///
/// Five runs of one unchanged binary moved a run's total by up to 2.7 percent while the
/// per-message slope repeated within half a percent: what moved was the C library's allocator,
/// whose work depends on how the other threads left the heap, and the deadline that closes a
/// partial batch. The limit is twice that movement, rounded up,
/// so an unchanged tree passes; the core's two percent would fail it.
pub const INSTRUCTION_LIMIT: f64 = 6.0;

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread runs the service, and that thread is what is counted.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// What the filling thread is asked to do.
enum Command {
    /// Append this many bodies under [`INPUT`], flush, and report back.
    Fill(usize, std_mpsc::Sender<()>),
}

/// The thread that writes the stream file: its own runtime, the `sea-streamer-file` client, and
/// the file's writer task, which the client starts on the runtime that opens the first producer.
struct Filler {
    commands: UnboundedSender<Command>,
    thread: JoinHandle<()>,
}

impl Filler {
    /// Creates the file and opens the first producer on it, before the service opens the file.
    fn open(path: &Path) -> Self {
        let (commands, mut received) = unbounded_channel();
        let (ready, opened) = std_mpsc::channel();
        let uri = FileId::new(path.to_string_lossy().into_owned())
            .to_streamer_uri()
            .expect("a path is a streamer uri");
        let thread = thread::spawn(move || {
            runtime().block_on(async move {
                let mut options = FileConnectOptions::default();
                options.set_create_if_not_exists(true);
                let writer = FileStreamer::connect(uri, options)
                    .await
                    .expect("the stream file opens");
                let producer = writer
                    .create_generic_producer(FileProducerOptions::default())
                    .await
                    .expect("the producer is created");
                let key = StreamKey::new(INPUT).expect("the stream key is valid");
                let body = json_body();
                ready.send(()).expect("the benchmark waits for the file");
                while let Some(Command::Fill(count, done)) = received.recv().await {
                    for _ in 0..count {
                        // The receipt is dropped: the client's queue orders the appends, and the
                        // flush below returns once every one of them is in the file.
                        let _receipt = producer
                            .send_to(&key, body.as_slice())
                            .expect("the body is queued");
                    }
                    producer
                        .clone()
                        .flush()
                        .await
                        .expect("the bodies reach the file");
                    done.send(()).expect("the benchmark waits for the fill");
                }
                let _ = writer.disconnect().await;
            });
        });
        opened.recv().expect("the filling thread opens the file");
        Self { commands, thread }
    }

    /// Appends `count` bodies and returns once they are in the file.
    fn fill(&self, count: usize) {
        let (done, filled) = std_mpsc::channel();
        self.commands
            .send(Command::Fill(count, done))
            .expect("the filling thread runs");
        filled.recv().expect("the filling thread fills the file");
    }

    /// Closes the thread's connection to the file and waits for the thread to end.
    fn close(self) {
        drop(self.commands);
        self.thread.join().expect("the filling thread ends");
    }
}

/// A service that is built but not started, and the thread that will fill its file.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    path: PathBuf,
    filler: Filler,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<FileBroker, Identity, (), Latch>;

/// Builds a one-handler service on a fresh stream file, ready to be started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("code-{}-{stamp}.ss", process::id()));
    let filler = Filler::open(&path);
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(FileBroker::new(path.to_string_lossy()), mount);
    Pending {
        runtime: runtime(),
        latch,
        path,
        filler,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Starts the service, fills its file, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        path,
        filler,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    filler.fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the file was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    drop(running);
    filler.close();
    drop(runtime);
    let _ = fs::remove_file(&path);
}
