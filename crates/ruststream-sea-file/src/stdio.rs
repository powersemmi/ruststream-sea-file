//! Standard input and output as one stream: a service that is a stage of a shell pipeline.
//!
//! [`StdioBroker`] records nothing and attaches the process's own pipes on
//! [`Broker::connect`]. Standard input is the subscription, standard output the publisher, so
//! `producer | service | consumer` works with ordinary command-line tools. A service on a
//! pipeline globs this form's [`prelude`] and names its policy [`Publish`](struct@Publish).
//!
//! - [StdioBroker][stdio-broker]: [`StdioBroker`], and [`loopback`](StdioBroker::loopback) for a
//!   stage that reads its own output ([loopback][loopback]).
//! - [Lines][lines]: the client's `[timestamp | stream_key | sequence | shard_id] payload` line
//!   format, read by a subscription on its stream key through [`StdioSubscriber`]; the end of
//!   standard input ends the subscription.
//! - [StdioPublish][stdio-publish]: [`Publish`](struct@Publish) and its live [`StdioPublisher`], one line per
//!   message on standard output.
//! - [Payloads][payloads]: binary, multi-line and padded payloads through the envelope.
//! - [Envelopes][envelopes]: [`envelope`](StdioBroker::envelope) and
//!   [`Publish::envelope`](StdioPublish::envelope), the format a line is written in.
//! - [out_retry][out-retry]: where a deferred copy goes, named at the mount site.
//! - [Shutdown][shutdown]: a stdio shutdown ends every stdio stream of the process.
//!
//! [stdio-broker]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/stdio-broker/
//! [envelopes]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/envelopes/
//! [loopback]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/loopback/
//! [lines]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/lines/
//! [stdio-publish]: https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/stdio-publish/
//! [payloads]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/payloads/
//! [out-retry]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/out-retry/
//! [shutdown]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/shutdown/

// Without the `testing` feature a link has one variant, so a `match` on it has a single arm; the
// matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::Stream;
#[cfg(feature = "testing")]
use ruststream::RawMessage;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    BatchSubscriber, Broker, BufferedSubscriber, ConnectedBroker, DefaultPublish, DescribeServer,
    Lend, NamedCopies, OutgoingMessage, PairError, PublishPolicy, Publisher, ServerSpec, Subscribe,
    Subscriber,
};
use sea_streamer_stdio::{StdioConnectOptions, StdioProducer, StdioProducerOptions, StdioStreamer};
use sea_streamer_types::{Producer as _, StreamKey, Streamer as _, StreamerUri};
use tokio::sync::OnceCell;

#[cfg(feature = "testing")]
use bytes::Bytes;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
#[cfg(all(feature = "testing", feature = "cbor"))]
use ruststream::codec::CborCodec;
#[cfg(all(feature = "testing", feature = "json"))]
use ruststream::codec::JsonCodec;
#[cfg(all(feature = "testing", feature = "msgpack"))]
use ruststream::codec::MsgpackCodec;
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::batching::BATCH_MAX_WAIT;
#[cfg(all(
    feature = "testing",
    any(feature = "json", feature = "cbor", feature = "msgpack")
))]
use crate::envelope::Framed;
#[cfg(feature = "testing")]
use crate::envelope::NoOp;
use crate::envelope::{self, Envelope, EnvelopeChoice, FromBroker, Medium, Rs};
use crate::error::{SeaFileError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{MemoryPipe, PipeQueue};
use crate::message::SeaMessage;
use crate::stdin::{self, InputQueue};
#[cfg(feature = "asyncapi")]
use crate::stream::EnvelopeSpec;

pub(crate) struct StdioCore {
    pub(crate) link: StdioLink,
    pub(crate) closed: AtomicBool,
    /// The broker's own setting, which the test harness reads to know where a publish goes. The
    /// field is there only with the `testing` feature.
    #[cfg(feature = "testing")]
    loopback: bool,
}

/// What a connected broker and every handle paired off it speak over: the process's own pipes,
/// or, under the `testing` feature, the in-memory ones the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the client's streamer itself and every
/// `match` on it is irrefutable: a production build carries no second transport and no branch to
/// it.
pub(crate) enum StdioLink {
    Stdio(Pipes),
    #[cfg(feature = "testing")]
    InProcess(Arc<MemoryPipe>),
}

/// The process's own pipes: standard output as the client opened it, and whether what this
/// broker writes comes back to the subscriptions of the process.
pub(crate) struct Pipes {
    streamer: StdioStreamer,
    loopback: bool,
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the pipes it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<StdioLink>() == size_of::<Pipes>());

impl StdioCore {
    fn ensure_open(&self) -> Result<(), SeaFileError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SeaFileError::NotConnected);
        }
        Ok(())
    }
}

impl std::fmt::Debug for StdioCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioCore")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

type StdioCell = Arc<OnceCell<Arc<StdioCore>>>;

/// Standard input and output as one stream: consume lines from stdin, publish lines to
/// stdout, in the client's `[timestamp | stream_key | seq] payload` line format.
///
/// When standard input ends, every subscription ends after the lines it has read, so a pipeline
/// stage stops when the stage before it does; a [`loopback`](Self::loopback) subscription also
/// reads what the process writes and stays open. Shutting this broker down ends every stdio
/// subscription and publisher in the process, after the lines each subscription has already
/// read: standard input and output are process-wide. The
/// [StdioBroker](https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/stdio-broker/)
/// page shows it in a service.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::stdio::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Line {
///     text: String,
/// }
///
/// #[derive(Debug, Outgoing, Serialize)]
/// #[outgoing(name = "upper")]
/// struct Upper {
///     text: String,
/// }
///
/// #[subscriber("lines", reply)]
/// async fn shout(line: &Line) -> Upper {
///     Upper { text: line.text.to_uppercase() }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("shout", "0.1.0")).with_broker(StdioBroker::new(), |b| {
///         b.include(shout).out_retry(Publish).to("lines.retry");
///     })
/// }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct StdioBroker<Env: Envelope = Rs> {
    loopback: bool,
    cell: StdioCell,
    envelope: Env,
}

impl StdioBroker {
    /// Records configuration only. No I/O.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Env: Envelope> StdioBroker<Env> {
    /// The [`Envelope`] every line of this broker is written and read in, unless a publish
    /// policy names its own. [`Rs`] when none is named.
    ///
    /// A test addresses the broker by the type this returns, `tb.broker::<StdioBroker<NoOp>>()`.
    /// The harness runs the built-in envelopes in process: [`Rs`] and
    /// [`NoOp`] always, and [`Framed`] over each
    /// codec whose feature (`json`, `cbor`, `msgpack`) this crate has enabled.
    ///
    /// # Examples
    ///
    /// A pipeline stage whose lines are JSON objects any line tool reads, headers inside:
    ///
    /// ```
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_sea_file::stdio::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Debug, Outgoing, Serialize)]
    /// #[outgoing(name = "receipts")]
    /// struct Receipt {
    ///     order: u64,
    /// }
    ///
    /// #[subscriber("orders", reply)]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { order: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("confirm", "0.1.0"))
    ///         .with_broker(StdioBroker::new().envelope(Framed::new(JsonCodec)), |b| {
    ///             b.include(confirm);
    ///         })
    /// }
    /// ```
    pub fn envelope<Chosen: Envelope>(self, envelope: Chosen) -> StdioBroker<Chosen> {
        StdioBroker {
            loopback: self.loopback,
            cell: self.cell,
            envelope,
        }
    }

    /// Loops published messages back to this process's own subscribers (for tests).
    ///
    /// Its subscriptions read what the process writes as well as standard input, so the end of
    /// standard input leaves them open until the shutdown.
    pub fn loopback(mut self) -> Self {
        self.loopback = true;
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> StdioPublisher<Env> {
        StdioPublisher {
            cell: Arc::clone(&self.cell),
            producer: Arc::new(OnceCell::new()),
            envelope: self.envelope.clone(),
        }
    }
}

impl<Env: Envelope> Broker for StdioBroker<Env> {
    type Error = SeaFileError;
    type Connected = ConnectedStdioBroker<Env>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                // The client's loopback feeds the client's own reader, which this crate does not
                // use: a written line comes back through `stdin::loop_back` instead.
                let streamer =
                    StdioStreamer::connect(StreamerUri::zero(), StdioConnectOptions::default())
                        .await
                        .map_err(|e| SeaFileError::Connect {
                            target: "stdio".to_owned(),
                            source: box_err(e),
                        })?;
                Ok::<_, SeaFileError>(Arc::new(StdioCore {
                    link: StdioLink::Stdio(Pipes {
                        streamer,
                        loopback: self.loopback,
                    }),
                    closed: AtomicBool::new(false),
                    #[cfg(feature = "testing")]
                    loopback: self.loopback,
                }))
            })
            .await?
            .clone();
        Ok(ConnectedStdioBroker {
            core,
            cell: self.cell,
            envelope: self.envelope,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, over a pipe
/// held in memory in place of the process's own standard input and output.
///
/// The pipe belongs to this broker and every clone of it. What the test injects arrives on its
/// standard input; what the service publishes is written to its standard output, and reaches the
/// service's own subscriptions only under [`loopback`](Self::loopback), as on a real pipe.
#[cfg(feature = "testing")]
impl<Env: Envelope> InProcess for StdioBroker<Env> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_init(async || {
                Arc::new(StdioCore {
                    link: StdioLink::InProcess(MemoryPipe::new(self.loopback)),
                    closed: AtomicBool::new(false),
                    loopback: self.loopback,
                })
            })
            .await
            .clone();
        Ok(ConnectedStdioBroker {
            core,
            cell: self.cell,
            envelope: self.envelope,
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(StdioBroker);
#[cfg(feature = "testing")]
ruststream::register_testable_broker!(StdioBroker<NoOp>);
#[cfg(all(feature = "testing", feature = "json"))]
ruststream::register_testable_broker!(StdioBroker<Framed<JsonCodec>>);
#[cfg(all(feature = "testing", feature = "cbor"))]
ruststream::register_testable_broker!(StdioBroker<Framed<CborCodec>>);
#[cfg(all(feature = "testing", feature = "msgpack"))]
ruststream::register_testable_broker!(StdioBroker<Framed<MsgpackCodec>>);

impl<Env: Envelope> DescribeServer for StdioBroker<Env> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("stdio")
    }
}

/// The typed witness that `connect` succeeded.
#[derive(Debug)]
pub struct ConnectedStdioBroker<Env: Envelope = Rs> {
    core: Arc<StdioCore>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: StdioCell,
    envelope: Env,
}

impl<Env: Envelope> ConnectedStdioBroker<Env> {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> StdioPublisher<Env> {
        StdioPublisher {
            cell: Arc::clone(&self.cell),
            producer: Arc::new(OnceCell::new()),
            envelope: self.envelope.clone(),
        }
    }
}

impl<Env: Envelope> ConnectedBroker for ConnectedStdioBroker<Env> {
    type Error = SeaFileError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        let streamer = match &self.core.link {
            StdioLink::Stdio(pipes) => &pipes.streamer,
            // The in-memory pipe belongs to this broker alone, so its shutdown ends only the
            // subscriptions on it.
            #[cfg(feature = "testing")]
            StdioLink::InProcess(pipe) => {
                pipe.finish();
                return Ok(());
            }
        };
        // Globally destructive by design: standard input and output are the process's, so every
        // stdio subscription and producer in the process ends. That is the honest meaning of
        // shutting down a process-wide transport.
        stdin::end_all();
        streamer
            .clone()
            .disconnect()
            .await
            .map_err(|e| SeaFileError::Connect {
                target: "stdio".to_owned(),
                source: box_err(e),
            })
    }
}

impl<Env: Envelope> Subscribe for ConnectedStdioBroker<Env> {
    type Subscriber = StdioSubscriber<Env>;
    /// The mount site names where a deferred copy goes, because nothing on this transport
    /// addresses the subscription: a publish writes to standard output and a subscription reads
    /// standard input, so the process downstream of the pipe is not the one that sent the
    /// message.
    ///
    /// A registration over stdio therefore names a destination - `.out_retry(Publish).to(name)`,
    /// or a transform that names one per delivery - and one that names neither refuses to start,
    /// which is the right answer for a pipeline stage: a delayed message that went nowhere is
    /// worse than a service that does not come up. ([`loopback`](StdioBroker::loopback) does
    /// route a publish back to this process, but it is a test aid, and an address that only held
    /// under it would break in the shape a service ships.)
    type Copies = NamedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl<Env: Envelope> ConnectedStdioBroker<Env> {
    /// Opens the subscription of `name`: standard input is read on a thread of its own, so
    /// nothing here waits.
    fn open(&self, name: &str) -> Result<StdioSubscriber<Env>, SeaFileError> {
        self.core.ensure_open()?;
        let key =
            StreamKey::new(name).map_err(|e| SeaFileError::Invalid(format!("'{name}': {e}")))?;
        let pipes = match &self.core.link {
            StdioLink::Stdio(pipes) => pipes,
            #[cfg(feature = "testing")]
            StdioLink::InProcess(pipe) => {
                return Ok(StdioSubscriber {
                    stream: name.to_owned(),
                    inner: BufferedSubscriber::new(StdioDeliveries::InProcess(
                        pipe.subscribe(key.name()),
                        self.envelope.clone(),
                    ))
                    .max_wait(BATCH_MAX_WAIT),
                });
            }
        };
        let input = stdin::subscribe(key, pipes.loopback).map_err(|e| SeaFileError::Subscribe {
            stream: name.to_owned(),
            source: box_err(e),
        })?;
        Ok(StdioSubscriber {
            stream: name.to_owned(),
            inner: BufferedSubscriber::new(StdioDeliveries::Reader(input, self.envelope.clone()))
                .max_wait(BATCH_MAX_WAIT),
        })
    }
}

impl<Env: Envelope> DefaultPublish for ConnectedStdioBroker<Env> {
    type Policy = StdioPublish;
}

/// The harness's view of the in-process transport: what it feeds to standard input, what it
/// reads back, and the coordinator it counts the in-flight deliveries with.
///
/// An injected message is a line arriving on standard input, so every subscription on its stream
/// key reads it. A publish is a line on standard output, which reaches the process downstream:
/// [`routes`](TestableBroker::routes) answers that it reaches this service's subscriptions of the
/// same stream key under [`loopback`](StdioBroker::loopback), and none of them otherwise.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the pipe `connect_in_process` produces. `inject` also panics on a line a pipe cannot carry: an
/// invalid stream key, or a message with no payload and no headers.
#[cfg(feature = "testing")]
impl<Env: Envelope> TestableBroker for ConnectedStdioBroker<Env> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let StdioLink::InProcess(pipe) = &self.core.link {
            pipe.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let pipe = self.memory_pipe("inject");
        let key = StreamKey::new(message.name()).unwrap_or_else(|err| {
            panic!(
                "the injected message to {:?} is not one a pipe carries: {err}",
                message.name()
            )
        });
        assert!(
            !(message.payload().is_empty() && message.headers().is_empty()),
            "the injected message to {:?} is empty, and a pipe drops an empty line",
            message.name(),
        );
        // The line an upstream stage of the pipeline writes for this message.
        let line = envelope::seal(
            &self.envelope,
            message.headers(),
            message.payload(),
            Medium::Line,
        )
        .unwrap_or_else(|err| {
            panic!(
                "the injected message to {:?} is not a line the broker's envelope writes: {err}",
                message.name()
            )
        });
        let sent = RawMessage::new(message.name(), Bytes::copy_from_slice(message.payload()))
            .with_headers(message.headers().clone());
        pipe.feed(key, line.into_owned(), sent);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.memory_pipe("published").published(name)
    }

    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        if !self.core.loopback {
            return Vec::new();
        }
        subscriptions
            .iter()
            .enumerate()
            .filter(|(_, name)| **name == destination)
            .map(|(position, _)| position)
            .collect()
    }
}

#[cfg(feature = "testing")]
impl<Env: Envelope> ConnectedStdioBroker<Env> {
    /// The in-memory pipe, which is all the harness drives.
    fn memory_pipe(&self, what: &str) -> &MemoryPipe {
        match &self.core.link {
            StdioLink::InProcess(pipe) => pipe,
            StdioLink::Stdio(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the pipe `connect_in_process` produces"
            ),
        }
    }
}

/// A subscription to one stream key on standard input; yields [`SeaMessage`]s.
///
/// Standard input has no retained log: there is no acknowledgement and no repositioning, and
/// both are reported as unsupported rather than pretended. Batches it does serve: standard input
/// is read one line at a time, so they are assembled here, capped at the size the mount site
/// named. The stream ends when standard input ends, after the lines it has read, unless the
/// broker loops back.
pub struct StdioSubscriber<Env: Envelope = Rs> {
    stream: String,
    inner: BufferedSubscriber<StdioDeliveries<Env>>,
}

impl<Env: Envelope> std::fmt::Debug for StdioSubscriber<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> Subscriber for StdioSubscriber<Env> {
    type Message = SeaMessage<Env>;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<SeaMessage<Env>, SeaFileError>> + Send + '_ {
        self.inner.stream()
    }
}

impl<Env: Envelope> BatchSubscriber for StdioSubscriber<Env> {
    type Batch = Vec<SeaMessage<Env>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SeaFileError>> + Send + '_ {
        self.inner.batches(size)
    }
}

/// A subscription's lines, before batching: one line per poll, in arrival order.
///
/// Without the `testing` feature there is one variant, so the type is the queue standard input
/// fills itself and every `match` on it is irrefutable.
enum StdioDeliveries<Env: Envelope> {
    /// The queue standard input fills, and the envelope its lines are opened with.
    Reader(InputQueue, Env),
    /// The in-process pipe's queue, and the envelope its lines are opened with.
    #[cfg(feature = "testing")]
    InProcess(PipeQueue, Env),
}

// The zero-cost promise of the in-process mode, held by the compiler.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<StdioDeliveries<Rs>>() == size_of::<InputQueue>());

impl<Env: Envelope> Subscriber for StdioDeliveries<Env> {
    type Message = SeaMessage<Env>;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<SeaMessage<Env>, SeaFileError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| match self {
            Self::Reader(queue, envelope) => queue.poll_next(cx, envelope).map(|next| next.map(Ok)),
            #[cfg(feature = "testing")]
            Self::InProcess(queue, envelope) => {
                queue.poll_next(cx, envelope).map(|next| next.map(Ok))
            }
        })
    }
}

/// Publishes messages to standard output.
///
/// The line format is the client's own. A line is text, ends at a newline, and the reader trims
/// it. The publisher's [`Envelope`] writes the headers and the payload into the line: the
/// default [`Rs`] envelopes a payload that is not UTF-8, holds a newline or begins or ends with
/// whitespace, and any message with headers; a line another envelope writes that a pipe would
/// change fails the publish with [`SeaFileError::Seal`]. The client silently drops empty lines,
/// so an empty line is rejected here instead.
///
/// A publish carries no per-message settings: a line on standard output takes a key and a
/// payload and nothing else, so [`Publisher::Options`] is the unit type and the publish builder
/// gains no step from this crate.
///
/// Standard output reaches the next process in the pipeline and never this process's own
/// standard input, so a registration that defers a delivery names where the copy goes, with
/// `.out_retry(Publish).to(key)` or a transform that names one per delivery; one that names
/// neither refuses to start. The
/// [StdioPublish](https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/stdio-publish/)
/// page shows it in a service.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::stdio::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[derive(Debug, Outgoing, Serialize)]
/// #[outgoing(name = "results")]
/// struct Done {
///     id: u64,
/// }
///
/// #[subscriber("jobs", reply)]
/// async fn work(job: &Job) -> Done {
///     tracing::info!(job.id, "done");
///     Done { id: job.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
///         b.include(work).out_retry(Publish).to("jobs.retry");
///     })
/// }
/// ```
#[derive(Clone)]
pub struct StdioPublisher<Env: Envelope = Rs> {
    cell: StdioCell,
    producer: Arc<OnceCell<StdioProducer>>,
    envelope: Env,
}

impl<Env: Envelope> std::fmt::Debug for StdioPublisher<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioPublisher").finish_non_exhaustive()
    }
}

impl<Env: Envelope> Publisher for StdioPublisher<Env> {
    /// A line on standard output is written from the bytes and keeps nothing: the client's
    /// `send_to` takes a slice, and an envelope that frames the payload writes a buffer of its
    /// own.
    type Payload = Lend;

    type Error = SeaFileError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> Result<(), Self::Error> {
        let core = self.cell.get().ok_or(SeaFileError::NotConnected)?;
        core.ensure_open()?;
        let line = envelope::seal(&self.envelope, msg.headers(), msg.payload(), Medium::Line)
            .map_err(|source| SeaFileError::Seal {
                stream: msg.name().to_owned(),
                source,
            })?;
        if line.is_empty() {
            return Err(SeaFileError::Invalid(
                "stdio drops empty lines; an empty message cannot be transmitted".into(),
            ));
        }
        let pipes = match &core.link {
            StdioLink::Stdio(pipes) => pipes,
            #[cfg(feature = "testing")]
            StdioLink::InProcess(pipe) => {
                let key = StreamKey::new(msg.name())
                    .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
                let sent = RawMessage::new(msg.name(), Bytes::copy_from_slice(msg.payload()))
                    .with_headers(msg.headers().clone());
                pipe.write(key, line.into_owned(), sent);
                return Ok(());
            }
        };
        let producer = self
            .producer
            .get_or_try_init(async || {
                pipes
                    .streamer
                    .create_generic_producer(StdioProducerOptions::default())
                    .await
                    .map_err(|e| SeaFileError::Publish {
                        stream: msg.name().to_owned(),
                        source: box_err(e),
                    })
            })
            .await?;
        let key = StreamKey::new(msg.name())
            .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
        let written = producer
            .send_to(&key, line.as_ref())
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })?
            .await
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })?;
        if pipes.loopback {
            stdin::loop_back(&written, line.into_owned());
        }
        Ok(())
    }
}

/// The publish policy for [`StdioPublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Debug, Outgoing, Serialize)]
/// struct Receipt {
///     order: u64,
/// }
///
/// #[subscriber("orders", reply("receipts"))]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(StdioBroker::new(), |b| {
///         b.include(confirm)
///             .out_reply(StdioPublish)
///             .out_retry(StdioPublish)
///             .to("orders.retry");
///     })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[must_use]
pub struct StdioPublish<Choice = FromBroker> {
    envelope: Choice,
}

/// The publish policy that writes in the broker's envelope, as a value: `out_reply(StdioPublish)`.
// A unit struct cannot carry the envelope parameter, so the value the mount site names is this
// constant of the same name; the type and the value live in separate namespaces.
#[allow(non_upper_case_globals)]
pub const StdioPublish: StdioPublish = StdioPublish {
    envelope: FromBroker,
};

impl<Choice> StdioPublish<Choice> {
    /// Writes every line through this policy in `envelope`, whatever the broker's is.
    ///
    /// # Examples
    ///
    /// The stage reads lines in the broker's default envelope and writes its receipts as bare
    /// payloads for a tool downstream that knows no envelope:
    ///
    /// ```
    /// use ruststream_sea_file::stdio::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Debug, Outgoing, Serialize)]
    /// #[outgoing(name = "receipts")]
    /// struct Receipt {
    ///     order: u64,
    /// }
    ///
    /// #[subscriber("orders", reply)]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { order: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("confirm", "0.1.0")).with_broker(StdioBroker::new(), |b| {
    ///         b.include(confirm).out_reply(Publish.envelope(NoOp));
    ///     })
    /// }
    /// ```
    pub fn envelope<Chosen: Envelope>(self, envelope: Chosen) -> StdioPublish<Chosen> {
        StdioPublish { envelope }
    }
}

impl<Env, Choice> PublishPolicy<ConnectedStdioBroker<Env>> for StdioPublish<Choice>
where
    Env: Envelope,
    Choice: EnvelopeChoice<Env>,
{
    type Live = StdioPublisher<Choice::Envelope>;

    fn pair(
        self,
        connected: &ConnectedStdioBroker<Env>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(StdioPublisher {
            cell: Arc::clone(&connected.cell),
            producer: Arc::new(OnceCell::new()),
            envelope: self.envelope.resolve(&connected.envelope),
        }))
    }

    /// The media type of the envelope a line through this policy is written in, when it has one.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, _channel: &str) -> Bindings {
        stdio_extension(<Choice::Envelope as Envelope>::CONTENT_TYPE)
    }
}

/// The extension key the stdio transport's channel description sits under.
#[cfg(feature = "asyncapi")]
const STDIO_EXTENSION: &str = "x-ruststream-stdio";

/// What the document reports about a stdio channel: the envelope of its lines.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
struct StdioChannel {
    envelope: EnvelopeSpec,
}

/// What a stdio channel adds to the generated `AsyncAPI` document: the media type of its
/// envelope's lines, and nothing for an envelope whose lines have none of their own.
#[cfg(feature = "asyncapi")]
fn stdio_extension(content_type: Option<&'static str>) -> Bindings {
    let Some(envelope) = content_type.map(EnvelopeSpec::new) else {
        return Bindings::new();
    };
    // A binding that fails to build is a binding the document goes without: a broker never holds
    // up a service over a description of itself.
    Binding::extension(STDIO_EXTENSION, &StdioChannel { envelope })
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// The publish policy of this form, under the name every form uses.
///
/// A mount site names the concept, never the transport: moving a service from one form to
/// another changes the prelude it globs and leaves the composition root alone. The prefixed
/// [`StdioPublish`](struct@StdioPublish) stays at the crate root, for a service that mixes both forms.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::stdio::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Debug, Outgoing, Serialize)]
/// struct Receipt {
///     order: u64,
/// }
///
/// #[subscriber("orders", reply("receipts"))]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(StdioBroker::new(), |b| {
///         b.include(confirm)
///             .out_reply(Publish)
///             .out_retry(Publish)
///             .to("orders.retry");
///     })
/// }
/// ```
pub use StdioPublish as Publish;

pub mod prelude {
    //! The imports a service on a shell pipeline writes every time, in one glob.
    //!
    //! The framework's prelude, this form's broker, and [`Publish`](struct@Publish). A subscription here is a
    //! plain stream key, so this form has no descriptor type.
    //!
    //! This is the routes-side vocabulary: a mount site globs it and names policies by concept.
    //! A handler body globs `ruststream::prelude` instead and bounds an injected publisher by
    //! the framework's capability traits, so the two vocabularies never meet in one file.
    //!
    //! # Examples
    //!
    //! ```
    //! use ruststream_sea_file::stdio::prelude::*;
    //! use serde::{Deserialize, Serialize};
    //!
    //! #[derive(Debug, Deserialize)]
    //! struct Job {
    //!     id: u64,
    //! }
    //!
    //! #[derive(Debug, Outgoing, Serialize)]
    //! #[outgoing(name = "results")]
    //! struct Done {
    //!     id: u64,
    //! }
    //!
    //! #[subscriber("jobs", reply)]
    //! async fn work(job: &Job) -> Done {
    //!     Done { id: job.id }
    //! }
    //!
    //! #[ruststream::app]
    //! fn app() -> impl App {
    //!     RustStream::new(AppInfo::new("pipeline", "0.1.0"))
    //!         .with_broker(StdioBroker::new(), |b| {
    //!             b.include(work).out_retry(Publish).to("jobs.retry");
    //!         })
    //! }
    //! ```

    pub use ruststream::prelude::*;

    // No capability traits: `SeaMessage` implements `Positioned`, but a pipe cannot seek back to
    // one - do not add it here.
    pub use crate::stdio::{Publish, StdioBroker};
    pub use crate::{Framed, NoOp, Rs};
}
