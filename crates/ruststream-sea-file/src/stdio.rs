//! Standard input and output as one stream: a service that is a stage of a shell pipeline.
//!
//! [`StdioBroker`] records nothing and attaches the process's own pipes on
//! [`Broker::connect`]. Standard input is the subscription, standard output the publisher, so
//! `producer | service | consumer` works with ordinary command-line tools. A service on a
//! pipeline globs this form's [`prelude`] and names its policy [`Publish`].
//!
//! Shutting this broker down ends every stdio consumer and producer in the process, not only the
//! ones it opened: the client's transport is process-wide.
//!
//! # The line format
//!
//! Lines follow the client's `[timestamp | stream_key | sequence | shard_id] payload` format,
//! with every meta field optional. The stream key is part of the line, so one process serves
//! several keys:
//!
//! ```text
//! echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' | ./pipeline run
//! ```
//!
//! # Subscribing
//!
//! A subscription here is the stream key itself, so this form has no descriptor type and
//! `#[subscriber("jobs")]` consumes the `jobs` key off standard input. Standard input keeps no
//! retained log: there is no acknowledgement (`ack` and `nack` report `AckError::Unsupported`)
//! and no repositioning, and a handler that reads the [`SeekHandle`](crate::SeekHandle) key does
//! not compile against this broker. Batches it does serve - the client reads one line at a time,
//! so `batch(n)` is honoured by assembling them here, and a partial batch goes out 10 ms after
//! its first delivery.
//!
//! # Publishing and deferred copies
//!
//! [`Publish`] pairs into [`StdioPublisher`], which writes a line to standard output under the
//! message's stream key. A line carries a key and a payload and nothing else, so this crate adds
//! no step to the publish builder. A message with no payload and no headers is rejected with
//! [`SeaFileError::Invalid`], because the client's line format
//! silently drops empty lines.
//!
//! Standard output reaches the next process in the pipeline and never this process's own
//! standard input, so nothing here addresses the subscription. A registration that defers a
//! delivery therefore names where the copy goes - `.out_retry(Publish).to("jobs.retry")`, or a
//! transform that names one per delivery - and one that names neither refuses to start. That is
//! the right answer for a pipeline stage: a delayed message that went nowhere is worse than a
//! service that does not come up.
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
//! #[subscriber("jobs", publish)]
//! async fn work(job: &Job) -> Done {
//!     Done { id: job.id }
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
//!         b.include(work).out_retry(Publish).to("jobs.retry");
//!     })
//! }
//! ```
//!
//! [`loopback`](StdioBroker::loopback) sends this process's standard output back into its own
//! standard input, so a stdio service runs in one process with no external commands. It is a test
//! aid: an address that only held under it would break in the shape a service ships.

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
use sea_streamer_types::{
    Consumer as _, ConsumerMode, ConsumerOptions as _, Producer as _, StreamKey, Streamer as _,
    StreamerUri,
};
use tokio::sync::{OnceCell, mpsc};

use crate::batching::BATCH_MAX_WAIT;
use crate::error::{SeaFileError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{MemoryPipe, PipeQueue};
use crate::message::SeaMessage;
use crate::wire;

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
    Stdio(StdioStreamer),
    #[cfg(feature = "testing")]
    InProcess(Arc<MemoryPipe>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the streamer it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<StdioLink>() == size_of::<StdioStreamer>());

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
/// # Examples
///
/// ```
/// use ruststream_sea_file::StdioBroker;
///
/// let broker = StdioBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct StdioBroker {
    loopback: bool,
    cell: StdioCell,
}

impl StdioBroker {
    /// Records configuration only. No I/O.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loops published messages back to this process's own subscribers (for tests).
    pub fn loopback(mut self) -> Self {
        self.loopback = true;
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> StdioPublisher {
        StdioPublisher {
            cell: Arc::clone(&self.cell),
            producer: Arc::new(OnceCell::new()),
        }
    }
}

impl Broker for StdioBroker {
    type Error = SeaFileError;
    type Connected = ConnectedStdioBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let mut options = StdioConnectOptions::default();
                options.set_loopback(self.loopback);
                let streamer = StdioStreamer::connect(StreamerUri::zero(), options)
                    .await
                    .map_err(|e| SeaFileError::Connect {
                        target: "stdio".to_owned(),
                        source: box_err(e),
                    })?;
                Ok::<_, SeaFileError>(Arc::new(StdioCore {
                    link: StdioLink::Stdio(streamer),
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
impl InProcess for StdioBroker {
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
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(StdioBroker);

impl DescribeServer for StdioBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("stdio")
    }
}

/// The typed witness that `connect` succeeded.
#[derive(Debug)]
pub struct ConnectedStdioBroker {
    core: Arc<StdioCore>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: StdioCell,
}

impl ConnectedStdioBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> StdioPublisher {
        StdioPublisher {
            cell: Arc::clone(&self.cell),
            producer: Arc::new(OnceCell::new()),
        }
    }
}

impl ConnectedBroker for ConnectedStdioBroker {
    type Error = SeaFileError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        let streamer = match &self.core.link {
            StdioLink::Stdio(streamer) => streamer,
            // The in-memory pipe belongs to this broker alone, so its shutdown reaches no other.
            #[cfg(feature = "testing")]
            StdioLink::InProcess(_) => return Ok(()),
        };
        // Globally destructive by the client's design: every stdio consumer and producer in
        // the process ends. That is the honest meaning of shutting down a process-wide
        // transport.
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

impl Subscribe for ConnectedStdioBroker {
    type Subscriber = StdioSubscriber;
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

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.core.ensure_open()?;
        let key =
            StreamKey::new(name).map_err(|e| SeaFileError::Invalid(format!("'{name}': {e}")))?;
        let streamer = match &self.core.link {
            StdioLink::Stdio(streamer) => streamer,
            #[cfg(feature = "testing")]
            StdioLink::InProcess(pipe) => {
                return Ok(StdioSubscriber {
                    stream: name.to_owned(),
                    inner: BufferedSubscriber::new(StdioDeliveries::InProcess(
                        pipe.subscribe(key.name()),
                    ))
                    .max_wait(BATCH_MAX_WAIT),
                });
            }
        };
        let consumer = streamer
            .create_consumer(
                &[key],
                sea_streamer_stdio::StdioConsumerOptions::new(ConsumerMode::RealTime),
            )
            .await
            .map_err(|e| SeaFileError::Subscribe {
                stream: name.to_owned(),
                source: box_err(e),
            })?;

        let (tx, rx) = mpsc::channel(64);
        let stream_name = name.to_owned();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tx.closed() => break,
                    next = consumer.next() => match next {
                        Ok(message) => {
                            if tx.send(Ok(SeaMessage::new(&message))).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = tx
                                .send(Err(SeaFileError::Receive {
                                    stream: stream_name.clone(),
                                    source: box_err(err),
                                }))
                                .await;
                            break;
                        }
                    },
                }
            }
        });
        Ok(StdioSubscriber {
            stream: name.to_owned(),
            inner: BufferedSubscriber::new(StdioDeliveries::Reader(rx)).max_wait(BATCH_MAX_WAIT),
        })
    }
}

impl DefaultPublish for ConnectedStdioBroker {
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
impl TestableBroker for ConnectedStdioBroker {
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
        pipe.feed(
            key,
            wire::encode(message.headers(), message.payload(), true),
        );
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
impl ConnectedStdioBroker {
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
/// both are reported as unsupported rather than pretended. Batches it does serve: the client
/// reads one line at a time, so they are assembled here, capped at the size the mount site
/// named.
pub struct StdioSubscriber {
    stream: String,
    inner: BufferedSubscriber<StdioDeliveries>,
}

impl std::fmt::Debug for StdioSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl Subscriber for StdioSubscriber {
    type Message = SeaMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<SeaMessage, SeaFileError>> + Send + '_ {
        self.inner.stream()
    }
}

impl BatchSubscriber for StdioSubscriber {
    type Batch = Vec<SeaMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SeaFileError>> + Send + '_ {
        self.inner.batches(size)
    }
}

/// A subscription's lines, before batching: one line per poll, in arrival order.
///
/// Without the `testing` feature there is one variant, so the type is the reader task's channel
/// itself and every `match` on it is irrefutable.
enum StdioDeliveries {
    /// The reader task's channel, fed from standard input.
    Reader(mpsc::Receiver<Result<SeaMessage, SeaFileError>>),
    /// The in-process pipe's queue.
    #[cfg(feature = "testing")]
    InProcess(PipeQueue),
}

// The zero-cost promise of the in-process mode, held by the compiler.
#[cfg(not(feature = "testing"))]
const _: () = assert!(
    size_of::<StdioDeliveries>() == size_of::<mpsc::Receiver<Result<SeaMessage, SeaFileError>>>()
);

impl Subscriber for StdioDeliveries {
    type Message = SeaMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<SeaMessage, SeaFileError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| match self {
            Self::Reader(rx) => rx.poll_recv(cx),
            #[cfg(feature = "testing")]
            Self::InProcess(queue) => queue.poll_next(cx).map(|next| next.map(Ok)),
        })
    }
}

/// Publishes messages to standard output.
///
/// The line format is the client's own. A line is text, ends at a newline, and the reader trims
/// it, so a payload that is not UTF-8, holds a newline or begins or ends with whitespace (and any
/// message with headers) travels in the text-safe envelope. The client silently drops
/// empty lines, so an empty payload is rejected here instead.
///
/// A publish carries no per-message settings: a line on standard output takes a key and a
/// payload and nothing else, so [`Publisher::Options`] is the unit type and the publish builder
/// gains no step from this crate.
#[derive(Clone)]
pub struct StdioPublisher {
    cell: StdioCell,
    producer: Arc<OnceCell<StdioProducer>>,
}

impl std::fmt::Debug for StdioPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioPublisher").finish_non_exhaustive()
    }
}

impl Publisher for StdioPublisher {
    /// A line on standard output is written from the bytes and keeps nothing: the client's
    /// `send_to` takes a slice, and the text-safe envelope is a buffer of this crate's own.
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
        if msg.payload().is_empty() && msg.headers().is_empty() {
            return Err(SeaFileError::Invalid(
                "stdio drops empty lines; an empty message cannot be transmitted".into(),
            ));
        }
        let streamer = match &core.link {
            StdioLink::Stdio(streamer) => streamer,
            #[cfg(feature = "testing")]
            StdioLink::InProcess(pipe) => {
                let key = StreamKey::new(msg.name())
                    .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
                pipe.write(key, wire::encode(msg.headers(), msg.payload(), true));
                return Ok(());
            }
        };
        let producer = self
            .producer
            .get_or_try_init(async || {
                streamer
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
        // force_text: the stdio line format rejects non-UTF-8 payloads.
        let payload = wire::encode(msg.headers(), msg.payload(), true);
        producer
            .send_to(&key, payload.as_slice())
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })?
            .await
            .map(|_| ())
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })
    }
}

/// The publish policy for [`StdioPublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::StdioPublish;
///
/// let policy = StdioPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct StdioPublish;

impl PublishPolicy<ConnectedStdioBroker> for StdioPublish {
    type Live = StdioPublisher;

    fn pair(
        self,
        connected: &ConnectedStdioBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

/// The publish policy of this form, under the name every form uses.
///
/// A mount site names the concept, never the transport: moving a service from one form to
/// another changes the prelude it globs and leaves the composition root alone. The prefixed
/// [`StdioPublish`] stays at the crate root, for a service that mixes both forms.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::stdio::Publish;
///
/// let policy = Publish::default();
/// # let _ = policy;
/// ```
pub use StdioPublish as Publish;

pub mod prelude {
    //! The imports a service on a shell pipeline writes every time, in one glob.
    //!
    //! The framework's prelude, this form's broker, and [`Publish`]. A subscription here is a
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
    //! #[subscriber("jobs", publish)]
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
}
