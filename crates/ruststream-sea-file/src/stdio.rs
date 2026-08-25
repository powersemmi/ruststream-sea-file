//! The stdio transport: [`StdioBroker`], standard input and output as one stream - a service
//! that composes with ordinary command-line tools.
//!
//! This is one of the crate's two forms. Its [`prelude`] is what a service on a shell pipeline
//! globs, and [`Publish`] is this form's publish policy under the name every form uses, so an
//! include site names the policy without naming the transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::Stream;
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, PairError,
    PublishPolicy, Publisher, ServerSpec, Subscribe, Subscriber,
};
use sea_streamer_stdio::{StdioConnectOptions, StdioProducer, StdioProducerOptions, StdioStreamer};
use sea_streamer_types::{
    Consumer as _, ConsumerMode, ConsumerOptions as _, Producer as _, StreamKey, Streamer as _,
    StreamerUri,
};
use tokio::sync::{OnceCell, mpsc};

use crate::error::{SeaFileError, box_err};
use crate::message::SeaMessage;
use crate::wire;

pub(crate) struct StdioCore {
    pub(crate) streamer: StdioStreamer,
    pub(crate) closed: AtomicBool,
}

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
                    streamer,
                    closed: AtomicBool::new(false),
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
        // Globally destructive by the client's design: every stdio consumer and producer in
        // the process ends. That is the honest meaning of shutting down a process-wide
        // transport.
        self.core
            .streamer
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

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.core.ensure_open()?;
        let key =
            StreamKey::new(name).map_err(|e| SeaFileError::Invalid(format!("'{name}': {e}")))?;
        let consumer = self
            .core
            .streamer
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
            rx,
        })
    }
}

impl DefaultPublish for ConnectedStdioBroker {
    type Policy = StdioPublish;
}

/// A subscription to one stream key on standard input; yields [`SeaMessage`]s.
///
/// Standard input has no retained log: there is no acknowledgement and no repositioning, and
/// both are reported as unsupported rather than pretended.
pub struct StdioSubscriber {
    stream: String,
    rx: mpsc::Receiver<Result<SeaMessage, SeaFileError>>,
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
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

/// Publishes messages to standard output.
///
/// The line format is the client's own; payloads must be text, so a non-UTF-8 payload (and
/// any message with headers) travels in the text-safe envelope. The client silently drops
/// empty lines, so an empty payload is rejected here instead.
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
    type Error = SeaFileError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.cell.get().ok_or(SeaFileError::NotConnected)?;
        core.ensure_open()?;
        if msg.payload().is_empty() && msg.headers().is_empty() {
            return Err(SeaFileError::Invalid(
                "stdio drops empty lines; an empty message cannot be transmitted".into(),
            ));
        }
        let producer = self
            .producer
            .get_or_try_init(async || {
                core.streamer
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

    async fn pair(self, connected: &ConnectedStdioBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

/// The plain publish policy of this form, under its prefix-free concept name.
///
/// A form's policy vocabulary is aliased this way in full: every policy the form supports
/// appears under the concept it implements, so an include site names the concept and never the
/// transport, and a concept the form lacks has no name at all. This form supports one policy,
/// so this is the whole of its vocabulary.
///
/// This is a publish *policy* - the value a mount site pairs with the connected broker. It is
/// unrelated to the framework's `runtime::Publish`, which is the publish builder a handler
/// calls.
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
    //! `use ruststream_sea_file::stdio::prelude::*;` brings in the framework's own prelude, the
    //! shared surface of this crate, and everything the stdio form adds: its broker and its
    //! policy vocabulary. A subscription here is a plain stream key, resolved through the
    //! framework's `Subscribe` capability, so this form has no descriptor type to import.
    //!
    //! That vocabulary is aliased to prefix-free concept names. Every publish policy a form
    //! supports appears under the concept it implements, with the transport prefix stripped, so
    //! an include site names the concept and never the transport; a concept a form does not
    //! support simply has no name here, and reaching for it fails at the `use` line. That is the
    //! manifest principle applied to the policy layer, and it is why moving a service between
    //! forms - or between brokers that follow the convention - changes the prelude it globs and
    //! leaves the composition root alone. This form supports one policy, so
    //! [`Publish`] is the whole of its vocabulary; the prefixed
    //! [`StdioPublish`](crate::StdioPublish) stays at the crate root for a mixed file that needs
    //! to tell the two forms' policies apart by name.
    //!
    //! It is also this form's capability manifest, and this form's is empty. That is the point
    //! of splitting the preludes per form: standard input is a pipe with no retained log, so
    //! there is nothing to reposition and nothing to reposition to, and a service that globs
    //! this prelude gets a compile error the moment it reaches for a capability the transport
    //! does not have. See the note on the manifest below.
    //!
    //! Globbing two form preludes together collides on `Publish` alone, which rustc reports as
    //! `E0659` at the `use` line rather than at a call site. That is the signal to glob
    //! [`ruststream_sea_file::prelude`](crate::prelude) instead and write `file::Publish` and
    //! `stdio::Publish` where the forms differ.
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
    //! #[derive(Debug, Serialize)]
    //! struct Done {
    //!     id: u64,
    //! }
    //!
    //! #[subscriber("jobs", publish("results"))]
    //! async fn work(job: &Job) -> Done {
    //!     Done { id: job.id }
    //! }
    //!
    //! #[ruststream::app]
    //! fn app() -> impl App {
    //!     RustStream::new(AppInfo::new("pipeline", "0.1.0"))
    //!         .with_broker(StdioBroker::new(), |b| {
    //!             b.include(work);
    //!         })
    //! }
    //! ```

    // The framework's prelude stops short of brokers on purpose, because which broker a service
    // runs on is the one thing every service states for itself. Importing a form prelude is that
    // statement, and a sharper one than a crate path alone: the transport is named on the `use`
    // line, so the framework's glob can ride along instead of being repeated underneath it.
    pub use ruststream::prelude::*;

    // This form's capability manifest is deliberately empty. `StdioSubscriber` implements no
    // `Seekable` and there is no stdio seeker to call `Seeker` on: standard input has no
    // retained log to move around in. `Positioned` is the near miss - both forms deliver the
    // same message type, so a stdio delivery does report a sequence - but the only thing that
    // consumes such a position is the file form's seeker, so carrying the trait here would
    // advertise a round trip this transport cannot make. A service that needs it says so
    // explicitly and takes on the question of what it means over a pipe.

    pub use crate::stdio::{Publish, StdioBroker};

    // Absent for the same reasons as at the crate root: the `testing` module, the connected
    // broker and live publisher and subscriber, `SeaMessage` and `SEQUENCE_HEADER`,
    // `SeaFileError`, and the contract traits.
}
