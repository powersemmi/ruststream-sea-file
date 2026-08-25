//! The stdio transport: [`StdioBroker`], standard input and output as one stream - a service
//! that composes with ordinary command-line tools.

use std::future::{Future, ready};
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

    fn pair(
        self,
        connected: &ConnectedStdioBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}
