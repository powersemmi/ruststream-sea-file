//! The file transport: [`FileBroker`] -> [`ConnectedFileBroker`], a persistent, replayable
//! stream on disk.
//!
//! This is one of the crate's two forms. Its [`prelude`] is what a service on stream files
//! globs, and [`Publish`] is this form's publish policy under the name every form uses, so an
//! include site names the policy without naming the transport.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, PairError,
    PublishPolicy, Publisher, ServerSpec, Subscribe,
};
use sea_streamer_file::{
    AutoStreamReset, FileConnectOptions, FileConsumerOptions, FileErr, FileId, FileProducer,
    FileProducerOptions, FileStreamer,
};
use sea_streamer_types::{
    ConsumerMode, ConsumerOptions as _, Producer as _, StreamErr, StreamKey, Streamer as _,
};
use tokio::sync::OnceCell;

use crate::error::{SeaFileError, box_err};
use crate::stream::FileStream;
use crate::subscriber::FileSubscriber;
use crate::wire;

pub(crate) struct Core {
    pub(crate) streamer: FileStreamer,
    pub(crate) producer: FileProducer,
    pub(crate) path: String,
    pub(crate) closed: AtomicBool,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), SeaFileError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SeaFileError::NotConnected);
        }
        Ok(())
    }
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("path", &self.path)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

/// The internal stream key the connect-time priming marker rides; no user subscription
/// matches it.
const PRIME_STREAM: &str = "ruststream-internal";

/// A persistent, replayable stream on disk for the `RustStream` messaging framework: what
/// survives restarts, records for replay, and needs no external broker.
///
/// `new` is synchronous and records only the path; the file opens in the consuming
/// [`Broker::connect`]. Not supported on Windows (an upstream constraint of the file client).
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::FileBroker;
///
/// let broker = FileBroker::new("/var/lib/orders.ss");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct FileBroker {
    path: String,
    create: bool,
    end_with_eos: bool,
    beacon_interval: Option<u32>,
    cell: CoreCell,
}

impl FileBroker {
    /// Records the path of the stream file (created on connect when missing). No I/O.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            create: true,
            end_with_eos: false,
            beacon_interval: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Requires the file to exist instead of creating it on connect.
    pub fn existing_only(mut self) -> Self {
        self.create = false;
        self
    }

    /// Writes an end-of-stream mark when the broker shuts down, so replay consumers of the
    /// finished file complete instead of waiting for more data.
    pub fn end_with_eos(mut self) -> Self {
        self.end_with_eos = true;
        self
    }

    /// The interval of the file's in-place index (must be a positive multiple of 1024);
    /// denser beacons make seeking finer-grained at the cost of file size.
    pub fn beacon_interval(mut self, bytes: u32) -> Self {
        self.beacon_interval = Some(bytes);
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> FilePublisher {
        FilePublisher {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl Broker for FileBroker {
    type Error = SeaFileError;
    type Connected = ConnectedFileBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let mut options = FileConnectOptions::default();
                if self.create {
                    options.set_create_if_not_exists(true);
                }
                if self.end_with_eos {
                    options.set_end_with_eos(true);
                }
                if let Some(interval) = self.beacon_interval {
                    options.set_beacon_interval(interval).map_err(|e| {
                        SeaFileError::Invalid(format!("invalid beacon interval: {e}"))
                    })?;
                }
                let file_id = FileId::new(self.path.clone());
                let uri = file_id
                    .to_streamer_uri()
                    .map_err(|e| SeaFileError::Connect {
                        target: self.path.clone(),
                        source: box_err(e),
                    })?;
                let streamer = FileStreamer::connect(uri, options).await.map_err(|e| {
                    SeaFileError::Connect {
                        target: self.path.clone(),
                        source: box_err(e),
                    }
                })?;
                let connect_err = |e: StreamErr<FileErr>| SeaFileError::Connect {
                    target: self.path.clone(),
                    source: box_err(e),
                };
                let producer = streamer
                    .create_generic_producer(FileProducerOptions::default())
                    .await
                    .map_err(connect_err)?;
                // Prime a fresh file with one marker on an internal stream key: the client
                // cannot finish creating a live consumer on a file with no content, and the
                // marker rides a key no user subscription matches.
                let fresh = fs::metadata(&self.path).map_or(true, |meta| meta.len() <= 128);
                if fresh {
                    let prime_key = StreamKey::new(PRIME_STREAM)
                        .map_err(|e| SeaFileError::Invalid(e.to_string()))?;
                    producer
                        .send_to(&prime_key, b"1".as_slice())
                        .map_err(connect_err)?
                        .await
                        .map_err(connect_err)?;
                    let mut flusher = producer.clone();
                    flusher.flush().await.map_err(connect_err)?;
                }
                Ok::<_, SeaFileError>(Arc::new(Core {
                    streamer,
                    producer,
                    path: self.path.clone(),
                    closed: AtomicBool::new(false),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedFileBroker {
            core,
            cell: self.cell,
        })
    }
}

impl DescribeServer for FileBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("file").with_description(self.path.clone())
    }
}

/// The typed witness that `connect` succeeded: the file is open.
#[derive(Debug)]
pub struct ConnectedFileBroker {
    pub(crate) core: Arc<Core>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedFileBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> FilePublisher {
        FilePublisher {
            cell: Arc::clone(&self.cell),
        }
    }

    /// Opens the subscription described by `descriptor`.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError`] when the descriptor is invalid, the consumer cannot be
    /// created, or the broker is shut down.
    pub async fn subscribe_stream(
        &self,
        descriptor: FileStream,
    ) -> Result<FileSubscriber, SeaFileError> {
        descriptor.validate()?;
        self.core.ensure_open()?;

        let key = StreamKey::new(descriptor.stream())
            .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", descriptor.stream())))?;
        let mut options = FileConsumerOptions::new(ConsumerMode::RealTime);
        // A live subscription tails the file; where reading begins is the framework's
        // start_at / Seek surface. Replay is the one mode a seek cannot express: it reads
        // the retained file from the start and completes the stream at its end.
        options.set_auto_stream_reset(if descriptor.replay_value() {
            AutoStreamReset::Earliest
        } else {
            AutoStreamReset::Latest
        });
        options.set_live_streaming(!descriptor.replay_value());
        let consumer = self
            .core
            .streamer
            .create_consumer(&[key], options)
            .await
            .map_err(|e| SeaFileError::Subscribe {
                stream: descriptor.stream().to_owned(),
                source: box_err(e),
            })?;
        Ok(FileSubscriber::spawn(
            descriptor.stream().to_owned(),
            consumer,
            descriptor.replay_value(),
        ))
    }
}

impl ConnectedBroker for ConnectedFileBroker {
    type Error = SeaFileError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        // Ends the file's shared producers (writing the end-of-stream mark when configured)
        // and flushes to disk. An already-ended producer is benign teardown noise: another
        // broker over the same file finished first.
        match self.core.streamer.clone().disconnect().await {
            Ok(()) | Err(StreamErr::Backend(FileErr::ProducerEnded)) => Ok(()),
            Err(e) => Err(SeaFileError::Connect {
                target: self.core.path.clone(),
                source: box_err(e),
            }),
        }
    }
}

impl Subscribe for ConnectedFileBroker {
    type Subscriber = FileSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_stream(FileStream::new(name)).await
    }
}

/// Publishes messages into the stream file.
///
/// User headers travel in a text-safe envelope applied only when headers are present, so a
/// file written without headers stays readable as a plain payload stream by other tools.
#[derive(Clone)]
pub struct FilePublisher {
    cell: CoreCell,
}

impl std::fmt::Debug for FilePublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePublisher").finish_non_exhaustive()
    }
}

impl Publisher for FilePublisher {
    type Error = SeaFileError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.cell.get().ok_or(SeaFileError::NotConnected)?;
        core.ensure_open()?;
        let producer = &core.producer;
        let key = StreamKey::new(msg.name())
            .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
        let payload = wire::encode(msg.headers(), msg.payload(), false);
        producer
            .send_to(&key, payload.as_slice())
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })?
            .await
            .map_err(|e| SeaFileError::Publish {
                stream: msg.name().to_owned(),
                source: box_err(e),
            })?;
        // Flush per publish: the sink buffers, and live subscribers (and external tails)
        // observe the file, not the buffer. A clone shares the same sink.
        let mut flusher = producer.clone();
        flusher.flush().await.map_err(|e| SeaFileError::Publish {
            stream: msg.name().to_owned(),
            source: box_err(e),
        })
    }
}

/// The publish policy for [`FilePublisher`].
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::FilePublish;
///
/// let policy = FilePublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct FilePublish;

impl PublishPolicy<ConnectedFileBroker> for FilePublish {
    type Live = FilePublisher;

    async fn pair(self, connected: &ConnectedFileBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedFileBroker {
    type Policy = FilePublish;
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
/// use ruststream_sea_file::file::Publish;
///
/// let policy = Publish::default();
/// # let _ = policy;
/// ```
pub use FilePublish as Publish;

pub mod prelude {
    //! The imports a service on a stream file writes every time, in one glob.
    //!
    //! `use ruststream_sea_file::file::prelude::*;` brings in the framework's own prelude, the
    //! shared surface of this crate, and everything the file form adds: its broker, its
    //! subscription descriptor, the position type its `start_at` clause and its seeker speak
    //! in, and its policy vocabulary.
    //!
    //! That vocabulary is aliased to prefix-free concept names. Every publish policy a form
    //! supports appears under the concept it implements, with the transport prefix stripped, so
    //! an include site names the concept and never the transport; a concept a form does not
    //! support simply has no name here, and reaching for it fails at the `use` line. That is the
    //! manifest principle applied to the policy layer, and it is why moving a service between
    //! forms - or between brokers that follow the convention - changes the prelude it globs and
    //! leaves the composition root alone. This form supports one policy, so
    //! [`Publish`] is the whole of its vocabulary; the prefixed
    //! [`FilePublish`](crate::FilePublish) stays at the crate root for a mixed file that needs
    //! to tell the two forms' policies apart by name.
    //!
    //! It is also this form's capability manifest. The framework's capability traits are
    //! optional, and the ones a service writes down are those it names in a bound and those
    //! whose methods it calls on a value it was handed; the glob carries exactly those, for
    //! this form. Reading a manifest therefore tells a reader what the transport underneath
    //! this service can do, not what the crate can do somewhere else.
    //!
    //! Globbing two form preludes together collides on `Publish` alone, which rustc reports as
    //! `E0659` at the `use` line rather than at a call site. That is the signal to glob
    //! [`ruststream_sea_file::prelude`](crate::prelude) instead and write `file::Publish` and
    //! `stdio::Publish` where the forms differ.
    //!
    //! # Examples
    //!
    //! ```
    //! use ruststream_sea_file::file::prelude::*;
    //! use serde::Deserialize;
    //!
    //! #[derive(Debug, Deserialize)]
    //! struct Order {
    //!     id: u64,
    //! }
    //!
    //! // Everything this handler needs came from the one glob, the `Seeker` trait behind
    //! // `seek` included - which is the capability manifest doing its job.
    //! #[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
    //! async fn handle(order: &Order, Seek(seeker): Seek<FileSeeker>) -> HandlerResult {
    //!     if order.id == 0 {
    //!         let _ = seeker.seek(FilePosition::end()).await;
    //!     }
    //!     HandlerResult::Ack
    //! }
    //!
    //! #[ruststream::app]
    //! fn app() -> impl App {
    //!     RustStream::new(AppInfo::new("orders", "0.1.0"))
    //!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
    //!             b.include(handle);
    //!         })
    //! }
    //! ```

    // The framework's prelude stops short of brokers on purpose, because which broker a service
    // runs on is the one thing every service states for itself. Importing a form prelude is that
    // statement, and a sharper one than a crate path alone: the transport is named on the `use`
    // line, so the framework's glob can ride along instead of being repeated underneath it.
    pub use ruststream::prelude::*;

    // This form's capability manifest: the capability traits a service writes down, which are
    // the ones it names in a bound and the ones whose methods it calls on a value it was handed.
    // Both here are the second kind - `Seeker::seek` on the seeker a `Seek<..>` parameter binds,
    // and `Positioned::position` on a delivered message, which on this form is a position the
    // seeker accepts back. This form implements no capability a service names in a bound: a
    // stream file has no transaction and no request-reply.
    pub use ruststream::{Positioned, Seeker};

    pub use crate::file::{FileBroker, Publish};
    pub use crate::{FilePosition, FileSeeker, FileStream};

    // Absent for the same reasons as at the crate root: the `testing` module, the connected
    // broker and live publisher and subscriber, `SeaMessage` and `SEQUENCE_HEADER`,
    // `SeaFileError`, and the contract traits. `Seekable` is absent although this form
    // implements it, because it sits on `FileSubscriber` - the subscriber side, which the
    // runtime's plumbing consumes. A service names the seeker type in `Seek<FileSeeker>` and
    // calls `Seeker` on what it gets back; it never names that trait.
}
