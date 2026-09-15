//! The stream file: a persistent, replayable log on disk that needs no server.
//!
//! [`FileBroker`] records the path of a `.ss` file and opens it on [`Broker::connect`], which
//! yields [`ConnectedFileBroker`]; [`ConnectedBroker::shutdown`] flushes and closes it. A
//! service on stream files globs this form's [`prelude`] and names its policy [`Publish`].
//!
//! # Subscribing
//!
//! [`FileStream::new(key)`](FileStream) is the subscription: one stream key inside the file. It
//! sits inline in the `#[subscriber(..)]` attribute and follows the live tail. A bare stream key
//! (`#[subscriber("orders")]`) opens the same subscription through the framework's `Subscribe`
//! capability and carries no descriptor, so it describes nothing in the generated document.
//!
//! [`replay`](FileStream::replay) reads the retained file from its start and completes the
//! subscription at the end of the file instead of following live writes: that is how a recorded
//! log is processed in one pass. Point it at a file whose writer called
//! [`end_with_eos`](FileBroker::end_with_eos), so the end is written into the file rather than
//! inferred from it. Replay is the one reading mode a position cannot express.
//!
//! # Positions and seeking
//!
//! Where a subscription begins is the `start_at(..)` clause, and where a running handler moves it
//! is the [`SeekHandle`](crate::SeekHandle) key. Both take a [`FilePosition`](crate::FilePosition):
//!
//! | Position | Meaning |
//! | --- | --- |
//! | `FilePosition::beginning()` | The start of the retained file. |
//! | `FilePosition::end()` | The tip of the stream. |
//! | `FilePosition::sequence(n)` | Message number `n`, redelivered inclusively. A stream key is numbered from one, and the numbering carries on when a later connection appends to the same file. |
//! | `FilePosition::timestamp(millis)` | The first message strictly later than that instant, in milliseconds since the Unix epoch. |
//!
//! A position read off a delivery is pinned: seeking back to it redelivers exactly that message,
//! then the rest of the log in order. Deliveries already queued from before a seek are discarded,
//! so the next message a handler sees comes from the new position.
//!
//! Seek to a position the stream has reached. A sequence past the last one written, and an
//! instant later than every message the file holds, are both refused, and the subscription ends
//! with the refusal rather than staying where it was: the transport reads forward looking for the
//! message and runs out of file. A captured position, the beginning and the tip are always safe.
//!
//! A handler reads two keys, as parameters of the `Ctx` extractor, and names no context type:
//! [`Position`](crate::Position) is this delivery's place in the file, and
//! [`SeekHandle`](crate::SeekHandle) is the subscription's own seeker. Both are fields of
//! [`FileContext`](crate::FileContext), which the runtime builds per delivery.
//!
//! ```
//! use ruststream_sea_file::file::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct Job {
//!     id: u64,
//!     poisoned: bool,
//! }
//!
//! #[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
//! async fn work(
//!     job: &Job,
//!     Ctx(at): Ctx<Position>,
//!     Ctx(seeker): Ctx<SeekHandle>,
//! ) -> HandlerOutcome {
//!     if job.poisoned && seeker.seek(FilePosition::end()).await.is_err() {
//!         return HandlerOutcome::retry();
//!     }
//!     println!("job {} sits at {at:?}", job.id);
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("jobs", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/jobs.ss"), |b| {
//!             b.include(work);
//!         })
//! }
//! ```
//!
//! A batch body declares `ctx: &mut Context<'_, FileBatchContext>` and reads the handle with
//! `ctx.context(SeekHandle)`. A batch spans many deliveries, so
//! [`FileBatchContext`](crate::FileBatchContext) holds no position, and each element's own
//! sequence is in its [`SEQUENCE_HEADER`](crate::SEQUENCE_HEADER) header. Asking a batch body for
//! a per-delivery position does not compile.
//!
//! # Batches
//!
//! A handler taking `&[T]` consumes a batch, and `batch(n)` at the mount site says how large one
//! may be. The client reads one entry at a time, so the batch is assembled here: it never holds
//! more than the size the mount site named, and holds fewer whenever that is all the file had. A
//! partial batch goes out 10 ms after its first delivery; the deadline is fixed, and what it
//! bounds is how long a batch waits at an idle tail.
//!
//! ```
//! use ruststream_sea_file::file::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct Reading {
//!     millivolts: u32,
//! }
//!
//! #[subscriber(FileStream::new("readings"))]
//! async fn aggregate(batch: &[Reading]) -> Vec<HandlerOutcome> {
//!     let total: u32 = batch.iter().map(|reading| reading.millivolts).sum();
//!     println!("batch of {}, {total} mV in total", batch.len());
//!     batch.iter().map(|_| HandlerOutcome::ack()).collect()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("readings", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/readings.ss"), |b| {
//!             b.include(aggregate.batch(nonzero!(4)))
//!                 .max_attempts(nonzero!(3u32))
//!                 .dead_letter("readings.dead");
//!         })
//! }
//! ```
//!
//! # Publishing
//!
//! [`Publish`] is the policy that pairs into [`FilePublisher`], which appends to the stream file
//! and flushes on every publish, so a live subscriber - or an external reader of the same file -
//! sees a message as soon as the call returns. It is this broker's default, so a reply goes
//! through it when the mount site names no other; `.out_reply(Publish)` names it for a reply and
//! `.out(Ledger, Publish)` for a slot under its own marker.
//!
//! A destination here is a stream key inside the broker, and the file itself is named once, in
//! [`FileBroker::new`]. A reply type may therefore declare its key with
//! `#[outgoing(name = "receipts")]` and the subscriber carry the bare `publish` clause; a reply
//! type that declares none is published where the mount site says, with `publish("receipts")`.
//!
//! An append takes a stream key and a payload and nothing else, so this crate adds no step to the
//! publish builder and a handler body that publishes keeps the framework prelude alone and the
//! plain bound `Out<impl Publisher, Marker>`.
//!
//! ```
//! use ruststream_sea_file::file::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Debug, Outgoing, Serialize)]
//! #[outgoing(name = "receipts")]
//! struct Receipt {
//!     order: u64,
//! }
//!
//! #[subscriber(FileStream::new("orders"), publish)]
//! async fn confirm(order: &Order) -> Receipt {
//!     Receipt { order: order.id }
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
//!             b.include(confirm).out_reply(Publish);
//!         })
//! }
//! ```
//!
//! # Delayed redelivery
//!
//! `HandlerOutcome::retry_after(delay)` has no transport to lean on here, so the framework's
//! fallback is the whole mechanism: it drops the delivery and, once the delay is over, publishes
//! a copy of the message with an incremented retry-count header. On a stream file the copy needs
//! nothing from the mount site. One stream key is both ends of the file - the subscription
//! reports the key it reads, a publisher appends under it - so the copy lands where the handler
//! reads. A replay reports the same key: the copy reaches it while the replay is still short of
//! the end of the retained region, and is left unread once it has passed it.
//!
//! A handler that keeps answering `retry_after` circulates its message until an operator
//! intervenes. `max_attempts(n)` is how many deliveries one message gets, the first included, and
//! `dead_letter(key)` is the stream key a spent delivery is written to instead of coming back; a
//! cap without a destination rejects the spent delivery and writes it nowhere. Neither transport
//! counts its own redeliveries, so the count is the framework's retry-count header, which every
//! copy carries. A registration mounted by a bare stream key declares the same cap directly to
//! the broker, with no descriptor in between, and the runtime applies it the same way.
//! `out_retry(policy)` replaces the publisher the copies leave through, once per
//! registration; the position is a slot, so `.transform(..)` runs on the copy and `.codec(..)`
//! encodes nothing, since the copy carries the delivery's own bytes.
//!
//! # The broker's settings
//!
//! Three optional settings, all applied when the broker connects:
//! [`existing_only`](FileBroker::existing_only) requires the file to exist instead of creating
//! it, [`end_with_eos`](FileBroker::end_with_eos) writes an end-of-stream mark on shutdown (a
//! live subscription ends on that mark, and nothing else ends one), and
//! [`beacon_interval`](FileBroker::beacon_interval) sets how far apart the file's beacons sit, in
//! bytes. A beacon summarises the streams written before it and is what makes the file seekable.

use std::fs;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage,
    PairError, PublishPolicy, Publisher, ServerSpec, Subscribe,
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
#[cfg(feature = "asyncapi")]
use crate::stream::channel_extension;
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

    /// Writes an end-of-stream mark when the broker shuts down, marking the file finished.
    ///
    /// A subscription that tails the file reads the mark and ends there, which is the only
    /// thing that ends a live subscription; a replay reads a marked file to its end and
    /// completes. Finish a file a reader will replay: without the mark the reader has to find
    /// the end of the file for itself, and it may report the end before it has delivered
    /// everything the file holds.
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
        ServerSpec::in_process("file").description(self.path.clone())
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
        )
        .await)
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
    /// A stream key is both ends of the file: a publisher on this broker appends under the key
    /// the subscription tails, so the runtime's deferred copies have an address and the mount
    /// site has nothing to name.
    ///
    /// This is what makes `retry_after` work on a stream file, and nothing else does: the
    /// transport keeps no consumer positions, so a `nack` cannot hand the message back.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_stream(FileStream::new(name)).await
    }
}

/// Publishes messages into the stream file.
///
/// User headers travel in a text-safe envelope applied only when headers are present, so a
/// file written without headers stays readable as a plain payload stream by other tools.
///
/// A publish carries no per-message settings: an append to a stream file takes a key and a
/// payload and nothing else, so [`Publisher::Options`] is the unit type and the publish builder
/// gains no step from this crate.
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
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> Result<(), Self::Error> {
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

    fn pair(
        self,
        connected: &ConnectedFileBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// Names the stream key a publish through this policy appends under: the destination the
    /// mount site resolved, which on this transport is the stream key itself.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        channel_extension(channel)
    }
}

impl DefaultPublish for ConnectedFileBroker {
    type Policy = FilePublish;
}

/// The policy pairs against the in-process transport too, so a routes file that names it -
/// `.out_reply(Publish)`, the way production writes it - mounts on
/// [`FileTestBroker`](crate::testing::FileTestBroker) unchanged.
///
/// A policy is pure declaration and this one carries no settings, so nothing is dropped in the
/// translation; what differs is the live publisher it pairs into, which appends to the stand-in's
/// retained log instead of writing a file. The file's own machinery - the header envelope, the
/// per-publish flush - belongs to [`FilePublisher`] and is covered against real stream files, so a
/// test here must not assert on the bytes a `.ss` file ends up holding.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedFileTestBroker> for FilePublish {
    type Live = crate::testing::FileTestPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedFileTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The stand-in describes a destination the way the file does, so a document built in a test
    /// is the document the service ships.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        channel_extension(channel)
    }
}

/// The publish policy of this form, under the name every form uses.
///
/// A mount site names the concept, never the transport: moving a service from one form to
/// another changes the prelude it globs and leaves the composition root alone. The prefixed
/// [`FilePublish`] stays at the crate root, for a service that mixes both forms.
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
    //! The framework's prelude, this form's broker, subscription descriptor, position and seeker
    //! types, the seeking capability traits and context keys, and [`Publish`].
    //!
    //! This is the routes-side vocabulary: a mount site globs it and names policies by concept.
    //! A handler body globs `ruststream::prelude` instead and bounds an injected publisher by
    //! the framework's capability traits, so the two vocabularies never meet in one file.
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
    //! #[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
    //! async fn handle(order: &Order, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    //!     if order.id == 0 {
    //!         let _ = seeker.seek(FilePosition::end()).await;
    //!     }
    //!     HandlerOutcome::ack()
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

    pub use ruststream::prelude::*;
    // `Seekable` is implemented here too, but on the subscriber the runtime consumes, never named
    // by a service - do not add it.
    pub use ruststream::{Positioned, Seeker};

    pub use crate::file::{FileBroker, Publish};
    pub use crate::{
        FileBatchContext, FileContext, FilePosition, FileSeeker, FileStream, Position, SeekHandle,
    };
}
