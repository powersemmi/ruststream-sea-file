//! The stream file: a log on disk that survives restarts, replays and repositions, with no server.
//!
//! [`FileBroker`] records the path of a `.ss` file and opens it on [`Broker::connect`], which
//! yields [`ConnectedFileBroker`]; [`ConnectedBroker::shutdown`] syncs and closes it. A service on
//! stream files globs this form's [`prelude`] and names its policy [`Publish`].
//!
//! - [FileBroker][file-broker]: [`FileBroker`] and its settings
//!   [`existing_only`](FileBroker::existing_only), [`end_with_eos`](FileBroker::end_with_eos)
//!   and [`beacon_interval`](FileBroker::beacon_interval).
//! - [FileStream][file-stream]: [`FileStream`], one stream key of the file,
//!   and its [`replay`](FileStream::replay) mode.
//! - [FilePosition][positions]: [`FilePosition`](crate::FilePosition), opened with
//!   [`start_at`][start-at] and moved with [`SeekHandle`](crate::SeekHandle) ([Seek][seek]).
//! - [Batches][batches]: a slice handler over [`FileSubscriber`].
//! - [FilePublish][file-publish]: [`Publish`] and its live [`FilePublisher`].
//! - [retry_after][retry-after]: a deferred copy appended under the key the subscription reads.
//! - [Shutdown][shutdown]: the file closed, and the publishers handed out before it.
//!
//! [file-broker]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/file-broker/
//! [file-stream]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/file-stream/
//! [positions]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/
//! [start-at]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/start-at/
//! [seek]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/seek/
//! [batches]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/batches/
//! [file-publish]: https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/file-publish/
//! [retry-after]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/retry-after/
//! [shutdown]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/shutdown/

// Without the `testing` feature a link has one variant, so a `match` on it has a single arm; the
// matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::collections::HashMap;
use std::fs;
use std::future::{Future, ready};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};

#[cfg(feature = "testing")]
use ruststream::RawMessage;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, Lend,
    OutgoingMessage, PairError, PublishPolicy, Publisher, ServerSpec, Subscribe,
};
use sea_streamer_file::{
    AutoStreamReset, FileConnectOptions, FileConsumerOptions, FileErr, FileId, FileProducer,
    FileProducerOptions, FileStreamer, MessageSource, StreamMode,
};
use sea_streamer_types::{
    ConsumerMode, ConsumerOptions as _, Producer as _, StreamErr, StreamKey, Streamer as _,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OnceCell};

use crate::error::{SeaFileError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::MemoryFile;
use crate::stream::FileStream;
#[cfg(feature = "asyncapi")]
use crate::stream::channel_extension;
use crate::subscriber::{FileSubscriber, Reader};
use crate::wire;

pub(crate) struct Core {
    pub(crate) link: Link,
    pub(crate) path: String,
    pub(crate) closed: AtomicBool,
}

/// The file's client handles: the streamer every consumer is created from and the producer every
/// publish appends through.
pub(crate) struct Disk {
    pub(crate) streamer: FileStreamer,
    /// Taken out and dropped by `shutdown`, never later.
    ///
    /// Why the lock is exclusive: once an append fails, the client's writer ends and leaves every
    /// request queued behind the failing one unanswered, so a publish issued beside it would never
    /// return. One append at a time keeps nothing queued behind a failure, and the failure is
    /// recorded so no later publish reaches the ended writer. The client also counts producer
    /// handles per path, on one process-wide task, and applies a dropped handle to whichever
    /// writer holds the path when the drop is processed: a handle this connection dropped after
    /// its writer ended would end the writer of the next connection over the same path, so
    /// `shutdown` takes the handle out under the lock, once the publish in flight is done with it.
    ///
    /// Brokers opened on one path in one process share the client's writer, so they share this
    /// appender too: one lock per file, not per broker.
    pub(crate) producer: Arc<Mutex<Appender>>,
    /// The runtime `connect` ran on. Every task of this connection runs there: the subscription
    /// drivers this crate starts, and the readers the client starts when a consumer opens, so a
    /// subscription opened from another runtime does not stop when that runtime does.
    pub(crate) runtime: Handle,
}

/// The producer of a connection, from connect to shutdown.
pub(crate) enum Appender {
    /// Appends go through.
    Open(FileProducer),
    /// An append failed and the client's writer ended with it; the handle is kept for `shutdown`
    /// to drop in order, and every later publish reports the failure.
    Failed(FileProducer, FileErr),
    /// `shutdown` took the handle out.
    Closed,
}

/// The appender of every stream file a broker of this process has open, by path.
static APPENDERS: LazyLock<Mutex<HashMap<String, Weak<Mutex<Appender>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The appender a broker opened earlier on `path` and has not shut down, which a broker opened
/// on the same path appends through as well.
async fn open_appender(
    appenders: &HashMap<String, Weak<Mutex<Appender>>>,
    path: &str,
) -> Option<Arc<Mutex<Appender>>> {
    let shared = appenders.get(path)?.upgrade()?;
    let open = !matches!(*shared.lock().await, Appender::Closed);
    open.then_some(shared)
}

/// What a connected broker and every handle paired off it speak over: the stream file, or, under
/// the `testing` feature, the in-memory one the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the client handles themselves and
/// every `match` on it is irrefutable: a production build carries no second transport and no
/// branch to it.
pub(crate) enum Link {
    File(Disk),
    #[cfg(feature = "testing")]
    InProcess(Arc<MemoryFile>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the client handles it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Link>() == size_of::<Disk>());

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

/// A log kept in one `.ss` stream file for the `RustStream` messaging framework: it survives
/// restarts, replays what it recorded, and needs no server.
///
/// `new` is synchronous and records only the path; the file opens in the consuming
/// [`Broker::connect`], which creates it when it is missing. The file transport builds on Unix
/// systems only, an upstream constraint of the file client.
///
/// A publish appends the message under its stream key and flushes it, so the bytes reach the
/// operating system before the call returns; the file is synced to the device at shutdown. The
/// file client caps a stream file at 16 GiB: a publish that reaches the cap fails, and every
/// later publish to that file fails too. Brokers opened on one path in one process share the
/// file's writer: once one of them shuts down, a publish through another fails with
/// [`SeaFileError::Publish`] carrying the client's `ProducerEnded`.
///
/// The [FileBroker](https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/file-broker/)
/// page shows it in a service.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/orders.ss").end_with_eos(), |b| {
///             b.include(handle);
///         })
/// }
/// ```
///
/// A test runs the app `main` runs. With the `testing` feature, `TestApp::start` connects this
/// broker to a stream file held in memory, and the test addresses it by its production type:
///
/// ```
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_sea_file::file::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, Outgoing, PartialEq)]
/// #[outgoing(name = "orders")]
/// pub struct Order {
///     pub id: u64,
/// }
///
/// #[derive(Debug, Deserialize, Serialize, Outgoing, PartialEq)]
/// #[outgoing(name = "receipts")]
/// pub struct Receipt {
///     pub order: u64,
/// }
///
/// #[subscriber(FileStream::new("orders"), reply)]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { order: order.id }
/// }
///
/// /// The app `main` runs.
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/orders.ss"), |b| {
///             b.include(confirm);
///         })
/// }
///
/// pub async fn confirms_an_order() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///
///     // The publish returns once the handler it woke has settled.
///     tb.broker::<FileBroker>()
///         .message(&Order { id: 1 })
///         .publish()
///         .await?;
///
///     tb.broker::<FileBroker>()
///         .subscriber("orders")
///         .assert_called(1)
///         .with(&Order { id: 1 });
///     tb.broker::<FileBroker>()
///         .published::<Receipt>("receipts")
///         .assert_called(1)
///         .with(&Receipt { order: 1 });
///
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # #[cfg(feature = "testing")]
/// # #[tokio::main(flavor = "multi_thread", worker_threads = 2)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     demo::confirms_an_order().await
/// # }
/// # #[cfg(not(feature = "testing"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct FileBroker {
    path: String,
    create: bool,
    end_with_eos: bool,
    beacon_interval: Option<u32>,
    /// The size the client caps the file at, lowered only by the unit tests, which cannot write
    /// the client's default of 16 GiB.
    #[cfg(test)]
    size_limit: Option<u64>,
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
            #[cfg(test)]
            size_limit: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    #[cfg(test)]
    const fn size_limit(mut self, bytes: u64) -> Self {
        self.size_limit = Some(bytes);
        self
    }

    /// Requires the file to exist instead of creating it on connect.
    pub fn existing_only(mut self) -> Self {
        self.create = false;
        self
    }

    /// Writes an end-of-stream mark when the broker shuts down, marking the file finished.
    ///
    /// A subscription that tails the file reads the mark and ends there, which is the only
    /// thing that ends a live subscription. A replay completes at the mark, or at the end of the
    /// file when there is none, after delivering every message before it.
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
                #[cfg(test)]
                if let Some(bytes) = self.size_limit {
                    options.set_file_size_limit(bytes);
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
                let mut appenders = APPENDERS.lock().await;
                if let Some(shared) = open_appender(&appenders, &self.path).await {
                    return Ok(Arc::new(Core {
                        link: Link::File(Disk {
                            streamer,
                            producer: shared,
                            runtime: Handle::current(),
                        }),
                        path: self.path.clone(),
                        closed: AtomicBool::new(false),
                    }));
                }
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
                let shared = Arc::new(Mutex::new(Appender::Open(producer)));
                appenders.retain(|_, appender| appender.strong_count() > 0);
                appenders.insert(self.path.clone(), Arc::downgrade(&shared));
                drop(appenders);
                Ok::<_, SeaFileError>(Arc::new(Core {
                    link: Link::File(Disk {
                        streamer,
                        producer: shared,
                        runtime: Handle::current(),
                    }),
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

/// The in-process mode: the connected form a test runs the production app against, over a stream
/// file held in memory in place of the one at the path.
///
/// The file is new and empty, and every clone of this broker shares it, as clones share the file
/// they connect. The settings are checked the way `connect` checks them, so a broker a service
/// could not connect is not one a test can connect either: the path must form a file address, and
/// the beacon interval must be a positive multiple of 1024. [`existing_only`](Self::existing_only)
/// reads the in-memory file as the one the operator provisioned.
#[cfg(feature = "testing")]
impl InProcess for FileBroker {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                if let Some(interval) = self.beacon_interval {
                    FileConnectOptions::default()
                        .set_beacon_interval(interval)
                        .map_err(|e| {
                            SeaFileError::Invalid(format!("invalid beacon interval: {e}"))
                        })?;
                }
                FileId::new(self.path.clone())
                    .to_streamer_uri()
                    .map_err(|e| SeaFileError::Connect {
                        target: self.path.clone(),
                        source: box_err(e),
                    })?;
                Ok::<_, SeaFileError>(Arc::new(Core {
                    link: Link::InProcess(MemoryFile::new(self.end_with_eos)),
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

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(FileBroker);

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
        let disk = match &self.core.link {
            Link::File(disk) => disk,
            #[cfg(feature = "testing")]
            Link::InProcess(file) => {
                return Ok(FileSubscriber::in_process(
                    file.subscribe(key.name(), descriptor.replay_value()),
                ));
            }
        };
        // A live subscription tails the file through the client's consumer; where reading begins
        // is the framework's start_at / Seek surface. Replay is the one mode a seek cannot
        // express: it reads the retained file from the start and completes the stream at its end,
        // and it reads the file itself - see `Reader::Replay` for why.
        let stream = descriptor.stream().to_owned();
        let subscribe_err = |source| SeaFileError::Subscribe {
            stream: stream.clone(),
            source,
        };
        let reader = if descriptor.replay_value() {
            let file_id = FileId::new(self.core.path.clone());
            let source = MessageSource::new(file_id, StreamMode::Replay)
                .await
                .map_err(|e| subscribe_err(box_err(e)))?;
            Reader::Replay {
                source: Box::new(source),
                key,
            }
        } else {
            let mut options = FileConsumerOptions::new(ConsumerMode::RealTime);
            options.set_auto_stream_reset(AutoStreamReset::Latest);
            // Stated rather than left to the client's default: a live subscription waits for the
            // writes that follow the end of the file instead of finishing there.
            options.set_live_streaming(true);
            // The client starts the consumer's reader tasks on the runtime that opens it, so the
            // opening runs on the connection's runtime: once per subscription, off the delivery
            // path.
            let streamer = disk.streamer.clone();
            let consumer = disk
                .runtime
                .spawn(async move { streamer.create_consumer(&[key], options).await })
                .await
                .map_err(|e| subscribe_err(box_err(e)))?
                .map_err(|e| subscribe_err(box_err(e)))?;
            Reader::Tail(consumer)
        };
        let core = Arc::clone(&self.core);
        Ok(FileSubscriber::spawn(
            &disk.runtime,
            stream,
            reader,
            move || core.closed.load(Ordering::Acquire),
        ))
    }
}

impl ConnectedBroker for ConnectedFileBroker {
    type Error = SeaFileError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        let disk = match &self.core.link {
            Link::File(disk) => disk,
            #[cfg(feature = "testing")]
            Link::InProcess(file) => {
                file.finish();
                return Ok(());
            }
        };
        // The lock waits for the publish in flight. A connection that another clone of this
        // broker, or another broker on the same path, already shut down holds no handle and has
        // nothing left to end.
        let taken = mem::replace(&mut *disk.producer.lock().await, Appender::Closed);
        let producer = match taken {
            Appender::Open(producer) | Appender::Failed(producer, _) => producer,
            Appender::Closed => return Ok(()),
        };
        // Ends the file's shared producers (writing the end-of-stream mark when configured)
        // and flushes to disk. An already-ended producer is benign teardown noise: another
        // broker over the same file finished first.
        let ended = disk.streamer.clone().disconnect().await;
        // The handle is dropped after the writer ended, so the end-of-stream mark is written; its
        // drop reaches the client's task as a request of its own. The second end is answered by
        // that task only after it processed the drop, against a path no writer holds any more, so
        // `shutdown` returns once nothing of this connection can reach a later one over the file.
        drop(producer);
        let _settled = disk.streamer.clone().disconnect().await;
        match ended {
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
///
/// A destination is a stream key inside the broker, and the file itself is named once, in
/// [`FileBroker::new`]. The
/// [FilePublish](https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/file-publish/)
/// page shows it in a service.
///
/// # Examples
///
/// A reply type declares its stream key, and the subscriber carries the bare `reply` clause:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
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
/// #[subscriber(FileStream::new("orders"), reply)]
/// async fn confirm(order: &Order) -> Receipt {
///     tracing::info!(order.id, "confirmed");
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
///             b.include(confirm).out_reply(Publish);
///         })
/// }
/// ```
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
    /// The append reads the bytes and keeps nothing: the client's `send_to` takes a slice, and
    /// the envelope this crate writes is a buffer of its own anyway.
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
        let key = StreamKey::new(msg.name())
            .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
        let payload = wire::encode(msg.headers(), msg.payload(), false);
        match &core.link {
            Link::File(disk) => {
                let mut appender = disk.producer.lock().await;
                appender.append(msg.name(), &key, &payload).await
            }
            .map_err(|err| match err {
                // The appender closed while this connection is open: another broker on the
                // same path shut down and ended the file's writer.
                SeaFileError::NotConnected if !core.closed.load(Ordering::Acquire) => {
                    SeaFileError::Publish {
                        stream: msg.name().to_owned(),
                        source: box_err(FileErr::ProducerEnded),
                    }
                }
                other => other,
            }),
            #[cfg(feature = "testing")]
            Link::InProcess(file) => {
                file.append(key, payload);
                Ok(())
            }
        }
    }
}

impl Appender {
    /// Appends one message through an open producer, and records a failure that ended the
    /// client's writer, so a later publish reports it instead of reaching that writer.
    async fn append(
        &mut self,
        stream: &str,
        key: &StreamKey,
        payload: &[u8],
    ) -> Result<(), SeaFileError> {
        let failed = |source| SeaFileError::Publish {
            stream: stream.to_owned(),
            source,
        };
        let producer = match self {
            Self::Open(producer) => producer,
            Self::Failed(_, err) => return Err(failed(box_err(err.take()))),
            // A `shutdown` ran between the open check and the lock.
            Self::Closed => return Err(SeaFileError::NotConnected),
        };
        let mut cause = match append(producer, key, payload).await {
            Ok(()) => return Ok(()),
            Err(StreamErr::Backend(cause)) => cause,
            Err(other) => return Err(failed(box_err(other))),
        };
        let reported = cause.take();
        if let Self::Open(producer) = mem::replace(self, Self::Closed) {
            *self = Self::Failed(producer, cause);
        }
        Err(failed(box_err(reported)))
    }
}

/// Appends one message and flushes it to the file.
///
/// The flush is per publish: the sink buffers, and live subscribers (and external tails) observe
/// the file, not the buffer.
async fn append(
    producer: &mut FileProducer,
    key: &StreamKey,
    payload: &[u8],
) -> Result<(), StreamErr<FileErr>> {
    producer.send_to(key, payload)?.await?;
    producer.flush().await
}

/// The publish policy for [`FilePublisher`].
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream_sea_file::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Debug, Outgoing, Serialize)]
/// struct Started {
///     service: String,
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("recorder", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/recorder.ss"), |b| {
///             b.after_startup(FilePublish, async move |publisher| -> io::Result<()> {
///                 publisher
///                     .message(&Started { service: "recorder".into() })
///                     .to("lifecycle")
///                     .publish()
///                     .await
///                     .map_err(io::Error::other)
///             });
///         })
/// }
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

/// The harness's view of the in-process transport: what it injects, what it reads back, and the
/// coordinator it counts the in-flight deliveries with.
///
/// A message written under a stream key reaches every subscription reading that key, which is the
/// equal-name rule the default [`routes`](TestableBroker::routes) answers. A replay reads it only
/// while it is still short of the end of the file.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the file `connect_in_process` produced. `inject` also panics on a stream key the file refuses.
#[cfg(feature = "testing")]
impl TestableBroker for ConnectedFileBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(file) = &self.core.link {
            file.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let file = self.memory_file("inject");
        let key = StreamKey::new(message.name()).unwrap_or_else(|err| {
            panic!(
                "the injected message to {:?} is not one a stream file takes: {err}",
                message.name()
            )
        });
        file.append(
            key,
            wire::encode(message.headers(), message.payload(), false),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.memory_file("published").published(name)
    }
}

#[cfg(feature = "testing")]
impl ConnectedFileBroker {
    /// The in-memory file, which is all the harness drives.
    fn memory_file(&self, what: &str) -> &MemoryFile {
        match &self.core.link {
            Link::InProcess(file) => file,
            Link::File(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the file `connect_in_process` produces"
            ),
        }
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
/// use ruststream_sea_file::file::prelude::*;
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
/// #[subscriber(FileStream::new("orders"), reply("receipts"))]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
///             b.include(confirm).out_reply(Publish);
///         })
/// }
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
    //!     if order.id == 0 && seeker.seek(FilePosition::end()).await.is_err() {
    //!         return HandlerOutcome::retry();
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::future::join_all;
    use ruststream::{Broker as _, ConnectedBroker as _, OutgoingMessage, Publisher as _};
    use sea_streamer_file::{FileErr, FileId, MessageSource, StreamMode};
    use sea_streamer_types::{Buffer as _, Message as _, StreamKey};
    use tokio::runtime::Runtime;
    use tokio::time::timeout;

    use super::FileBroker;

    /// How long one step may take before the test calls it stuck: each is a local file write.
    const STEP: Duration = Duration::from_secs(10);

    /// A limit small enough for a few messages to reach it.
    const LIMIT: u64 = 8 * 1024;

    /// What one publish appends: its number, so the file can be checked message by message.
    fn body(n: usize) -> Vec<u8> {
        let mut bytes = format!("{n:06}").into_bytes();
        bytes.resize(300, b'.');
        bytes
    }

    /// Every message of `key` that is wholly in the file, by its number.
    async fn read_back(path: &str, key: &StreamKey) -> Vec<usize> {
        let mut source = MessageSource::new(FileId::new(path), StreamMode::Replay)
            .await
            .expect("the file opens for a replay");
        let mut found = Vec::new();
        loop {
            match source.next().await {
                Ok(msg) if msg.message.stream_key() == *key => {
                    let bytes = msg.message.message().as_bytes().to_vec();
                    let n = std::str::from_utf8(&bytes[..6]).expect("a number").parse();
                    found.push(n.expect("a number"));
                }
                Ok(_) => {}
                Err(FileErr::NotEnoughBytes | FileErr::StreamEnded) => return found,
                Err(e) => panic!("the file reads back: {e}"),
            }
        }
    }

    /// Publishes `count` messages into a file capped at [`LIMIT`], `concurrent` at a time,
    /// spread over `brokers` brokers opened on the file, and returns the numbers of the publishes
    /// that reported success.
    async fn publish_past_the_limit(
        path: &str,
        count: usize,
        concurrent: usize,
        brokers: usize,
    ) -> Vec<usize> {
        let mut connected = Vec::new();
        let mut publishers = Vec::new();
        for _ in 0..brokers {
            let broker = FileBroker::new(path).size_limit(LIMIT);
            connected.push(broker.clone().connect().await.expect("connects"));
            publishers.push(broker.publisher());
        }
        let mut succeeded = Vec::new();
        for wave in (0..count).collect::<Vec<_>>().chunks(concurrent) {
            let results = join_all(wave.iter().map(|&n| {
                let publisher = &publishers[n % brokers];
                async move {
                    let bytes = body(n);
                    let publish =
                        publisher.publish(OutgoingMessage::new("orders", &bytes[..]), None);
                    let result = timeout(STEP, publish).await.unwrap_or_else(|_| {
                        panic!("publish {n} never returned ({concurrent} at a time)")
                    });
                    (n, result)
                }
            }))
            .await;
            succeeded.extend(
                results
                    .into_iter()
                    .filter(|(_, r)| r.is_ok())
                    .map(|(n, _)| n),
            );
        }
        for connected in connected {
            let ended = timeout(STEP, connected.shutdown()).await;
            assert!(
                ended.is_ok(),
                "the broker never shut down ({concurrent} at a time)"
            );
        }
        succeeded
    }

    // The client caps a stream file, and once an append fails its writer takes no more. A
    // publish must still return, and one this crate reports as done must be one the file holds
    // whole: a reader of the file is the only place a message lives once the publish returned.
    #[test]
    fn a_publish_at_the_size_limit_returns_and_one_reported_done_is_whole_in_the_file() {
        // The client's producers live on one process-wide task, spawned on the first runtime
        // that touches them, so every case runs on this one.
        let rt = Runtime::new().expect("a runtime");
        rt.block_on(async {
            // One publish at a time, eight at once through one broker, and eight at once through
            // eight brokers opened on the one file, which share its writer.
            for (concurrent, brokers) in [(1, 1), (8, 1), (8, 8)] {
                let path = std::env::temp_dir()
                    .join(format!(
                        "ruststream-sea-limit-{}-{concurrent}-{brokers}.ss",
                        std::process::id()
                    ))
                    .to_string_lossy()
                    .into_owned();
                let _stale = std::fs::remove_file(&path);
                let succeeded = publish_past_the_limit(&path, 64, concurrent, brokers).await;
                let key = StreamKey::new("orders").expect("a stream key");
                let held = timeout(STEP, read_back(&path, &key)).await;
                let held = held.unwrap_or_else(|_| {
                    panic!("reading the file back never ended ({concurrent} at a time)")
                });
                std::fs::remove_file(&path).expect("the file is removed");
                assert!(
                    succeeded.len() < 64,
                    "the limit refused nothing ({concurrent} at a time)"
                );
                for n in &succeeded {
                    assert!(
                        held.contains(n),
                        "publish {n} reported success but the file does not hold it whole \
                         ({concurrent} at a time; reported {succeeded:?}, held {held:?})"
                    );
                }
            }
        });
    }
}
