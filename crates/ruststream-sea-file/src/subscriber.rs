//! [`FileSubscriber`]: a stream of deliveries backed by a driver task that also serves
//! repositioning.
//!
//! The client's `seek`/`rewind` need `&mut Consumer` and are explicitly not cancel-safe, so
//! a driver task owns the consumer: seeks arrive as commands and run to completion outside
//! any `select!`, while `next()` (which is cancel-safe) feeds the delivery channel.
//!
//! Batches sit on top of that channel rather than in the client, which reads one message at a
//! time; see [`crate::batching`] for why, and for the deadline that closes a partial one.
//!
//! A replay reads the file itself rather than through the client's consumer; see [`Reader`] for
//! why.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::Stream;
#[cfg(feature = "testing")]
use futures::future::Either;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Positioned,
    Seekable, Seeker, Subscriber,
};
use sea_streamer_file::{FileConsumer, FileErr, MessageSource, SeekTarget, is_end_of_stream};
use sea_streamer_types::{Consumer as _, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp};
use tokio::sync::{mpsc, oneshot};

use crate::batching::BATCH_MAX_WAIT;
use crate::error::{SeaFileError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::{FileQueue, LogSeeker};
use crate::message::{FilePosition, SeaMessage};

/// How many undelivered messages may sit between the driver and the consumer.
const CHANNEL_CAPACITY: usize = 64;

pub(crate) struct SeekCmd {
    position: FilePosition,
    /// The generation this reposition opens. The driver starts stamping deliveries with it only
    /// once the reposition has run, so everything read at the previous position keeps the
    /// previous generation and is discarded on the way out.
    epoch: u64,
    done: oneshot::Sender<Result<(), SeaFileError>>,
}

pub(crate) struct Stamped {
    epoch: u64,
    item: Option<Result<SeaMessage, SeaFileError>>,
}

/// A subscription to one stream key in the file; yields [`FileMessage`]s.
///
/// Dropping the subscriber stops the driver task. The stream also ends on its own when the file
/// does: a replay reaches the end of what was retained, and a live subscription reaches the
/// end-of-stream mark a writer left with
/// [`end_with_eos`](crate::FileBroker::end_with_eos).
pub struct FileSubscriber {
    // Kept alongside the buffer so the stream key stays readable without reaching through it.
    stream: Arc<str>,
    // The driver task's deliveries plus client-side batching: the file client reads one message
    // at a time, so batches are assembled here - see the `batching` module.
    inner: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for FileSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl FileSubscriber {
    /// The stream key this subscription consumes.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream
    }

    pub(crate) fn spawn(stream: String, reader: Reader) -> Self {
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        tokio::spawn(drive(reader, out_tx, cmd_rx, stream.clone()));
        let stream: Arc<str> = Arc::from(stream);
        Self {
            stream: Arc::clone(&stream),
            inner: BufferedSubscriber::new(Deliveries::Driver(Driven {
                stream,
                rx: out_rx,
                cmd: cmd_tx,
                epoch,
            }))
            .max_wait(BATCH_MAX_WAIT),
        }
    }

    /// A subscription on the in-process transport, which needs no driver task: a seek is applied
    /// inside the subscription's own poll.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(queue: FileQueue) -> Self {
        Self {
            stream: Arc::clone(queue.stream()),
            inner: BufferedSubscriber::new(Deliveries::InProcess(queue)).max_wait(BATCH_MAX_WAIT),
        }
    }
}

impl Subscriber for FileSubscriber {
    type Message = FileMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
        self.inner.stream()
    }
}

impl BatchSubscriber for FileSubscriber {
    type Batch = Vec<FileMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SeaFileError>> + Send + '_ {
        self.inner.batches(size)
    }
}

impl Seekable for FileSubscriber {
    type Seeker = FileSeeker;

    fn seeker(&self) -> FileSeeker {
        // Batching does not move the subscription: this is the driver's own handle, reached
        // through the buffer.
        self.inner.seeker()
    }
}

/// A subscription's deliveries, before batching: one message per poll, in the file's order.
///
/// Without the `testing` feature there is one variant, so the type is the driver's channel itself
/// and every `match` on it is irrefutable: a production build carries no second transport and no
/// branch to it.
enum Deliveries {
    /// The driver task's channel, fed from the stream file.
    Driver(Driven),
    /// The in-process transport's queue.
    #[cfg(feature = "testing")]
    InProcess(FileQueue),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// deliveries exactly the size of the driver's channel they wrap.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Deliveries>() == size_of::<Driven>());

impl Subscriber for Deliveries {
    type Message = FileMessage;
    type Error = SeaFileError;

    #[cfg(not(feature = "testing"))]
    fn stream(&mut self) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
        let Self::Driver(driven) = self;
        driven.stream()
    }

    #[cfg(feature = "testing")]
    fn stream(&mut self) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
        match self {
            Self::Driver(driven) => Either::Left(driven.stream()),
            Self::InProcess(queue) => Either::Right(in_process_stream(queue)),
        }
    }
}

impl Seekable for Deliveries {
    type Seeker = FileSeeker;

    fn seeker(&self) -> FileSeeker {
        match self {
            Self::Driver(driven) => driven.seeker(),
            #[cfg(feature = "testing")]
            Self::InProcess(queue) => {
                FileSeeker::in_process(Arc::clone(queue.stream()), queue.seeker())
            }
        }
    }
}

/// The in-process queue's deliveries, each carrying the subscription's seeker the way a file's do.
#[cfg(feature = "testing")]
fn in_process_stream(
    queue: &mut FileQueue,
) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
    let seeker = Arc::new(FileSeeker::in_process(
        Arc::clone(queue.stream()),
        queue.seeker(),
    ));
    futures::stream::poll_fn(move |cx| {
        queue.poll_next(cx).map(|next| {
            next.map(|message| {
                Ok(FileMessage {
                    message,
                    seeker: Arc::clone(&seeker),
                })
            })
        })
    })
}

/// The driver task's deliveries: one message per poll, in publish order.
struct Driven {
    stream: Arc<str>,
    rx: mpsc::Receiver<Stamped>,
    cmd: mpsc::UnboundedSender<SeekCmd>,
    epoch: Arc<AtomicU64>,
}

impl Driven {
    fn stream(&mut self) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
        // Minted once per opened stream, before the closure takes the receiver: every delivery
        // then carries a reference-counted clone, so the per-delivery context that reads the
        // seek handle by key allocates nothing.
        let seeker = Arc::new(self.seeker());
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call). Items queued under an older generation
        // (before a seek) are discarded here; `item: None` marks the clean end of the stream.
        futures::stream::poll_fn(move |cx| {
            loop {
                match self.rx.poll_recv(cx) {
                    std::task::Poll::Ready(Some(stamped)) => {
                        if stamped.epoch == self.epoch.load(Ordering::Acquire) {
                            return std::task::Poll::Ready(stamped.item.map(|item| {
                                item.map(|message| FileMessage {
                                    message,
                                    seeker: Arc::clone(&seeker),
                                })
                            }));
                        }
                    }
                    std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        })
    }

    fn seeker(&self) -> FileSeeker {
        FileSeeker {
            stream: Arc::clone(&self.stream),
            backend: SeekBackend::Driver {
                cmd: self.cmd.clone(),
                epoch: Arc::clone(&self.epoch),
            },
        }
    }
}

/// A delivery from a stream file: the transport's [`SeaMessage`] plus the subscription's
/// reposition handle.
///
/// The handle is what makes the file transport's per-delivery context
/// ([`FileContext`](crate::FileContext)) buildable, and carrying it in the message type is what
/// keeps that context off the transports that cannot seek: standard input yields a plain
/// [`SeaMessage`], so a handler declaring the seeking context does not compile against it.
pub struct FileMessage {
    message: SeaMessage,
    seeker: Arc<FileSeeker>,
}

impl std::fmt::Debug for FileMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileMessage")
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl FileMessage {
    /// The stream key this message was published to.
    #[must_use]
    pub fn stream(&self) -> &str {
        self.message.stream()
    }

    /// The reposition handle of the subscription that delivered this message.
    pub(crate) fn seeker(&self) -> &FileSeeker {
        &self.seeker
    }
}

impl Positioned for FileMessage {
    type Position = FilePosition;

    fn position(&self) -> FilePosition {
        self.message.position()
    }
}

impl IncomingMessage for FileMessage {
    fn payload(&self) -> &[u8] {
        self.message.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.message.headers()
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        self.message.ack()
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.message.nack(requeue)
    }
}

/// What a [`FileSeeker`] repositions: the subscription it was minted from.
///
/// One variant per transport that delivers under the file form's contexts, each carrying only
/// its own machinery, so a handle can never hold the wrong half. The seeker itself is one type
/// on purpose: it is the value the [`SeekHandle`](crate::SeekHandle) key yields, so a handler
/// that seeks reads the same way against a stream file and against the in-process transport its
/// tests run on.
#[derive(Clone)]
enum SeekBackend {
    /// The stream file's driver task, which owns the client consumer.
    Driver {
        cmd: mpsc::UnboundedSender<SeekCmd>,
        epoch: Arc<AtomicU64>,
    },
    /// The in-process transport's log, repositioned inside the subscription's own poll.
    #[cfg(feature = "testing")]
    Log(LogSeeker),
}

/// Repositions a [`FileSubscriber`] while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker), and carried to handlers by the
/// [`SeekHandle`](crate::SeekHandle) context key.
#[derive(Clone)]
pub struct FileSeeker {
    // Arc rather than String: the per-delivery context clones the handle, and the clone must
    // stay allocation-free on the dispatch path.
    stream: Arc<str>,
    backend: SeekBackend,
}

impl FileSeeker {
    /// The seeker of an in-process subscription.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(stream: Arc<str>, log: LogSeeker) -> Self {
        Self {
            stream,
            backend: SeekBackend::Log(log),
        }
    }

    /// The stream key of the subscription this handle repositions.
    #[must_use]
    pub fn stream_key(&self) -> &str {
        &self.stream
    }

    fn dead(&self, why: &'static str) -> SeaFileError {
        SeaFileError::Seek {
            stream: self.stream.to_string(),
            source: Box::from(why),
        }
    }
}

impl std::fmt::Debug for FileSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSeeker")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl Seeker for FileSeeker {
    type Position = FilePosition;
    type Error = SeaFileError;

    async fn seek(&self, to: FilePosition) -> Result<(), SeaFileError> {
        match &self.backend {
            SeekBackend::Driver { cmd, epoch } => {
                // Opening the new generation here is what makes the reader discard everything
                // from the old position: deliveries already queued, and any the driver reads
                // between this bump and the reposition it is about to run.
                let opened = epoch.fetch_add(1, Ordering::Release) + 1;
                let (done, wait) = oneshot::channel();
                cmd.send(SeekCmd {
                    position: to,
                    epoch: opened,
                    done,
                })
                .map_err(|_| self.dead("the subscription's driver task has shut down"))?;
                wait.await
                    .map_err(|_| self.dead("the subscription's driver task has shut down"))?
            }
            // The in-process transport needs no task: the target is handed to the subscriber,
            // which applies it at the top of its next poll, inside the reaction the test
            // harness drives to quiescence.
            #[cfg(feature = "testing")]
            SeekBackend::Log(log) => log.request(to),
        }
    }
}

/// The shard every message of a stream file is written to: the file transport has one per key.
const SHARD: ShardId = ShardId::new(0);

/// What a subscription's driver task reads the file through.
pub(crate) enum Reader {
    /// The client's consumer, which follows the live tail.
    Tail(FileConsumer),
    /// The file read directly, from the beginning, one message at a time.
    ///
    /// The client's own replay reads ahead of its consumer and, at the end of a file with no
    /// end-of-stream mark, reports the end while that read-ahead is still undelivered, so the
    /// tail of the file never reaches the handler. Read here, nothing sits between the file and
    /// the delivery channel, and the end is reported after the last message was sent on.
    Replay {
        source: Box<MessageSource>,
        key: StreamKey,
    },
}

impl Reader {
    /// The next message of the subscription's stream key.
    async fn next(&mut self) -> Result<SharedMessage, StreamErr<FileErr>> {
        match self {
            Self::Tail(consumer) => consumer.next().await,
            Self::Replay { source, key } => loop {
                let message = source.next().await.map_err(StreamErr::Backend)?.message;
                if is_end_of_stream(&message) {
                    return Err(StreamErr::Backend(FileErr::StreamEnded));
                }
                // One file holds every stream key, the client's internal ones included.
                if message.header().stream_key() == key {
                    return Ok(message.to_shared());
                }
            },
        }
    }

    /// Moves the subscription; not cancel-safe, so it runs to completion on the driver task.
    async fn reposition(&mut self, target: SeekTarget) -> Result<(), FileErr> {
        match self {
            Self::Tail(consumer) => consumer.seek_to(target).await,
            Self::Replay { source, key } => source.seek(key, &SHARD, target).await,
        }
    }
}

/// A receive failure that means the stream ended cleanly: the writer's end-of-stream mark,
/// or the end of the file in replay mode.
fn is_clean_end(err: &StreamErr<FileErr>) -> bool {
    matches!(
        err,
        StreamErr::Backend(FileErr::StreamEnded | FileErr::NotEnoughBytes)
    )
}

async fn drive(
    mut reader: Reader,
    out: mpsc::Sender<Stamped>,
    mut cmd_rx: mpsc::UnboundedReceiver<SeekCmd>,
    stream: String,
) {
    // The generation the reader is actually positioned in. It advances when a reposition has
    // run, never when one is merely requested, so a delivery read at the old position cannot be
    // stamped with the generation the request opened and pass the reader's filter.
    let mut applied = 0;
    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(SeekCmd { position, epoch: opened, done }) = cmd else { break };
                let target = match position {
                    FilePosition::Beginning => SeekTarget::Beginning,
                    FilePosition::End => SeekTarget::End,
                    FilePosition::Sequence(sequence) => SeekTarget::SeqNo(sequence),
                    FilePosition::Timestamp(millis) => {
                        let nanos = i128::from(millis) * 1_000_000;
                        match Timestamp::from_unix_timestamp_nanos(nanos) {
                            Ok(timestamp) => SeekTarget::Timestamp(timestamp),
                            Err(err) => {
                                applied = opened;
                                let _ = done.send(Err(SeaFileError::Invalid(format!(
                                    "'{millis}' is not a valid timestamp: {err}"
                                ))));
                                continue;
                            }
                        }
                    }
                };
                // The client's seek is not cancel-safe: it runs here to completion, never
                // inside a racing select arm. A read it interrupted is discarded with the old
                // position.
                let result = reader.reposition(target).await;
                // The generation the request opened starts here, whether or not the reposition
                // succeeded: a failed one leaves the subscription where it was, and what it
                // reads next is current again rather than discarded for ever.
                applied = opened;
                let _ = done.send(result.map_err(|e| SeaFileError::Seek {
                    stream: stream.clone(),
                    source: box_err(e),
                }));
            }
            () = out.closed() => break,
            next = reader.next() => {
                if !forward(next, &out, &stream, applied).await {
                    break;
                }
            }
        }
    }
}

/// Forwards one outcome of the client's `next` into the delivery channel, stamped with the
/// generation it was read under.
///
/// Returns `false` when the driver has nothing left to do: the stream ended, the read failed, or
/// nothing is listening any more.
async fn forward(
    next: Result<SharedMessage, StreamErr<FileErr>>,
    out: &mpsc::Sender<Stamped>,
    stream: &str,
    epoch: u64,
) -> bool {
    let receive_error = |err| Stamped {
        epoch,
        item: Some(Err(SeaFileError::Receive {
            stream: stream.to_owned(),
            source: box_err(err),
        })),
    };
    match next {
        Ok(message) => {
            let item = Stamped {
                epoch,
                item: Some(Ok(SeaMessage::new(&message))),
            };
            out.send(item).await.is_ok()
        }
        Err(err) if is_clean_end(&err) => {
            // A stream that ended is not a failure. Both endings arrive here: the writer's
            // end-of-stream mark, which is the only thing that ends a live subscription, and the
            // end of the retained file, which ends a replay.
            let _ = out.send(Stamped { epoch, item: None }).await;
            false
        }
        Err(err) => {
            let _ = out.send(receive_error(err)).await;
            false
        }
    }
}
