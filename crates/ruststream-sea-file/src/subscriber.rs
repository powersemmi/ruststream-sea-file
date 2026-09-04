//! [`FileSubscriber`]: a stream of deliveries backed by a driver task that also serves
//! repositioning.
//!
//! The client's `seek`/`rewind` need `&mut Consumer` and are explicitly not cancel-safe, so
//! a driver task owns the consumer: seeks arrive as commands and run to completion outside
//! any `select!`, while `next()` (which is cancel-safe) feeds the delivery channel.
//!
//! Batches sit on top of that channel rather than in the client, which reads one message at a
//! time; see [`crate::batching`] for why, and for the deadline that closes a partial one.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::Stream;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Positioned,
    Seekable, Seeker, Subscriber,
};
use sea_streamer_file::{FileConsumer, FileErr};
use sea_streamer_types::{Consumer as _, SeqPos, StreamErr, Timestamp};
use tokio::sync::{mpsc, oneshot};

use crate::batching::BATCH_MAX_WAIT;
use crate::error::{SeaFileError, box_err};
use crate::message::{FilePosition, SeaMessage};

/// How many undelivered messages may sit between the driver and the consumer.
const CHANNEL_CAPACITY: usize = 64;

pub(crate) struct SeekCmd {
    position: FilePosition,
    done: oneshot::Sender<Result<(), SeaFileError>>,
}

pub(crate) struct Stamped {
    epoch: u64,
    item: Option<Result<SeaMessage, SeaFileError>>,
}

/// A subscription to one stream key in the file; yields [`FileMessage`]s.
///
/// Dropping the subscriber stops the driver task. A replay subscription completes (the
/// stream ends) at the end of the file.
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

    pub(crate) fn spawn(stream: String, consumer: FileConsumer, replay: bool) -> Self {
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        tokio::spawn(drive(
            consumer,
            out_tx,
            cmd_rx,
            stream.clone(),
            replay,
            Arc::clone(&epoch),
        ));
        let stream: Arc<str> = Arc::from(stream);
        Self {
            stream: Arc::clone(&stream),
            inner: BufferedSubscriber::new(Deliveries {
                stream,
                rx: out_rx,
                cmd: cmd_tx,
                epoch,
            })
            .max_wait(BATCH_MAX_WAIT),
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

/// The driver task's deliveries, before batching: one message per poll, in publish order.
struct Deliveries {
    stream: Arc<str>,
    rx: mpsc::Receiver<Stamped>,
    cmd: mpsc::UnboundedSender<SeekCmd>,
    epoch: Arc<AtomicU64>,
}

impl Subscriber for Deliveries {
    type Message = FileMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<FileMessage, SeaFileError>> + Send + '_ {
        // Minted once per opened stream, before the closure takes the receiver: every delivery
        // then carries a reference-counted clone, so the per-delivery context that reads the
        // seek handle by key allocates nothing.
        let seeker = Arc::new(Seekable::seeker(&*self));
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call). Items queued under an older generation
        // (before a seek) are discarded here; `item: None` marks a clean end of a replay.
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
}

impl Seekable for Deliveries {
    type Seeker = FileSeeker;

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
    /// The in-process retained log of the `testing` transport, repositioned inside the
    /// subscriber's own poll.
    #[cfg(feature = "testing")]
    Log(crate::testing::LogSeeker),
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
    /// The seeker of an in-process subscription on the `testing` transport.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(stream: Arc<str>, log: crate::testing::LogSeeker) -> Self {
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
                // Bump the generation first: deliveries already queued (or an in-flight forward)
                // belong to the pre-seek position and are discarded on the way out.
                epoch.fetch_add(1, Ordering::Release);
                let (done, wait) = oneshot::channel();
                cmd.send(SeekCmd { position: to, done })
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

/// A receive failure that means the stream ended cleanly: the writer's end-of-stream mark,
/// or the end of a dead file in replay mode.
fn is_clean_end(err: &StreamErr<FileErr>) -> bool {
    matches!(
        err,
        StreamErr::Backend(FileErr::StreamEnded | FileErr::NotEnoughBytes)
    )
}

async fn drive(
    mut consumer: FileConsumer,
    out: mpsc::Sender<Stamped>,
    mut cmd_rx: mpsc::UnboundedReceiver<SeekCmd>,
    stream: String,
    replay: bool,
    epoch: Arc<AtomicU64>,
) {
    loop {
        // Captured before awaiting: a delivery resolved out of `next()` was positioned before
        // any seek that lands mid-await, so it must carry the pre-await generation - stamping
        // after the await would let a concurrent seek's bump leak onto a stale delivery.
        let current = epoch.load(Ordering::Acquire);
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(SeekCmd { position, done }) = cmd else { break };
                // The client's seek is not cancel-safe: it runs here to completion, never
                // inside a racing select arm.
                let result = match position {
                    FilePosition::Beginning => consumer.rewind(SeqPos::Beginning).await,
                    FilePosition::End => consumer.rewind(SeqPos::End).await,
                    FilePosition::Sequence(sequence) => {
                        consumer.rewind(SeqPos::At(sequence)).await
                    }
                    FilePosition::Timestamp(millis) => {
                        let nanos = i128::from(millis) * 1_000_000;
                        match Timestamp::from_unix_timestamp_nanos(nanos) {
                            Ok(timestamp) => consumer.seek(timestamp).await,
                            Err(err) => {
                                let _ = done.send(Err(SeaFileError::Invalid(format!(
                                    "'{millis}' is not a valid timestamp: {err}"
                                ))));
                                continue;
                            }
                        }
                    }
                };
                let _ = done.send(result.map_err(|e| SeaFileError::Seek {
                    stream: stream.clone(),
                    source: box_err(e),
                }));
            }
            () = out.closed() => break,
            next = consumer.next() => {
                match next {
                    Ok(message) => {
                        let item = Stamped {
                            epoch: current,
                            item: Some(Ok(SeaMessage::new(&message))),
                        };
                        if out.send(item).await.is_err() {
                            break;
                        }
                    }
                    Err(err) if is_clean_end(&err) => {
                        if replay {
                            // A finished replay completes the subscription.
                            let _ = out.send(Stamped { epoch: current, item: None }).await;
                        } else {
                            let _ = out
                                .send(Stamped {
                                    epoch: current,
                                    item: Some(Err(SeaFileError::Receive {
                                        stream: stream.clone(),
                                        source: box_err(err),
                                    })),
                                })
                                .await;
                        }
                        break;
                    }
                    Err(err) => {
                        let _ = out
                            .send(Stamped {
                                epoch: current,
                                item: Some(Err(SeaFileError::Receive {
                                    stream: stream.clone(),
                                    source: box_err(err),
                                })),
                            })
                            .await;
                        break;
                    }
                }
            }
        }
    }
}
