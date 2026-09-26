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
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::TrySendError;
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
/// [`end_with_eos`](crate::FileBroker::end_with_eos). A replay that ended can still be moved by
/// its seeker until the subscriber is dropped, and its stream then delivers again.
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

    /// Starts the driver on `runtime`, the one the broker connected on, whichever runtime opens
    /// the subscription.
    pub(crate) fn spawn(runtime: &Handle, stream: String, reader: impl Source) -> Self {
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        runtime.spawn(drive(
            reader,
            out_tx,
            cmd_rx,
            Arc::clone(&epoch),
            stream.clone(),
        ));
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
    Driver { cmd: mpsc::UnboundedSender<SeekCmd> },
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
            SeekBackend::Driver { cmd } => {
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

/// What the driver task reads a subscription through and repositions: the stream file's
/// [`Reader`] in a service, a scripted log in this module's tests.
pub(crate) trait Source: Send + 'static {
    /// The next message of the subscription's stream key.
    fn next(&mut self) -> impl Future<Output = Result<SharedMessage, StreamErr<FileErr>>> + Send;

    /// Moves the subscription; not cancel-safe, so it runs to completion on the driver task.
    fn reposition(
        &mut self,
        target: SeekTarget,
    ) -> impl Future<Output = Result<(), FileErr>> + Send;

    /// Whether a seek can still move the reader once it reported the end of the stream.
    fn repositions_after_end(&self) -> bool;
}

impl Source for Reader {
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

    async fn reposition(&mut self, target: SeekTarget) -> Result<(), FileErr> {
        match self {
            Self::Tail(consumer) => consumer.seek_to(target).await,
            Self::Replay { source, key } => source.seek(key, &SHARD, target).await,
        }
    }

    fn repositions_after_end(&self) -> bool {
        // The client's consumer task ends with the stream it tails; the file itself stays
        // readable from anywhere.
        matches!(self, Self::Replay { .. })
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

/// What the driver does once a read's outcome is delivered.
enum After {
    /// Reads on.
    Read,
    /// The stream ended cleanly: nothing is read until a seek moves the reader again.
    Idle,
    /// The stream ended or the read failed, and the reader cannot move again: the driver stops.
    Stop,
}

/// The delivery one outcome of the reader's `next` produces, and what the driver does after it.
fn delivery(
    next: Result<SharedMessage, StreamErr<FileErr>>,
    reader: &impl Source,
    stream: &str,
) -> (Option<Result<SeaMessage, SeaFileError>>, After) {
    match next {
        Ok(message) => (Some(Ok(SeaMessage::new(&message))), After::Read),
        // A stream that ended is not a failure. Both endings arrive here: the writer's
        // end-of-stream mark, which is the only thing that ends a live subscription, and the end
        // of the retained file, which ends a replay.
        Err(err) if is_clean_end(&err) => {
            let after = if reader.repositions_after_end() {
                After::Idle
            } else {
                After::Stop
            };
            (None, after)
        }
        Err(err) => (
            Some(Err(SeaFileError::Receive {
                stream: stream.to_owned(),
                source: box_err(err),
            })),
            After::Stop,
        ),
    }
}

async fn drive(
    mut reader: impl Source,
    out: mpsc::Sender<Stamped>,
    mut cmd_rx: mpsc::UnboundedReceiver<SeekCmd>,
    epoch: Arc<AtomicU64>,
    stream: String,
) {
    // The generation the reader is positioned in, published through `epoch` to the subscription's
    // filter. Only this task advances it, and only once a reposition has run.
    let mut generation = 0;
    // Set once the stream ended. The driver then serves seeks only, and the subscription stays
    // repositionable until it is dropped.
    let mut idle = false;
    'read: loop {
        let next = tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                if serve(&mut reader, cmd, &mut generation, &epoch, &stream).await {
                    idle = false;
                }
                continue;
            }
            () = out.closed() => break,
            next = reader.next(), if !idle => next,
        };
        let (item, after) = delivery(next, &reader, &stream);
        let permit = match out.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Closed(())) => break,
            // The subscription is behind. The handler that stopped draining the channel may be
            // the one waiting on a seek, so seeks are served while the driver waits for room.
            Err(TrySendError::Full(())) => loop {
                tokio::select! {
                    biased;
                    cmd = cmd_rx.recv() => {
                        let Some(cmd) = cmd else { break 'read };
                        if serve(&mut reader, cmd, &mut generation, &epoch, &stream).await {
                            // What was read belongs to the old position and goes with it.
                            idle = false;
                            continue 'read;
                        }
                    }
                    permit = out.reserve() => match permit {
                        Ok(permit) => break permit,
                        Err(_) => break 'read,
                    },
                }
            },
        };
        permit.send(Stamped {
            epoch: generation,
            item,
        });
        match after {
            After::Read => {}
            After::Idle => idle = true,
            After::Stop => break,
        }
    }
}

/// Runs one seek on the reader and answers it; `true` when the reader moved.
///
/// A reader that moved opens a new generation: everything read before it, queued for the handler
/// or still held by the driver, is discarded on the way out. A refused seek leaves the reader and
/// every queued delivery where they were.
async fn serve(
    reader: &mut impl Source,
    SeekCmd { position, done }: SeekCmd,
    generation: &mut u64,
    epoch: &AtomicU64,
    stream: &str,
) -> bool {
    let target = match position {
        FilePosition::Beginning => SeekTarget::Beginning,
        FilePosition::End => SeekTarget::End,
        FilePosition::Sequence(sequence) => SeekTarget::SeqNo(sequence),
        FilePosition::Timestamp(millis) => {
            let nanos = i128::from(millis) * 1_000_000;
            match Timestamp::from_unix_timestamp_nanos(nanos) {
                Ok(timestamp) => SeekTarget::Timestamp(timestamp),
                Err(err) => {
                    let _ = done.send(Err(SeaFileError::Invalid(format!(
                        "'{millis}' is not a valid timestamp: {err}"
                    ))));
                    return false;
                }
            }
        }
    };
    // The client's seek is not cancel-safe: it runs here to completion, never inside a racing
    // select arm.
    let result = reader.reposition(target).await;
    let moved = result.is_ok();
    if moved {
        // Published before the answer (Release, paired with the filter's Acquire load), so once
        // the seek returns, nothing read at the old position reaches the handler.
        *generation += 1;
        epoch.store(*generation, Ordering::Release);
    }
    let _ = done.send(result.map_err(|e| SeaFileError::Seek {
        stream: stream.to_owned(),
        source: box_err(e),
    }));
    moved
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::time::Duration;

    use futures::StreamExt;
    use sea_streamer_file::SeekErr;
    use sea_streamer_types::{MessageHeader, SeqNo};

    use super::*;
    use crate::wire;

    /// Longer than anything the driver waits for on its own: on the paused clock it elapses only
    /// once the runtime has nothing left to run, so reaching it means the seek can never finish.
    const STUCK: Duration = Duration::from_secs(60);

    /// A stream key's log held in memory, read the way a replay reads a file: every message in
    /// order, then the end of the file. A read is always ready, so the driver fills the delivery
    /// channel without yielding and parks only where it waits for room.
    struct Log {
        messages: Vec<SharedMessage>,
        next: usize,
    }

    impl Log {
        fn of(count: u8) -> Self {
            let key = StreamKey::new("orders").expect("the key is valid");
            let messages = (1..=count)
                .map(|body| {
                    let bytes = wire::encode(&HeaderMap::new(), &[body], false);
                    let length = bytes.len();
                    SharedMessage::new(
                        MessageHeader::new(
                            key.clone(),
                            SHARD,
                            SeqNo::from(body),
                            Timestamp::now_utc(),
                        ),
                        bytes,
                        0,
                        length,
                    )
                })
                .collect();
            Self { messages, next: 0 }
        }
    }

    impl Source for Log {
        fn next(&mut self) -> impl Future<Output = Result<SharedMessage, StreamErr<FileErr>>> {
            let message = self.messages.get(self.next).cloned();
            if message.is_some() {
                self.next += 1;
            }
            ready(message.ok_or(StreamErr::Backend(FileErr::NotEnoughBytes)))
        }

        fn reposition(&mut self, target: SeekTarget) -> impl Future<Output = Result<(), FileErr>> {
            let next = match target {
                SeekTarget::Beginning => 0,
                SeekTarget::End => self.messages.len(),
                // Past the last message, as the file refuses it: the reader stays where it was.
                SeekTarget::SeqNo(sequence) if sequence > self.messages.len() as u64 => {
                    return ready(Err(FileErr::SeekErr(SeekErr::OutOfBound)));
                }
                SeekTarget::SeqNo(sequence) => {
                    usize::try_from(sequence.saturating_sub(1)).expect("the sequence fits")
                }
                SeekTarget::Timestamp(_) => unreachable!("these tests seek by sequence"),
            };
            self.next = next;
            ready(Ok(()))
        }

        fn repositions_after_end(&self) -> bool {
            true
        }
    }

    /// What the subscription delivers next: the body of a message, or `None` at its end.
    async fn next_body<S>(stream: &mut S) -> Option<u8>
    where
        S: Stream<Item = Result<FileMessage, SeaFileError>> + Unpin,
    {
        tokio::time::timeout(STUCK, stream.next())
            .await
            .expect("the subscription moves on")
            .map(|next| next.expect("the delivery is ok").payload()[0])
    }

    /// A handler seeking while more messages are queued than the delivery channel holds: the
    /// driver is parked on the full channel, and the seek must still reach it.
    #[tokio::test(start_paused = true)]
    async fn a_seek_completes_while_the_delivery_channel_is_full() {
        let backlog = u8::try_from(CHANNEL_CAPACITY * 2).expect("the backlog fits");
        let mut subscriber =
            FileSubscriber::spawn(&Handle::current(), "orders".to_owned(), Log::of(backlog));
        let seeker = subscriber.seeker();
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_body(&mut stream).await, Some(1));
        // Lets the driver run until it parks: the channel is full and a read waits for room.
        tokio::task::yield_now().await;

        tokio::time::timeout(STUCK, seeker.seek(FilePosition::sequence(100)))
            .await
            .expect("a seek completes while the delivery channel is full")
            .expect("the seek succeeds");

        for expected in 100..=backlog {
            assert_eq!(next_body(&mut stream).await, Some(expected));
        }
        assert_eq!(next_body(&mut stream).await, None, "the replay ends");
    }

    /// A handler seeking once the replay has read to the end of the file, while the end it
    /// reported is still queued behind the last message: the end belongs to the old position,
    /// and the replay goes on from the new one.
    #[tokio::test(start_paused = true)]
    async fn a_seek_after_the_replay_reached_the_end_replays_again() {
        let mut subscriber =
            FileSubscriber::spawn(&Handle::current(), "orders".to_owned(), Log::of(3));
        let seeker = subscriber.seeker();
        let mut stream = std::pin::pin!(subscriber.stream());
        for expected in 1..=3 {
            assert_eq!(next_body(&mut stream).await, Some(expected));
        }
        // Lets the driver read past the last message and report the end.
        tokio::task::yield_now().await;

        tokio::time::timeout(STUCK, seeker.seek(FilePosition::beginning()))
            .await
            .expect("the seek completes")
            .expect("a seek after the end of the file succeeds");

        for expected in 1..=3 {
            assert_eq!(next_body(&mut stream).await, Some(expected));
        }
        assert_eq!(next_body(&mut stream).await, None, "the replay ends again");
    }

    /// A seek after the subscription already yielded its end: the subscription is still open, so
    /// the seek moves it and its stream delivers again.
    #[tokio::test(start_paused = true)]
    async fn a_seek_after_the_stream_ended_reopens_it() {
        let mut subscriber =
            FileSubscriber::spawn(&Handle::current(), "orders".to_owned(), Log::of(2));
        let seeker = subscriber.seeker();
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_body(&mut stream).await, Some(1));
        assert_eq!(next_body(&mut stream).await, Some(2));
        assert_eq!(next_body(&mut stream).await, None);

        tokio::time::timeout(STUCK, seeker.seek(FilePosition::sequence(2)))
            .await
            .expect("the seek completes")
            .expect("a seek after the stream ended succeeds");

        assert_eq!(next_body(&mut stream).await, Some(2));
        assert_eq!(next_body(&mut stream).await, None);
    }

    /// A refused seek leaves the subscription where it was: what was already read and queued
    /// for the handler is still delivered, then the rest of the file.
    #[tokio::test(start_paused = true)]
    async fn a_refused_seek_keeps_what_was_queued() {
        let mut subscriber =
            FileSubscriber::spawn(&Handle::current(), "orders".to_owned(), Log::of(5));
        let seeker = subscriber.seeker();
        let mut stream = std::pin::pin!(subscriber.stream());
        assert_eq!(next_body(&mut stream).await, Some(1));
        // Lets the driver queue the rest of the file and its end.
        tokio::task::yield_now().await;

        tokio::time::timeout(STUCK, seeker.seek(FilePosition::sequence(9)))
            .await
            .expect("the seek completes")
            .expect_err("no message carries that sequence");

        for expected in 2..=5 {
            assert_eq!(next_body(&mut stream).await, Some(expected));
        }
        assert_eq!(next_body(&mut stream).await, None);
    }
}
