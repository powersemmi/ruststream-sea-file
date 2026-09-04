//! [`FileTestSubscriber`] and [`FileTestMessage`], plus the seeker that repositions them.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::task::Poll;

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Positioned,
    Seekable, Subscriber, testing::Coordinator,
};

use crate::error::SeaFileError;
use crate::message::{FilePosition, SEQUENCE_HEADER};
use crate::paging::PAGE_MAX_WAIT;
use crate::subscriber::FileSeeker;
use crate::testing::broker::TestState;
use crate::testing::router::{
    Delivery, DeliveryReceiver, DeliverySender, SeekControl, SubscriptionId,
};

/// Repositions one in-process subscription over the broker's retained log.
///
/// The half of [`FileSeeker`] the `testing` transport is built on: the target is recorded here
/// and applied by [`FileTestSubscriber`] inside its own poll, so a seek stays part of the
/// reaction the [`TestApp`](ruststream::testing::TestApp) harness drives to quiescence rather
/// than racing it on a task of its own.
#[derive(Clone)]
pub(crate) struct LogSeeker {
    state: Arc<TestState>,
    id: SubscriptionId,
    control: Arc<SeekControl>,
}

impl LogSeeker {
    /// Resolves `to` against the retained log and hands the target to the subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError::Seek`] once the subscription is closed.
    pub(crate) fn request(&self, to: FilePosition) -> Result<(), SeaFileError> {
        self.state
            .router
            .request_seek(self.id, to, &self.control, self.state.coordinator())
    }
}

/// Subscriber returned by [`ConnectedFileTestBroker`](crate::testing::ConnectedFileTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes. It pages the way the real transports do - on the client, through the framework's
/// buffer - so a page handler mounts here exactly as it mounts on a stream file.
pub struct FileTestSubscriber {
    // Kept alongside the buffer so the stream key stays readable without reaching through it.
    stream: Arc<str>,
    inner: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for FileTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileTestSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl FileTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        stream: String,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        let stream: Arc<str> = Arc::from(stream);
        Self {
            stream: Arc::clone(&stream),
            inner: BufferedSubscriber::new(Deliveries {
                state,
                id,
                stream,
                rx,
                requeue,
                seek: Arc::new(SeekControl::default()),
                coordinator,
            })
            .max_wait(PAGE_MAX_WAIT),
        }
    }
}

impl Subscriber for FileTestSubscriber {
    type Message = FileTestMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

impl BatchSubscriber for FileTestSubscriber {
    type Batch = Vec<FileTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SeaFileError>> + Send + '_ {
        self.inner.batches(size)
    }
}

impl Seekable for FileTestSubscriber {
    type Seeker = FileSeeker;

    fn seeker(&self) -> FileSeeker {
        self.inner.seeker()
    }
}

/// The subscription's queued deliveries, before paging: one per poll, in log order.
struct Deliveries {
    state: Arc<TestState>,
    id: SubscriptionId,
    stream: Arc<str>,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    seek: Arc<SeekControl>,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl Deliveries {
    /// Applies a reposition requested through a [`FileSeeker`], if one is pending.
    ///
    /// Runs at the top of the subscriber's own poll, which is what makes `&mut` access to the
    /// receiver possible: everything queued before the seek is drained, then the retained suffix
    /// from the target on is re-enqueued.
    fn apply_pending_seek(&mut self) {
        let Some(replay) = self.seek.take_pending() else {
            return;
        };
        // Everything up to here is what the replay itself carries, so a queued copy of it is
        // dropped rather than delivered twice. A message published after the seek resolved sits
        // at or past this cutoff, is not in the replay, and must survive the swap - it belongs
        // after the replayed region, which is where it is put back.
        let cutoff = replay
            .last()
            .map_or_else(|| self.seek.watermark(), |last| last.sequence + 1);
        let mut arrived_after = Vec::new();
        while let Ok(delivery) = self.rx.try_recv() {
            if delivery.sequence < cutoff {
                // Every drained delivery was counted in flight when it was enqueued.
                if let Some(coordinator) = &self.coordinator {
                    coordinator.consumed();
                }
            } else {
                arrived_after.push(delivery);
            }
        }
        // The replay was already counted in flight by the seek itself, before the drain above
        // released the queued deliveries: decrementing first would let the in-flight count touch
        // zero mid-swap, and a concurrent quiescence wait (`TestApp::settle`) could observe that
        // instant and return before the replayed deliveries were processed.
        for delivery in replay.into_iter().chain(arrived_after) {
            // The send cannot fail: this subscriber holds both ends of its own channel.
            let _ = self.requeue.send(delivery);
        }
    }
}

impl Drop for Deliveries {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Seekable for Deliveries {
    type Seeker = FileSeeker;

    fn seeker(&self) -> FileSeeker {
        FileSeeker::in_process(
            Arc::clone(&self.stream),
            LogSeeker {
                state: Arc::clone(&self.state),
                id: self.id,
                control: Arc::clone(&self.seek),
            },
        )
    }
}

impl Subscriber for Deliveries {
    type Message = FileTestMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        // Minted once per opened stream, before the closure takes the receiver: every delivery
        // then carries a reference-counted clone, so building a per-delivery context allocates
        // nothing - the same shape the file transport uses.
        let seeker = Arc::new(Seekable::seeker(&*self));
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            // Same ordering as the in-memory reference broker: register, then apply a pending
            // seek, so a seek requested while the stream was parked is not missed.
            self.seek.waker.register(cx.waker());
            self.apply_pending_seek();
            loop {
                match self.rx.poll_recv(cx) {
                    // A stale pre-seek copy (a publish that raced the seek): drop it, the replay
                    // already covers everything from the watermark on.
                    Poll::Ready(Some(delivery)) if delivery.sequence < self.seek.watermark() => {
                        if let Some(coordinator) = &coordinator {
                            coordinator.consumed();
                        }
                    }
                    Poll::Ready(Some(delivery)) => {
                        return Poll::Ready(Some(Ok(FileTestMessage::new(
                            delivery,
                            requeue.clone(),
                            Arc::clone(&seeker),
                            coordinator.clone(),
                        ))));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
    }
}

/// Message handed to handlers from a [`FileTestSubscriber`].
///
/// It reports its position in the broker's retained log and carries the subscription's seeker,
/// exactly as a stream file's delivery does, so the file form's contexts and keys build off it
/// unchanged and a seeking service needs no edit to run under the harness.
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect.
pub struct FileTestMessage {
    delivery: Option<Delivery>,
    headers: HeaderMap,
    sequence: u64,
    requeue: DeliverySender,
    seeker: Arc<FileSeeker>,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for FileTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for FileTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileTestMessage")
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

impl FileTestMessage {
    fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        seeker: Arc<FileSeeker>,
        coordinator: Option<Coordinator>,
    ) -> Self {
        let sequence = delivery.sequence;
        // The same well-known header the file transport writes, so a page body that reads
        // positions off its elements works identically here.
        let mut headers = delivery.headers.clone();
        headers.insert(SEQUENCE_HEADER, sequence.to_string());
        Self {
            delivery: Some(delivery),
            headers,
            sequence,
            requeue,
            seeker,
            coordinator,
        }
    }

    /// The reposition handle of the subscription that delivered this message.
    pub(crate) fn seeker(&self) -> &FileSeeker {
        &self.seeker
    }
}

impl Positioned for FileTestMessage {
    type Position = FilePosition;

    fn position(&self) -> FilePosition {
        FilePosition::Sequence(self.sequence)
    }
}

impl IncomingMessage for FileTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        if self.delivery.is_some() {
            &self.headers
        } else {
            EMPTY.get_or_init(HeaderMap::new)
        }
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("FileTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }
}
