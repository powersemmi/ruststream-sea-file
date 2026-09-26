//! The in-process stream file: a retained, positioned log per stream key, in memory.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use ruststream::RawMessage;
use ruststream::testing::Coordinator;
use sea_streamer_types::{SharedMessage, StreamKey, Timestamp};

use crate::error::SeaFileError;
use crate::in_process::queue::{
    Entry, EntryReceiver, EntrySender, Registry, Release, SubscriptionId, raw, release_one,
    release_queued, stamp,
};
use crate::message::{FilePosition, SeaMessage};

/// One stream file, held in memory: what the connected broker, its publishers and its
/// subscriptions share when the harness connected the broker in process.
pub(crate) struct MemoryFile {
    inner: Mutex<FileInner>,
    coordinator: OnceLock<Coordinator>,
    /// The broker's own setting: whether its shutdown writes the end-of-stream mark.
    end_with_eos: bool,
}

#[derive(Default)]
struct FileInner {
    registry: Registry,
    /// Every message the file holds, per stream key, in the order it was written. A message's
    /// sequence is its place in its key's list, counted from one, the way the file counts it.
    streams: HashMap<String, Vec<SharedMessage>>,
    /// Set once the broker's shutdown wrote the end-of-stream mark.
    marked: bool,
}

impl std::fmt::Debug for MemoryFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryFile")
            .field("end_with_eos", &self.end_with_eos)
            .finish_non_exhaustive()
    }
}

impl MemoryFile {
    /// An empty file, finished with the end-of-stream mark on shutdown when `end_with_eos`.
    pub(crate) fn new(end_with_eos: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::default(),
            coordinator: OnceLock::new(),
            end_with_eos,
        })
    }

    fn lock(&self) -> MutexGuard<'_, FileInner> {
        // A panic under the lock leaves the log as consistent as each step of it: the file goes
        // on serving rather than turning one failed test assertion into a cascade.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the coordinator the harness installs, once.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// Appends one encoded message under `key` and hands it to every subscription reading it.
    pub(crate) fn append(&self, key: StreamKey, bytes: Vec<u8>) {
        let mut inner = self.lock();
        let entries = inner.streams.entry(key.name().to_owned()).or_default();
        let message = stamp(key, entries.len() as u64 + 1, bytes);
        entries.push(message.clone());
        inner.registry.fan_out(
            message.header().stream_key().name(),
            &message,
            self.coordinator(),
        );
    }

    /// Every message written under `name`, in order, with its envelope opened.
    pub(crate) fn published(&self, name: &str) -> Vec<RawMessage> {
        self.lock()
            .streams
            .get(name)
            .map(|entries| entries.iter().map(raw).collect())
            .unwrap_or_default()
    }

    /// Opens a subscription on `key`: at the tip of the log, or, for a replay, over everything
    /// the log holds, completing at its end.
    pub(crate) fn subscribe(self: &Arc<Self>, key: &str, replay: bool) -> FileQueue {
        let mut inner = self.lock();
        let (id, requeue, rx) = inner.registry.open(key);
        if replay {
            for message in inner.streams.get(key).into_iter().flatten() {
                if requeue.send(Entry::Message(message.clone())).is_ok()
                    && let Some(coordinator) = self.coordinator()
                {
                    coordinator.enqueued();
                }
            }
        }
        drop(inner);
        FileQueue {
            file: Arc::clone(self),
            id,
            stream: Arc::from(key),
            rx,
            requeue,
            seek: Arc::new(SeekControl::default()),
            replay,
        }
    }

    /// What the broker's shutdown does to the file: when the broker writes one, the end-of-stream
    /// mark ends every subscription. Without it a live subscription goes on waiting, as it does
    /// on a file.
    pub(crate) fn finish(&self) {
        if self.end_with_eos {
            let mut inner = self.lock();
            inner.marked = true;
            inner.registry.end_all();
        }
    }

    /// Resolves `to` against the log, hands the resulting replay to the subscription and wakes it.
    ///
    /// The replay is counted in flight here rather than where it is applied, so a harness waiting
    /// for the reaction to settle never sees the gap between the seek and the poll that applies
    /// it. It is handed over under the file's lock, so a replay looking for its end never misses a
    /// seek that was already resolved.
    fn request_seek(
        &self,
        id: SubscriptionId,
        stream: &str,
        to: FilePosition,
        control: &SeekControl,
        replay: bool,
    ) -> Result<(), SeaFileError> {
        let refused = |why: &str| SeaFileError::Seek {
            stream: stream.to_owned(),
            source: Box::from(why),
        };
        let mut inner = self.lock();
        // A replay that read the whole file is still open to a seek, as the file's own reader is.
        if control.ended() {
            return Err(refused("the subscription has been closed"));
        }
        // On a file, the client's consumer ends with the stream it tails.
        if !replay && inner.marked {
            return Err(refused("the stream ended at its end-of-stream mark"));
        }
        let entries = inner
            .streams
            .get(stream)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let target = match resolve(entries, to) {
            Ok(target) => target,
            Err(Unresolved::Invalid(why)) => return Err(SeaFileError::Invalid(why)),
            // A replay reads the file itself, and that reader stays where it was.
            Err(Unresolved::Refused(why)) if replay => return Err(refused(why)),
            Err(Unresolved::Refused(why)) => {
                // The file client's consumer does not survive a refused seek: it reports the
                // refusal and then the end of the stream, so this subscription ends the same way.
                inner.registry.close(id);
                drop(inner);
                control.ended.store(true, Ordering::Release);
                control.waker.wake();
                return Err(refused(why));
            }
        };
        let replay: Vec<SharedMessage> = entries
            .iter()
            .skip(usize::try_from(target.saturating_sub(1)).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        if let Some(coordinator) = self.coordinator() {
            for _ in &replay {
                coordinator.enqueued();
            }
        }
        // Watermark first (Release, paired with the Acquire load in the delivery filter), then the
        // replay: a poll that takes the replay sees its watermark.
        control.watermark.store(target, Ordering::Release);
        *control
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(replay);
        drop(inner);
        control.waker.wake();
        Ok(())
    }

    /// Ends a replay that has read everything the file holds: closes the subscription when its
    /// queue is empty and no seek is waiting, or returns what arrived first.
    ///
    /// Under the file's lock, so a message appended at that moment is either in the queue already
    /// or never sent to a subscription that has ended.
    fn finish_replay(
        &self,
        id: SubscriptionId,
        rx: &mut EntryReceiver,
        control: &SeekControl,
    ) -> ReplayEnd {
        let mut inner = self.lock();
        if control.has_pending() {
            return ReplayEnd::Seek;
        }
        if let Ok(entry) = rx.try_recv() {
            return ReplayEnd::Next(entry);
        }
        inner.registry.close(id);
        ReplayEnd::Done
    }

    fn unsubscribe(&self, id: SubscriptionId) {
        self.lock().registry.close(id);
    }
}

/// What a replay found at the end of its queue.
enum ReplayEnd {
    /// A message or the mark arrived after all.
    Next(Entry),
    /// A seek was handed over; apply it before looking again.
    Seek,
    /// Nothing more: the replay has delivered the whole file.
    Done,
}

/// Shared between a subscription's polling side and the seekers minted off it.
///
/// A seek is a handoff, not an in-place mutation: the seeker resolves the target and wakes the
/// stream, and the subscription applies it inside its own poll, where it holds its queue. Doing it
/// there rather than on a task of its own keeps the reposition inside the reaction the harness
/// drives to a standstill.
#[derive(Default)]
struct SeekControl {
    /// The replay the seek resolved to, taken by the subscription inside its next poll.
    pending: Mutex<Option<Vec<SharedMessage>>>,
    /// Deliveries stamped below this sequence are stale copies from before a seek, and are
    /// dropped by the polling side.
    watermark: AtomicU64,
    /// Set when a seek was refused, which ends the subscription, and when the subscription is
    /// dropped.
    ended: AtomicBool,
    /// Wakes the subscription's stream after `pending` or `ended` is set.
    waker: AtomicWaker,
}

impl SeekControl {
    fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::Acquire)
    }

    fn ended(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }

    fn has_pending(&self) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn take_pending(&self) -> Option<Vec<SharedMessage>> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// Repositions one in-process subscription over the file's log: the half of
/// [`FileSeeker`](crate::FileSeeker) the in-process mode is built on.
#[derive(Clone)]
pub(crate) struct LogSeeker {
    file: Arc<MemoryFile>,
    id: SubscriptionId,
    stream: Arc<str>,
    control: Arc<SeekControl>,
    replay: bool,
}

impl LogSeeker {
    /// Resolves `to` against the log and hands the target to the subscription.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError::Seek`] once the subscription is closed, once a live subscription
    /// read the end-of-stream mark, and when the position names nothing the log holds (which ends
    /// a live subscription and leaves a replay where it was), and [`SeaFileError::Invalid`] for an
    /// instant no timestamp can hold.
    pub(crate) fn request(&self, to: FilePosition) -> Result<(), SeaFileError> {
        self.file
            .request_seek(self.id, &self.stream, to, &self.control, self.replay)
    }
}

/// One in-process subscription's queue of deliveries, in log order.
pub(crate) struct FileQueue {
    file: Arc<MemoryFile>,
    id: SubscriptionId,
    stream: Arc<str>,
    rx: EntryReceiver,
    /// The sending end of this subscription's own queue, where a seek's replay goes.
    requeue: EntrySender,
    seek: Arc<SeekControl>,
    /// Whether this subscription completes when it has read everything the file holds.
    replay: bool,
}

impl FileQueue {
    /// The stream key this subscription reads.
    pub(crate) fn stream(&self) -> &Arc<str> {
        &self.stream
    }

    /// The reposition handle of this subscription.
    pub(crate) fn seeker(&self) -> LogSeeker {
        LogSeeker {
            file: Arc::clone(&self.file),
            id: self.id,
            stream: Arc::clone(&self.stream),
            control: Arc::clone(&self.seek),
            replay: self.replay,
        }
    }

    /// The next delivery, or `None` once the subscription has ended: at the end-of-stream mark, at
    /// the end of the file for a replay, or after a refused seek.
    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<SeaMessage>> {
        // Register first, then apply a pending seek, so a seek requested while the stream was
        // parked is not missed.
        self.seek.waker.register(cx.waker());
        if self.seek.ended() {
            return Poll::Ready(None);
        }
        self.apply_pending_seek();
        loop {
            let entry = match self.rx.poll_recv(cx) {
                Poll::Ready(Some(entry)) => entry,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending if self.replay => {
                    match self.file.finish_replay(self.id, &mut self.rx, &self.seek) {
                        ReplayEnd::Next(entry) => entry,
                        ReplayEnd::Seek => {
                            self.apply_pending_seek();
                            continue;
                        }
                        ReplayEnd::Done => return Poll::Ready(None),
                    }
                }
                Poll::Pending => return Poll::Pending,
            };
            match entry {
                Entry::End => return Poll::Ready(None),
                // A stale copy from before a seek: the replay already covers it.
                Entry::Message(message) if *message.header().sequence() < self.seek.watermark() => {
                    release_one(self.file.coordinator());
                }
                Entry::Message(message) => {
                    return Poll::Ready(Some(
                        SeaMessage::new(&message).released_by(Release::of(self.file.coordinator())),
                    ));
                }
            }
        }
    }

    /// Applies a reposition requested through a seeker, if one is pending: everything queued
    /// before the seek is dropped, then the log from the target on is queued again.
    fn apply_pending_seek(&mut self) {
        let Some(replay) = self.seek.take_pending() else {
            return;
        };
        // Everything up to the replay's last message is what the replay itself carries, so a
        // queued copy of it is dropped rather than delivered twice. A message appended after the
        // seek resolved sits past this cutoff, is not in the replay, and is put back after it.
        let cutoff = replay.last().map_or_else(
            || self.seek.watermark(),
            |last| *last.header().sequence() + 1,
        );
        let mut arrived_after = Vec::new();
        while let Ok(entry) = self.rx.try_recv() {
            match entry {
                Entry::Message(message) if *message.header().sequence() < cutoff => {
                    release_one(self.file.coordinator());
                }
                entry => arrived_after.push(entry),
            }
        }
        // The replay was counted in flight by the seek, before the drain above released what was
        // queued: releasing first would let the count touch zero mid-swap.
        for entry in replay.into_iter().map(Entry::Message).chain(arrived_after) {
            // Cannot fail: this queue holds both ends of its own channel.
            let _ = self.requeue.send(entry);
        }
    }
}

impl Drop for FileQueue {
    fn drop(&mut self) {
        self.seek.ended.store(true, Ordering::Release);
        self.file.unsubscribe(self.id);
        release_queued(&mut self.rx, self.file.coordinator());
    }
}

/// Why a position resolves to nothing.
enum Unresolved {
    /// The position names no message the log holds; the file refuses it and the subscription ends.
    Refused(&'static str),
    /// The position is not one the transport can express; the subscription stays where it was.
    Invalid(String),
}

/// Resolves a position against one stream key's log into the sequence the subscription resumes
/// at, counting from one. `End` is the one position past the last entry.
fn resolve(entries: &[SharedMessage], to: FilePosition) -> Result<u64, Unresolved> {
    let retained = entries.len() as u64;
    match to {
        FilePosition::Beginning => Ok(1),
        FilePosition::End => Ok(retained + 1),
        // Sequence zero is the position before the first message, which the file reads as the
        // start; past the last one there is nothing to resume at.
        FilePosition::Sequence(sequence) if sequence <= retained => Ok(sequence.max(1)),
        FilePosition::Sequence(_) => Err(Unresolved::Refused("no message carries that sequence")),
        FilePosition::Timestamp(millis) => {
            let instant = Timestamp::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
                .map_err(|err| {
                    Unresolved::Invalid(format!("'{millis}' is not a valid timestamp: {err}"))
                })?;
            // The earliest message strictly later than the instant, as the file resolves it.
            entries
                .iter()
                .position(|message| *message.header().timestamp() > instant)
                .map(|index| index as u64 + 1)
                .ok_or(Unresolved::Refused("no message is later than that instant"))
        }
    }
}
