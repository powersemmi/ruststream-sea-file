//! The in-process pipe: standard input the test feeds, standard output the service writes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll};

use ruststream::RawMessage;
use ruststream::testing::Coordinator;
use sea_streamer_types::{SeqNo, StreamKey};

use crate::envelope::Envelope;
use crate::in_process::queue::{
    Entry, EntryReceiver, Registry, Release, SubscriptionId, release_queued, stamp,
};
use crate::message::SeaMessage;

/// One process's standard input and output, held in memory: what the connected broker, its
/// publishers and its subscriptions share when the harness connected the broker in process.
pub(crate) struct MemoryPipe {
    inner: Mutex<PipeInner>,
    coordinator: OnceLock<Coordinator>,
    /// The broker's own setting: whether what the service writes comes back to its own input.
    loopback: bool,
}

#[derive(Default)]
struct PipeInner {
    registry: Registry,
    /// Every message that crossed the pipe in either direction, per stream key, in order, as it
    /// was sent.
    lines: HashMap<String, Vec<RawMessage>>,
    /// The sequence the next line on each key carries. The two directions count apart: a line on
    /// standard input carries the number its producer gave it, and a line on standard output the
    /// number this process's producer gives it, each from zero, as the client counts.
    input: HashMap<String, SeqNo>,
    output: HashMap<String, SeqNo>,
}

impl std::fmt::Debug for MemoryPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPipe")
            .field("loopback", &self.loopback)
            .finish_non_exhaustive()
    }
}

/// Takes the next sequence of `key` from `counters`.
fn next_sequence(counters: &mut HashMap<String, SeqNo>, key: &str) -> SeqNo {
    let next = counters.entry(key.to_owned()).or_default();
    let sequence = *next;
    *next += 1;
    sequence
}

impl MemoryPipe {
    /// A pipe with nothing on it yet; what the service writes comes back to it under `loopback`.
    pub(crate) fn new(loopback: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::default(),
            coordinator: OnceLock::new(),
            loopback,
        })
    }

    fn lock(&self) -> MutexGuard<'_, PipeInner> {
        // A panic under the lock leaves the pipe as consistent as each step of it.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the coordinator the harness installs, once.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// A line arriving on standard input: every subscription on its key reads it.
    /// `sent` is the message as it was sent, which the harness reads back.
    pub(crate) fn feed(&self, key: StreamKey, bytes: Vec<u8>, sent: RawMessage) {
        let mut guard = self.lock();
        let inner = &mut *guard;
        let sequence = next_sequence(&mut inner.input, key.name());
        let line = stamp(key, sequence, bytes);
        inner
            .registry
            .fan_out(line.header().stream_key().name(), &line, self.coordinator());
        inner
            .lines
            .entry(line.header().stream_key().name().to_owned())
            .or_default()
            .push(sent);
        drop(guard);
    }

    /// A line the service writes to standard output. It reaches the process downstream, which
    /// is not this one, so its own subscriptions read it only under loopback.
    /// `sent` is the message as it was sent, which the harness reads back.
    pub(crate) fn write(&self, key: StreamKey, bytes: Vec<u8>, sent: RawMessage) {
        let mut guard = self.lock();
        let inner = &mut *guard;
        let sequence = next_sequence(&mut inner.output, key.name());
        let line = stamp(key, sequence, bytes);
        if self.loopback {
            inner
                .registry
                .fan_out(line.header().stream_key().name(), &line, self.coordinator());
        }
        inner
            .lines
            .entry(line.header().stream_key().name().to_owned())
            .or_default()
            .push(sent);
        drop(guard);
    }

    /// Every message that crossed the pipe under `name`, in order, as it was sent: the envelope
    /// that sealed it is the one that opens it.
    pub(crate) fn published(&self, name: &str) -> Vec<RawMessage> {
        self.lock().lines.get(name).cloned().unwrap_or_default()
    }

    /// Opens a subscription on `key`. Standard input keeps nothing, so it reads what arrives from
    /// here on.
    pub(crate) fn subscribe(self: &Arc<Self>, key: &str) -> PipeQueue {
        let (id, _, rx) = self.lock().registry.open(key);
        PipeQueue {
            pipe: Arc::clone(self),
            id,
            rx,
        }
    }

    /// What the broker's shutdown does to the pipe: it ends every subscription on it, after the
    /// lines each has already read, as the real shutdown ends every consumer of standard input.
    pub(crate) fn finish(&self) {
        self.lock().registry.end_all();
    }

    fn unsubscribe(&self, id: SubscriptionId) {
        self.lock().registry.close(id);
    }
}

/// One in-process subscription on standard input, in arrival order.
pub(crate) struct PipeQueue {
    pipe: Arc<MemoryPipe>,
    id: SubscriptionId,
    rx: EntryReceiver,
}

impl PipeQueue {
    /// The next line, or `None` once the subscription has ended.
    pub(crate) fn poll_next<Env: Envelope>(
        &mut self,
        cx: &mut Context<'_>,
        envelope: &Env,
    ) -> Poll<Option<SeaMessage<Env>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Entry::Message(line))) => Poll::Ready(Some(
                SeaMessage::open(line, envelope).released_by(Release::of(self.pipe.coordinator())),
            )),
            Poll::Ready(Some(Entry::End) | None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PipeQueue {
    fn drop(&mut self) {
        self.pipe.unsubscribe(self.id);
        release_queued(&mut self.rx, self.pipe.coordinator());
    }
}
