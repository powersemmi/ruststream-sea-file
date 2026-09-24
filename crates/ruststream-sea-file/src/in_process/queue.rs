//! What the in-process file and pipe share: the subscription registry that fans a message out,
//! the entries a subscription's queue carries, and the token that tells the harness a delivery is
//! done.

use std::collections::HashMap;

use ruststream::RawMessage;
use ruststream::testing::Coordinator;
use sea_streamer_types::{
    Buffer as _, Message as _, MessageHeader, SeqNo, ShardId, SharedMessage, StreamKey, Timestamp,
};
use tokio::sync::mpsc;

use crate::wire;

/// The shard every message is written to: both transports have one per stream key.
const SHARD: ShardId = ShardId::new(0);

/// One item of a subscription's queue.
pub(crate) enum Entry {
    /// A message, as the client would have read it off the transport.
    Message(SharedMessage),
    /// The end-of-stream mark: the subscription completes here.
    End,
}

pub(crate) type EntrySender = mpsc::UnboundedSender<Entry>;
pub(crate) type EntryReceiver = mpsc::UnboundedReceiver<Entry>;

/// Opaque handle of one subscription inside a [`Registry`].
pub(crate) type SubscriptionId = u64;

struct Subscription {
    key: String,
    sender: EntrySender,
}

/// The open subscriptions of one in-process file or pipe.
///
/// It lives inside its owner's lock, so a fan-out, an unsubscribe and a replay's end-of-file check
/// are one atomic step against each other: a message is either in a subscription's queue before
/// that subscription looks for the end, or it is never sent to it.
#[derive(Default)]
pub(crate) struct Registry {
    next_id: SubscriptionId,
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl Registry {
    /// Opens a subscription on `key` and returns its id and both ends of its queue. The sender is
    /// the one the fan-out uses, so the subscription can put a seek's replay back into its own
    /// queue.
    pub(crate) fn open(&mut self, key: &str) -> (SubscriptionId, EntrySender, EntryReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let id = self.next_id;
        self.next_id += 1;
        self.subscriptions.insert(
            id,
            Subscription {
                key: key.to_owned(),
                sender: sender.clone(),
            },
        );
        (id, sender, receiver)
    }

    /// Closes a subscription. A second close of the same one is a no-op.
    pub(crate) fn close(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    /// The stream key of an open subscription.
    pub(crate) fn key(&self, id: SubscriptionId) -> Option<&str> {
        self.subscriptions.get(&id).map(|sub| sub.key.as_str())
    }

    /// Sends `message` to every open subscription on `key`, counting each send in flight.
    pub(crate) fn fan_out(
        &self,
        key: &str,
        message: &SharedMessage,
        coordinator: Option<&Coordinator>,
    ) {
        for sub in self.subscriptions.values() {
            if sub.key == key
                && sub.sender.send(Entry::Message(message.clone())).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Sends the end-of-stream mark to every open subscription, after what each has queued.
    pub(crate) fn end_all(&self) {
        for sub in self.subscriptions.values() {
            let _ = sub.sender.send(Entry::End);
        }
    }
}

/// A delivery the harness counted in flight, released exactly once when the delivery is dropped.
///
/// Neither transport settles a delivery, so the drop is where one ends: a refused `ack` reaches it
/// like any other end.
pub(crate) struct Release(Coordinator);

impl Release {
    /// The token of a delivery counted in flight under `coordinator`, or none outside a harness
    /// run.
    pub(crate) fn of(coordinator: Option<&Coordinator>) -> Option<Self> {
        coordinator.cloned().map(Self)
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        self.0.consumed();
    }
}

/// A message as the client would have read it: the encoded bytes under a header carrying the
/// stream key, the sequence and the moment it was written.
pub(crate) fn stamp(key: StreamKey, sequence: SeqNo, bytes: Vec<u8>) -> SharedMessage {
    let length = bytes.len();
    SharedMessage::new(
        MessageHeader::new(key, SHARD, sequence, Timestamp::now_utc()),
        bytes,
        0,
        length,
    )
}

/// What a test reads back from the log: the message with its envelope opened, the way a
/// subscription would have received it.
pub(crate) fn raw(message: &SharedMessage) -> RawMessage {
    let (headers, payload) = wire::decode(message.message().as_bytes());
    RawMessage::new(message.header().stream_key().name(), payload).with_headers(headers)
}

/// Releases one delivery that was counted in flight and will not be delivered.
pub(crate) fn release_one(coordinator: Option<&Coordinator>) {
    if let Some(coordinator) = coordinator {
        coordinator.consumed();
    }
}

/// Releases every counted message still queued in `receiver`: a queue dropped with deliveries in
/// it must not leave the harness waiting for them.
pub(crate) fn release_queued(receiver: &mut EntryReceiver, coordinator: Option<&Coordinator>) {
    while let Ok(entry) = receiver.try_recv() {
        if let Entry::Message(_) = entry {
            release_one(coordinator);
        }
    }
}
