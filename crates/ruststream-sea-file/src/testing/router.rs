//! Subscription registry, retained log and fanout for the in-process file-stream stand-in.
//!
//! Core routing plus the one transport property a stream file's handlers are written against: the
//! log is retained and positioned, so a subscription can be repositioned inside it. An exact-name
//! match fans a published message out to every live subscription on that name, each message is
//! stamped with its index in that name's log, and the log is what a seek replays from. The file's
//! own machinery (files, beacons, end-of-stream marks) is transport behaviour and is not simulated.

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::task::AtomicWaker;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use crate::error::SeaFileError;
use crate::message::FilePosition;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// Zero-based index of this message in its address's retained log. Stable across requeues and
    /// replays, so a redelivered message reports the same [`FilePosition`].
    pub(crate) sequence: u64,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

/// One retained message, with the wall-clock instant a timestamp seek resolves against.
struct LogEntry {
    message: RawMessage,
    at_millis: u64,
}

struct Subscription {
    address: String,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<LogEntry>>,
}

/// Shared between a subscription's polling side and the seekers minted off it.
///
/// A seek is a handoff, not an in-place mutation: the seeker records the target and wakes the
/// stream, and the subscriber applies it inside its own poll, where `&mut` access to the receiver
/// is available. Doing it there rather than on a driver task is what keeps the reposition inside
/// the reaction the harness drives to quiescence.
#[derive(Default)]
pub(crate) struct SeekControl {
    /// The replay the seek resolved to, taken by the subscriber inside its next poll.
    ///
    /// The deliveries themselves rather than the target: resolving them under the log lock makes
    /// the replay exact against a concurrent publish, and lets the seek count them in flight
    /// before it returns, so a harness driving to quiescence can never observe the gap between
    /// the seek and the poll that applies it.
    pending: Mutex<Option<Vec<Delivery>>>,
    /// Deliveries stamped below this position are stale pre-seek copies (a publish that raced the
    /// seek) and are dropped by the polling side.
    watermark: AtomicU64,
    /// Wakes the subscriber's stream task after `pending` is set.
    pub(crate) waker: AtomicWaker,
}

impl SeekControl {
    /// The stale-delivery cutoff, read on the polling side for every delivery.
    ///
    /// Acquire pairs with the Release store in [`AddressRouter::request_seek`]: a poll that
    /// observes a delivery enqueued after a seek also observes that seek's watermark.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::Acquire)
    }

    /// Takes the pending replay, if a seek was requested since the last poll.
    pub(crate) fn take_pending(&self) -> Option<Vec<Delivery>> {
        self.pending
            .lock()
            .expect("sea-file test seek mutex poisoned")
            .take()
    }
}

/// In-memory exact-address router over a retained, positioned log.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    /// Registers a subscription on `address` and returns the channel pair the subscriber will
    /// use, together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue = true)` and a replay.
    pub(crate) fn subscribe(
        &self,
        address: String,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("sea-file test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    address,
                    sender: tx.clone(),
                },
            );
        (id, tx, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("sea-file test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Appends `payload` to `address`'s retained log and fans it out to every subscription on
    /// that name. Under a harness run every live enqueue is counted with
    /// [`Coordinator::enqueued`].
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot = RawMessage::new(address, payload.clone()).with_headers(headers.clone());
        let mut to_notify: Vec<DeliverySender> = Vec::new();
        let sequence;
        {
            let mut state = self
                .state
                .lock()
                .expect("sea-file test router mutex poisoned");
            let entries = state.log.entry(address.to_owned()).or_default();
            sequence = entries.len() as u64;
            entries.push(LogEntry {
                message: snapshot,
                at_millis: now_millis(),
            });
            for sub in state.subscriptions.values() {
                if sub.address == address {
                    to_notify.push(sub.sender.clone());
                }
            }
        }

        let delivery = Delivery {
            payload,
            headers,
            sequence,
        };
        for tx in to_notify {
            if tx.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Resolves `to` against the retained log, hands the resulting replay to the subscription and
    /// wakes it.
    ///
    /// The replay is counted in flight here rather than where it is applied: the seek returns to
    /// the handler (or to the mount that opened the subscription) only once the harness can see
    /// the work it created, so quiescence is never observed in the gap between the two.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError::Seek`] once the subscription is gone - dropped, or cleared by the
    /// broker's shutdown - which is the in-process reading of seeking through a dead handle.
    pub(crate) fn request_seek(
        &self,
        id: SubscriptionId,
        to: FilePosition,
        control: &SeekControl,
        coordinator: Option<&Coordinator>,
    ) -> Result<(), SeaFileError> {
        let (target, replay) = {
            let state = self
                .state
                .lock()
                .expect("sea-file test router mutex poisoned");
            let Some(subscription) = state.subscriptions.get(&id) else {
                return Err(SeaFileError::Seek {
                    stream: String::from("<closed>"),
                    source: Box::from("the subscription has been closed"),
                });
            };
            let entries = state
                .log
                .get(&subscription.address)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let target = resolve(entries, to);
            let replay = entries
                .iter()
                .enumerate()
                .skip(usize::try_from(target).unwrap_or(usize::MAX))
                .map(|(sequence, entry)| Delivery {
                    payload: entry.message.payload().to_vec().into(),
                    headers: entry.message.headers().clone(),
                    sequence: sequence as u64,
                })
                .collect::<Vec<_>>();
            drop(state);
            (target, replay)
        };
        if let Some(coordinator) = coordinator {
            for _ in &replay {
                coordinator.enqueued();
            }
        }
        // Watermark first (Release, paired with the Acquire load in the delivery filter), then the
        // replay: a poll that takes the replay must see its watermark.
        control.watermark.store(target, Ordering::Release);
        *control
            .pending
            .lock()
            .expect("sea-file test seek mutex poisoned") = Some(replay);
        control.waker.wake();
        Ok(())
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("sea-file test router mutex poisoned")
            .log
            .get(address)
            .map(|entries| entries.iter().map(|e| e.message.clone()).collect())
            .unwrap_or_default()
    }

    /// Drops every subscription and clears the retained log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .expect("sea-file test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
    }
}

/// Resolves a position against one address's retained log, clamped to its end.
///
/// `End` and a sequence past the tail both land on the tail, where the subscription waits for the
/// next publish - the in-process reading of "skip everything retained".
fn resolve(entries: &[LogEntry], to: FilePosition) -> u64 {
    let end = entries.len() as u64;
    match to {
        FilePosition::Beginning => 0,
        FilePosition::End => end,
        FilePosition::Sequence(sequence) => sequence.min(end),
        // The retained log stamps each entry, so the timestamp form resolves the same way the
        // file does: the earliest entry strictly later than the instant.
        FilePosition::Timestamp(millis) => entries
            .iter()
            .position(|entry| entry.at_millis > millis)
            .map_or(end, |index| index as u64),
    }
}

/// Milliseconds since the Unix epoch, the unit [`FilePosition::Timestamp`] is expressed in.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .state
            .lock()
            .expect("sea-file test router mutex poisoned");
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
