//! The delayed retry, the cap and the dead-letter key, run as a service against a real stream
//! file.
//!
//! This transport settles nothing, so `retry_after` rests entirely on the runtime republishing a
//! copy under the stream key the subscription reads. That composition - a refused `nack`, a timer,
//! a publish through the broker's default policy, and the file delivering the copy back - is what
//! the in-process stand cannot prove, because it is the file that has to deliver it.
//!
//! Each test starts a real service over a temp file and watches the file from a second broker, so
//! what it asserts is what a reader of that file sees: the audit trail one handler leaves, or the
//! spent delivery the cap carried to the dead-letter key.

mod common;

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Publisher, Subscribe, Subscriber};
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::{ConnectedFileBroker, SeaFileError};
use serde::{Deserialize, Serialize};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the handler asks the runtime to hold the message back. Short, because the clock here
/// is the real one: the service runs against a real file, not under a paused-time harness.
const RETRY_DELAY: Duration = Duration::from_millis(150);

/// How long a delivery that must never arrive is given to arrive.
const ABSENCE_BUDGET: Duration = Duration::from_millis(500);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u64,
}

/// One line per delivery, on a stream key of its own: which order, and which attempt this was.
///
/// Publishing it is how the handler's view of its own deliveries reaches a reader of the file.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
#[outgoing(name = "attempts")]
struct Attempt {
    id: u64,
    attempt: u64,
}

/// The slot the trail leaves through.
#[derive(OutSlot)]
#[publishes(Attempt)]
struct Trail;

/// Reads the delivery's attempt from the framework's retry-count header, the only count on a
/// transport that keeps none of its own.
fn attempt<Cx, State>(ctx: &Context<'_, Cx, State>) -> u64 {
    ctx.headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Defers its first delivery and accepts the copy that comes back, leaving a line for each.
#[subscriber(FileStream::new("orders"))]
async fn reconcile(
    order: &Order,
    ctx: &mut Context,
    Out(trail): Out<impl Publisher, Trail>,
) -> HandlerOutcome {
    let attempt = attempt(ctx);
    if trail
        .message(&Attempt {
            id: order.id,
            attempt,
        })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::drop();
    }
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// Never ready: every delivery asks for another attempt, which is what a cap is for.
#[subscriber(FileStream::new("parcels"))]
async fn never_ready(
    order: &Order,
    ctx: &mut Context,
    Out(trail): Out<impl Publisher, Trail>,
) -> HandlerOutcome {
    let _ = trail
        .message(&Attempt {
            id: order.id,
            attempt: attempt(ctx),
        })
        .publish()
        .await;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same, on a stream key with no dead-letter destination beside its cap, and mounted by a
/// bare stream key rather than a descriptor - so the cap reaches the broker through its own
/// `declare_retry` with nothing in between.
#[subscriber("crates")]
async fn never_ready_uncollected(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A reader of the same file, opened before the service writes anything to it.
async fn observer(path: &str) -> ConnectedFileBroker {
    FileBroker::new(path)
        .connect()
        .await
        .expect("the observing broker opens the file")
}

/// Puts one order on a stream key of the file, the way a producer outside the service would.
async fn inject(broker: &ConnectedFileBroker, stream: &str, order: &Order) {
    broker
        .publisher()
        .message(order)
        .to(stream)
        .publish()
        .await
        .expect("publish succeeds");
}

/// The next delivery on a subscription, decoded.
async fn next<T, S>(stream: &mut S, what: &str) -> T
where
    T: for<'de> Deserialize<'de>,
    S: futures::Stream<Item = Result<ruststream_sea_file::FileMessage, SeaFileError>> + Unpin,
{
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{what}: a delivery must arrive"))
        .unwrap_or_else(|| panic!("{what}: the stream must stay open"))
        .unwrap_or_else(|e| panic!("{what}: the delivery must be ok, got {e}"));
    serde_json::from_slice(message.payload())
        .unwrap_or_else(|e| panic!("{what}: the delivery must decode, got {e}"))
}

/// Fails when anything arrives within the budget.
async fn nothing_more<S>(stream: &mut S, what: &str)
where
    S: futures::Stream<Item = Result<ruststream_sea_file::FileMessage, SeaFileError>> + Unpin,
{
    if let Ok(unexpected) = tokio::time::timeout(ABSENCE_BUDGET, stream.next()).await {
        panic!("{what}: nothing more must arrive, got {unexpected:?}");
    }
}

/// A `retry_after` on a stream file, end to end: the delivery is dropped, the copy is appended
/// under the key the subscription reads, and the handler sees it again with the count raised.
#[test]
fn a_delayed_retry_comes_back_through_the_file() {
    common::on_a_file(async {
        let path = common::tmp_path("retry-copy");
        let reader = observer(&path).await;
        let mut trail = reader
            .subscribe("attempts")
            .await
            .expect("subscription opens");
        let mut trail = pin!(trail.stream());

        let app = RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(
            FileBroker::new(&path),
            |b| {
                b.include(reconcile).out(Trail, Publish).build();
            },
        );
        let running = app.start().await.expect("the service starts");

        inject(&reader, "orders", &Order { id: 1 }).await;

        assert_eq!(
            next::<Attempt, _>(&mut trail, "first delivery").await,
            Attempt { id: 1, attempt: 0 },
        );
        // The copy the runtime published after the delay, read back off the file.
        assert_eq!(
            next::<Attempt, _>(&mut trail, "the deferred copy").await,
            Attempt { id: 1, attempt: 1 },
        );
        nothing_more(&mut trail, "after the copy was accepted").await;

        running.shutdown().await.expect("the service stops");
        reader.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The cap counts deliveries, the first included, and the spent one is written to the
/// dead-letter stream key instead of coming back.
#[test]
fn the_cap_carries_the_spent_delivery_to_the_dead_letter_stream_key() {
    common::on_a_file(async {
        let path = common::tmp_path("retry-cap");
        let reader = observer(&path).await;
        let mut trail = reader
            .subscribe("attempts")
            .await
            .expect("subscription opens");
        let mut dead = reader
            .subscribe("parcels.dead")
            .await
            .expect("subscription opens");
        let mut trail = pin!(trail.stream());
        let mut dead = pin!(dead.stream());

        let app = RustStream::new(AppInfo::new("capped", "0.1.0")).with_broker(
            FileBroker::new(&path),
            |b| {
                b.include(never_ready)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("parcels.dead")
                    .out(Trail, Publish)
                    .build();
            },
        );
        let running = app.start().await.expect("the service starts");

        inject(&reader, "parcels", &Order { id: 7 }).await;

        for expected in 0..3 {
            assert_eq!(
                next::<Attempt, _>(&mut trail, "a capped delivery").await,
                Attempt {
                    id: 7,
                    attempt: expected,
                },
            );
        }
        assert_eq!(
            next::<Order, _>(&mut dead, "the spent delivery").await,
            Order { id: 7 },
        );
        // Nothing is left in flight: the delivery at the cap was carried away, not deferred.
        nothing_more(&mut trail, "after the cap was spent").await;

        running.shutdown().await.expect("the service stops");
        reader.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// A cap with no destination beside it ends the circulation by rejecting the spent delivery, and
/// writes it nowhere: the stream key holds the injected order and the one copy that brought the
/// second delivery, and nothing else. The registration is a bare stream key, so the broker is the
/// one that accepted the declaration.
#[test]
fn a_cap_without_a_destination_stops_the_copies_and_writes_nothing() {
    common::on_a_file(async {
        let path = common::tmp_path("retry-uncollected");
        let reader = observer(&path).await;
        let mut appended = reader
            .subscribe("crates")
            .await
            .expect("subscription opens");
        let mut appended = pin!(appended.stream());

        let app = RustStream::new(AppInfo::new("capped", "0.1.0")).with_broker(
            FileBroker::new(&path),
            |b| {
                b.include(never_ready_uncollected)
                    .max_attempts(nonzero!(2u32));
            },
        );
        let running = app.start().await.expect("the service starts");

        inject(&reader, "crates", &Order { id: 8 }).await;

        assert_eq!(
            next::<Order, _>(&mut appended, "the injected order").await,
            Order { id: 8 },
        );
        assert_eq!(
            next::<Order, _>(&mut appended, "the one deferred copy").await,
            Order { id: 8 },
        );
        nothing_more(&mut appended, "after the cap was spent").await;

        running.shutdown().await.expect("the service stops");
        reader.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}
