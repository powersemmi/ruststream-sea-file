//! Where a delayed retry goes on each transport, and what a publish through a slot carries with
//! it.
//!
//! Neither transport settles a delivery: `ack` and `nack` report `AckError::Unsupported`, so a
//! handler asking for a pause before another attempt cannot be served by the transport. The
//! runtime's own fallback is the whole retry story here, and it works only if the subscription
//! says where a copy reaches it again. A stream file says its stream key. Standard output says
//! nothing, because the process downstream of the pipe is not the one that sent the message;
//! that half is asserted in `integration_sea.rs`, which owns the one stdio transport a test
//! binary may attach.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::{Outcome, TestApp};
use ruststream::{Broker, ConnectedBroker};
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::testing::FileTestBroker;
use serde::{Deserialize, Serialize};

/// How long the handler asks the runtime to hold the message back.
const RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u64,
}

/// The audit line every handled order leaves, on a stream key of its own.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
#[outgoing(name = "audit")]
struct Audited {
    id: u64,
}

/// Defers the first delivery and accepts the copy that comes back.
#[subscriber(FileStream::new("orders"))]
async fn reconcile(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(order.id, 1);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A stream file has no settlement, so the deferred copy is the only way a `retry_after` reaches
/// the handler again. It arrives because the descriptor reports the stream key: a publisher on
/// this broker appends under that key and the subscription reads the append.
#[tokio::test(start_paused = true)]
async fn a_delayed_retry_comes_back_to_the_handler_on_a_stream_file() {
    let broker = FileTestBroker::new();
    // --8<-- [start:out_retry]
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(broker, |b| {
        b.include(reconcile).out_retry(Publish);
    });
    // --8<-- [end:out_retry]
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<FileTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // The delay is real: nothing comes back before it has elapsed.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<FileTestBroker>()
        .subscriber("orders")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    assert_eq!(
        tb.broker::<FileTestBroker>()
            .subscriber("orders")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// A replay reads the region the file already held and completes, so a copy written afterwards
/// would never be read. The descriptor says so, and a registration binding the deferred-retry
/// position over it refuses to start instead of dropping every delayed message.
#[subscriber(FileStream::new("orders").replay())]
async fn audit_the_finished_file(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[tokio::test]
async fn a_retry_over_a_replay_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("redelivery-replay", "0.1.0")).with_broker(
        FileTestBroker::new(),
        |b| {
            b.include(audit_the_finished_file).out_retry(Publish);
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a replay cannot address its retries and must not start");
    let message = failed.to_string();
    assert!(message.contains("orders"), "{message}");
    assert!(message.contains("out_retry"), "{message}");
}

/// A publish written by the file broker reaches a subscription opened under the same stream key.
/// This is the promise the descriptor's answer makes, read directly off the transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reported_address_is_the_stream_key_on_both_forms() {
    use ruststream::{RedeliveryAddress, Subscribe, SubscriptionSource};

    let connected = FileTestBroker::new()
        .connect()
        .await
        .expect("transport connects");
    assert_eq!(
        Subscribe::redelivery_address(&connected, "orders"),
        Some(RedeliveryAddress::new("orders")),
    );
    assert_eq!(
        SubscriptionSource::redelivery_address(&FileStream::new("orders"), &connected)
            .await
            .expect("reporting an address must not fail"),
        Some(RedeliveryAddress::new("orders")),
    );
    assert_eq!(
        SubscriptionSource::redelivery_address(&FileStream::new("orders").replay(), &connected)
            .await
            .expect("reporting an address must not fail"),
        None,
        "a replay completes at the end of the retained region and reaches no later write",
    );
    connected.shutdown().await.expect("shutdown succeeds");
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Ledger;

/// Publishes through a slot, with no step touching the publish.
#[subscriber(FileStream::new("settings"))]
async fn record(order: &Order, Out(ledger): Out<impl Publisher, Ledger>) -> HandlerOutcome {
    if ledger
        .message(&Audited { id: order.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Neither transport has a per-message setting: an append to a stream file and a line on standard
/// output take a key and a payload and nothing else. So this crate's `Publisher::Options` is the
/// unit type, it ships no builder step, and every publish carries the policy's own settings. The
/// assertion pins that, so a step smuggled in later has to be justified rather than noticed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_carries_no_per_message_options() {
    let app =
        RustStream::new(AppInfo::new("options", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(record).out(Ledger, Publish).build();
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 4 })
        .to("settings")
        .publish()
        .await
        .expect("publish");

    tb.out::<Ledger>()
        .assert_called_once()
        .assert_options_default();
    tb.broker::<FileTestBroker>()
        .published::<Audited>("audit")
        .assert_called_once()
        .with(&Audited { id: 4 });
}
