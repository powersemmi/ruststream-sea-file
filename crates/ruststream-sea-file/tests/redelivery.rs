//! Where a delayed retry goes on each transport, what a registration declares about its retries,
//! and what a publish through a slot carries with it.
//!
//! Neither transport settles a delivery: `ack` and `nack` report `AckError::Unsupported`, so a
//! handler asking for a pause before another attempt cannot be served by the transport. The
//! runtime's own fallback is the whole retry story here, and it rests on where a copy reaches the
//! subscription again. A stream file says its stream key and needs nothing from the mount site.
//! Standard output says nothing, because the process downstream of the pipe is not the one that
//! sent the message, so a stdio registration names a destination itself. Each transport is driven
//! on its own in-process stand, which is what makes that difference visible in a test.

#![cfg(feature = "testing")]

use std::time::Duration;

// The derive and the value a publish transform mutates share the name in different namespaces:
// the derive is the macro `Outgoing`, the value is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    Subscribe, nonzero,
};
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::testing::{ConnectedFileTestBroker, FileTestBroker};
use ruststream_sea_file::{ConnectedFileBroker, ConnectedStdioBroker};
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

/// Reads the delivery's attempt from the framework's retry-count header, which is the only count
/// on a transport that keeps none of its own.
fn attempt<Cx, State>(ctx: &Context<'_, Cx, State>) -> u64 {
    ctx.headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Defers the first delivery and accepts the copy that comes back.
#[subscriber(FileStream::new("orders"))]
async fn reconcile(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    assert_eq!(order.id, 1);
    if attempt(ctx) == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A stream file has no settlement, so the deferred copy is the only way a `retry_after` reaches
/// the handler again. It arrives with nothing named at the mount site: the descriptor reports the
/// stream key, and the copy leaves through the publisher the runtime pairs from the broker's own
/// default policy.
#[tokio::test(start_paused = true)]
async fn a_delayed_retry_comes_back_to_the_handler_on_a_stream_file() {
    let broker = FileTestBroker::new();
    // --8<-- [start:mount]
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(broker, |b| {
        b.include(reconcile);
    });
    // --8<-- [end:mount]
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

/// Never ready: every delivery asks for another attempt, which is what a cap is for.
#[subscriber(FileStream::new("parcels"))]
async fn never_ready(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The cap counts deliveries, the first included, and the destination is where the last one goes.
/// Neither transport counts its own redeliveries, so the count is the framework's header, which
/// the copies carry.
#[tokio::test(start_paused = true)]
async fn the_cap_sends_the_last_delivery_to_the_dead_letter_stream_key() {
    // --8<-- [start:declaration]
    let app =
        RustStream::new(AppInfo::new("capped", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(3u32))
                .dead_letter("parcels.dead");
        });
    // --8<-- [end:declaration]
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 7 })
        .to("parcels")
        .publish()
        .await
        .expect("publish");

    // Two copies come back; the third delivery is the last the cap allows.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<FileTestBroker>()
        .subscriber("parcels")
        .assert_called(3);
    tb.broker::<FileTestBroker>()
        .published::<Order>("parcels.dead")
        .assert_called_once()
        .with(&Order { id: 7 });

    // Nothing is left in flight: the delivery at the cap was carried away, not deferred.
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<FileTestBroker>()
        .subscriber("parcels")
        .assert_called(3);
}

/// The same handler on a stream key of its own, mounted by a bare name rather than a descriptor.
#[subscriber("pallets")]
async fn never_ready_by_name(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A registration mounted by a bare stream key declares its retries to the broker directly, with
/// no descriptor in between. A stream file addresses its own copies, so the declaration is
/// accepted and the runtime applies it: the cap counts the same deliveries and the spent one goes
/// to the same stream key.
#[tokio::test(start_paused = true)]
async fn a_bare_stream_key_carries_its_declaration_to_the_broker() {
    let app =
        RustStream::new(AppInfo::new("capped", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(never_ready_by_name)
                .max_attempts(nonzero!(2u32))
                .dead_letter("pallets.dead");
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 11 })
        .to("pallets")
        .publish()
        .await
        .expect("publish");

    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.broker::<FileTestBroker>()
        .subscriber("pallets")
        .assert_called(2);
    tb.broker::<FileTestBroker>()
        .published::<Order>("pallets.dead")
        .assert_called_once()
        .with(&Order { id: 11 });
}

/// The same handler on a stream key of its own: a cap with no destination beside it ends the
/// circulation by rejecting the spent delivery, and writes it nowhere.
#[subscriber(FileStream::new("crates"))]
async fn never_ready_uncollected(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[tokio::test(start_paused = true)]
async fn a_cap_without_a_destination_stops_the_copies_and_writes_nothing() {
    let app =
        RustStream::new(AppInfo::new("capped", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(never_ready_uncollected)
                .max_attempts(nonzero!(2u32));
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 8 })
        .to("crates")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<FileTestBroker>()
        .subscriber("crates")
        .assert_called(2);
    // The stream key carries the injected message and the one copy that brought the second
    // delivery, and nothing else: the spent delivery was rejected rather than carried anywhere.
    tb.broker::<FileTestBroker>()
        .published::<Order>("crates")
        .assert_called(2);
}

/// Stamps the deferred copy with the subscription the delivery came from. A transform on the
/// retry position reads the delivery being retried, the way a reply's does, and takes the options
/// position every publish transform takes. Neither transport has a per-message setting, so this
/// one stays generic over them and mounts on any publisher.
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-deferred-from", cx.name().to_owned());
    }
}

/// Defers the first delivery the way `reconcile` does, on a stream key of its own.
#[subscriber(FileStream::new("invoices"))]
async fn settle(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    assert_eq!(order.id, 2);
    if attempt(ctx) == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The deferred copy travels the retry position's pipeline, so what a transform puts on it is on
/// the message the subscription reads back. Naming the publisher is what `out_retry` is for once
/// the descriptor already addresses the copies.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    // --8<-- [start:out_retry]
    let app = RustStream::new(AppInfo::new("redelivery-stamp", "0.1.0")).with_broker(
        FileTestBroker::new(),
        |b| {
            b.include(settle)
                .out_retry(Publish)
                .transform(DeferredStamp);
        },
    );
    // --8<-- [end:out_retry]
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker::<FileTestBroker>()
        .message(&Order { id: 2 })
        .to("invoices")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("settle");

    tb.broker::<FileTestBroker>()
        .subscriber("invoices")
        .assert_called(2);
    tb.broker::<FileTestBroker>()
        .published::<Order>("invoices")
        .with_header("x-deferred-from", "invoices");
}

/// A publish written by the broker reaches a subscription opened under the same stream key. This
/// is the promise the descriptor's answer makes, read directly off the transport, and it holds in
/// both reading modes: a replay reads the same key, and a copy appended while it is still short
/// of the end of the retained region is one it reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reported_address_is_the_stream_key_in_both_reading_modes() {
    let connected = FileTestBroker::new()
        .connect()
        .await
        .expect("transport connects");
    assert_eq!(
        RedeliveryAddressed::redelivery_address(&FileStream::new("orders"), &connected)
            .await
            .expect("reporting an address must not fail"),
        RedeliveryAddress::new("orders"),
    );
    assert_eq!(
        RedeliveryAddressed::redelivery_address(&FileStream::new("orders").replay(), &connected)
            .await
            .expect("reporting an address must not fail"),
        RedeliveryAddress::new("orders"),
    );
    connected.shutdown().await.expect("shutdown succeeds");
}

/// The stdio stand, which answers about retry copies the way a pipe answers: the mount site names
/// the destination, and a registration that names none does not start.
mod stdio_stand {
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;
    use ruststream_sea_file::testing::StdioTestBroker;
    use serde::{Deserialize, Serialize};

    use super::RETRY_DELAY;

    #[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
    struct Job {
        id: u64,
    }

    /// Asks for a pause before another attempt, which on a pipe means a copy sent downstream.
    #[subscriber("jobs")]
    async fn work(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::retry_after(RETRY_DELAY)
    }

    /// The refusal a real pipeline stage gets, under the harness: the stand declares the copy path
    /// a pipe has, so the runtime raises the same error here, naming the subscription and the step
    /// that fixes it.
    #[tokio::test]
    async fn a_mount_without_a_destination_refuses_to_start() {
        let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
            StdioTestBroker::new(),
            |b| {
                b.include(work);
            },
        );

        let failed = TestApp::start(app)
            .await
            .expect_err("a registration whose copies go nowhere must not start");
        let message = failed.to_string();
        assert!(message.contains("jobs"), "{message}");
        assert!(message.contains("NamedCopies"), "{message}");
        assert!(message.contains(".to("), "{message}");
    }

    /// With the destination named, the copy leaves through the publisher and the stand records it
    /// under that stream key - what a service writes to its standard output, read back. It does
    /// not come back to the subscription: the next process in the pipeline is the one that reads
    /// it.
    #[tokio::test(start_paused = true)]
    async fn a_named_destination_records_the_copy_the_service_wrote() {
        let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
            StdioTestBroker::new(),
            |b| {
                b.include(work).out_retry(Publish).to("jobs.retry");
            },
        );
        let tb = TestApp::start(app).await.expect("startup failed");

        tb.broker::<StdioTestBroker>()
            .message(&Job { id: 3 })
            .to("jobs")
            .publish()
            .await
            .expect("publish");
        tb.advance(RETRY_DELAY + Duration::from_millis(1))
            .await
            .expect("settle");

        tb.broker::<StdioTestBroker>()
            .subscriber("jobs")
            .assert_called_once();
        tb.broker::<StdioTestBroker>()
            .published::<Job>("jobs.retry")
            .assert_called_once()
            .with(&Job { id: 3 });
    }
}

/// The copy path each transport declares, held to by the compiler rather than by a comment.
///
/// A stream key is both ends of the file, so a registration on one needs nothing from its mount
/// site. Standard output reaches the next process in the pipeline and never this one's standard
/// input, so a stdio registration names the destination itself, and the runtime refuses to start
/// one that names none. Naming the types here attaches no transport: stdio's own round trip is
/// covered over a real pipe in `integration_sea.rs`, which owns the one stdio transport a test
/// binary may attach. The stand answers the same, so the refusal is reproducible under the
/// harness.
#[test]
fn each_transport_declares_who_addresses_its_retry_copies() {
    const fn addresses_its_copies<C: Subscribe<Copies = AddressedCopies>>() {}
    const fn names_its_destination<C: Subscribe<Copies = NamedCopies>>() {}

    addresses_its_copies::<ConnectedFileBroker>();
    addresses_its_copies::<ConnectedFileTestBroker>();
    names_its_destination::<ConnectedStdioBroker>();
    // The stand answers what the pipe answers, so a registration refused in production is refused
    // under the harness rather than passing a test it has no right to pass.
    names_its_destination::<ruststream_sea_file::testing::ConnectedStdioTestBroker>();
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
