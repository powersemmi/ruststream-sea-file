//! Every subscription form this crate ships resolves against the in-process broker.
//!
//! A service names its subscription once, in the declaration that ships. If that same
//! declaration did not resolve against [`FileTestBroker`], a test would have to name the
//! subscription differently from production and would cover a wiring nobody runs. So each form
//! is mounted here exactly as a service writes it - the file form's descriptor, its replay mode,
//! and the bare stream key a mount site without a descriptor writes - and driven through the
//! `TestApp` harness.
//!
//! What the stand-in does with each form is asserted rather than assumed: the descriptor's two
//! reading modes are told apart on one run, so the replay mode landing at the start of the
//! retained log is checked, not just documented.

#![cfg(feature = "testing")]

use std::future::{Future, ready};

use ruststream::testing::TestApp;
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::testing::FileTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u64,
}

/// The confirming handler's reply, which the declaration's `publish` clause carries to
/// `receipts` - how a run of that handler becomes observable to a test.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Receipt {
    id: u64,
}

/// The file form as a service writes it: the transport's own descriptor, and a reply bound to a
/// destination at the declaration.
#[subscriber(FileStream::new("orders"), publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The replay mode of the same descriptor: reading opens at the start of what is retained.
#[subscriber(FileStream::new("recorded").replay())]
async fn replayed(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// The plain descriptor over its own stream, to tell the two reading modes apart in one run: it
/// follows the tail, so what was recorded before it opened is not its.
#[subscriber(FileStream::new("tailed"))]
async fn tailed(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// The bare stream key: what a mount site writes when the form has no descriptor of its own (the
/// stdio form), and what the file form's `Subscribe` capability resolves.
#[subscriber("audit")]
async fn audited(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// The manual path's body for the same descriptor: one source value, mounted through
/// `subscriber(..)` instead of the decorator.
struct Archive;

impl Handle<Order> for Archive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
        let _ = order;
        ready(Ok(()))
    }
}

/// Appends `orders` to the broker's retained log before any subscription opens, the way a
/// producer that ran earlier would have left them in a stream file.
async fn record(
    broker: &FileTestBroker,
    stream: &str,
    orders: impl IntoIterator<Item = Order>,
) -> Result<(), Box<dyn std::error::Error>> {
    let publisher = broker.publisher();
    for order in orders {
        publisher.message(&order).to(stream).publish().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_descriptor_form_mounts_on_the_in_process_broker()
-> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("sources", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(confirm);
        });
    let tb = TestApp::start(app).await?;

    tb.message(&Order { id: 1 }).to("orders").publish().await?;

    tb.broker::<FileTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(
        tb.broker::<FileTestBroker>()
            .published::<Receipt>("receipts")
            .decoded(),
        vec![Receipt { id: 1 }],
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replay_mode_opens_at_the_start_of_the_retained_log()
-> Result<(), Box<dyn std::error::Error>> {
    // Two streams recorded before the service exists, one per reading mode.
    let broker = FileTestBroker::new();
    let run = || (1..=3).map(|id| Order { id });
    record(&broker, "recorded", run()).await?;
    record(&broker, "tailed", run()).await?;

    let app = RustStream::new(AppInfo::new("sources", "0.1.0")).with_broker(broker, |b| {
        b.include(replayed);
        b.include(tailed);
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    tb.broker::<FileTestBroker>()
        .subscriber("recorded")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
    // The plain descriptor follows the tail, so the same recorded run is not delivered to it.
    tb.broker::<FileTestBroker>()
        .subscriber("tailed")
        .assert_called(0);

    // What it does follow is everything published from here on.
    tb.message(&Order { id: 4 }).to("tailed").publish().await?;
    tb.broker::<FileTestBroker>()
        .subscriber("tailed")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_stream_key_mounts_on_the_in_process_broker()
-> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("sources", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(audited);
        });
    let tb = TestApp::start(app).await?;

    tb.message(&Order { id: 1 }).to("audit").publish().await?;

    tb.broker::<FileTestBroker>()
        .subscriber("audit")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_descriptor_mounts_through_the_manual_path_too()
-> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("sources", "0.1.0")).with_broker(FileTestBroker::new(), |b| {
            b.include(subscriber(FileStream::new("archive"), Archive).build());
        });
    let tb = TestApp::start(app).await?;

    tb.message(&Order { id: 1 }).to("archive").publish().await?;

    tb.broker::<FileTestBroker>()
        .subscriber("archive")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    Ok(())
}
