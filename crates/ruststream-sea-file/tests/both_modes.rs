//! One test body, run twice: with the file broker in process, and against a real stream file.
//!
//! The app is the one `main` runs, handed to the harness unchanged; only the start call differs.
//! In process the delay of a deferred copy passes on the paused clock. Against the file it passes
//! in real time, and the copy is appended to the file and read back by the live subscription.

#![cfg(feature = "testing")]

mod common;

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// Long enough to tell a deferred copy from an immediate one, short enough for a live run.
const WAITED: Duration = Duration::from_millis(200);

/// The file the in-process run is built with; nothing is opened at it.
const PATH: &str = "/var/lib/orders/orders.ss";

#[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "checkout")]
struct Checkout {
    order: u64,
}

#[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// Asks for the first delivery back after [`WAITED`], and accepts the copy, which carries the
/// framework's retry count.
#[subscriber(FileStream::new("orders"))]
async fn defer_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(WAITED);
    }
    HandlerOutcome::ack()
}

/// Confirms an order on the stream key its reply type declares.
#[subscriber(FileStream::new("checkout"), publish)]
async fn confirm(checkout: &Checkout) -> Receipt {
    Receipt {
        order: checkout.order,
    }
}

/// The app `main` runs, on the file it is handed.
fn app(path: &str) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(FileBroker::new(path), |b| {
        b.include(defer_once);
        b.include(confirm);
    })
}

/// The body both modes run: a deferred copy comes back after the delay and is accepted, and a
/// reply lands on the stream key its type declares.
async fn a_deferred_copy_comes_back_and_a_reply_lands(tb: TestApp<()>) {
    tb.broker::<FileBroker>()
        .message(&Order { id: 7 })
        .publish()
        .await
        .expect("publish drives the first delivery to its settlement");
    tb.broker::<FileBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(WAITED));

    tb.advance(WAITED).await.expect("the deferred copy arrives");

    tb.broker::<FileBroker>()
        .subscriber("orders")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    // The stream key holds the order and the copy the runtime appended for it.
    tb.broker::<FileBroker>()
        .published::<Order>("orders")
        .assert_called(2)
        .with(&Order { id: 7 });

    tb.broker::<FileBroker>()
        .message(&Checkout { order: 8 })
        .publish()
        .await
        .expect("publish drives the reply");
    tb.broker::<FileBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order: 8 });

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_deferred_copy_comes_back_and_a_reply_lands_in_process() {
    let tb = TestApp::start(app(PATH)).await.expect("start");
    a_deferred_copy_comes_back_and_a_reply_lands(tb).await;
}

#[test]
fn a_deferred_copy_comes_back_and_a_reply_lands_on_a_stream_file() {
    common::on_a_file(async {
        let path = common::tmp_path("both-modes");
        let tb = TestApp::start_live(app(&path))
            .await
            .expect("start against the file");
        a_deferred_copy_comes_back_and_a_reply_lands(tb).await;
        let _ = std::fs::remove_file(&path);
    });
}
