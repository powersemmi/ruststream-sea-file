//! `end_with_eos`: a recorder marks its stream file finished when it shuts down, and a reader
//! tailing the file ends at that mark. The live test runs both on one file in the temp directory.
//!
//! ```text
//! cargo run --example file_broker_eos --features testing -- run
//! cargo test --example file_broker_eos --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[subscriber(FileStream::new("orders"), reply)]
async fn record(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

// --8<-- [start:writer]
/// The recorder: when it shuts down, the file is marked finished.
fn writer(path: &str) -> impl App<State = ()> {
    let broker = FileBroker::new(path).end_with_eos();
    RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    writer("/var/lib/orders/orders.ss")
}
// --8<-- [end:writer]

/// A reader of the recorder's file: it reads the receipts from the start of the file and ends at
/// the mark.
#[subscriber(FileStream::new("receipts"), start_at(FilePosition::beginning()))]
async fn audit(receipt: &Receipt) -> HandlerOutcome {
    tracing::info!(receipt.order, "receipt audited");
    HandlerOutcome::ack()
}

fn reader(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("auditor", "0.1.0")).with_broker(
        FileBroker::new(path).existing_only(),
        |b| {
            b.include(audit);
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::time::Duration;

    use ruststream::runtime::PublishError;
    use ruststream::testing::{TestApp, TestError};
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Receipt, app, common, reader, writer};

    /// How long the reader's harness waits for a receipt it published to be read.
    const READ_WITHIN: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn the_recorder_writes_a_receipt_per_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[test]
    fn a_reader_ends_at_the_mark_a_finished_run_left() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("eos");
            // A recorder run that ends: its shutdown leaves the mark at the end of the file.
            TestApp::start_live(writer(&path)).await?.shutdown().await?;

            // The auditor reads the file from its start and ends at the mark, so a receipt
            // appended after it is never read.
            let auditor = TestApp::start_live_within(reader(&path), READ_WITHIN).await?;
            let unread = auditor
                .broker::<FileBroker>()
                .message(&Receipt { order: 2 })
                .publish()
                .await;
            assert!(
                matches!(
                    unread,
                    Err(PublishError::Publish(TestError::NotSettled { .. }))
                ),
                "a receipt after the mark was read: {unread:?}",
            );
            auditor
                .broker::<FileBroker>()
                .subscriber("receipts")
                .assert_not_called();

            auditor.shutdown().await?;
            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
