//! The event log of the guide of the same name: a shop that records a receipt for each order in
//! its stream file, a follower of the receipts, and an audit job that totals a finished file in
//! one pass.
//!
//! ```text
//! cargo run --example guide_event_log --features testing -- run
//! cargo test --example guide_event_log --features testing
//! ```

// A plain build runs the shop; the audit job is a program of its own, which the tests start.
#![cfg_attr(not(test), allow(dead_code))]

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:record]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
    amount_cents: u64,
    /// Where the order sits under `orders`, so a reader can move back to it.
    sequence: u64,
}

/// Records a receipt for each order, appended to the same file under `receipts`.
#[subscriber(FileStream::new("orders"), reply)]
async fn checkout(order: &Order, Ctx(at): Ctx<Position>) -> Receipt {
    let sequence = match at {
        FilePosition::Sequence(sequence) => sequence,
        // A delivery always sits at a sequence; the other forms name places to start from.
        FilePosition::Beginning | FilePosition::End | FilePosition::Timestamp(_) => 0,
    };
    Receipt {
        order: order.id,
        amount_cents: order.amount_cents,
        sequence,
    }
}

/// Follows the receipts as the shop writes them.
#[subscriber(FileStream::new("receipts"))]
async fn notify(receipt: &Receipt) -> HandlerOutcome {
    tracing::info!(receipt.order, "customer notified");
    HandlerOutcome::ack()
}

fn shop(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(file, |b| {
        b.include(checkout);
        b.include(notify);
    })
}

fn main() -> ExitCode {
    run_main(|| shop(FileBroker::new("/var/lib/shop/orders.ss")))
}
// --8<-- [end:record]

// --8<-- [start:audit]
/// Totals the receipts of a finished file, up to 64 at a time; one outcome settles the batch.
#[subscriber(FileStream::new("receipts").replay())]
async fn total(batch: &[Receipt]) -> HandlerOutcome {
    let cents: u64 = batch.iter().map(|receipt| receipt.amount_cents).sum();
    tracing::info!(receipts = batch.len(), cents, "receipts totalled");
    HandlerOutcome::ack()
}

/// The audit job: it reads the shop's file and never creates one.
fn audit(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(file.existing_only(), |b| {
        b.include(total.batch(nonzero!(64)));
    })
}
// --8<-- [end:audit]

#[cfg(test)]
#[path = "../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Order, Receipt, audit, shop};

    const PATH: &str = "/var/lib/shop/orders.ss";

    fn order(id: u64) -> Order {
        Order {
            id,
            amount_cents: 100 * id,
        }
    }

    fn receipt(order: u64, sequence: u64) -> Receipt {
        Receipt {
            order,
            amount_cents: 100 * order,
            sequence,
        }
    }

    #[tokio::test]
    async fn an_order_gets_a_receipt_that_the_follower_reads() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(shop(FileBroker::new(PATH))).await?;

        tb.broker::<FileBroker>().message(&order(1)).publish().await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&receipt(1, 1));
        tb.broker::<FileBroker>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&receipt(1, 1))
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_audit_reads_the_finished_file_once_and_stops() -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new(PATH);
        record(&file, (1..=3).map(|order| receipt(order, order))).await?;
        let tb = TestApp::start(audit(file)).await?;
        tb.settle().await?;

        tb.broker::<FileBroker>()
            .subscriber("receipts")
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());

        // The replay completed at the end of the file: a later receipt is not read.
        tb.broker::<FileBroker>()
            .message(&receipt(4, 4))
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .subscriber("receipts")
            .assert_batch_sizes(&[3]);
        tb.shutdown().await?;
        Ok(())
    }

    /// Two runs of the shop on one stream file: the second numbers its orders on from where the
    /// first left the file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restarted_shop_numbers_its_orders_on_from_the_end_of_the_file()
    -> Result<(), Box<dyn Error>> {
        // A file of this run's own, so nothing an earlier run appended is read.
        let path = env::temp_dir().join(format!("guide-event-log-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();

        let first = TestApp::start_live(shop(FileBroker::new(&path))).await?;
        for id in [1, 2] {
            first
                .broker::<FileBroker>()
                .message(&order(id))
                .publish()
                .await?;
        }
        first
            .broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called(2)
            .with(&receipt(2, 2));
        first.shutdown().await?;

        let second = TestApp::start_live(shop(FileBroker::new(&path))).await?;
        second
            .broker::<FileBroker>()
            .message(&order(3))
            .publish()
            .await?;
        second
            .broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&receipt(3, 3));
        second.shutdown().await?;

        fs::remove_file(&path)?;
        Ok(())
    }
}
