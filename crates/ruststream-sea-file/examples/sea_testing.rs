//! Testing a service in process: the production app, its broker included, run by `TestApp`
//! with no stream file and no pipe.
//!
//! ```text
//! cargo test --example sea_testing --features testing
//! cargo run --example sea_testing --features testing -- run
//! ```

// A plain build runs the file service; the stage is exercised by the tests only.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream_sea_file::prelude::*;
use serde::{Deserialize, Serialize};

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

/// Answers each order with a receipt.
#[subscriber(FileStream::new("orders"), reply)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

/// The service on a stream file: the app `main` runs, and the one the tests hand the harness.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(confirm);
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    job: u64,
}

/// Answers each `jobs` line with a `results` line.
#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    Done { job: job.id }
}

/// The same kind of service as a pipeline stage.
fn stage() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(work).out_retry(StdioPublish).to("jobs.retry");
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::prelude::*;

    use super::{Done, Job, Order, Receipt, app, stage};

    // --8<-- [start:file]
    #[tokio::test]
    async fn an_order_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:file]

    // --8<-- [start:stdio]
    #[tokio::test]
    async fn a_job_line_gets_a_result_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(stage()).await?;

        // A line on the stage's standard input.
        tb.broker::<StdioBroker>()
            .message(&Job { id: 7 })
            .to("jobs")
            .publish()
            .await?;

        // A line the stage wrote to its standard output.
        tb.broker::<StdioBroker>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { job: 7 });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:stdio]
}
