//! The service and the stage of `examples/sea_testing.rs` written without the macros, under the
//! same tests: `TestApp` drives a manual registration exactly as it drives a macro one.
//!
//! ```text
//! cargo test --example manual_sea_testing --features testing
//! cargo run --example manual_sea_testing --features testing -- run
//! ```

// A plain build runs the file service; the stage is exercised by the tests only.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// Answers each order with a receipt.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

/// The service on a stream file: the app `main` runs, and the one the tests hand the harness.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(
                subscriber(FileStream::new("orders"), Confirm)
                    .reply()
                    .build(),
            );
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "results")]
struct Done {
    job: u64,
}

/// Answers each `jobs` line with a `results` line.
struct Work;

impl Handle<Job, Done> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        ready(Ok(Done { job: job.id }))
    }
}

/// The same kind of service as a pipeline stage.
fn stage() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("jobs", Work).reply().build())
            .out_retry(StdioPublish)
            .to("jobs.retry");
    })
}

fn main() -> ExitCode {
    run_main(app)
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
