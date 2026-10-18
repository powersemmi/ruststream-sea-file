//! The pipeline stage of the Getting started guide: the same handler on standard input and output
//! instead of a stream file.
//!
//! ```text
//! echo '[orders] {"id":1,"amount_cents":1250}' \
//!     | cargo run --example guide_getting_started_pipe --features testing -- run
//! cargo test --example guide_getting_started_pipe --features testing
//! ```

// --8<-- [start:service]
use ruststream_sea_file::stdio::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    if order.amount_cents == 0 {
        tracing::warn!(order.id, "order without an amount dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(order.id, order.amount_cents, "order taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(take_order).out_retry(Publish).to("orders.retry");
    })
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;
    use serde::Serialize;

    use super::{Order, app, take_order};

    /// An order as the stage before this one writes it to its standard output.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    #[tokio::test]
    async fn an_order_line_is_taken() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<StdioBroker>()
            .message(&Placed {
                id: 1,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order {
                id: 1,
                amount_cents: 1250,
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// A stage names where a deferred copy goes, or it does not start: its standard output
    /// reaches the next process, never its own input.
    #[tokio::test]
    async fn a_stage_that_names_no_retry_destination_does_not_start() {
        let stage =
            RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(StdioBroker::new(), |b| {
                b.include(take_order);
            });

        let refused = TestApp::start(stage)
            .await
            .expect_err("a stage with nowhere to put its copies must not start");

        assert!(refused.to_string().contains("orders"), "{refused}");
    }
}
