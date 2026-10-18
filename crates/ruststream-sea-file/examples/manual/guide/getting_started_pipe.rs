//! The pipeline stage of the Getting started guide written without the macros: the same handler
//! type, on standard input and output instead of a stream file.
//!
//! ```text
//! echo '[orders] {"id":1,"amount_cents":1250}' \
//!     | cargo run --example manual_guide_getting_started_pipe --features testing -- run
//! cargo test --example manual_guide_getting_started_pipe --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:service]
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

struct TakeOrder;

impl Handle<Order> for TakeOrder {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if order.amount_cents == 0 {
            tracing::warn!(order.id, "order without an amount dropped");
            return Err(HandlerOutcome::drop());
        }
        tracing::info!(order.id, order.amount_cents, "order taken");
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("orders", TakeOrder).build())
            .out_retry(Publish)
            .to("orders.retry");
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

// `#[outgoing(name = "orders")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Order, TakeOrder, app};

    #[tokio::test]
    async fn an_order_line_is_taken() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 1,
            amount_cents: 1250,
        };

        tb.broker::<StdioBroker>().message(&order).publish().await?;

        tb.broker::<StdioBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&order)
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
                b.include(subscriber("orders", TakeOrder).build());
            });

        let refused = TestApp::start(stage)
            .await
            .expect_err("a stage with nowhere to put its copies must not start");

        assert!(refused.to_string().contains("orders"), "{refused}");
    }
}
