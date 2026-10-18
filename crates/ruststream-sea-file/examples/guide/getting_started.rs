//! The first service of the Getting started guide: one handler on the `orders` stream key of a
//! stream file, read from the start of the file.
//!
//! ```text
//! cargo run --example guide_getting_started --features testing -- run
//! cargo test --example guide_getting_started --features testing
//! ```

// --8<-- [start:service]
use ruststream_sea_file::file::prelude::*;
use serde::Deserialize;

#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
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
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/tmp/orders.ss"),
        |b| {
            b.include(take_order);
        },
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;
    use serde::Serialize;

    use super::{Order, app};

    /// An order as the shop that places it appends it to the file.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    #[tokio::test]
    async fn an_order_with_an_amount_is_taken() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Placed {
                id: 1,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
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

    #[tokio::test]
    async fn an_order_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Placed {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
