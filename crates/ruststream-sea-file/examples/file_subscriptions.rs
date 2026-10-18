//! A bare stream key in `#[subscriber]`: the subscription reads what is appended under that key
//! in the stream file from the moment the service starts.
//!
//! ```text
//! cargo test --example file_subscriptions --features testing
//! cargo run --example file_subscriptions --features testing -- run
//! ```

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:bare]
/// Ships every order appended under the `orders` key while the service runs.
#[subscriber("orders")]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "shipping");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(ship);
        },
    )
}
// --8<-- [end:bare]

/// A stream key with a character the file does not take.
#[subscriber("orders/eu")]
async fn ship_eu(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "shipping");
    HandlerOutcome::ack()
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, app, ship_eu};

    #[tokio::test]
    async fn an_order_appended_under_the_key_is_shipped() -> Result<(), Box<dyn Error>> {
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
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_key_the_file_does_not_take_stops_the_service_at_startup() {
        let app = RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
            FileBroker::new("/var/lib/shop/orders.ss"),
            |b| {
                b.include(ship_eu);
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("a stream key holds letters, digits, `.`, `_` and `-` only");
        assert!(refused.to_string().contains("orders/eu"), "{refused}");
    }
}
