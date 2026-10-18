//! A service over a stream file: it reads the `orders` stream key from the start of the file and
//! needs no server.
//!
//! ```text
//! cargo run --example file_service --features testing -- run
//! cargo test --example file_service --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_sea_file::file::prelude::*;

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order read");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/orders/orders.ss"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_on_the_stream_key_is_read_and_acknowledged() -> Result<(), Box<dyn Error>> {
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
}
