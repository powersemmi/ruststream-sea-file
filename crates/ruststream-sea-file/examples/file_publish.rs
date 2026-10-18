//! A publish on a stream file: the message is appended under its stream key and flushed to the
//! file before the call returns, so a reader of that key sees it.
//!
//! ```text
//! cargo test --example file_publish --features testing
//! cargo run --example file_publish --features testing -- run
//! ```

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Ships every order appended under `orders`.
#[subscriber("orders")]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "shipping");
    HandlerOutcome::ack()
}

// --8<-- [start:recorder]
/// The record a service leaves in the file each time it starts.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "lifecycle")]
struct Started {
    service: String,
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(ship);
            // Runs once the subscriptions are open; a refused append fails the startup.
            b.after_startup(Publish, async move |file| {
                file.message(&Started {
                    service: "shipping".to_owned(),
                })
                .publish()
                .await
            });
        },
    )
}
// --8<-- [end:recorder]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Started, app};

    #[tokio::test]
    async fn the_service_records_its_start_in_the_file() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .published::<Started>("lifecycle")
            .assert_called_once()
            .with(&Started {
                service: "shipping".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
