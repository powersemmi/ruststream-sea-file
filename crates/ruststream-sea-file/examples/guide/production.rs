//! The service of the Running in production guide: a checkout on the shop's stream file, at the
//! path the deployment names.
//!
//! ```text
//! ORDERS_FILE=/tmp/orders.ss cargo run --example guide_production --features testing -- run
//! cargo test --example guide_production --features testing
//! ```

use std::env;

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// The shop's stream file, at the path the deployment names.
fn broker() -> FileBroker {
    let path = env::var("ORDERS_FILE").unwrap_or_else(|_| "/var/lib/shop/orders.ss".to_owned());
    // A beacon every 256 KiB: a seek reads at most that far from the nearest one.
    FileBroker::new(path).beacon_interval(256 * 1024)
}
// --8<-- [end:broker]

// --8<-- [start:service]
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

#[subscriber(FileStream::new("orders"), reply)]
async fn checkout(order: &Order) -> Receipt {
    tracing::info!(order.id, "order confirmed");
    Receipt { order: order.id }
}

fn service(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(file, |b| {
        b.include(checkout);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Receipt, app, service};

    #[tokio::test]
    async fn the_production_settings_start_and_confirm_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_beacon_interval_off_the_1024_byte_grid_stops_the_start() {
        let file = FileBroker::new("/var/lib/shop/orders.ss").beacon_interval(1000);

        let refused = TestApp::start(service(file))
            .await
            .expect_err("1000 is no multiple of 1024");

        // The broker's refusal is the cause of the start's error.
        let cause = format!("{refused:?}");
        assert!(cause.contains("beacon interval"), "{cause}");
    }
}
