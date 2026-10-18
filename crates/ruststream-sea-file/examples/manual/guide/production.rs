//! The service of the Running in production guide written without the macros: the handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand. The broker is the same code in both forms.
//!
//! ```text
//! ORDERS_FILE=/tmp/orders.ss cargo run --example manual_guide_production --features testing -- run
//! cargo test --example manual_guide_production --features testing
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
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
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: u64,
}

struct Checkout;

impl Handle<Order, Receipt> for Checkout {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Receipt { order: order.id }))
    }
}

fn service(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(file, |b| {
        b.include(
            subscriber(FileStream::new("orders"), Checkout)
                .reply()
                .build(),
        );
    })
}

fn app() -> impl App<State = ()> {
    service(broker())
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

// `#[outgoing(name = ..)]` by hand: each type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

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
