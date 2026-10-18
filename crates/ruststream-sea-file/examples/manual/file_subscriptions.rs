//! The service of `examples/file_subscriptions.rs` written without the macros: a stream key in
//! `subscriber(..)` reads what is appended under that key while the service runs.
//!
//! ```text
//! cargo test --example manual_file_subscriptions --features testing
//! cargo run --example manual_file_subscriptions --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:bare]
/// Ships every order appended under the `orders` key while the service runs.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "shipping");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(subscriber("orders", Ship).build());
        },
    )
}
// --8<-- [end:bare]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Ship, app};

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
                b.include(subscriber("orders/eu", Ship).build());
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("a stream key holds letters, digits, `.`, `_` and `-` only");
        assert!(refused.to_string().contains("orders/eu"), "{refused}");
    }
}
