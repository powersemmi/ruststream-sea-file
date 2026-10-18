//! The service of `examples/file_broker.rs` without the macros: the handler is a named type and
//! `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_file_broker --features testing -- run
//! cargo test --example manual_file_broker --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../../tests/common/mod.rs"]
mod common;

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

// --8<-- [start:broker]
/// The service on the stream file at `path`.
fn service(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(FileBroker::new(path), |b| {
        b.include(
            subscriber(FileStream::new("orders"), Confirm)
                .reply()
                .build(),
        );
    })
}

fn app() -> impl App<State = ()> {
    service("/var/lib/orders/orders.ss")
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: u64,
}

// `#[outgoing(name = "receipts")]` by hand.
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
    use std::fs;
    use std::path::Path;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Receipt, app, common, service};

    /// The body both modes run: the order is read and its receipt appended under `receipts`.
    async fn confirms_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        confirms_an_order(TestApp::start(app()).await?).await
    }

    #[test]
    fn startup_creates_the_missing_file() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("manual-file-broker");
            assert!(!Path::new(&path).exists(), "the run starts with no file");

            let tb = TestApp::start_live(service(&path)).await?;
            assert!(Path::new(&path).exists(), "startup created {path}");
            confirms_an_order(tb).await?;

            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
