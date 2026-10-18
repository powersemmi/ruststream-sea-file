//! The service of `examples/sea_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_sea_broker_shutdown --features testing -- run
//! cargo test --example manual_sea_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::{fs, io};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../../tests/common/mod.rs"]
mod common;

struct Record;

impl Handle<Order, Receipt> for Record {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

// --8<-- [start:hooks]
/// Moves the finished stream file aside, so the next run starts a new one.
fn archive(path: &str) -> io::Result<()> {
    fs::rename(path, format!("{path}.done"))
}

fn service(path: &str) -> impl App<State = ()> {
    let finished = path.to_owned();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Runs after the broker has shut down: the file is synced and closed.
        .after_shutdown(async move |_state| archive(&finished))
        .with_broker(FileBroker::new(path), |b| {
            b.include(
                subscriber(FileStream::new("orders"), Record)
                    .reply()
                    .build(),
            );
        })
}
// --8<-- [end:hooks]

fn app() -> impl App<State = ()> {
    service("/var/lib/orders/orders.ss")
}

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

    /// The body both modes run: the receipt is written, and the shutdown succeeds.
    async fn records_then_shuts_down(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
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
        records_then_shuts_down(TestApp::start(app()).await?).await
    }

    #[test]
    fn the_closed_file_is_moved_aside() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("manual-shutdown");
            records_then_shuts_down(TestApp::start_live(service(&path)).await?).await?;

            // The service's own `after_shutdown` hook has moved the file aside.
            let done = format!("{path}.done");
            assert!(!Path::new(&path).exists(), "{path} is still in place");
            // The moved file is a whole stream file: a reader opens it.
            let reader = RustStream::new(AppInfo::new("reader", "0.1.0"))
                .register_broker(FileBroker::new(&done).existing_only());
            TestApp::start_live(reader).await?.shutdown().await?;

            fs::remove_file(&done)?;
            Ok(())
        })
    }
}
