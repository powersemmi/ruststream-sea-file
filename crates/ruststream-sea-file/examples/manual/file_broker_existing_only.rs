//! The service of `examples/file_broker_existing_only.rs` without the macros: the handler is a
//! named type and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_file_broker_existing_only --features testing -- run
//! cargo test --example manual_file_broker_existing_only --features testing
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

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order audited");
        ready(Ok(()))
    }
}

// --8<-- [start:reader]
/// A reader of the file the order service writes: it never creates the file itself.
fn reader(path: &str) -> impl App<State = ()> {
    let broker = FileBroker::new(path).existing_only();
    RustStream::new(AppInfo::new("auditor", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(FileStream::new("orders"), Audit).build());
    })
}

fn app() -> impl App<State = ()> {
    reader("/var/lib/orders/orders.ss")
}
// --8<-- [end:reader]

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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::path::Path;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::SeaFileError;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, app, common, reader};

    /// The order service that writes the file: here it only creates it.
    fn writer(path: &str) -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).register_broker(FileBroker::new(path))
    }

    /// Whether `err` or one of its sources is a `SeaFileError::Connect`.
    fn is_connect_error(err: &(dyn Error + 'static)) -> bool {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if matches!(
                err.downcast_ref::<SeaFileError>(),
                Some(SeaFileError::Connect { .. })
            ) {
                return true;
            }
            cause = err.source();
        }
        false
    }

    /// In process the file in memory stands for the one the operator provisioned.
    #[tokio::test]
    async fn in_process_the_file_is_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[test]
    fn a_missing_file_fails_startup() {
        common::rt().block_on(async {
            let path = common::tmp_path("manual-existing-only-missing");

            let Err(failed) = TestApp::start_live(reader(&path)).await else {
                panic!("a reader of a missing file must not start");
            };
            assert!(is_connect_error(&failed), "{failed:?}");
            assert!(!Path::new(&path).exists(), "the reader created {path}");
        });
    }

    #[test]
    fn a_file_another_service_created_opens() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("manual-existing-only-present");
            TestApp::start_live(writer(&path)).await?.shutdown().await?;

            let tb = TestApp::start_live(reader(&path)).await?;
            tb.broker::<FileBroker>()
                .message(&Order { id: 8 })
                .publish()
                .await?;
            tb.broker::<FileBroker>()
                .subscriber("orders")
                .assert_called_once()
                .with(&Order { id: 8 });

            tb.shutdown().await?;
            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
