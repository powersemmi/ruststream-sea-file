//! `FileBroker::new`: the service names its stream file, and startup creates the file when it is
//! missing. The in-process test runs on a log in memory; the live one opens a real file in the
//! temp directory.
//!
//! ```text
//! cargo run --example file_broker --features testing -- run
//! cargo test --example file_broker --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[subscriber(FileStream::new("orders"), reply)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

// --8<-- [start:broker]
/// The service on the stream file at `path`.
fn service(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(FileBroker::new(path), |b| {
        b.include(confirm);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service("/var/lib/orders/orders.ss")
}
// --8<-- [end:broker]

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
            let path = common::tmp_path("file-broker");
            assert!(!Path::new(&path).exists(), "the run starts with no file");

            let tb = TestApp::start_live(service(&path)).await?;
            assert!(Path::new(&path).exists(), "startup created {path}");
            confirms_an_order(tb).await?;

            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
