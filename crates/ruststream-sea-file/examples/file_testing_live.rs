//! One test body, two modes: in process under `TestApp::start`, and against a real stream file
//! under `TestApp::start_live`. The file is a temporary one, so no stand is needed.
//!
//! ```text
//! cargo test --example file_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

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

/// Answers each order with a receipt.
#[subscriber(FileStream::new("orders"), reply)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

/// The service on the stream file at `path`.
fn app(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(FileBroker::new(path), |b| {
        b.include(confirm);
    })
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("/var/lib/shop/orders.ss")
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Receipt, app};

    // --8<-- [start:live]
    /// The body both modes run; only the start call differs.
    async fn confirms_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        confirms_an_order(TestApp::start(app("/var/lib/shop/orders.ss")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_a_stream_file() -> Result<(), Box<dyn Error>> {
        // A file of this run's own, so nothing an earlier run appended is read.
        let path = env::temp_dir().join(format!("ruststream-sea-file-live-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();

        confirms_an_order(TestApp::start_live(app(&path)).await?).await?;
        fs::remove_file(&path)?;
        Ok(())
    }
    // --8<-- [end:live]
}
