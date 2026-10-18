//! Shutdown: once the broker has flushed and closed the stream file, an `after_shutdown` hook
//! moves the finished file aside. The live test runs on a file in the temp directory and runs the
//! hook's function after the harness shut the broker down.
//!
//! ```text
//! cargo run --example sea_broker_shutdown --features testing -- run
//! cargo test --example sea_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::{fs, io};

use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[subscriber(FileStream::new("orders"), reply)]
async fn record(order: &Order) -> Receipt {
    Receipt { order: order.id }
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
            b.include(record);
        })
}
// --8<-- [end:hooks]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service("/var/lib/orders/orders.ss")
}

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
            let path = common::tmp_path("shutdown");
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
