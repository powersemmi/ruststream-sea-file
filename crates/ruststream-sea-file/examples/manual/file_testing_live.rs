//! The service of `examples/file_testing_live.rs` written without the macros, under the same
//! two tests: one in process, one on a temporary stream file.
//!
//! ```text
//! cargo test --example manual_file_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// Answers each order with a receipt.
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

/// The service on the stream file at `path`.
fn app(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(FileBroker::new(path), |b| {
        b.include(
            subscriber(FileStream::new("orders"), Confirm)
                .reply()
                .build(),
        );
    })
}

fn main() -> ExitCode {
    run_main(|| app("/var/lib/shop/orders.ss"))
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
