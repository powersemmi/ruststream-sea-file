//! The service of `examples/file_start_at.rs` written without the macros: `.start_at(position)`
//! in the mount chain takes the position the service stored.
//!
//! ```text
//! cargo test --example manual_file_start_at --features testing
//! cargo run --example manual_file_start_at --features testing -- run
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

// --8<-- [start:resume]
/// Invoices each order once, across restarts.
struct Invoice;

impl Handle<Order> for Invoice {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "invoiced");
        ready(Ok(()))
    }
}

/// The service, opened at the first sequence the previous run did not invoice.
fn app(file: FileBroker, resume_at: u64) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(file, |b| {
        b.include(
            subscriber(FileStream::new("orders"), Invoice)
                .start_at(FilePosition::sequence(resume_at))
                .build(),
        );
    })
}
// --8<-- [end:resume]

/// Where the previous run stopped; a service keeps it in its own store.
const CHECKPOINT: u64 = 1;

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/shop/orders.ss"), CHECKPOINT))
}

#[cfg(test)]
#[path = "../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Order, app};

    fn invoiced(tb: &TestApp<()>) -> Vec<u64> {
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect()
    }

    #[tokio::test]
    async fn the_service_resumes_at_the_stored_sequence() -> Result<(), Box<dyn Error>> {
        // Five orders; the previous run invoiced the first three.
        let file = FileBroker::new("/var/lib/shop/orders.ss");
        record(&file, (1..=5).map(|id| Order { id })).await?;
        let tb = TestApp::start(app(file, 4)).await?;
        tb.settle().await?;

        assert_eq!(invoiced(&tb), [4, 5]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_sequence_past_the_next_one_stops_the_service_at_startup()
    -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new("/var/lib/shop/orders.ss");
        record(&file, (1..=2).map(|id| Order { id })).await?;

        let refused = TestApp::start(app(file, 9))
            .await
            .expect_err("the file writes sequence 3 next, so 9 is too far ahead");

        assert!(refused.to_string().contains("sequence"), "{refused}");
        Ok(())
    }

    /// Two runs of the service on one stream file: the second resumes where the first stopped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_resumes_on_the_file_at_the_stored_sequence() -> Result<(), Box<dyn Error>> {
        let path =
            env::temp_dir().join(format!("ruststream-sea-file-start-at-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();

        // The shop appends three orders while the service is down; the file numbers them 1 to 3.
        let shop = RustStream::new(AppInfo::new("shop", "0.1.0"))
            .with_broker(FileBroker::new(&path), |_| {});
        let shop = TestApp::start_live(shop).await?;
        for id in 1..=3 {
            shop.broker::<FileBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }
        shop.shutdown().await?;

        // The service invoiced order 1 before it stopped, so it comes back at sequence 2.
        let tb = TestApp::start_live(app(FileBroker::new(&path), 2)).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 4 })
            .publish()
            .await?;
        // A live publish returns once the subscription has handled as many orders as the test
        // published, so the first of them is what decides.
        assert_eq!(invoiced(&tb).first(), Some(&2));
        tb.shutdown().await?;

        fs::remove_file(&path)?;
        Ok(())
    }
}
