//! A replay: the subscription reads every message the file holds under its key, from the start,
//! and completes at the end of the file instead of following new writes.
//!
//! ```text
//! cargo test --example file_replay --features testing
//! cargo run --example file_replay --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:replay]
/// Audits a finished day of orders in one pass.
#[subscriber(FileStream::new("orders").replay())]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
}

/// The audit of the day's file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(file, |b| {
        b.include(audit);
    })
}

fn main() -> ExitCode {
    // The shop wrote the day's file; the audit reads it and never creates one.
    run_main(|| app(FileBroker::new("/var/lib/shop/2026-10-17.ss").existing_only()))
}
// --8<-- [end:replay]

#[cfg(test)]
#[path = "support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Order, app};

    #[tokio::test]
    async fn the_replay_reads_the_whole_file_and_completes() -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new("/var/lib/shop/2026-10-17.ss");
        record(&file, (1..=3).map(|id| Order { id })).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        let audited: Vec<u64> = tb
            .broker::<FileBroker>()
            .subscriber("orders")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(audited, [1, 2, 3]);

        // The replay completed at the end of the file, so a later append is not read.
        tb.broker::<FileBroker>()
            .message(&Order { id: 4 })
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called(3);
        tb.shutdown().await?;
        Ok(())
    }
}
