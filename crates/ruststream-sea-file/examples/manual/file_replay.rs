//! The service of `examples/file_replay.rs` written without the macros: the descriptor in
//! `subscriber(..)` carries the replay mode.
//!
//! ```text
//! cargo test --example manual_file_replay --features testing
//! cargo run --example manual_file_replay --features testing -- run
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

// --8<-- [start:replay]
/// Audits a finished day of orders in one pass.
struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "audited");
        ready(Ok(()))
    }
}

/// The audit of the day's file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(file, |b| {
        b.include(subscriber(FileStream::new("orders").replay(), Audit).build());
    })
}

fn main() -> ExitCode {
    // The shop wrote the day's file; the audit reads it and never creates one.
    run_main(|| app(FileBroker::new("/var/lib/shop/2026-10-17.ss").existing_only()))
}
// --8<-- [end:replay]

#[cfg(test)]
#[path = "../support/recorded_stream.rs"]
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
