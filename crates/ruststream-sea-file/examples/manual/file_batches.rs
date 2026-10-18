//! The service of `examples/file_batches.rs` written without the macros: a body over `[T]` is a
//! batch handler, and `.batch(n)` in the chain is its largest batch.
//!
//! ```text
//! cargo test --example manual_file_batches --features testing
//! cargo run --example manual_file_batches --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "readings")]
struct Reading {
    sensor: u32,
    millivolts: u32,
}

// --8<-- [start:batch]
/// Sums a batch of readings; `Ok` settles the whole batch.
struct Aggregate;

impl Handle<[Reading]> for Aggregate {
    fn handle(
        &self,
        batch: &[Reading],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let total: u32 = batch.iter().map(|reading| reading.millivolts).sum();
        tracing::info!(readings = batch.len(), total, "aggregated");
        ready(Ok(()))
    }
}
// --8<-- [end:batch]

// --8<-- [start:mount]
/// The service on the sensors' stream file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(file, |b| {
        b.include(
            subscriber(FileStream::new("readings"), Aggregate)
                .batch(nonzero!(4))
                .start_at(FilePosition::beginning())
                .build(),
        );
    })
}
// --8<-- [end:mount]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/sensors/readings.ss")))
}

#[cfg(test)]
#[path = "../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Reading, app};

    #[tokio::test]
    async fn ten_recorded_readings_arrive_in_batches_of_at_most_four() -> Result<(), Box<dyn Error>>
    {
        let file = FileBroker::new("/var/lib/sensors/readings.ss");
        let readings = (0..10).map(|sensor| Reading {
            sensor,
            millivolts: 100 * sensor,
        });
        record(&file, readings).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        tb.broker::<FileBroker>()
            .subscriber("readings")
            .assert_batch_sizes(&[4, 4, 2])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
