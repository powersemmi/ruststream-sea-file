//! A stage of a shell pipeline: each subscription reads the lines of its stream key off standard
//! input, and a reply is a line on standard output.
//!
//! ```text
//! cargo test --example stdio_lines --features testing
//! echo '[2026-10-18T09:00:00 | jobs | 1] {"id":7}' \
//!     | cargo run --example stdio_lines --features testing -- run
//! ```

use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

// --8<-- [start:stage]
/// Answers each `jobs` line with a `results` line for the next stage.
#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(work).out_retry(Publish).to("jobs.retry");
    })
}
// --8<-- [end:stage]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Done, Job, app};

    #[tokio::test]
    async fn the_stage_answers_the_lines_of_its_key_only() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let stdin = tb.broker::<StdioBroker>();
        stdin.message(&Job { id: 7 }).to("jobs").publish().await?;
        stdin.message(&Job { id: 8 }).to("chores").publish().await?;

        tb.broker::<StdioBroker>()
            .subscriber("jobs")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.broker::<StdioBroker>()
            .published::<Done>("results")
            .assert_called_once()
            .with(&Done { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
