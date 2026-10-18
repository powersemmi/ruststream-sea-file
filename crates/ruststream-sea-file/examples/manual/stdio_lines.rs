//! The stage of `examples/stdio_lines.rs` written without the macros: a stream key in
//! `subscriber(..)` reads the lines of that key off standard input, and `.reply()` answers on
//! standard output.
//!
//! ```text
//! cargo test --example manual_stdio_lines --features testing
//! echo '[2026-10-18T09:00:00 | jobs | 1] {"id":7}' \
//!     | cargo run --example manual_stdio_lines --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

// --8<-- [start:stage]
/// Answers each `jobs` line with a `results` line for the next stage.
struct Work;

impl Handle<Job, Done> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Done, HandlerOutcome>> {
        ready(Ok(Done { id: job.id }))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("jobs", Work).reply().build())
            .out_retry(Publish)
            .to("jobs.retry");
    })
}
// --8<-- [end:stage]

fn main() -> ExitCode {
    run_main(app)
}

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
