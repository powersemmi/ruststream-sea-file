//! The stage of `examples/stdio_out_retry.rs` written without the macros: `.out_retry(Publish)
//! .to(key)` after `include` names where a deferred copy goes on a pipe.
//!
//! ```text
//! cargo test --example manual_stdio_out_retry --features testing
//! echo '[jobs] {"id":7}' | cargo run --example manual_stdio_out_retry --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
}

/// How long a job waits before its copy leaves for the retry stage.
const RETRY_DELAY: Duration = Duration::from_secs(5);

// --8<-- [start:stage]
/// Hands every job it cannot run now to the retry stage, after a pause.
struct Work;

impl Handle<Job> for Work {
    fn handle(
        &self,
        job: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(job.id, "busy, deferring");
        ready(Err(HandlerOutcome::retry_after(RETRY_DELAY)))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("jobs", Work).build())
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

    use super::{Job, RETRY_DELAY, Work, app};

    #[tokio::test(start_paused = true)]
    async fn the_copy_leaves_under_the_named_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<StdioBroker>()
            .message(&Job { id: 7 })
            .to("jobs")
            .publish()
            .await?;
        tb.advance(RETRY_DELAY).await?;

        // The copy goes downstream; this stage reads the job once.
        tb.broker::<StdioBroker>()
            .subscriber("jobs")
            .assert_called_once();
        tb.broker::<StdioBroker>()
            .published::<Job>("jobs.retry")
            .assert_called_once()
            .with(&Job { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_registration_that_names_no_destination_does_not_start() {
        let app =
            RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(StdioBroker::new(), |b| {
                b.include(subscriber("jobs", Work).build());
            });

        let refused = TestApp::start(app)
            .await
            .expect_err("a deferred copy on a pipe has nowhere to go");
        let refused = refused.to_string();
        assert!(refused.contains("jobs"), "{refused}");
        assert!(refused.contains(".to("), "{refused}");
    }
}
