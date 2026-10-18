//! `out_retry(Publish).to(key)` on a pipe: a deferred copy is a line for the next stage under the
//! key the mount site names, and a stdio registration that names none does not start.
//!
//! ```text
//! cargo test --example stdio_out_retry --features testing
//! echo '[jobs] {"id":7}' | cargo run --example stdio_out_retry --features testing -- run
//! ```

use std::time::Duration;

use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

/// How long a job waits before its copy leaves for the retry stage.
const RETRY_DELAY: Duration = Duration::from_secs(5);

// --8<-- [start:stage]
/// Hands every job it cannot run now to the retry stage, after a pause.
#[subscriber("jobs")]
async fn work(job: &Job) -> HandlerOutcome {
    tracing::info!(job.id, "busy, deferring");
    HandlerOutcome::retry_after(RETRY_DELAY)
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

    use super::{Job, RETRY_DELAY, app, work};

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
                b.include(work);
            });

        let refused = TestApp::start(app)
            .await
            .expect_err("a deferred copy on a pipe has nowhere to go");
        let refused = refused.to_string();
        assert!(refused.contains("jobs"), "{refused}");
        assert!(refused.contains(".to("), "{refused}");
    }
}
