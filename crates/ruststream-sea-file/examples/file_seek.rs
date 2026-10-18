//! `Ctx<SeekHandle>`: a handler moves its own running subscription to another place in the
//! stream file; what was queued from the old place is discarded.
//!
//! ```text
//! cargo test --example file_seek --features testing
//! cargo run --example file_seek --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// A job; the producer sets `resume_at` when it knows the jobs up to that sequence cannot be
/// processed.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    resume_at: Option<u64>,
}

// --8<-- [start:handler]
/// Skips a run of jobs known to fail: the subscription jumps to `resume_at`.
#[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    tracing::info!(job.id, "worked");
    let Some(resume_at) = job.resume_at else {
        return HandlerOutcome::ack();
    };
    if let Err(error) = seeker.seek(FilePosition::sequence(resume_at)).await {
        tracing::warn!(job.id, resume_at, %error, "seek refused");
    }
    HandlerOutcome::ack()
}

/// The worker on the job file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(file, |b| {
        b.include(work);
    })
}
// --8<-- [end:handler]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/jobs/jobs.ss")))
}

#[cfg(test)]
#[path = "support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Job, app};

    fn worked(tb: &TestApp<()>) -> Vec<u64> {
        tb.broker::<FileBroker>()
            .subscriber("jobs")
            .received::<Job>()
            .into_iter()
            .map(|job| job.id)
            .collect()
    }

    /// Five jobs at sequences 1 to 5; job 2 says the jobs up to sequence 4 cannot be processed.
    fn run(resume_at: u64) -> Vec<Job> {
        (1..=5)
            .map(|id| Job {
                id,
                resume_at: (id == 2).then_some(resume_at),
            })
            .collect()
    }

    #[tokio::test]
    async fn the_jobs_before_resume_at_are_skipped() -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new("/var/lib/jobs/jobs.ss");
        record(&file, run(4)).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        assert_eq!(worked(&tb), [1, 2, 4, 5]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_sequence_the_file_has_not_written_ends_the_subscription()
    -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new("/var/lib/jobs/jobs.ss");
        record(&file, run(9)).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        // The seek is refused, and the live subscription ends with the refusal.
        tb.broker::<FileBroker>()
            .message(&Job {
                id: 6,
                resume_at: None,
            })
            .publish()
            .await?;
        assert_eq!(worked(&tb), [1, 2]);
        tb.shutdown().await?;
        Ok(())
    }
}
