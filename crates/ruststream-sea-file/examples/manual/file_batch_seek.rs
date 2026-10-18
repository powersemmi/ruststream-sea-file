//! The service of `examples/file_batch_seek.rs` written without the macros: a batch body typed
//! over `FileBatchContext` reaches the subscription's seek handle by its key.
//!
//! ```text
//! cargo test --example manual_file_batch_seek --features testing
//! cargo run --example manual_file_batch_seek --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// A job; the producer sets `resume_at` when it knows the jobs up to that sequence cannot be
/// processed.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    resume_at: Option<u64>,
}

// --8<-- [start:batch]
/// A batch spans many deliveries, so its context is the subscription's: the seek handle, and no
/// position. The move applies to the batches after this one.
struct Drain;

impl Handle<[Job], (), (), FileBatchContext> for Drain {
    async fn handle(
        &self,
        jobs: &[Job],
        _outs: &(),
        ctx: &mut Context<'_, FileBatchContext>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        tracing::info!(jobs = jobs.len(), "drained");
        let Some(resume_at) = jobs.iter().find_map(|job| job.resume_at) else {
            return Ok(());
        };
        if let Err(error) = ctx
            .context(SeekHandle)
            .seek(FilePosition::sequence(resume_at))
            .await
        {
            tracing::warn!(resume_at, %error, "seek refused");
        }
        Ok(())
    }
}

/// The worker on the job file `main` hands it, three jobs at a time.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(file, |b| {
        b.include(
            subscriber(FileStream::new("jobs"), Drain)
                .batch(nonzero!(3))
                .start_at(FilePosition::beginning())
                .build(),
        );
    })
}
// --8<-- [end:batch]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/jobs/jobs.ss")))
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
    use super::{Job, app};

    #[tokio::test]
    async fn the_batch_after_the_seek_starts_at_resume_at() -> Result<(), Box<dyn Error>> {
        // Six jobs at sequences 1 to 6; job 1 says the jobs up to sequence 5 cannot be processed.
        let file = FileBroker::new("/var/lib/jobs/jobs.ss");
        let jobs = (1..=6).map(|id| Job {
            id,
            resume_at: (id == 1).then_some(5),
        });
        record(&file, jobs).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        let batches: Vec<Vec<u64>> = tb
            .broker::<FileBroker>()
            .subscriber("jobs")
            .batches::<Job>()
            .iter()
            .map(|batch| batch.iter().map(|job| job.id).collect())
            .collect();
        assert_eq!(batches, [vec![1, 2, 3], vec![5, 6]]);
        tb.shutdown().await?;
        Ok(())
    }
}
