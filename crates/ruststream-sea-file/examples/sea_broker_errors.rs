//! `SeaFileError` where a service meets it: a stdio stage writes each job's note as it is, and an
//! empty note is refused, because a pipe drops an empty line.
//!
//! ```text
//! cargo test --example sea_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_sea_file::stdio::prelude::*;

/// The slot notes leave through.
#[derive(OutSlot)]
#[publishes(Note)]
struct Notes;

// --8<-- [start:refused]
#[subscriber("jobs")]
async fn annotate(job: &Job, Out(notes): Out<impl Publisher, Notes>) -> HandlerOutcome {
    let note = Note(job.note.clone().into_bytes());
    if let Err(err) = notes.message(&note).publish().await {
        // An empty note is refused, and the job is done without it.
        tracing::warn!(subscription = "jobs", job.id, error = %err, "note not written");
    }
    HandlerOutcome::ack()
}
// --8<-- [end:refused]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("notes", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(annotate)
            .out(Notes, Publish)
            .out_retry(Publish)
            .to("jobs.retry")
            .build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
struct Job {
    id: u64,
    note: String,
}

/// A note, written to standard output as its bytes.
#[derive(Debug, Serialized, Outgoing)]
#[outgoing(name = "notes")]
struct Note(Vec<u8>);

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Job, app};

    #[tokio::test]
    async fn a_note_is_written_as_it_is() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<StdioBroker>()
            .message(&Job {
                id: 1,
                note: "rush".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<()>("notes")
            .assert_called_once()
            .with_raw(b"rush");
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_note_is_refused_and_the_job_goes_on() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<StdioBroker>()
            .message(&Job {
                id: 2,
                note: String::new(),
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<()>("notes")
            .assert_not_called();
        tb.broker::<StdioBroker>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
