//! The service of `examples/sea_broker_errors.rs` without the macros: the handler is a named type,
//! the slot a marker type, the note's bytes are written by hand, and so is `main`.
//!
//! ```text
//! cargo test --example manual_sea_broker_errors --features testing
//! ```

use std::convert::Infallible;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::stdio::prelude::*;

/// The slot notes leave through.
struct Notes;

impl OutSlot for Notes {
    const NAME: &'static str = "Notes";
    // `Note` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Notes> for Note {}

// --8<-- [start:refused]
struct Annotate;

impl<Entry> Handle<Job, (), Outs<(Entry,)>> for Annotate
where
    Entry: OutEntry<Notes, Wire: Publisher>,
{
    async fn handle(
        &self,
        job: &Job,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let note = Note(job.note.clone().into_bytes());
        if let Err(err) = outs.get(Notes).message(&note).publish().await {
            // An empty note is refused, and the job is done without it.
            tracing::warn!(subscription = "jobs", job.id, error = %err, "note not written");
        }
        Ok(())
    }
}
// --8<-- [end:refused]

fn app() -> impl App {
    RustStream::new(AppInfo::new("notes", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("jobs", Annotate).build())
            .out(Notes, Publish)
            .out_retry(Publish)
            .to("jobs.retry")
            .build();
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Job {
    id: u64,
    note: String,
}

// `#[outgoing(name = "jobs")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Job {
    type Form = FixedName;
    const DESTINATION: &'static str = "jobs";
}

impl MessageHeaders for Job {
    type Contract = NoHeaders;
}

/// A note, written to standard output as its bytes.
#[derive(Debug)]
struct Note(Vec<u8>);

// `#[derive(Serialized)]` by hand: the note's bytes leave as they are, with no codec.
impl Serialized for Note {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Note {
    type Wire = SerializedWire;
}

// `#[outgoing(name = "notes")]` by hand.
impl OutgoingDestination for Note {
    type Form = FixedName;
    const DESTINATION: &'static str = "notes";
}

impl MessageHeaders for Note {
    type Contract = NoHeaders;
}

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
