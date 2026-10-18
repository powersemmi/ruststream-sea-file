//! A service as one stage of a shell pipeline: standard input is the subscription, standard
//! output is the publisher.
//!
//! The broker needs no server and no file. Lines arrive on stdin in the client's
//! `[timestamp | stream_key | seq] payload` format, and the handler's return value is
//! published back to stdout under the stream key its own type declares. When standard input
//! ends, the subscription ends, and the stage shuts down and exits.
//!
//! ```text
//! echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' \
//!     | cargo run --example stdio_pipeline
//! ```

// --8<-- [start:pipeline]
use ruststream::runtime::RustStreamError;
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
#[outgoing(name = "results")]
struct Done {
    id: u64,
}

#[subscriber("jobs", reply)]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        // Standard output never reaches this process's own standard input, so the mount site
        // says where a deferred copy goes: the next stage of the pipeline reads it under that
        // stream key.
        b.include(work).out_retry(Publish).to("jobs.retry");
    })
}
// --8<-- [end:pipeline]

// --8<-- [start:exit]
/// Runs the stage until its input ends: the end of standard input ends the subscription, and
/// the stage then shuts down and exits.
#[tokio::main]
async fn main() -> Result<(), RustStreamError> {
    let running = app().start().await?;
    let mut health = running.health();
    while health.is_running() && health.subscriptions().active > 0 {
        health.changed().await;
    }
    running.shutdown().await
}
// --8<-- [end:exit]
