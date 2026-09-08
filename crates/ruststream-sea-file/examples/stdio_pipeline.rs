//! A service as one stage of a shell pipeline: standard input is the subscription, standard
//! output is the publisher.
//!
//! The broker needs no server and no file. Lines arrive on stdin in the client's
//! `[timestamp | stream_key | seq] payload` format, and the handler's return value is
//! published back to stdout under the `publish(..)` stream key.
//!
//! ```text
//! echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' \
//!     | cargo run --example stdio_pipeline -- run
//! ```

// --8<-- [start:pipeline]
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Done {
    id: u64,
}

#[subscriber("jobs", publish("results"))]
async fn work(job: &Job) -> Done {
    Done { id: job.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(work);
    })
}
// --8<-- [end:pipeline]
