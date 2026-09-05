//! Processing a recorded stream file a batch at a time: the producer records a run in the
//! scope's `after_startup` hook, the handler settles it in batches, and the demo file is removed
//! in `after_shutdown`, once the broker has closed it - no hand-written runtime setup anywhere.
//!
//! ```text
//! cargo run --example file_batches -- run
//! ```

use std::{fs, io};

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

const DEMO_FILE: &str = "/tmp/ruststream-file-batches-example.ss";

#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Reading {
    sensor: u32,
    millivolts: u32,
}

// --8<-- [start:batch]
/// Takes a whole batch at once and returns one outcome per element. The elements are decoded
/// before the body runs, so a batch that is short is simply a batch the transport had no more
/// deliveries for.
#[subscriber(FileStream::new("readings"), start_at(FilePosition::beginning()))]
async fn aggregate(batch: &[Reading]) -> Vec<HandlerOutcome> {
    let total: u32 = batch.iter().map(|reading| reading.millivolts).sum();
    println!("batch of {} readings, {total} mV in total", batch.len());
    batch.iter().map(|_| HandlerOutcome::ack()).collect()
}
// --8<-- [end:batch]

// --8<-- [start:mount]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("readings", "0.1.0"))
        // The demo file is transient: remove it after the brokers have shut down and the
        // stream file is closed and flushed.
        .after_shutdown(async move |_state| -> io::Result<()> { fs::remove_file(DEMO_FILE) })
        .with_broker(FileBroker::new(DEMO_FILE), |b| {
            b.after_startup(Publish, async move |publisher| -> io::Result<()> {
                for sensor in 0..10u32 {
                    publisher
                        .message(&Reading {
                            sensor,
                            millivolts: 100 * sensor,
                        })
                        .to("readings")
                        .publish()
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            });
            // The batch size is the whole of what a mount site says about batching; how a batch
            // is filled is the transport's business.
            b.include(aggregate.batch(nonzero!(4)));
        })
}
// --8<-- [end:mount]
