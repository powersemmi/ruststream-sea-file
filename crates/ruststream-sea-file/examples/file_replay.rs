//! Replay and repositioning on the default start path: the `start_at` clause opens the
//! subscription at the beginning of the file (recorded history replays before anything
//! live), the `Seek` handler parameter skips a poisoned region, the first publish rides the
//! scope's `after_startup` lifecycle hook, and the demo file is removed in `after_shutdown`,
//! once the broker has closed it - no hand-written runtime setup anywhere.
//!
//! ```text
//! cargo run --example file_replay -- run
//! ```

use std::{fs, io};

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, Seek};
use ruststream::{OutgoingMessage, Publisher, Seeker, subscriber};
use ruststream_sea_file::{FileBroker, FilePosition, FilePublish, FileSeeker, FileStream};
use serde::{Deserialize, Serialize};

const DEMO_FILE: &str = "/tmp/ruststream-file-replay-example.ss";

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: u64,
}

/// Replays the jobs from the beginning of the file; on the poison marker it jumps to the
/// live tail, dropping everything still queued from the replay.
#[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
async fn replay(job: &Job, Seek(seeker): Seek<FileSeeker>) -> HandlerResult {
    if job.id == 999 {
        println!("poison marker: skipping the rest of the recorded region");
        if seeker.seek(FilePosition::end()).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    println!("replayed job {}", job.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("replay-demo", "0.1.0"))
        // The demo file is transient: remove it after the brokers have shut down and the
        // stream file is closed and flushed.
        .after_shutdown(async move |_state| -> io::Result<()> { fs::remove_file(DEMO_FILE) })
        .with_broker(FileBroker::new(DEMO_FILE), |b| {
            // The recording side, as a lifecycle hook: it runs once the broker is connected
            // and the subscriptions are open, so the paired publisher arrives live.
            b.after_startup(FilePublish, async move |publisher| -> io::Result<()> {
                for id in [1u64, 999, 3, 4] {
                    let payload = serde_json::to_vec(&Job { id }).map_err(io::Error::other)?;
                    publisher
                        .publish(OutgoingMessage::new("jobs", payload.as_slice()))
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            });
            b.include(replay);
        })
}
