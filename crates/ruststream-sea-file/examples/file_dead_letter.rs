//! `dead_letter(key)` writes the delivery that reaches the cap under a stream key of its own,
//! payload and headers as they arrived.
//!
//! ```text
//! cargo test --example file_dead_letter --features testing
//! cargo run --example file_dead_letter --features testing -- run
//! ```

use std::time::Duration;

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "labels")]
struct Label {
    parcel: u64,
}

/// How long a label waits before the printer is tried again.
const RETRY_DELAY: Duration = Duration::from_secs(10);

// --8<-- [start:handler]
/// Prints a parcel label; the printer is jammed, so every attempt asks for another.
#[subscriber(FileStream::new("labels"))]
async fn print(label: &Label) -> HandlerOutcome {
    tracing::info!(label.parcel, "printer jammed");
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/labels.ss"),
        |b| {
            b.include(print)
                .max_attempts(nonzero!(3u32))
                .dead_letter("labels.dead");
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Label, RETRY_DELAY, app};

    #[tokio::test(start_paused = true)]
    async fn the_spent_label_lands_on_the_dead_letter_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let mut headers = HeaderMap::new();
        headers.insert("x-warehouse", "north");
        tb.broker::<FileBroker>()
            .message(&Label { parcel: 7 })
            .with_headers(headers)
            .publish()
            .await?;
        tb.advance(RETRY_DELAY).await?;
        tb.advance(RETRY_DELAY).await?;

        tb.broker::<FileBroker>()
            .subscriber("labels")
            .assert_called(3);
        tb.broker::<FileBroker>()
            .published::<Label>("labels.dead")
            .assert_called_once()
            .with(&Label { parcel: 7 })
            .with_header("x-warehouse", "north")
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.shutdown().await?;
        Ok(())
    }
}
