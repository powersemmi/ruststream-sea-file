//! `max_attempts(n)` caps the deliveries of one message, the first included; the count travels
//! in the framework's retry-count header that every copy carries.
//!
//! ```text
//! cargo test --example file_max_attempts --features testing
//! cargo run --example file_max_attempts --features testing -- run
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
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
async fn print(label: &Label, ctx: &mut Context<'_>) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u32>().ok())
        .unwrap_or(0);
    tracing::info!(label.parcel, attempt, "printer jammed");
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/labels.ss"),
        |b| {
            b.include(print).max_attempts(nonzero!(3u32));
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
    async fn the_third_delivery_is_the_last() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Label { parcel: 7 })
            .publish()
            .await?;
        tb.advance(RETRY_DELAY).await?;
        tb.advance(RETRY_DELAY).await?;

        tb.broker::<FileBroker>()
            .subscriber("labels")
            .assert_called(3);
        // The label and the two copies that brought the second and third deliveries.
        tb.broker::<FileBroker>()
            .published::<Label>("labels")
            .assert_called(3)
            .with_header(RETRY_COUNT_HEADER, "2");

        // The delivery at the cap was rejected, so nothing else comes back.
        tb.advance(RETRY_DELAY).await?;
        tb.broker::<FileBroker>()
            .subscriber("labels")
            .assert_called(3);
        tb.shutdown().await?;
        Ok(())
    }
}
