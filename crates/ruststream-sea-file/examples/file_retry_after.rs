//! `retry_after` on a stream file: the runtime appends a copy of the delivery under the key the
//! subscription reads once the delay has passed, and the subscription reads it again.
//!
//! ```text
//! cargo test --example file_retry_after --features testing
//! cargo run --example file_retry_after --features testing -- run
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

/// How long a payment waits before the next attempt.
const RETRY_DELAY: Duration = Duration::from_secs(30);

// --8<-- [start:handler]
/// Charges a payment; the first attempt finds the card processor busy and asks again later.
#[subscriber(FileStream::new("payments"))]
async fn charge(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    // A copy carries the framework's retry count; the first delivery carries none.
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        tracing::info!(payment.id, "processor busy, trying later");
        return HandlerOutcome::retry_after(RETRY_DELAY);
    }
    tracing::info!(payment.id, "charged");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/payments.ss"),
        |b| {
            b.include(charge);
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Payment, RETRY_DELAY, app};

    #[tokio::test(start_paused = true)]
    async fn the_copy_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Payment { id: 7 })
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY_DELAY));

        tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
            .await?;
        tb.broker::<FileBroker>()
            .subscriber("payments")
            .assert_called_once();

        tb.advance(Duration::from_millis(1)).await?;
        tb.broker::<FileBroker>()
            .subscriber("payments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        // The copy is appended under the key the subscription reads, with its count.
        tb.broker::<FileBroker>()
            .published::<Payment>("payments")
            .assert_called(2)
            .with(&Payment { id: 7 })
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }
}
