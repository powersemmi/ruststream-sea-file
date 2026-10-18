//! The service of `examples/file_max_attempts.rs` written without the macros: `.max_attempts(n)`
//! follows `include` as it does on the macro form.
//!
//! ```text
//! cargo test --example manual_file_max_attempts --features testing
//! cargo run --example manual_file_max_attempts --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "labels")]
struct Label {
    parcel: u64,
}

/// How long a label waits before the printer is tried again.
const RETRY_DELAY: Duration = Duration::from_secs(10);

// --8<-- [start:handler]
/// Prints a parcel label; the printer is jammed, so every attempt asks for another.
struct Print;

impl Handle<Label> for Print {
    fn handle(
        &self,
        label: &Label,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let attempt = ctx
            .headers()
            .get_str(RETRY_COUNT_HEADER)
            .and_then(|count| count.parse::<u32>().ok())
            .unwrap_or(0);
        tracing::info!(label.parcel, attempt, "printer jammed");
        ready(Err(HandlerOutcome::retry_after(RETRY_DELAY)))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/labels.ss"),
        |b| {
            b.include(subscriber(FileStream::new("labels"), Print).build())
                .max_attempts(nonzero!(3u32));
        },
    )
}
// --8<-- [end:handler]

fn main() -> ExitCode {
    run_main(app)
}

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
