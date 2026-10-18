//! The service of `examples/file_dead_letter.rs` written without the macros: `.dead_letter(key)`
//! follows `include` as it does on the macro form.
//!
//! ```text
//! cargo test --example manual_file_dead_letter --features testing
//! cargo run --example manual_file_dead_letter --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

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
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(label.parcel, "printer jammed");
        ready(Err(HandlerOutcome::retry_after(RETRY_DELAY)))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("labels", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/labels.ss"),
        |b| {
            b.include(subscriber(FileStream::new("labels"), Print).build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("labels.dead");
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
