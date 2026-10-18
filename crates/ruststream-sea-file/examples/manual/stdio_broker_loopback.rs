//! The services of `examples/stdio_broker_loopback.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_stdio_broker_loopback --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::stdio::prelude::*;

struct Count;

impl Handle<Text, Counted> for Count {
    fn handle(
        &self,
        text: &Text,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Counted, HandlerOutcome>> {
        ready(Ok(Counted {
            words: text.body.split_whitespace().count(),
        }))
    }
}

struct Total;

impl Handle<Counted> for Total {
    fn handle(
        &self,
        counted: &Counted,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(counted.words, "words counted");
        ready(Ok(()))
    }
}

// --8<-- [start:stage]
/// Both stages in one process, as a test runs them.
fn app() -> impl App {
    let broker = StdioBroker::new().loopback();
    RustStream::new(AppInfo::new("word-count", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("lines", Count).reply().build())
            .out_retry(Publish)
            .to("lines.retry");
        b.include(subscriber("counts", Total).build())
            .out_retry(Publish)
            .to("counts.retry");
    })
}
// --8<-- [end:stage]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Text {
    body: String,
}

// `#[outgoing(name = "lines")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Text {
    type Form = FixedName;
    const DESTINATION: &'static str = "lines";
}

impl MessageHeaders for Text {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Counted {
    words: usize,
}

// `#[outgoing(name = "counts")]` by hand.
impl OutgoingDestination for Counted {
    type Form = FixedName;
    const DESTINATION: &'static str = "counts";
}

impl MessageHeaders for Counted {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Counted, Text, app};

    #[tokio::test]
    async fn the_count_comes_back_to_the_second_stage() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<StdioBroker>()
            .message(&Text {
                body: "one two three".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .subscriber("counts")
            .assert_called_once()
            .with(&Counted { words: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
