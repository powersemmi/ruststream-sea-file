//! The service of `examples/stdio_broker.rs` without the macros: the handler is a named type and
//! `main` is written by hand.
//!
//! ```text
//! echo '[2024-01-01T00:00:00 | lines | 1] {"body":"one two three"}' \
//!     | cargo run --example manual_stdio_broker --features testing -- run
//! cargo test --example manual_stdio_broker --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:stage]
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

fn app() -> impl App {
    RustStream::new(AppInfo::new("word-count", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("lines", Count).reply().build())
            .out_retry(Publish)
            .to("lines.retry");
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
    async fn a_line_read_on_input_is_answered_on_output() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<StdioBroker>()
            .message(&Text {
                body: "one two three".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<Counted>("counts")
            .assert_called_once()
            .with(&Counted { words: 3 });
        tb.shutdown().await?;
        Ok(())
    }
}
