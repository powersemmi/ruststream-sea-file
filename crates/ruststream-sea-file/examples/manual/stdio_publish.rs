//! The stage of `examples/stdio_publish.rs` written without the macros: `.reply()` writes the
//! body's answer as one line on standard output.
//!
//! ```text
//! cargo test --example manual_stdio_publish --features testing
//! echo '[words] {"text":"hello"}' \
//!     | cargo run --example manual_stdio_publish --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Word {
    text: String,
}

// --8<-- [start:stage]
/// The line this stage writes for the next one.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shouts")]
struct Shout {
    text: String,
}

/// Answers each word with the same word in capitals.
struct ShoutBack;

impl Handle<Word, Shout> for ShoutBack {
    fn handle(
        &self,
        word: &Word,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Shout, HandlerOutcome>> {
        ready(Ok(Shout {
            text: word.text.to_uppercase(),
        }))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shouter", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(subscriber("words", ShoutBack).reply().build())
            .out_retry(Publish)
            .to("words.retry");
    })
}
// --8<-- [end:stage]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Shout, Word, app};

    #[tokio::test]
    async fn a_word_leaves_as_a_line_under_the_reply_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<StdioBroker>()
            .message(&Word {
                text: "hello".to_owned(),
            })
            .to("words")
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<Shout>("shouts")
            .assert_called_once()
            .with(&Shout {
                text: "HELLO".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    /// A payload with no bytes and no headers, the one message a line cannot carry.
    #[derive(Outgoing, Serialized)]
    #[outgoing(name = "blanks")]
    struct Blank(Vec<u8>);

    struct BlankBack;

    impl Handle<Word, Blank> for BlankBack {
        fn handle(
            &self,
            _word: &Word,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Blank, HandlerOutcome>> {
            ready(Ok(Blank(Vec::new())))
        }
    }

    #[tokio::test]
    async fn an_empty_message_is_refused_and_nothing_is_written() -> Result<(), Box<dyn Error>> {
        let app =
            RustStream::new(AppInfo::new("blank", "0.1.0")).with_broker(StdioBroker::new(), |b| {
                b.include(subscriber("words", BlankBack).reply().build())
                    .out_retry(Publish)
                    .to("words.retry");
            });
        let tb = TestApp::start(app).await?;

        tb.broker::<StdioBroker>()
            .message(&Word {
                text: "hello".to_owned(),
            })
            .to("words")
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<()>("blanks")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
