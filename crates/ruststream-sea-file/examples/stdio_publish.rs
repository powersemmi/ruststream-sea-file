//! A publish on a pipe: one line on standard output under the message's stream key, which the
//! next stage of the pipeline reads.
//!
//! ```text
//! cargo test --example stdio_publish --features testing
//! echo '[words] {"text":"hello"}' | cargo run --example stdio_publish --features testing -- run
//! ```

use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Word {
    text: String,
}

// --8<-- [start:stage]
/// The line this stage writes for the next one.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shouts")]
struct Shout {
    text: String,
}

/// Answers each word with the same word in capitals.
#[subscriber("words", reply)]
async fn shout(word: &Word) -> Shout {
    Shout {
        text: word.text.to_uppercase(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shouter", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(shout).out_retry(Publish).to("words.retry");
    })
}
// --8<-- [end:stage]

#[cfg(test)]
mod tests {
    use std::error::Error;

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

    #[subscriber("words", reply)]
    async fn blank(_word: &Word) -> Blank {
        Blank(Vec::new())
    }

    #[tokio::test]
    async fn an_empty_message_is_refused_and_nothing_is_written() -> Result<(), Box<dyn Error>> {
        let app =
            RustStream::new(AppInfo::new("blank", "0.1.0")).with_broker(StdioBroker::new(), |b| {
                b.include(blank).out_retry(Publish).to("words.retry");
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
