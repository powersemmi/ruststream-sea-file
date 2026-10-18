//! `StdioBroker`: a service as one stage of a shell pipeline. It reads text lines under the
//! `lines` stream key on standard input and writes a word count for each under `counts` on
//! standard output.
//!
//! ```text
//! echo '[2024-01-01T00:00:00 | lines | 1] {"body":"one two three"}' \
//!     | cargo run --example stdio_broker --features testing -- run
//! cargo test --example stdio_broker --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:stage]
use ruststream_sea_file::stdio::prelude::*;

#[subscriber("lines", reply)]
async fn count(text: &Text) -> Counted {
    Counted {
        words: text.body.split_whitespace().count(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("word-count", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        b.include(count).out_retry(Publish).to("lines.retry");
    })
}
// --8<-- [end:stage]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "lines")]
struct Text {
    body: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "counts")]
struct Counted {
    words: usize,
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
