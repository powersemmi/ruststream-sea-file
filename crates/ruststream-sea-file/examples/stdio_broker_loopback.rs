//! `loopback`: two stages of a pipeline in one process. What the counting stage writes to
//! standard output comes back on standard input, where the totalling stage reads it.
//!
//! ```text
//! cargo test --example stdio_broker_loopback --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_sea_file::stdio::prelude::*;

#[subscriber("lines", reply)]
async fn count(text: &Text) -> Counted {
    Counted {
        words: text.body.split_whitespace().count(),
    }
}

#[subscriber("counts")]
async fn total(counted: &Counted) -> HandlerOutcome {
    tracing::info!(counted.words, "words counted");
    HandlerOutcome::ack()
}

// --8<-- [start:stage]
/// Both stages in one process, as a test runs them.
#[ruststream::app]
fn app() -> impl App {
    let broker = StdioBroker::new().loopback();
    RustStream::new(AppInfo::new("word-count", "0.1.0")).with_broker(broker, |b| {
        b.include(count).out_retry(Publish).to("lines.retry");
        b.include(total).out_retry(Publish).to("counts.retry");
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
