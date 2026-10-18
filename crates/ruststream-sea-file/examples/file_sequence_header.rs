//! `SEQUENCE_HEADER`: every delivery carries its sequence number as a header, from one on a
//! stream file.
//!
//! ```text
//! cargo test --example file_sequence_header --features testing
//! cargo run --example file_sequence_header --features testing -- run
//! ```

use ruststream_sea_file::SEQUENCE_HEADER;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// An order and the sequence the file gave it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "numbered")]
struct Numbered {
    order: u64,
    sequence: u64,
}

// --8<-- [start:handler]
/// Numbers each order with the sequence it sits at in the file.
#[subscriber(FileStream::new("orders"), reply)]
async fn number(order: &Order, ctx: &mut Context<'_>) -> Result<Numbered, HandlerOutcome> {
    let Some(sequence) = ctx
        .headers()
        .get_str(SEQUENCE_HEADER)
        .and_then(|sequence| sequence.parse().ok())
    else {
        return Err(HandlerOutcome::drop());
    };
    tracing::info!(order.id, sequence, "numbered");
    Ok(Numbered {
        order: order.id,
        sequence,
    })
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("numbering", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(number);
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::SEQUENCE_HEADER;
    use ruststream_sea_file::file::prelude::*;

    use super::{Numbered, Order, app};

    #[tokio::test]
    async fn the_file_numbers_a_key_from_one() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in [7, 8] {
            tb.broker::<FileBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let numbered = tb
            .broker::<FileBroker>()
            .published::<Numbered>("numbered")
            .decoded();
        assert_eq!(
            numbered,
            [
                Numbered {
                    order: 7,
                    sequence: 1
                },
                Numbered {
                    order: 8,
                    sequence: 2
                },
            ],
        );
        tb.shutdown().await?;
        Ok(())
    }

    mod stdio_form {
        use std::error::Error;

        use ruststream::testing::TestApp;
        use ruststream_sea_file::stdio::prelude::*;

        use super::{Order, SEQUENCE_HEADER};

        #[subscriber("orders")]
        async fn read(_order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
            if ctx.headers().get_str(SEQUENCE_HEADER) == Some("0") {
                HandlerOutcome::ack()
            } else {
                HandlerOutcome::drop()
            }
        }

        /// A pipe counts the lines of a key from zero.
        #[tokio::test]
        async fn a_pipe_numbers_a_key_from_zero() -> Result<(), Box<dyn Error>> {
            let app = RustStream::new(AppInfo::new("numbering", "0.1.0")).with_broker(
                StdioBroker::new(),
                |b| {
                    b.include(read).out_retry(Publish).to("orders.retry");
                },
            );
            let tb = TestApp::start(app).await?;

            tb.broker::<StdioBroker>()
                .message(&Order { id: 7 })
                .publish()
                .await?;

            tb.broker::<StdioBroker>()
                .subscriber("orders")
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok(())
        }
    }
}
