//! The crate prelude: one service on both transports. It records orders from a stream file and
//! answers orders read on standard input, so it writes the prefixed policy names.
//!
//! ```text
//! cargo test --example sea_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:mixed]
use ruststream_sea_file::prelude::*;

#[subscriber(FileStream::new("orders"), reply("receipts"))]
async fn record(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[subscriber("orders", reply("receipts"))]
async fn answer(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileBroker::new("/var/lib/orders/orders.ss"), |b| {
            b.include(record).out_reply(FilePublish);
        })
        .with_broker(StdioBroker::new(), |b| {
            b.include(answer)
                .out_reply(StdioPublish)
                .out_retry(StdioPublish)
                .to("orders.retry");
        })
}
// --8<-- [end:mixed]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn each_broker_answers_on_its_own_transport() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        tb.broker::<StdioBroker>()
            .message(&Order { id: 2 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 1 });
        tb.broker::<StdioBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 2 });
        tb.shutdown().await?;
        Ok(())
    }
}
