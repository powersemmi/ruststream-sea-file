//! The service of `examples/sea_broker_prelude.rs` without the macros: the handlers are named
//! types and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sea_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:mixed]
use ruststream_sea_file::prelude::*;

struct Record;

impl Handle<Order, Receipt> for Record {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

struct Answer;

impl Handle<Order, Receipt> for Answer {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileBroker::new("/var/lib/orders/orders.ss"), |b| {
            let record = subscriber(FileStream::new("orders"), Record);
            b.include(record.reply().to("receipts").build())
                .out_reply(FilePublish);
        })
        .with_broker(StdioBroker::new(), |b| {
            b.include(subscriber("orders", Answer).reply().to("receipts").build())
                .out_reply(StdioPublish)
                .out_retry(StdioPublish)
                .to("orders.retry");
        })
}
// --8<-- [end:mixed]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's stream key.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
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
