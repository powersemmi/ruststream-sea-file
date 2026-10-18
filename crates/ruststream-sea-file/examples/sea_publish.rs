//! The form's one policy, `Publish`, carries a reply and a slot publish: a reply goes to the
//! stream key its type declares, and a slot publish to the key of the message it sends.
//!
//! ```text
//! cargo test --example sea_publish --features testing
//! cargo run --example sea_publish --features testing -- run
//! ```

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:reply]
/// The answer to an order, appended under the key the type declares.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// The audit line of an order, on a key of its own.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct Audited {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Ledger;

/// Answers each order with a receipt and writes its audit line through the ledger slot.
#[subscriber(FileStream::new("orders"), reply)]
async fn confirm(
    order: &Order,
    Out(ledger): Out<impl Publisher, Ledger>,
) -> Result<Receipt, HandlerOutcome> {
    if let Err(error) = ledger.message(&Audited { order: order.id }).publish().await {
        tracing::warn!(order.id, %error, "the audit line was not written");
        return Err(HandlerOutcome::retry());
    }
    Ok(Receipt { order: order.id })
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            // The reply needs no policy: the runtime pairs the form's own `Publish`.
            b.include(confirm).out(Ledger, Publish).build();
        },
    )
}
// --8<-- [end:reply]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Audited, Ledger, Order, Receipt, app};

    #[tokio::test]
    async fn an_order_gets_a_receipt_and_an_audit_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 });
        tb.broker::<FileBroker>()
            .published::<Audited>("audit")
            .assert_called_once()
            .with(&Audited { order: 7 });
        // A publish takes no per-message setting on this transport.
        tb.out::<Ledger>()
            .assert_called_once()
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
