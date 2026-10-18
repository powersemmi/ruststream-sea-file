//! `Ctx<Position>`: a handler reads where its delivery sits in the stream file, the value a service
//! stores to resume from or seeks back to.
//!
//! ```text
//! cargo test --example file_position_key --features testing
//! cargo run --example file_position_key --features testing -- run
//! ```

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// What the invoicing ledger keeps per order: the order and the sequence it was read at.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoiced {
    order: u64,
    sequence: u64,
}

// --8<-- [start:handler]
/// Records each invoice with the sequence of the order it came from, so the next run knows
/// where to resume.
#[subscriber(FileStream::new("orders"), reply)]
async fn invoice(order: &Order, Ctx(at): Ctx<Position>) -> Invoiced {
    tracing::info!(order.id, ?at, "invoiced");
    let sequence = match at {
        FilePosition::Sequence(sequence) => sequence,
        // A delivery always sits at a sequence; the other forms name places to start or seek to.
        FilePosition::Beginning | FilePosition::End | FilePosition::Timestamp(_) => 0,
    };
    Invoiced {
        order: order.id,
        sequence,
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(invoice);
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Invoiced, Order, app};

    #[tokio::test]
    async fn each_invoice_carries_the_sequence_of_its_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in [10, 20] {
            tb.broker::<FileBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        // A stream key is numbered from one.
        assert_eq!(
            tb.broker::<FileBroker>()
                .published::<Invoiced>("invoices")
                .decoded(),
            [
                Invoiced {
                    order: 10,
                    sequence: 1
                },
                Invoiced {
                    order: 20,
                    sequence: 2
                },
            ],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
