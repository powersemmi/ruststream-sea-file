//! The service of `examples/file_position_key.rs` written without the macros: a body typed over
//! `FileContext` reads the delivery's position with `ctx.context(Position)`.
//!
//! ```text
//! cargo test --example manual_file_position_key --features testing
//! cargo run --example manual_file_position_key --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// What the invoicing ledger keeps per order: the order and the sequence it was read at.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices")]
struct Invoiced {
    order: u64,
    sequence: u64,
}

// --8<-- [start:handler]
/// Records each invoice with the sequence of the order it came from, so the next run knows
/// where to resume.
struct Invoice;

impl Handle<Order, Invoiced, (), FileContext> for Invoice {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, FileContext>,
    ) -> impl Future<Output = Result<Invoiced, HandlerOutcome>> {
        let at = ctx.context(Position);
        tracing::info!(order.id, ?at, "invoiced");
        let sequence = match at {
            FilePosition::Sequence(sequence) => sequence,
            // A delivery always sits at a sequence; the other forms name places to start or
            // seek to.
            FilePosition::Beginning | FilePosition::End | FilePosition::Timestamp(_) => 0,
        };
        ready(Ok(Invoiced {
            order: order.id,
            sequence,
        }))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(
                subscriber(FileStream::new("orders"), Invoice)
                    .reply()
                    .build(),
            );
        },
    )
}
// --8<-- [end:handler]

fn main() -> ExitCode {
    run_main(app)
}

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
