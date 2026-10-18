//! The service of `examples/file_position.rs` written without the macros: `.start_at(position)`
//! is a step of the mount chain.
//!
//! ```text
//! cargo test --example manual_file_position --features testing
//! cargo run --example manual_file_position --features testing -- run
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
    total_cents: u64,
}

// --8<-- [start:positions]
/// Rebuilds the revenue report from the first order the file retains, then follows new orders.
struct Report;

impl Handle<Order> for Report {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.total_cents, "counted");
        ready(Ok(()))
    }
}

/// The report on the shop's stream file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("revenue", "0.1.0")).with_broker(file, |b| {
        b.include(
            subscriber(FileStream::new("orders"), Report)
                .start_at(FilePosition::beginning())
                .build(),
        );
    })
}
// --8<-- [end:positions]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/shop/orders.ss")))
}

#[cfg(test)]
#[path = "../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Order, app};

    fn order(id: u64) -> Order {
        Order {
            id,
            total_cents: 100 * id,
        }
    }

    #[tokio::test]
    async fn the_report_counts_the_retained_orders_then_the_new_ones() -> Result<(), Box<dyn Error>>
    {
        let file = FileBroker::new("/var/lib/shop/orders.ss");
        record(&file, (1..=2).map(order)).await?;
        let tb = TestApp::start(app(file)).await?;
        tb.settle().await?;

        tb.broker::<FileBroker>()
            .message(&order(3))
            .publish()
            .await?;

        let counted: Vec<u64> = tb
            .broker::<FileBroker>()
            .subscriber("orders")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(counted, [1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}
