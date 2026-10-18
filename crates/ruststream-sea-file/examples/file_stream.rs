//! `FileStream`: the descriptor of one stream key in the file, written inline in `#[subscriber]`.
//! A plain descriptor follows the live tail of its key.
//!
//! ```text
//! cargo test --example file_stream --features testing
//! cargo run --example file_stream --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:descriptor]
/// Ships the orders appended under the `orders` key after the service started.
#[subscriber(FileStream::new("orders"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "shipping");
    HandlerOutcome::ack()
}

/// The service on the stream file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(file, |b| {
        b.include(ship);
    })
}
// --8<-- [end:descriptor]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/shop/orders.ss")))
}

#[cfg(test)]
#[path = "support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Order, app};

    #[tokio::test]
    async fn the_subscription_follows_the_tail_of_its_key() -> Result<(), Box<dyn Error>> {
        // Two orders an earlier run of the shop left in the file.
        let file = FileBroker::new("/var/lib/shop/orders.ss");
        record(&file, (1..=2).map(|id| Order { id })).await?;
        let tb = TestApp::start(app(file)).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 3 })
            .publish()
            .await?;

        // What the file held when the service started is not read; what came after is.
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
