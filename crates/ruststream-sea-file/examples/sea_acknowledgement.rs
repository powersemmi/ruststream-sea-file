//! `ack` settles nothing on a stream file: the file keeps no record of how far a service read,
//! so a subscription that starts at the beginning reads every acknowledged message again.
//!
//! ```text
//! cargo test --example sea_acknowledgement --features testing
//! cargo run --example sea_acknowledgement --features testing -- run
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

// --8<-- [start:handler]
/// Invoices every order the file holds, on every start.
#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn invoice(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "invoiced");
    // Reported as unsupported: the next start reads this order again.
    HandlerOutcome::ack()
}

/// The invoicing service on the file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(file, |b| {
        b.include(invoice);
    })
}
// --8<-- [end:handler]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/shop/orders.ss")))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_handler_acknowledges_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(FileBroker::new("/var/lib/shop/orders.ss"))).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// Two runs on one stream file: the second reads the orders the first acknowledged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_reads_the_acknowledged_orders_again() -> Result<(), Box<dyn Error>> {
        let path = env::temp_dir().join(format!(
            "ruststream-sea-file-acknowledgement-{}.ss",
            process::id()
        ));
        let path = path.to_string_lossy().into_owned();

        let first = TestApp::start_live(app(FileBroker::new(&path))).await?;
        for id in 1..=2 {
            first
                .broker::<FileBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }
        first
            .broker::<FileBroker>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        first.shutdown().await?;

        let second = TestApp::start_live(app(FileBroker::new(&path))).await?;
        second
            .broker::<FileBroker>()
            .message(&Order { id: 3 })
            .publish()
            .await?;
        // A live publish returns once the subscription has handled one delivery, and the file
        // hands it order 1 first.
        let invoiced = second
            .broker::<FileBroker>()
            .subscriber("orders")
            .received::<Order>();
        assert_eq!(invoiced.first(), Some(&Order { id: 1 }));
        second.shutdown().await?;

        fs::remove_file(&path)?;
        Ok(())
    }
}
