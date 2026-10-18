//! The service the Testing guide tests: a checkout that answers each order with a receipt, holds
//! a large order for a fraud check and sends an order held three times to review.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// The largest order the checkout confirms at once; a larger one is held for a fraud check.
const CHECK_LIMIT_CENTS: u64 = 100_000;

/// How long a held order waits before the checkout looks at it again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
    amount_cents: u64,
}

#[subscriber(FileStream::new("orders"), reply)]
async fn checkout(order: &Order) -> Result<Receipt, HandlerOutcome> {
    if order.amount_cents == 0 {
        tracing::warn!(order.id, "order without an amount dropped");
        return Err(HandlerOutcome::drop());
    }
    if order.amount_cents > CHECK_LIMIT_CENTS {
        tracing::info!(order.id, "order held for a fraud check");
        return Err(HandlerOutcome::retry_after(HOLD));
    }
    Ok(Receipt {
        order: order.id,
        amount_cents: order.amount_cents,
    })
}

fn service(file: FileBroker) -> RustStream {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(file, |b| {
        b.include(checkout)
            .max_attempts(nonzero!(3u32))
            .dead_letter("orders.review");
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(FileBroker::new("/var/lib/shop/orders.ss"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sea_file::file::prelude::*;

    use super::{HOLD, Order, Receipt, app, service};

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn an_order_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        an_order_gets_a_receipt_in(tb).await
    }

    async fn an_order_gets_a_receipt_in(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<FileBroker>()
            .message(&Order {
                id: 1,
                amount_cents: 2_500,
            })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order: 1,
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn an_order_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .message(&Order {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn an_order_held_three_times_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let large = Order {
            id: 3,
            amount_cents: 250_000,
        };

        tb.broker::<FileBroker>().message(&large).publish().await?;
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The delay ends in a copy of the order, appended under `orders`.
        tb.advance(HOLD).await?;
        tb.broker::<FileBroker>()
            .published::<Order>("orders")
            .assert_called(2)
            .with(&large)
            .with_header(RETRY_COUNT_HEADER, "1");

        // The third delivery is the last one: it is written under `orders.review` as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<FileBroker>().subscriber("orders").outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<FileBroker>()
            .published::<Order>("orders.review")
            .assert_called_once()
            .with(&large);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_gets_a_receipt_on_a_stream_file() -> Result<(), Box<dyn Error>> {
        // A file of this run's own, so nothing an earlier run appended is read.
        let path = env::temp_dir().join(format!("checkout-{}.ss", process::id()));
        let file = FileBroker::new(path.to_string_lossy());
        let tb = TestApp::start_live(service(file)).await?;
        an_order_gets_a_receipt_in(tb).await?;
        fs::remove_file(&path)?;
        Ok(())
    }
    // --8<-- [end:live]
}
