//! The shipping service of the Retries and dead letters guide written without the macros: each
//! handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main`
//! is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_retries --features testing -- run
//! cargo test --example manual_guide_retries --features testing
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// The orders whose payment has cleared; stands in for the payment service's client.
#[derive(Debug, Default)]
struct Payments {
    cleared: Mutex<HashSet<u64>>,
}

impl Payments {
    fn has_cleared(&self, order: u64) -> bool {
        self.cleared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&order)
    }

    #[cfg(test)]
    fn clear(&self, order: u64) {
        self.cleared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(order);
    }
}

#[derive(Debug, Default, Clone)]
struct Clients {
    payments: Arc<Payments>,
}

// --8<-- [start:ship]
/// How long an order waits before the service asks the payment service again.
const RECHECK: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

struct Ship;

impl Handle<Order, (), (), (), Clients> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !ctx.state().payments.has_cleared(order.id) {
            // The payment may clear later: ask again after a pause.
            return ready(Err(HandlerOutcome::retry_after(RECHECK)));
        }
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
}
// --8<-- [end:ship]

// --8<-- [start:dead_letters]
struct Review;

impl Handle<Order, (), (), (), Clients> for Review {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(order.id, deliveries, "unpaid order needs a person to look at it");
        ready(Ok(()))
    }
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
fn shipping(file: FileBroker) -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("shipping", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(file, |b| {
            // Three deliveries of one order, the first included; then the dead-letter key.
            b.include(subscriber(FileStream::new("orders"), Ship).build())
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
            b.include(subscriber(FileStream::new("orders.dead"), Review).build());
        })
}

fn app() -> impl App<State = Clients> {
    shipping(FileBroker::new("/var/lib/shop/orders.ss"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

// `#[outgoing(name = "orders")]` by hand: the type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::{env, fs, process};

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sea_file::file::prelude::*;

    use super::{Clients, Order, Payments, RECHECK, app, shipping};

    #[tokio::test(start_paused = true)]
    async fn an_order_ships_once_its_payment_clears() -> Result<(), Box<dyn Error>> {
        let payments = Arc::new(Payments::default());
        let clients = Clients {
            payments: Arc::clone(&payments),
        };
        let tb = TestApp::with_state(app(), |_| clients).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        payments.clear(1);
        tb.advance(RECHECK).await?;

        assert_eq!(
            tb.broker::<FileBroker>().subscriber("orders").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        // The copy is a second append under `orders`, with the retry count raised by one.
        tb.broker::<FileBroker>()
            .published::<Order>("orders")
            .assert_called(2)
            .with(&Order { id: 1 })
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    /// The body both modes run: an order that never clears is delivered three times, then
    /// written under the dead-letter key, where `review` reads it.
    async fn an_unpaid_order_goes_to_the_dead_letters(
        tb: TestApp<Clients>,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<FileBroker>()
            .message(&Order { id: 2 })
            .publish()
            .await?;
        tb.advance(RECHECK).await?;
        tb.advance(RECHECK).await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called(3);
        tb.broker::<FileBroker>()
            .published::<Order>("orders.dead")
            .assert_called_once()
            .with(&Order { id: 2 })
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.broker::<FileBroker>()
            .subscriber("orders.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_unpaid_order_goes_to_the_dead_letters_in_process() -> Result<(), Box<dyn Error>> {
        an_unpaid_order_goes_to_the_dead_letters(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unpaid_order_goes_to_the_dead_letters_on_a_stream_file()
    -> Result<(), Box<dyn Error>> {
        // A file of this run's own, so nothing an earlier run appended is read.
        let path = env::temp_dir().join(format!("manual-guide-retries-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();

        let tb = TestApp::start_live(shipping(FileBroker::new(&path))).await?;
        an_unpaid_order_goes_to_the_dead_letters(tb).await?;
        fs::remove_file(&path)?;
        Ok(())
    }
}
