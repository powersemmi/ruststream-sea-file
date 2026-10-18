//! The invoicing service of the Resuming and seeking guide written without the macros: the
//! handler is a type whose body reads the delivery's position from `FileContext` and the
//! checkpoint from the state, and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_guide_resume --features testing -- run
//! cargo test --example manual_guide_resume --features testing
//! ```

use std::convert::Infallible;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::{fs, io};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/common/mod.rs"]
mod common;

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

// --8<-- [start:checkpoint]
/// The sequence of the last order the service invoiced, kept in a file of its own.
#[derive(Debug, Clone)]
struct Checkpoint {
    path: Arc<Path>,
}

impl Checkpoint {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
        }
    }

    /// The stored sequence; `None` until the first order is invoiced.
    fn load(&self) -> Option<u64> {
        fs::read_to_string(&self.path).ok()?.trim().parse().ok()
    }

    fn store(&self, sequence: u64) -> io::Result<()> {
        fs::write(&self.path, sequence.to_string())
    }
}

#[derive(Debug, Clone)]
struct Invoicing {
    checkpoint: Checkpoint,
}
// --8<-- [end:checkpoint]

// --8<-- [start:service]
struct Invoice;

impl Handle<Order, (), (), FileContext, Invoicing> for Invoice {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, FileContext, Invoicing>,
    ) -> Result<(), HandlerOutcome> {
        let FilePosition::Sequence(sequence) = ctx.context(Position) else {
            return Err(HandlerOutcome::drop());
        };
        let checkpoint = &ctx.state().checkpoint;
        // The run opens at the stored sequence, so the order the last run ended on comes again.
        if checkpoint.load().is_some_and(|stored| sequence <= stored) {
            return Err(HandlerOutcome::drop());
        }
        tracing::info!(order.id, sequence, "invoiced");
        if let Err(error) = checkpoint.store(sequence) {
            tracing::error!(order.id, sequence, %error, "checkpoint not stored");
        }
        Ok(())
    }
}

fn invoicing(file: FileBroker, checkpoint: Checkpoint) -> impl App<State = Invoicing> {
    let start = checkpoint
        .load()
        .map_or(FilePosition::beginning(), FilePosition::sequence);
    RustStream::new(AppInfo::new("invoicing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Invoicing { checkpoint }))
        .with_broker(file, |b| {
            b.include(
                subscriber(FileStream::new("orders"), Invoice)
                    .start_at(start)
                    .build(),
            );
        })
}

fn main() -> ExitCode {
    run_main(|| {
        invoicing(
            FileBroker::new("/var/lib/shop/orders.ss"),
            Checkpoint::new("/var/lib/shop/invoicing.checkpoint"),
        )
    })
}
// --8<-- [end:service]

#[cfg(test)]
#[path = "../../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Checkpoint, Invoice, Invoicing, Order, common, invoicing};

    const PATH: &str = "/var/lib/shop/orders.ss";

    /// A checkpoint file of this test's own, holding `stored` when it is `Some`.
    fn checkpoint(name: &str, stored: Option<u64>) -> Result<Checkpoint, Box<dyn Error>> {
        let path = env::temp_dir().join(format!(
            "ruststream-sea-manual-guide-resume-{name}-{}.checkpoint",
            process::id()
        ));
        match stored {
            Some(sequence) => fs::write(&path, sequence.to_string())?,
            None => {
                let _ = fs::remove_file(&path);
            }
        }
        Ok(Checkpoint::new(path))
    }

    fn read(tb: &TestApp<Invoicing>) -> Vec<u64> {
        tb.broker::<FileBroker>()
            .subscriber("orders")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect()
    }

    #[tokio::test]
    async fn a_run_resumes_at_the_stored_sequence() -> Result<(), Box<dyn Error>> {
        // Five orders; the last run invoiced the first three.
        let file = FileBroker::new(PATH);
        record(&file, (1..=5).map(|id| Order { id })).await?;
        let stored = checkpoint("resumes", Some(3))?;
        let tb = TestApp::start(invoicing(file, stored.clone())).await?;
        tb.settle().await?;

        // Order 3 comes again and is passed over; 4 and 5 are invoiced.
        assert_eq!(read(&tb), [3, 4, 5]);
        assert_eq!(
            tb.broker::<FileBroker>().subscriber("orders").outcomes(),
            [Outcome::Drop, Outcome::Ack, Outcome::Ack],
        );
        assert_eq!(stored.load(), Some(5));
        tb.shutdown().await?;
        fs::remove_file(&stored.path)?;
        Ok(())
    }

    #[tokio::test]
    async fn the_first_run_reads_the_file_from_the_beginning() -> Result<(), Box<dyn Error>> {
        let file = FileBroker::new(PATH);
        record(&file, (1..=2).map(|id| Order { id })).await?;
        let stored = checkpoint("first-run", None)?;
        let tb = TestApp::start(invoicing(file, stored.clone())).await?;
        tb.settle().await?;

        assert_eq!(read(&tb), [1, 2]);
        assert_eq!(stored.load(), Some(2));
        tb.shutdown().await?;
        fs::remove_file(&stored.path)?;
        Ok(())
    }

    /// The service opened at `start` instead of at its checkpoint.
    fn opened_at(file: FileBroker, start: FilePosition) -> impl App<State = Invoicing> {
        let checkpoint = Checkpoint::new(env::temp_dir().join("unused.checkpoint"));
        RustStream::new(AppInfo::new("invoicing", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(Invoicing { checkpoint }))
            .with_broker(file, |b| {
                b.include(
                    subscriber(FileStream::new("orders"), Invoice)
                        .start_at(start)
                        .build(),
                );
            })
    }

    /// Two runs of the service on one stream file: the second opens at the order the first ended
    /// on. A run opened one sequence past the last order the file holds is refused.
    #[test]
    fn a_restart_opens_at_the_order_the_last_run_ended_on() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("manual-guide-resume");
            let stored = checkpoint("restart", None)?;

            let first = TestApp::start_live(invoicing(FileBroker::new(&path), stored.clone())).await?;
            for id in [1, 2] {
                first
                    .broker::<FileBroker>()
                    .message(&Order { id })
                    .publish()
                    .await?;
            }
            first.shutdown().await?;
            assert_eq!(stored.load(), Some(2));

            let second = TestApp::start_live(invoicing(FileBroker::new(&path), stored.clone())).await?;
            second
                .broker::<FileBroker>()
                .message(&Order { id: 3 })
                .publish()
                .await?;
            // The publish returns once the subscription handled one delivery: the first one.
            assert_eq!(read(&second).first(), Some(&2));
            second.shutdown().await?;

            // The file holds orders 1 to 3, so a run opened after the last of them is refused.
            let past_the_end = TestApp::start_live(opened_at(
                FileBroker::new(&path),
                FilePosition::sequence(4),
            ))
            .await;
            assert!(past_the_end.is_err(), "sequence 4 is not written yet");

            fs::remove_file(&path)?;
            fs::remove_file(&stored.path)?;
            Ok(())
        })
    }
}
