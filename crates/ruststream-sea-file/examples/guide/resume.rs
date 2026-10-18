//! The invoicing service of the Resuming and seeking guide: the handler reads the delivery's
//! position and the checkpoint the last run stored, and the app opens the subscription at that
//! checkpoint.
//!
//! ```text
//! cargo run --example guide_resume --features testing -- run
//! cargo test --example guide_resume --features testing
//! ```

use std::convert::Infallible;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::{fs, io};

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
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

/// The app's state: each field reaches a handler as `State<FieldType>`.
#[derive(Debug, Clone, FromRef)]
struct Invoicing {
    checkpoint: Checkpoint,
}
// --8<-- [end:checkpoint]

// --8<-- [start:service]
#[subscriber(FileStream::new("orders"))]
async fn invoice(
    order: &Order,
    Ctx(at): Ctx<Position>,
    State(checkpoint): State<Checkpoint>,
) -> HandlerOutcome {
    let FilePosition::Sequence(sequence) = at else {
        return HandlerOutcome::drop();
    };
    // The run opens at the stored sequence, so the order the last run ended on comes again.
    if checkpoint.load().is_some_and(|stored| sequence <= stored) {
        return HandlerOutcome::drop();
    }
    tracing::info!(order.id, sequence, "invoiced");
    if let Err(error) = checkpoint.store(sequence) {
        tracing::error!(order.id, sequence, %error, "checkpoint not stored");
    }
    HandlerOutcome::ack()
}

fn invoicing(file: FileBroker, checkpoint: Checkpoint) -> impl App<State = Invoicing> {
    let start = checkpoint
        .load()
        .map_or(FilePosition::beginning(), FilePosition::sequence);
    RustStream::new(AppInfo::new("invoicing", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Invoicing { checkpoint }))
        .with_broker(file, |b| {
            b.include(invoice.start_at(start));
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
#[path = "../support/recorded_stream.rs"]
mod recorded_stream;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sea_file::file::prelude::*;

    use super::recorded_stream::record;
    use super::{Checkpoint, Invoicing, Order, invoicing};

    const PATH: &str = "/var/lib/shop/orders.ss";

    /// A checkpoint file of this test's own, holding `stored` when it is `Some`.
    fn checkpoint(name: &str, stored: Option<u64>) -> Result<Checkpoint, Box<dyn Error>> {
        let path = env::temp_dir().join(format!(
            "ruststream-sea-guide-resume-{name}-{}.checkpoint",
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

    /// Two runs of the service on one stream file: the second opens at the order the first ended
    /// on, and passes it over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_opens_at_the_order_the_last_run_ended_on() -> Result<(), Box<dyn Error>> {
        // A file of this run's own, so nothing an earlier run appended is read.
        let path = env::temp_dir().join(format!("guide-resume-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();
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
        // The publish returns once the subscription has handled a delivery, and the first one
        // is the order the last run ended on.
        let broker = second.broker::<FileBroker>();
        let resumed = broker.subscriber("orders");
        assert_eq!(resumed.received::<Order>().first(), Some(&Order { id: 2 }));
        assert_eq!(resumed.outcomes().first(), Some(&Outcome::Drop));
        second.shutdown().await?;

        fs::remove_file(&path)?;
        fs::remove_file(&stored.path)?;
        Ok(())
    }
}
