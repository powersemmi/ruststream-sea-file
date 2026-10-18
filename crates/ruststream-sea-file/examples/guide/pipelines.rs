//! The stage of the Shell pipelines guide written without the macros: the handler is a type with
//! an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! This is the binary a pipeline runs, and the one the guide's tests spawn as a child process;
//! `manual_guide_pipelines_tests` compiles this file again to run its tests.
//!
//! ```text
//! echo '[orders] {"id":7,"quantity":3,"unit_cents":250}' \
//!     | cargo run --example manual_guide_pipelines --features testing -- run
//! cargo build --example manual_guide_pipelines --features testing
//! cargo test --example manual_guide_pipelines_tests --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

// --8<-- [start:stage]
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    quantity: u64,
    unit_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: u64,
    total_cents: u64,
}

struct Price;

impl Handle<Order, Receipt> for Price {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Receipt, HandlerOutcome> {
        let total_cents = order.quantity * order.unit_cents;
        // The log goes to standard error; standard output carries only messages.
        tracing::info!(order.id, total_cents, "order priced");
        Ok(Receipt {
            order: order.id,
            total_cents,
        })
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(StdioBroker::new(), |b| {
        // A deferred copy leaves on standard output under this key, for a later stage.
        b.include(subscriber("orders", Price).reply().build())
            .out_retry(Publish)
            .to("orders.retry");
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:stage]

// `#[outgoing(name = ..)]` by hand: each type names the stream key it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;

    use super::{Order, Receipt, app};

    // --8<-- [start:test]
    #[tokio::test]
    async fn an_order_line_gets_a_receipt_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<StdioBroker>()
            .message(&Order {
                id: 7,
                quantity: 3,
                unit_cents: 250,
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order: 7,
                total_cents: 750,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:test]

    /// How long the stage is given to answer a line.
    const ANSWER_TIMEOUT: Duration = Duration::from_secs(20);

    /// How long a stage whose input ended is watched for an exit.
    const EXIT_WATCH: Duration = Duration::from_secs(2);

    /// The stage binary the current build made beside this test, found the way the crate's
    /// pipeline suite finds its own: the newest plain build in the first `examples` directory
    /// above the test binary that holds one.
    fn stage_binary() -> PathBuf {
        let name = "manual_guide_pipelines";
        let exe = std::env::current_exe().expect("the test binary has a path");
        exe.ancestors()
            .skip(1)
            .map(|ancestor| ancestor.join("examples"))
            .find_map(|dir| example_in(&dir, name))
            .unwrap_or_else(|| {
                panic!(
                    "the `{name}` example must be built beside the tests, in an `examples` \
                     directory above {}",
                    exe.display(),
                )
            })
    }

    /// The newest build of the `name` example in one `examples` directory, under its plain name
    /// or a hashed one, leaving out test-harness builds.
    fn example_in(dir: &Path, name: &str) -> Option<PathBuf> {
        std::fs::read_dir(dir)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension().is_none()
                    && path
                        .file_name()
                        .and_then(|file| file.to_str())
                        .is_some_and(|file| file == name || file.starts_with(&format!("{name}-")))
                    && !is_test_harness(path)
            })
            .filter_map(|path| {
                let built = std::fs::metadata(&path)
                    .and_then(|meta| meta.modified())
                    .ok()?;
                Some((built, path))
            })
            .max_by_key(|(built, _)| *built)
            .map(|(_, path)| path)
    }

    /// Whether `binary` is a test-harness build: libtest names its thread setting in every
    /// binary it links into, and the example proper never links it.
    fn is_test_harness(binary: &Path) -> bool {
        let marker = b"RUST_TEST_THREADS";
        std::fs::read(binary).is_ok_and(|bytes| {
            bytes
                .windows(marker.len())
                .any(|window| window == marker.as_slice())
        })
    }

    /// The stage on real pipes: it answers the `orders` lines, passes over a line under another
    /// key, and keeps running once its input has ended.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stage_answers_its_key_and_outlives_its_input() -> Result<(), Box<dyn Error>> {
        let mut stage = Command::new(stage_binary())
            .arg("run")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;

        let mut input = stage.stdin.take().ok_or("the stage reads standard input")?;
        input
            .write_all(
                b"[orders] {\"id\":7,\"quantity\":3,\"unit_cents\":250}\n\
                  [refunds] {\"id\":8,\"quantity\":1,\"unit_cents\":100}\n\
                  [orders] {\"id\":9,\"quantity\":2,\"unit_cents\":50}\n",
            )
            .await?;
        input.flush().await?;

        let mut output = BufReader::new(stage.stdout.take().ok_or("the stage writes output")?)
            .lines();
        let mut answers = Vec::new();
        while answers.len() < 2 {
            let line = tokio::time::timeout(ANSWER_TIMEOUT, output.next_line())
                .await?
                .map_err(|error| format!("the stage's output is unreadable: {error}"))?
                .ok_or("the stage closed its output")?;
            answers.push(line);
        }
        for (answer, receipt) in answers.iter().zip([
            r#"{"order":7,"total_cents":750}"#,
            r#"{"order":9,"total_cents":100}"#,
        ]) {
            assert!(answer.contains("| receipts |"), "{answer}");
            assert!(answer.ends_with(receipt), "{answer}");
        }

        // End of input: the stage still runs, and a signal is what stops it.
        drop(input);
        let exited = tokio::time::timeout(EXIT_WATCH, stage.wait()).await;
        assert!(exited.is_err(), "the stage exited at the end of its input: {exited:?}");
        stage.kill().await?;
        Ok(())
    }
}
