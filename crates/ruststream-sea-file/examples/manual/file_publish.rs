//! The service of `examples/file_publish.rs` written without the macros: an `after_startup` hook
//! appends a record to the stream file through the form's `Publish`.
//!
//! ```text
//! cargo test --example manual_file_publish --features testing
//! cargo run --example manual_file_publish --features testing -- run
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

/// Ships every order appended under `orders`.
struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "shipping");
        ready(Ok(()))
    }
}

// --8<-- [start:recorder]
/// The record a service leaves in the file each time it starts.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "lifecycle")]
struct Started {
    service: String,
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/shop/orders.ss"),
        |b| {
            b.include(subscriber("orders", Ship).build());
            // Runs once the subscriptions are open; a refused append fails the startup.
            b.after_startup(Publish, async move |file| {
                file.message(&Started {
                    service: "shipping".to_owned(),
                })
                .publish()
                .await
            });
        },
    )
}
// --8<-- [end:recorder]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Started, app};

    #[tokio::test]
    async fn the_service_records_its_start_in_the_file() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<FileBroker>()
            .published::<Started>("lifecycle")
            .assert_called_once()
            .with(&Started {
                service: "shipping".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
