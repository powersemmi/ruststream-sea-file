//! The service of `examples/sea_asyncapi.rs` without the macros: the handlers are named types and
//! `main` is written by hand. The tests read the same document.
//!
//! ```text
//! cargo run --example manual_sea_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_sea_asyncapi --features asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::prelude::*;

// --8<-- [start:servers]
/// The stream file the service records orders in.
fn file_broker() -> FileBroker {
    FileBroker::new("/var/lib/ruststream/orders.ss")
}

/// The pipeline the service is a stage of.
fn stdio_broker() -> StdioBroker {
    StdioBroker::new()
}
// --8<-- [end:servers]

// --8<-- [start:channels]
/// Reads `orders` through the descriptor and appends a receipt under `receipts`.
struct Record;

impl Handle<Order, Receipt> for Record {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

/// Reads `lines` on standard input by its bare stream key and answers under `lines.out`.
struct Tee;

impl Handle<Order, Receipt> for Tee {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}
// --8<-- [end:channels]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "1.0.0"))
        .with_broker_labeled("file", file_broker(), |b| {
            let record = subscriber(FileStream::new("orders"), Record);
            b.include(record.reply().to("receipts").build())
                .out_reply(FilePublish);
        })
        .with_broker_labeled("pipe", stdio_broker(), |b| {
            b.include(subscriber("lines", Tee).reply().to("lines.out").build())
                .out_reply(StdioPublish)
                .out_retry(StdioPublish)
                .to("lines.retry");
        })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the mount site names the reply's stream key.
impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::app;

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(&app()).to_json()?)?)
    }

    #[test]
    fn each_broker_is_a_server_without_a_host() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"],
            json!({
                "file": { "protocol": "file", "description": "/var/lib/ruststream/orders.ss" },
                "pipe": { "protocol": "stdio" },
            })
        );
        Ok(())
    }

    #[test]
    fn a_file_channel_carries_its_stream_key() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        for key in ["orders", "receipts"] {
            assert_eq!(
                channels[key]["bindings"],
                json!({ "x-ruststream-file": { "streamKey": key } }),
                "{channels}",
            );
        }
        Ok(())
    }

    #[test]
    fn a_stdio_channel_describes_nothing() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        for key in ["lines", "lines.out"] {
            assert!(channels[key].get("bindings").is_none(), "{channels}");
        }
        Ok(())
    }
}
