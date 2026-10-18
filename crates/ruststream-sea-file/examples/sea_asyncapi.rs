//! What the `asyncapi` feature puts into the generated document for a service on both
//! transports. The tests read the document the service builds.
//!
//! ```text
//! cargo run --example sea_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example sea_asyncapi --features asyncapi
//! ```

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
#[subscriber(FileStream::new("orders"), reply("receipts"))]
async fn record(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

/// Reads `lines` on standard input by its bare stream key and answers under `lines.out`.
#[subscriber("lines", reply("lines.out"))]
async fn tee(order: &Order) -> Receipt {
    Receipt { order: order.id }
}
// --8<-- [end:channels]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "1.0.0"))
        .with_broker_labeled("file", file_broker(), |b| {
            b.include(record).out_reply(FilePublish);
        })
        .with_broker_labeled("pipe", stdio_broker(), |b| {
            b.include(tee)
                .out_reply(StdioPublish)
                .out_retry(StdioPublish)
                .to("lines.retry");
        })
}
// --8<-- [end:app]

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Receipt {
    order: u64,
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
