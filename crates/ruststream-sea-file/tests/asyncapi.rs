//! What the two transports put into a generated `AsyncAPI` document.
//!
//! The specification's protocol keys are a closed list and neither a file transport nor stdio is
//! on it, so a binding object of their own is not available: what the file transport knows travels
//! in the `x-ruststream-file` extension, on the channel a subscription reads and on the channel a
//! publish appends to, and stdio says nothing beyond its protocol name.
//! Both servers are in-process, so neither carries a host, and neither protocol has versions a
//! client has to match, so neither carries a protocol version.

#![cfg(all(feature = "testing", feature = "asyncapi"))]

use ruststream::asyncapi::build_spec;
use ruststream::nonzero;
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::{FileBroker, StdioBroker, StdioPublish};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The stream file the document describes; nothing opens it, the description is built from the
/// configuration alone.
const PATH: &str = "/var/lib/ruststream/orders.ss";

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Receipt {
    id: u64,
}

/// Reads one stream key of the file, through the descriptor that describes its channel.
#[subscriber(FileStream::new("orders"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Reads a stream key on standard input, through the bare-name form, which carries no descriptor
/// and therefore describes nothing, and answers on a second key.
#[subscriber("lines", publish("lines.out"))]
async fn tee(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn document() -> Value {
    let app = RustStream::new(AppInfo::new("sea", "0.1.0"))
        .with_broker_labeled("file", FileBroker::new(PATH), |b| {
            b.include(reconcile)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
        })
        .with_broker_labeled("pipe", StdioBroker::new(), |b| {
            b.include(tee)
                .out(Reply, StdioPublish)
                .out_retry(StdioPublish)
                .to("lines.retry");
        });
    let json = build_spec(&app)
        .to_json()
        .expect("the generated document must serialize");
    serde_json::from_str(&json).expect("valid JSON")
}

/// The file channel carries the stream key the subscription reads, under the extension key this
/// crate owns. The path is not repeated here: it is the server's description.
#[test]
fn a_file_subscription_describes_its_stream_key() {
    let value = document();
    let bindings = &value["channels"]["orders"]["bindings"];

    assert_eq!(bindings["x-ruststream-file"]["streamKey"], "orders");
    // An extension is not a binding, so it carries no version of the specification's.
    assert!(
        bindings["x-ruststream-file"]
            .get("bindingVersion")
            .is_none(),
        "an x- extension must not claim a bindingVersion: {bindings}",
    );
}

/// Nothing is bound to a subscription opened by bare name, so there is nothing to describe: the
/// stdio channel comes out with no bindings object at all.
#[test]
fn a_stdio_channel_describes_nothing() {
    let value = document();

    assert!(
        value["channels"]["lines"].get("bindings").is_none(),
        "stdio has no binding in the specification and nothing of its own to add: {}",
        value["channels"]["lines"],
    );
}

/// Both servers are in-process: the protocol name and, for the file, the path a reader needs to
/// find the stream. No host, because there is nothing to connect to over a network, and no
/// protocol version, because neither name covers incompatible versions.
#[test]
fn the_servers_report_a_coordinate_and_nothing_else() {
    let value = document();

    let file = &value["servers"]["file"];
    assert_eq!(file["protocol"], "file");
    assert_eq!(file["description"], PATH);
    assert!(file.get("host").is_none(), "{file}");
    assert!(file.get("protocolVersion").is_none(), "{file}");
    assert!(file.get("bindings").is_none(), "{file}");

    let pipe = &value["servers"]["pipe"];
    assert_eq!(pipe["protocol"], "stdio");
    assert!(pipe.get("host").is_none(), "{pipe}");
    assert!(pipe.get("description").is_none(), "{pipe}");
    assert!(pipe.get("protocolVersion").is_none(), "{pipe}");
    assert!(pipe.get("bindings").is_none(), "{pipe}");
}

/// The excerpt the documentation shows, held to the document the crate actually builds.
#[test]
fn the_documented_excerpt_is_the_one_the_crate_emits() {
    let value = document();
    let expected: Value = serde_json::from_str(
        r#"{
          "file": {
            "protocol": "file",
            "description": "/var/lib/ruststream/orders.ss"
          },
          "pipe": {
            "protocol": "stdio"
          }
        }"#,
    )
    .expect("valid JSON");

    assert_eq!(value["servers"], expected);
    assert_eq!(
        value["channels"]["orders"]["bindings"],
        serde_json::json!({ "x-ruststream-file": { "streamKey": "orders" } }),
    );
}

/// A registration's cap and dead-letter destination reach the document as the framework's own
/// extension, and the destination becomes a channel the service publishes to.
#[test]
fn the_declaration_reaches_the_document_and_the_destination_is_a_channel() {
    let value = document();

    assert_eq!(
        value["operations"]["receive_orders"]["x-ruststream-retry"],
        serde_json::json!({ "maxAttempts": 3, "deadLetter": "orders.dead" }),
    );
    assert_eq!(value["channels"]["orders.dead"]["address"], "orders.dead");
}

/// A stream key names both ends of the file, so a channel the service publishes to carries the
/// same extension a channel it reads does. The key is the destination the mount site resolved,
/// which is the only place a publish policy can learn it: the policy itself holds no name.
#[test]
fn a_file_publish_destination_describes_its_stream_key() {
    let value = document();

    assert_eq!(
        value["channels"]["orders.dead"]["bindings"],
        serde_json::json!({ "x-ruststream-file": { "streamKey": "orders.dead" } }),
    );
}

/// Stdio has nothing of the protocol's to report on either end, so the key a reply is written to
/// comes out as a channel with no bindings object at all.
#[test]
fn a_stdio_publish_destination_describes_nothing() {
    let value = document();

    let reply = &value["channels"]["lines.out"];
    assert_eq!(reply["address"], "lines.out");
    assert!(
        reply.get("bindings").is_none(),
        "a stdio publisher has nothing of the protocol's to report: {reply}",
    );
}
