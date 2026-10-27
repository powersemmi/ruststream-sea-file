//! The envelopes a service chooses, run through the production apps in process: a round trip per
//! format on a stream file and on a pipe, records of another format read as payloads, `NoOp`
//! refusing what it cannot write, and headers parsed only when something reads them.

#![cfg(all(feature = "testing", feature = "json"))]

use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::HeaderMap;
use ruststream::codec::JsonCodec;
use ruststream::testing::TestApp;
use ruststream_sea_file::envelope::{Envelope, Medium, SealError};
use ruststream_sea_file::prelude::*;
use serde::{Deserialize, Serialize};

use crate::common;

/// The header contract an order and its receipt carry.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Tenant {
    tenant: String,
}

/// A header contract that may be absent.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct MaybeTenant {
    tenant: Option<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders", headers = Tenant)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts", headers = Tenant)]
struct Receipt {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Receipts;

/// A reading with no header contract.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "feed")]
struct Reading {
    millivolts: u32,
}

/// What a subscription saw of a delivery's tenant header.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "seen")]
struct Seen {
    millivolts: u32,
    tenant: Option<String>,
}

fn acme() -> Tenant {
    Tenant {
        tenant: "acme".to_owned(),
    }
}

/// Confirms an order under the tenant it arrived with, on either form.
#[subscriber("orders")]
async fn confirm(
    order: &Order,
    Headers(tenant): Headers<Tenant>,
    Out(receipts): Out<impl Publisher, Receipts>,
) -> HandlerOutcome {
    let receipt = Receipt { order: order.id };
    if receipts
        .message(&receipt)
        .with_headers(&tenant)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Reports a reading and whatever tenant header it carried.
#[subscriber("feed", reply)]
async fn report(reading: &Reading, Headers(meta): Headers<MaybeTenant>) -> Seen {
    Seen {
        millivolts: reading.millivolts,
        tenant: meta.tenant,
    }
}

const FILE: &str = "/var/lib/ruststream/envelopes.ss";

#[tokio::test]
async fn framed_json_carries_the_headers_through_a_stream_file() -> Result<(), Box<dyn Error>> {
    let broker = FileBroker::new(FILE).envelope(Framed::new(JsonCodec));
    let app = RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm).out(Receipts, FilePublish).build();
    });
    let tb = TestApp::start(app).await?;

    tb.broker::<FileBroker<Framed<JsonCodec>>>()
        .message(&Order { id: 7 })
        .with_headers(&acme())
        .publish()
        .await?;

    tb.broker::<FileBroker<Framed<JsonCodec>>>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order: 7 })
        .with_header("tenant", "acme");
    tb.shutdown().await?;
    Ok(())
}

/// On a real stream file each record is the JSON document the page shows, and the subscription
/// reads the order and its header back out of it.
#[test]
fn framed_json_records_on_a_real_stream_file_are_json_documents() {
    common::on_a_file(async {
        let path = common::tmp_path("envelopes");
        let broker = FileBroker::new(&path).envelope(Framed::new(JsonCodec));
        let app = RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(broker, |b| {
            b.include(confirm).out(Receipts, FilePublish).build();
        });
        let tb = TestApp::start_live(app)
            .await
            .expect("start against the file");

        tb.broker::<FileBroker<Framed<JsonCodec>>>()
            .message(&Order { id: 7 })
            .with_headers(&acme())
            .publish()
            .await
            .expect("publish");
        tb.broker::<FileBroker<Framed<JsonCodec>>>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 })
            .with_header("tenant", "acme");
        tb.shutdown().await.expect("shutdown");

        let bytes = fs::read(&path).expect("the stream file is readable");
        let holds = |needle: &[u8]| bytes.windows(needle.len()).any(|window| window == needle);
        assert!(holds(
            br#"{"headers":{"tenant":"acme"},"payload":"{\"id\":7}"}"#
        ));
        assert!(holds(
            br#"{"headers":{"tenant":"acme"},"payload":"{\"order\":7}"}"#
        ));
        let _ = fs::remove_file(&path);
    });
}

#[tokio::test]
async fn framed_json_carries_the_headers_through_a_pipe() -> Result<(), Box<dyn Error>> {
    let broker = StdioBroker::new().envelope(Framed::new(JsonCodec));
    let app = RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm)
            .out(Receipts, StdioPublish)
            .out_retry(StdioPublish)
            .to("orders.retry")
            .build();
    });
    let tb = TestApp::start(app).await?;

    tb.broker::<StdioBroker<Framed<JsonCodec>>>()
        .message(&Order { id: 7 })
        .with_headers(&acme())
        .publish()
        .await?;

    tb.broker::<StdioBroker<Framed<JsonCodec>>>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order: 7 })
        .with_header("tenant", "acme");
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn no_envelope_carries_a_bare_payload_through_a_stream_file() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("plant", "0.1.0")).with_broker(
        FileBroker::new(FILE).envelope(NoOp),
        |b| {
            b.include(report);
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<FileBroker<NoOp>>()
        .message(&Reading { millivolts: 5 })
        .publish()
        .await?;

    tb.broker::<FileBroker<NoOp>>()
        .published::<Seen>("seen")
        .assert_called_once()
        .with(&Seen {
            millivolts: 5,
            tenant: None,
        });
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn no_envelope_carries_a_bare_payload_through_a_pipe() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("plant", "0.1.0")).with_broker(
        StdioBroker::new().envelope(NoOp),
        |b| {
            b.include(report).out_retry(StdioPublish).to("feed.retry");
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<StdioBroker<NoOp>>()
        .message(&Reading { millivolts: 5 })
        .publish()
        .await?;

    tb.broker::<StdioBroker<NoOp>>()
        .published::<Seen>("seen")
        .assert_called_once()
        .with(&Seen {
            millivolts: 5,
            tenant: None,
        });
    tb.shutdown().await?;
    Ok(())
}

/// Reads the feed as JSON documents, whatever the broker writes.
#[subscriber(FileStream::<Framed<JsonCodec>>::new("feed"), reply)]
async fn report_framed(reading: &Reading, Headers(meta): Headers<MaybeTenant>) -> Seen {
    Seen {
        millivolts: reading.millivolts,
        tenant: meta.tenant,
    }
}

/// The default envelope writes a reading with no headers as it is, which is no JSON document: the
/// subscription reading documents takes it for a payload with no headers.
#[tokio::test]
async fn a_record_of_another_format_reads_as_a_payload() -> Result<(), Box<dyn Error>> {
    let app =
        RustStream::new(AppInfo::new("plant", "0.1.0")).with_broker(FileBroker::new(FILE), |b| {
            b.include(report_framed);
        });
    let tb = TestApp::start(app).await?;

    tb.broker::<FileBroker>()
        .message(&Reading { millivolts: 5 })
        .publish()
        .await?;

    tb.broker::<FileBroker>()
        .subscriber("feed")
        .assert_called_once()
        .with(&Reading { millivolts: 5 });
    tb.broker::<FileBroker>()
        .published::<Seen>("seen")
        .assert_called_once()
        .with(&Seen {
            millivolts: 5,
            tenant: None,
        });
    tb.shutdown().await?;
    Ok(())
}

/// Forwards each order as a bare payload, for a stage that knows no envelope.
#[subscriber("orders")]
async fn forward(order: &Order, Out(feed): Out<impl Publisher, Feed>) -> HandlerOutcome {
    if feed
        .message(&Reading {
            millivolts: u32::try_from(order.id).unwrap_or(u32::MAX),
        })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[derive(OutSlot)]
#[publishes(Reading)]
struct Feed;

/// On a pipe that reads back what it writes, a bare line a stage wrote for a tool downstream
/// reaches a subscription reading documents as a payload with no headers.
#[tokio::test]
async fn a_line_of_another_format_reads_as_a_payload() -> Result<(), Box<dyn Error>> {
    let broker = StdioBroker::new()
        .loopback()
        .envelope(Framed::new(JsonCodec));
    let app = RustStream::new(AppInfo::new("stage", "0.1.0")).with_broker(broker, |b| {
        b.include(forward)
            .out(Feed, StdioPublish.envelope(NoOp))
            .out_retry(StdioPublish)
            .to("orders.retry")
            .build();
        b.include(report).out_retry(StdioPublish).to("feed.retry");
    });
    let tb = TestApp::start(app).await?;

    tb.broker::<StdioBroker<Framed<JsonCodec>>>()
        .message(&Order { id: 5 })
        .with_headers(&acme())
        .publish()
        .await?;

    tb.broker::<StdioBroker<Framed<JsonCodec>>>()
        .published::<Seen>("seen")
        .assert_called_once()
        .with(&Seen {
            millivolts: 5,
            tenant: None,
        });
    tb.shutdown().await?;
    Ok(())
}

/// A receipt with headers, written through a policy that has no room for them.
#[tokio::test]
async fn no_envelope_refuses_a_publish_with_a_header() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        FileBroker::new(FILE),
        |b| {
            b.include(confirm)
                .out(Receipts, FilePublish.envelope(NoOp))
                .build();
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<FileBroker>()
        .message(&Order { id: 7 })
        .with_headers(&acme())
        .publish()
        .await?;

    tb.broker::<FileBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::retry());
    tb.broker::<FileBroker>()
        .published::<Receipt>("receipts")
        .assert_not_called();
    tb.shutdown().await?;
    Ok(())
}

/// The error a refused publish returns names the header and the destination.
#[tokio::test]
async fn a_refused_publish_names_the_header_and_the_destination() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(
        FileBroker::new(FILE).envelope(NoOp),
        |b| {
            b.after_startup(FilePublish, async move |publisher| {
                publisher
                    .message(&Order { id: 7 })
                    .with_headers(&acme())
                    .publish()
                    .await
            });
        },
    );
    let Err(err) = TestApp::start(app).await else {
        panic!("the publish with a header must fail");
    };
    let text = format!("{err:#}");
    assert!(text.contains("'orders'"), "{text}");
    assert!(text.contains("'tenant'"), "{text}");
    Ok(())
}

/// Bytes a codec does not touch.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "lines")]
struct Line(Vec<u8>);

/// A pipe with no envelope cannot carry a payload a line would cut, and the publish says so.
#[tokio::test]
async fn no_envelope_on_a_pipe_refuses_a_payload_a_line_cannot_carry() -> Result<(), Box<dyn Error>>
{
    let app = RustStream::new(AppInfo::new("stage", "0.1.0")).with_broker(
        StdioBroker::new().envelope(NoOp),
        |b| {
            b.after_startup(StdioPublish, async move |publisher| {
                publisher
                    .message(&Line(b"first\nsecond".to_vec()))
                    .publish()
                    .await
            });
        },
    );
    let Err(err) = TestApp::start(app).await else {
        panic!("the publish of a multi-line payload must fail");
    };
    let text = format!("{err:#}");
    assert!(text.contains("'lines'"), "{text}");
    assert!(text.contains("a line cannot carry"), "{text}");
    Ok(())
}

/// The default envelope, counting how often it parses headers.
#[derive(Debug, Clone, Default)]
struct Counting(Arc<AtomicUsize>);

impl Envelope for Counting {
    type Opened = <Rs as Envelope>::Opened;

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        Rs.seal(headers, payload, medium)
    }

    fn open(&self, record: &[u8]) -> Self::Opened {
        Rs.open(record)
    }

    fn payload<'r>(&self, record: &'r [u8], opened: &'r Self::Opened) -> &'r [u8] {
        Rs.payload(record, opened)
    }

    fn headers(&self, record: &[u8], opened: &Self::Opened) -> HeaderMap {
        self.0.fetch_add(1, Ordering::Relaxed);
        Rs.headers(record, opened)
    }
}

ruststream::register_testable_broker!(FileBroker<Counting>);

/// Reads the payload alone.
#[subscriber("plain")]
async fn plain(reading: &Reading) -> HandlerOutcome {
    let _ = reading.millivolts;
    HandlerOutcome::ack()
}

/// Reads the payload and the headers.
#[subscriber("tagged")]
async fn tagged(reading: &Reading, Headers(meta): Headers<MaybeTenant>) -> HandlerOutcome {
    let _ = (reading.millivolts, meta.tenant);
    HandlerOutcome::ack()
}

/// A reading as a test sends it: under a tenant header, to the subscription the test names.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(headers = Tenant)]
struct Sample {
    millivolts: u32,
}

/// A subscription that never reads headers never parses the envelope; one that reads them parses
/// it once per delivery.
#[tokio::test]
async fn headers_are_parsed_only_where_a_handler_reads_them() -> Result<(), Box<dyn Error>> {
    let envelope = Counting::default();
    let parsed = Arc::clone(&envelope.0);
    let app = RustStream::new(AppInfo::new("plant", "0.1.0")).with_broker(
        FileBroker::new(FILE).envelope(envelope),
        |b| {
            b.include(plain);
            b.include(tagged);
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<FileBroker<Counting>>()
        .message(&Sample { millivolts: 5 })
        .to("plain")
        .with_headers(&acme())
        .publish()
        .await?;
    tb.broker::<FileBroker<Counting>>()
        .subscriber("plain")
        .assert_called_once();
    assert_eq!(
        parsed.load(Ordering::Relaxed),
        0,
        "unread headers were parsed"
    );

    tb.broker::<FileBroker<Counting>>()
        .message(&Sample { millivolts: 6 })
        .to("tagged")
        .with_headers(&acme())
        .publish()
        .await?;
    tb.broker::<FileBroker<Counting>>()
        .subscriber("tagged")
        .assert_called_once();
    assert_eq!(parsed.load(Ordering::Relaxed), 1);
    tb.shutdown().await?;
    Ok(())
}
