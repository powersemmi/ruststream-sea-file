//! End-to-end checks over real stream files and the stdio loopback - all local, no external
//! broker.

mod common;

use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher,
    Subscribe, Subscriber,
};
use ruststream_sea_file::{FileBroker, FileStream, StdioBroker};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

fn tmp_path(name: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir()
        .join(format!(
            "ruststream-sea-it-{name}-{}-{}.ss",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn file_roundtrip_preserves_payload_and_headers() {
    common::rt().block_on(async {
        let path = tmp_path("roundtrip");
        let connected = FileBroker::new(&path).connect().await.expect("file opens");
        let mut subscriber = connected
            .subscribe("orders")
            .await
            .expect("subscription opens");

        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("x-tenant", "acme");
        let publisher = connected.publisher();
        publisher
            .publish(OutgoingMessage::new("orders", b"{\"id\":1}".as_slice()).with_headers(headers))
            .await
            .expect("publish succeeds");

        let mut stream = pin!(subscriber.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"{\"id\":1}");
        assert_eq!(
            message.headers().get_str("content-type"),
            Some("application/json")
        );
        assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
        // The transport keeps no consumer positions: acknowledgement is honestly unsupported.
        assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn a_finished_file_replays_and_completes() {
    common::rt().block_on(async {
        let path = tmp_path("replay");

        // Record a stream and finish it with an end-of-stream mark.
        {
            let connected = FileBroker::new(&path)
                .end_with_eos()
                .connect()
                .await
                .expect("file opens");
            let publisher = connected.publisher();
            for i in 0..3u8 {
                publisher
                    .publish(OutgoingMessage::new("orders", [i].as_slice()))
                    .await
                    .expect("publish succeeds");
            }
            connected.shutdown().await.expect("shutdown succeeds");
        }

        // Replay it against a fresh broker: every message, then completion.
        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders").replay())
            .await
            .expect("replay opens");
        let mut stream = pin!(subscriber.stream());
        for i in 0..3u8 {
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            assert_eq!(message.payload(), [i].as_slice());
        }
        let end = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("completion arrives");
        assert!(end.is_none(), "a finished replay must complete the stream");

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn stdio_loopback_round_trips_binary_payloads() {
    common::rt().block_on(async {
        let connected = StdioBroker::new()
            .loopback()
            .connect()
            .await
            .expect("stdio attaches");
        let mut subscriber = connected
            .subscribe("pipe")
            .await
            .expect("subscription opens");

        let raw = [0u8, 159, 146, 150, 255];
        let publisher = connected.publisher();
        publisher
            .publish(OutgoingMessage::new("pipe", raw.as_slice()))
            .await
            .expect("publish succeeds");

        let mut stream = pin!(subscriber.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        // The stdio line format is text; the envelope carried the binary payload through it.
        assert_eq!(message.payload(), raw.as_slice());

        connected.shutdown().await.expect("shutdown succeeds");
    });
}
