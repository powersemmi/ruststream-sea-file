//! End-to-end checks over real stream files and the stdio loopback - all local, no external
//! broker.

mod common;

use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    AckError, BatchSubscriber, Broker, ConnectedBroker, HeaderMap, IncomingMessage,
    OutgoingMessage, Publisher, Subscribe, Subscriber,
};
use ruststream_sea_file::{FileBroker, FileStream, StdioBroker};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// The batch size the stdio check opens its subscription at: smaller than the run, so a batch
/// carrying more than the mount site asked for is caught rather than missed.
const STDIO_BATCH: NonZeroUsize = NonZeroUsize::new(2).unwrap();

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

        let mut headers = HeaderMap::new();
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

/// Both stdio checks share one test: shutting the transport down ends every stdio consumer and
/// producer in the process, so a second stdio test running beside this one would be torn down by
/// it.
#[test]
fn stdio_loopback_carries_binary_payloads_and_batches() {
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

        {
            let mut stream = pin!(subscriber.stream());
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            // The stdio line format is text; the envelope carried the binary payload through it.
            assert_eq!(message.payload(), raw.as_slice());
        }

        // Standard input delivers one line at a time, so the batches are assembled on the client;
        // what the mount site asks for is still the cap a batch may never exceed.
        for i in 0..3u8 {
            publisher
                .publish(OutgoingMessage::new("pipe", [i].as_slice()))
                .await
                .expect("publish succeeds");
        }
        let mut received = Vec::new();
        let mut batches = pin!(subscriber.batches(STDIO_BATCH));
        while received.len() < 3 {
            let batch = tokio::time::timeout(RECV_TIMEOUT, batches.next())
                .await
                .expect("batch arrives")
                .expect("stream is open")
                .expect("batch is ok");
            assert!(!batch.is_empty(), "a yielded batch must not be empty");
            assert!(
                batch.len() <= STDIO_BATCH.get(),
                "a batch must never carry more than the size it was opened with",
            );
            received.extend(batch.iter().map(|msg| msg.payload().to_vec()));
        }
        assert_eq!(received, vec![vec![0], vec![1], vec![2]]);

        connected.shutdown().await.expect("shutdown succeeds");
    });
}
