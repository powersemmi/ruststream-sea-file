//! End-to-end checks over real stream files and the stdio loopback - all local, no external
//! broker.

mod common;

use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
#[cfg(feature = "testing")]
use ruststream::testing::TestableBroker;
use ruststream::{
    AckError, BatchSubscriber, Broker, ConnectedBroker, HeaderMap, IncomingMessage,
    OutgoingMessage, Publisher, Subscribe, Subscriber,
};
#[cfg(feature = "testing")]
use ruststream_sea_file::testing::FileTestBroker;
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
            .publish(
                OutgoingMessage::new("orders", b"{\"id\":1}".as_slice()).with_headers(headers),
                None,
            )
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
                    .publish(OutgoingMessage::new("orders", [i].as_slice()), None)
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

/// The in-process transport answers settlement the way a stream file answers it. Both are read
/// here in one test, because the value of the answer is that the two agree: a stand-in that
/// claimed a settlement would make a handler's retry look effective under test and lose the
/// message against a file.
#[cfg(feature = "testing")]
#[test]
fn the_in_process_transport_settles_the_way_a_stream_file_does() {
    common::rt().block_on(async {
        let path = tmp_path("settlement");
        let file = FileBroker::new(&path).connect().await.expect("file opens");
        let mut from_file = file.subscribe("orders").await.expect("subscription opens");
        file.publisher()
            .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
            .await
            .expect("publish succeeds");

        let in_process = FileTestBroker::new()
            .connect()
            .await
            .expect("transport connects");
        let mut from_transport = in_process
            .subscribe("orders")
            .await
            .expect("subscription opens");
        in_process.inject(OutgoingMessage::new("orders", b"one".as_slice()));

        let filed = {
            let mut stream = pin!(from_file.stream());
            tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok")
        };
        let stubbed = {
            let mut stream = pin!(from_transport.stream());
            tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok")
        };
        assert_eq!(filed.payload(), stubbed.payload());
        assert!(matches!(filed.ack().await, Err(AckError::Unsupported)));
        assert!(matches!(stubbed.ack().await, Err(AckError::Unsupported)));

        // The requeue too: neither transport takes a message back, so a handler asking for one
        // is refused on both rather than served by the stand-in alone.
        file.publisher()
            .publish(OutgoingMessage::new("orders", b"two".as_slice()), None)
            .await
            .expect("publish succeeds");
        in_process.inject(OutgoingMessage::new("orders", b"two".as_slice()));
        let filed = {
            let mut stream = pin!(from_file.stream());
            tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok")
        };
        let stubbed = {
            let mut stream = pin!(from_transport.stream());
            tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok")
        };
        assert!(matches!(filed.nack(true).await, Err(AckError::Unsupported)));
        assert!(matches!(
            stubbed.nack(true).await,
            Err(AckError::Unsupported)
        ));

        file.shutdown().await.expect("shutdown succeeds");
        in_process.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The end-of-stream mark is what ends a live subscription, and it ends it cleanly: a writer
/// that finished the file is not a receive failure for the reader that was tailing it.
#[test]
fn a_live_subscription_completes_at_the_end_of_stream_mark() {
    common::rt().block_on(async {
        let path = tmp_path("live-eos");

        let writer = FileBroker::new(&path)
            .end_with_eos()
            .connect()
            .await
            .expect("file opens");
        let reader = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");

        // A plain descriptor tails the file, so this subscription is the live one.
        let mut subscriber = reader
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let mut stream = pin!(subscriber.stream());

        writer
            .publisher()
            .publish(OutgoingMessage::new("orders", b"only".as_slice()), None)
            .await
            .expect("publish succeeds");
        let delivered = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(delivered.payload(), b"only".as_slice());

        // The writer finishes the file under a reader that is still tailing it.
        writer.shutdown().await.expect("shutdown succeeds");

        let end = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the end arrives rather than hanging");
        assert!(
            end.is_none(),
            "a live subscription must complete at the end-of-stream mark, got {end:?}",
        );

        reader.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// A replay reads its first delivery while the subscription is being opened, so this pins the
/// case where there is nothing to read: opening must return, and the stream must end, rather
/// than wait for a message that is never coming.
#[test]
fn a_replay_of_a_key_with_no_messages_finishes_instead_of_waiting() {
    common::rt().block_on(async {
        let path = tmp_path("replay-empty-key");

        {
            let connected = FileBroker::new(&path)
                .end_with_eos()
                .connect()
                .await
                .expect("file opens");
            connected
                .publisher()
                .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
                .await
                .expect("publish succeeds");
            connected.shutdown().await.expect("shutdown succeeds");
        }

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        // The file holds messages, but none under this key.
        let mut subscriber = tokio::time::timeout(
            RECV_TIMEOUT,
            connected.subscribe_stream(FileStream::new("audit").replay()),
        )
        .await
        .expect("opening the replay returns rather than waiting")
        .expect("replay opens");

        let mut stream = pin!(subscriber.stream());
        let end = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the end arrives rather than hanging");
        assert!(
            end.is_none(),
            "a replay with nothing to read must end, got {end:?}",
        );

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
            .publish(OutgoingMessage::new("pipe", raw.as_slice()), None)
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
                .publish(OutgoingMessage::new("pipe", [i].as_slice()), None)
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

        // Nothing on this transport addresses the subscription: a publish goes to standard
        // output and the subscription reads standard input. Saying so is what makes a
        // registration binding `out_retry` over stdio refuse to start, instead of writing every
        // delayed message into the next stage of the pipeline. The loopback above is a test aid,
        // and an address that only held under it would break in the shape a service ships.
        assert_eq!(connected.redelivery_address("pipe"), None);

        connected.shutdown().await.expect("shutdown succeeds");
    });
}
