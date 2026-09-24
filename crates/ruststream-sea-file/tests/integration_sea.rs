//! End-to-end checks over real stream files and the stdio loopback - all local, no external
//! broker.

mod common;

use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
#[cfg(feature = "testing")]
use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{
    AckError, BatchSubscriber, Broker, ConnectedBroker, HeaderMap, IncomingMessage,
    OutgoingMessage, Positioned, Publisher, Seekable, Seeker, Subscribe, Subscriber,
};
use ruststream_sea_file::{
    FileBroker, FilePosition, FileStream, SEQUENCE_HEADER, SeaFileError, StdioBroker,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// The batch size the stdio check opens its subscription at: smaller than the run, so a batch
/// carrying more than the mount site asked for is caught rather than missed.
const STDIO_BATCH: NonZeroUsize = NonZeroUsize::new(2).unwrap();

#[test]
fn file_roundtrip_preserves_payload_and_headers() {
    common::on_a_file(async {
        let path = common::tmp_path("roundtrip");
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
        // The file's own sequence for this stream key, reported twice and in agreement: as a
        // header a handler can read, and as the position a seek takes back.
        assert_eq!(message.headers().get_str(SEQUENCE_HEADER), Some("1"));
        assert_eq!(message.position(), FilePosition::sequence(1));
        // The transport keeps no consumer positions: acknowledgement is honestly unsupported.
        assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn a_finished_file_replays_and_completes() {
    common::on_a_file(async {
        let path = common::tmp_path("replay");

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

/// Bodies the unmarked replay records: past the thousand messages the file client reads ahead of
/// its consumer, and not a multiple of it, so the end of the file falls inside a read-ahead
/// window that is still full.
const UNMARKED_REPLAY: usize = 1_537;

/// A replay of a file with no end-of-stream mark finds the end by running out of file, and
/// delivers every message the file holds before it reports that end.
#[test]
fn a_replay_delivers_the_whole_file_before_it_ends() {
    common::on_a_file(async {
        let path = common::tmp_path("replay-unmarked");

        {
            let connected = FileBroker::new(&path).connect().await.expect("file opens");
            let publisher = connected.publisher();
            for i in 0..UNMARKED_REPLAY {
                let body = u32::try_from(i).expect("the count fits").to_be_bytes();
                publisher
                    .publish(OutgoingMessage::new("orders", body.as_slice()), None)
                    .await
                    .expect("publish succeeds");
            }
            connected.shutdown().await.expect("shutdown succeeds");
        }

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders").replay())
            .await
            .expect("replay opens");
        let mut handled = 0_usize;
        {
            let mut stream = pin!(subscriber.stream());
            while let Some(next) = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("the replay moves on")
            {
                let message = next.expect("delivery is ok");
                let expected = u32::try_from(handled)
                    .expect("the count fits")
                    .to_be_bytes();
                assert_eq!(message.payload(), expected.as_slice(), "in publish order");
                handled += 1;
            }
        }
        assert_eq!(
            handled, UNMARKED_REPLAY,
            "the replay ended before it delivered the whole file"
        );

        drop(subscriber);
        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The in-process transport answers settlement the way a stream file answers it. Both are read
/// here in one test, because the value of the answer is that the two agree: an in-process
/// transport that claimed a settlement would make a handler's retry look effective under test and lose the
/// message against a file.
#[cfg(feature = "testing")]
#[test]
fn the_in_process_transport_settles_the_way_a_stream_file_does() {
    common::on_a_file(async {
        let path = common::tmp_path("settlement");
        let file = FileBroker::new(&path).connect().await.expect("file opens");
        let mut from_file = file.subscribe("orders").await.expect("subscription opens");
        file.publisher()
            .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
            .await
            .expect("publish succeeds");

        let in_process = FileBroker::new(&path)
            .connect_in_process()
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
        // is refused on both rather than served in process alone.
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

/// The in-process transport positions the way a stream file positions, read side by side for the
/// reason settlement is: an in-process log numbered differently, or one that let a service resume
/// from a position the file has not reached, would pass a test the file refuses.
#[cfg(feature = "testing")]
#[test]
fn the_in_process_transport_positions_the_way_a_stream_file_does() {
    common::on_a_file(async {
        let path = common::tmp_path("positions");
        let file = FileBroker::new(&path).connect().await.expect("file opens");
        let mut from_file = file.subscribe("orders").await.expect("subscription opens");
        file.publisher()
            .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
            .await
            .expect("publish succeeds");

        let in_process = FileBroker::new(&path)
            .connect_in_process()
            .await
            .expect("transport connects");
        let mut from_transport = in_process
            .subscribe("orders")
            .await
            .expect("subscription opens");
        in_process.inject(OutgoingMessage::new("orders", b"one".as_slice()));

        // Minted before the streams borrow the subscribers, and past the end of a log holding one
        // message.
        let filed_seeker = from_file.seeker();
        let stubbed_seeker = from_transport.seeker();
        let past_the_end = FilePosition::sequence(99);

        let mut filed = pin!(from_file.stream());
        let first = tokio::time::timeout(RECV_TIMEOUT, filed.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(first.position(), FilePosition::sequence(1));
        filed_seeker
            .seek(past_the_end)
            .await
            .expect_err("the file refuses a position it has not reached");
        let ended = tokio::time::timeout(RECV_TIMEOUT, filed.next())
            .await
            .expect("the end arrives rather than hanging");
        assert!(ended.is_none(), "got {ended:?}");

        let mut stubbed = pin!(from_transport.stream());
        let first = tokio::time::timeout(RECV_TIMEOUT, stubbed.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(
            first.position(),
            FilePosition::sequence(1),
            "the in-process file numbers a stream key from one, as the file does",
        );
        stubbed_seeker
            .seek(past_the_end)
            .await
            .expect_err("the in-process file refuses what the file refuses");
        let ended = tokio::time::timeout(RECV_TIMEOUT, stubbed.next())
            .await
            .expect("the end arrives rather than hanging");
        assert!(
            ended.is_none(),
            "the in-process file ends the subscription a refused seek left behind, got {ended:?}",
        );

        file.shutdown().await.expect("shutdown succeeds");
        in_process.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The end-of-stream mark is what ends a live subscription, and it ends it cleanly: a writer
/// that finished the file is not a receive failure for the reader that was tailing it.
#[test]
fn a_live_subscription_completes_at_the_end_of_stream_mark() {
    common::on_a_file(async {
        let path = common::tmp_path("live-eos");

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
    common::on_a_file(async {
        let path = common::tmp_path("replay-empty-key");

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

/// `existing_only` is the difference between a reader and a writer: a service that must find the
/// file already there fails to connect rather than creating an empty one and reporting nothing.
#[test]
fn existing_only_refuses_a_missing_file_and_opens_one_that_is_there() {
    common::on_a_file(async {
        let path = common::tmp_path("existing-only");

        let missing = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect_err("a missing file must not be created under existing_only");
        assert!(
            matches!(missing, SeaFileError::Connect { .. }),
            "opening a missing file must report a connect failure, got {missing:?}",
        );
        assert!(
            !std::path::Path::new(&path).exists(),
            "a refused connect must leave no file behind",
        );

        // The same configuration over a file that does exist opens it.
        let writer = FileBroker::new(&path).connect().await.expect("file opens");
        writer.shutdown().await.expect("shutdown succeeds");
        let reader = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("an existing file opens under existing_only");
        reader.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The beacon interval the broker was configured with, read back out of the file the broker
/// wrote: the format keeps it in the header, and it is what a reader seeks by.
///
/// The layout is the client's: a two-byte mark and a version, the file name as a length-prefixed
/// string, a millisecond timestamp, then the interval as a big-endian `u32`.
fn beacon_interval_of(path: &str) -> u32 {
    let header = std::fs::read(path).expect("the stream file is readable");
    let name_len = header[3] as usize;
    let at = 4 + name_len + 8;
    u32::from_be_bytes(
        header[at..at + 4]
            .try_into()
            .expect("the header holds four bytes of interval"),
    )
}

/// Beacons are what makes a stream file seekable, and `beacon_interval` is how dense they are.
/// The setting is refused rather than rounded when it is not a positive multiple of 1024, so a
/// file is never written with an interval its author did not ask for.
#[test]
fn the_beacon_interval_reaches_the_file_and_an_invalid_one_is_refused() {
    common::on_a_file(async {
        let dense_path = common::tmp_path("beacons-dense");
        let dense = FileBroker::new(&dense_path)
            .beacon_interval(1024)
            .connect()
            .await
            .expect("file opens");
        dense.shutdown().await.expect("shutdown succeeds");
        assert_eq!(beacon_interval_of(&dense_path), 1024);

        // The default the client applies when the broker names none, so the assertion above
        // reads as the setting taking effect rather than as a coincidence.
        let plain_path = common::tmp_path("beacons-default");
        let plain = FileBroker::new(&plain_path).connect().await.expect("opens");
        plain.shutdown().await.expect("shutdown succeeds");
        assert_eq!(beacon_interval_of(&plain_path), 1024 * 1024);

        let invalid_path = common::tmp_path("beacons-invalid");
        let refused = FileBroker::new(&invalid_path)
            .beacon_interval(1000)
            .connect()
            .await
            .expect_err("an interval that is not a multiple of 1024 must be refused");
        assert!(
            matches!(refused, SeaFileError::Invalid(ref why) if why.contains("beacon interval")),
            "an invalid interval must name itself, got {refused:?}",
        );
        assert!(
            !std::path::Path::new(&invalid_path).exists(),
            "a refused configuration must write no file",
        );

        let _ = std::fs::remove_file(&dense_path);
        let _ = std::fs::remove_file(&plain_path);
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
            // The client numbers the lines of a stream key from zero, unlike a stream file.
            assert_eq!(message.headers().get_str(SEQUENCE_HEADER), Some("0"));
            // A pipe keeps no retained log, so neither settlement is pretended: what a handler
            // asks for here is refused rather than silently dropped.
            assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

            publisher
                .publish(OutgoingMessage::new("pipe", raw.as_slice()), None)
                .await
                .expect("publish succeeds");
            let requeued = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            assert!(matches!(
                requeued.nack(true).await,
                Err(AckError::Unsupported)
            ));
        }

        // The client's line format drops an empty line, so a message that would become one is
        // refused at the publisher rather than lost on the way to the next process.
        let empty = publisher
            .publish(OutgoingMessage::new("pipe", b"".as_slice()), None)
            .await
            .expect_err("an empty message must not be transmitted");
        assert!(
            matches!(empty, SeaFileError::Invalid(ref why) if why.contains("empty")),
            "an empty message must say why it cannot travel, got {empty:?}",
        );

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

        connected.shutdown().await.expect("shutdown succeeds");

        // The publisher handed out before the shutdown outlives the connection, and the pipe it
        // wrote to is gone: it must say so rather than accept a line nobody will ever read.
        let after = publisher
            .publish(OutgoingMessage::new("pipe", b"late".as_slice()), None)
            .await
            .expect_err("a publish through a closed transport must error");
        assert!(
            matches!(after, SeaFileError::NotConnected),
            "a publish after shutdown must report a closed transport, got {after:?}",
        );
    });
}
