//! Every position a stream file accepts, applied to a real stream file.
//!
//! The conformance seeking suite covers the position a delivery reports: it captures one and
//! seeks back to it, which is the portable half of the contract. The other three forms are this
//! transport's own vocabulary - the start of the retained file, its tip, and an instant - and
//! nothing proved they reach the client's rewind correctly, nor what the numbers in a captured
//! position mean. That is what this file is for, so each test drives a real file and reads the
//! deliveries the file gives back.
//!
//! One shared runtime, for the reason `common` gives: the file client's producer dispatcher is
//! process-wide and outlives no per-test runtime.

mod common;

use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Positioned, Publisher, Seekable,
    Seeker, Subscriber,
};
use ruststream_sea_file::{FileBroker, FilePosition, FileStream, FileSubscriber, SeaFileError};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Appends `payloads` to one stream key of the file, through a broker of its own, and closes it.
///
/// This is the producer that ran before the service: a separate connection, finished and flushed,
/// so what the tests below read is what survived on disk rather than what a live handle holds.
async fn record(path: &str, stream: &str, payloads: impl IntoIterator<Item = u8>) {
    let connected = FileBroker::new(path).connect().await.expect("file opens");
    let publisher = connected.publisher();
    for payload in payloads {
        publisher
            .publish(OutgoingMessage::new(stream, [payload].as_slice()), None)
            .await
            .expect("publish succeeds");
    }
    connected.shutdown().await.expect("shutdown succeeds");
}

/// The next delivery, or a failure naming what was being waited for.
async fn next_payload<S>(stream: &mut S, what: &str) -> (u8, FilePosition)
where
    S: futures::Stream<Item = Result<ruststream_sea_file::FileMessage, SeaFileError>> + Unpin,
{
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{what}: a delivery must arrive"))
        .unwrap_or_else(|| panic!("{what}: the stream must stay open"))
        .unwrap_or_else(|e| panic!("{what}: the delivery must be ok, got {e}"));
    (message.payload()[0], message.position())
}

/// Milliseconds since the Unix epoch, the unit [`FilePosition::timestamp`] takes.
fn now_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_millis(),
    )
    .expect("milliseconds fit in a u64")
}

/// What a captured position actually holds, and what a literal
/// [`FilePosition::sequence`] therefore means: the file numbers each stream key from one and
/// keeps counting when a later connection appends to it.
///
/// The number reaches services - it is the delivery's position and the `stream-sequence` header -
/// so a service that logs it, or resumes from one it stored, depends on this.
#[test]
fn the_file_numbers_a_stream_key_from_one_and_keeps_counting_across_a_reopen() {
    common::on_a_file(async {
        let path = common::tmp_path("numbering");
        record(&path, "orders", 1..=3).await;
        // A second connection over the same file: the numbering continues rather than restarting.
        record(&path, "orders", 4..=5).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());
        seeker
            .seek(FilePosition::beginning())
            .await
            .expect("seeking to the start of the file succeeds");

        let mut seen = Vec::new();
        for _ in 0..5 {
            seen.push(next_payload(&mut stream, "numbering").await);
        }
        assert_eq!(
            seen,
            vec![
                (1, FilePosition::sequence(1)),
                (2, FilePosition::sequence(2)),
                (3, FilePosition::sequence(3)),
                (4, FilePosition::sequence(4)),
                (5, FilePosition::sequence(5)),
            ],
        );

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The start of the retained file, and with it the durability the transport is for: a connection
/// that ended left the messages on disk, and a later one reads them all back.
#[test]
fn the_beginning_position_replays_what_an_earlier_connection_left_on_disk() {
    common::on_a_file(async {
        let path = common::tmp_path("beginning");
        record(&path, "orders", 1..=3).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());

        seeker
            .seek(FilePosition::beginning())
            .await
            .expect("seeking to the start of the file succeeds");
        for expected in 1..=3u8 {
            let (payload, _) = next_payload(&mut stream, "beginning").await;
            assert_eq!(payload, expected, "the retained file replays in order");
        }

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// A literal sequence, the form a service writes when it resumes from a position it stored: the
/// message with that number comes back, and the rest of the file follows it in order.
#[test]
fn a_literal_sequence_position_resumes_at_that_message() {
    common::on_a_file(async {
        let path = common::tmp_path("sequence");
        record(&path, "orders", 1..=4).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());

        seeker
            .seek(FilePosition::sequence(3))
            .await
            .expect("seeking to a sequence succeeds");
        // Inclusive: the message numbered 3 is redelivered, not skipped.
        assert_eq!(next_payload(&mut stream, "sequence").await.0, 3);
        assert_eq!(next_payload(&mut stream, "sequence").await.0, 4);

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// The tip of the stream: what the file already held is behind the subscription, and what a
/// producer writes next is not.
#[test]
fn the_end_position_leaves_the_retained_file_behind() {
    common::on_a_file(async {
        let path = common::tmp_path("end");
        record(&path, "orders", 1..=3).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let publisher = connected.publisher();
        let mut stream = pin!(subscriber.stream());

        // Back to the start first, so the jump to the tip has something to skip over.
        seeker
            .seek(FilePosition::beginning())
            .await
            .expect("seeking to the start of the file succeeds");
        assert_eq!(next_payload(&mut stream, "end: replay").await.0, 1);

        seeker
            .seek(FilePosition::end())
            .await
            .expect("seeking to the tip succeeds");
        publisher
            .publish(OutgoingMessage::new("orders", [9u8].as_slice()), None)
            .await
            .expect("publish succeeds");

        // The retained messages 2 and 3 are behind the subscription now; the live one is not.
        assert_eq!(next_payload(&mut stream, "end: live").await.0, 9);

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// An instant the file has messages after: the epoch is before every message the file holds, so
/// the whole file follows it.
#[test]
fn a_timestamp_position_resumes_strictly_after_the_instant() {
    common::on_a_file(async {
        let path = common::tmp_path("timestamp");
        record(&path, "orders", 1..=3).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());

        seeker
            .seek(FilePosition::timestamp(0))
            .await
            .expect("seeking to the epoch succeeds");
        for expected in 1..=3u8 {
            let (payload, _) = next_payload(&mut stream, "timestamp: from the epoch").await;
            assert_eq!(payload, expected);
        }

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// A position past the end of the stream - an instant later than every message, a sequence the
/// file never wrote - is refused, and the subscription ends with it.
///
/// The transport looks forward for the message and runs out of file, and the consumer it was
/// looking with does not survive that. So a seek target has to be one the stream holds, which
/// makes a captured position (or the beginning, or the tip) the safe form, and this is the one
/// piece of the position vocabulary a service must not get wrong.
#[test]
fn a_position_past_the_end_of_the_stream_is_refused_and_ends_the_subscription() {
    common::on_a_file(async {
        for (label, past_the_end) in [
            // A minute ahead of the clock: nothing the file holds is later than that.
            ("timestamp", FilePosition::timestamp(now_millis() + 60_000)),
            ("sequence", FilePosition::sequence(99)),
        ] {
            let path = common::tmp_path("past-the-end");
            record(&path, "orders", 1..=3).await;

            let connected = FileBroker::new(&path)
                .existing_only()
                .connect()
                .await
                .expect("file reopens");
            let mut subscriber = connected
                .subscribe_stream(FileStream::new("orders"))
                .await
                .expect("subscription opens");
            let seeker = subscriber.seeker();
            let mut stream = pin!(subscriber.stream());

            let refused = seeker
                .seek(past_the_end)
                .await
                .expect_err(&format!("{label}: a position past the end must be refused"));
            assert!(
                matches!(refused, SeaFileError::Seek { ref stream, .. } if stream == "orders"),
                "{label}: a refused seek must name the subscription, got {refused:?}",
            );

            let end = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .unwrap_or_else(|_| panic!("{label}: the subscription must not hang"));
            assert!(
                end.is_none(),
                "{label}: a refused seek ends the subscription, got {end:?}",
            );

            connected.shutdown().await.expect("shutdown succeeds");
            let _ = std::fs::remove_file(&path);
        }
    });
}

/// A millisecond count no instant answers to is refused, and the subscription is left where it
/// was rather than somewhere unpredictable.
#[test]
fn a_timestamp_outside_the_representable_range_is_refused() {
    common::on_a_file(async {
        let path = common::tmp_path("timestamp-invalid");
        record(&path, "orders", 1..=2).await;

        let connected = FileBroker::new(&path)
            .existing_only()
            .connect()
            .await
            .expect("file reopens");
        let mut subscriber = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        let seeker = subscriber.seeker();
        let publisher = connected.publisher();
        let mut stream = pin!(subscriber.stream());

        let refused = seeker
            .seek(FilePosition::timestamp(u64::MAX))
            .await
            .expect_err("a millisecond count no instant answers to must be refused");
        assert!(
            matches!(refused, SeaFileError::Invalid(ref why) if why.contains("valid timestamp")),
            "a refused instant must name itself, got {refused:?}",
        );

        // The subscription is still the live one it was, so a publish still reaches it.
        publisher
            .publish(OutgoingMessage::new("orders", [9u8].as_slice()), None)
            .await
            .expect("publish succeeds");
        assert_eq!(next_payload(&mut stream, "after a refused seek").await.0, 9);

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// Opens a replay of `path`'s `orders` key over a fresh connection.
async fn replay(path: &str) -> (ruststream_sea_file::ConnectedFileBroker, FileSubscriber) {
    let connected = FileBroker::new(path)
        .existing_only()
        .connect()
        .await
        .expect("file reopens");
    let subscriber = connected
        .subscribe_stream(FileStream::new("orders").replay())
        .await
        .expect("replay opens");
    (connected, subscriber)
}

/// The end of a replay, or a failure naming what was being waited for.
async fn replay_end<S>(stream: &mut S, what: &str)
where
    S: futures::Stream<Item = Result<ruststream_sea_file::FileMessage, SeaFileError>> + Unpin,
{
    let end = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{what}: the replay must end"));
    assert!(end.is_none(), "{what}: the replay ends, got {end:?}");
}

/// A replay that has read the whole file is still a subscription: a seek moves it back, and the
/// file is delivered again from there.
#[test]
fn a_seek_after_a_replay_ended_replays_from_the_new_position() {
    common::on_a_file(async {
        let path = common::tmp_path("replay-ended");
        record(&path, "orders", 1..=3).await;

        let (connected, mut subscriber) = replay(&path).await;
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());
        for expected in 1..=3u8 {
            assert_eq!(next_payload(&mut stream, "first pass").await.0, expected);
        }
        replay_end(&mut stream, "first pass").await;

        seeker
            .seek(FilePosition::sequence(2))
            .await
            .expect("seeking a replay that reached the end succeeds");
        for expected in 2..=3u8 {
            assert_eq!(next_payload(&mut stream, "second pass").await.0, expected);
        }
        replay_end(&mut stream, "second pass").await;

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// A refused seek leaves a replay where it was: what it had already read is still delivered, and
/// the rest of the file after it.
#[test]
fn a_refused_seek_leaves_a_replay_where_it_was() {
    common::on_a_file(async {
        let path = common::tmp_path("replay-refused");
        record(&path, "orders", 1..=3).await;

        let (connected, mut subscriber) = replay(&path).await;
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());
        assert_eq!(next_payload(&mut stream, "before the seek").await.0, 1);

        seeker
            .seek(FilePosition::sequence(99))
            .await
            .expect_err("no message carries that sequence");
        for expected in 2..=3u8 {
            assert_eq!(
                next_payload(&mut stream, "after the seek").await.0,
                expected
            );
        }
        replay_end(&mut stream, "after the seek").await;

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}

/// More messages than the subscription buffers: a handler that seeks while the rest of the file
/// waits for room still moves the subscription.
#[test]
fn a_seek_moves_a_subscription_with_a_backlog_larger_than_its_buffer() {
    const BACKLOG: u8 = 200;
    common::on_a_file(async {
        let path = common::tmp_path("backlog");
        record(&path, "orders", 1..=BACKLOG).await;

        let (connected, mut subscriber) = replay(&path).await;
        let seeker = subscriber.seeker();
        let mut stream = pin!(subscriber.stream());
        assert_eq!(next_payload(&mut stream, "before the seek").await.0, 1);

        tokio::time::timeout(RECV_TIMEOUT, seeker.seek(FilePosition::sequence(150)))
            .await
            .expect("a seek under a backlog completes")
            .expect("the seek succeeds");
        for expected in 150..=BACKLOG {
            assert_eq!(
                next_payload(&mut stream, "after the seek").await.0,
                expected
            );
        }
        replay_end(&mut stream, "after the seek").await;

        connected.shutdown().await.expect("shutdown succeeds");
        let _ = std::fs::remove_file(&path);
    });
}
