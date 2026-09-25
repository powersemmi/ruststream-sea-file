//! What the brokers' in-process mode answers where a stream file or a pipe answers something
//! particular: the stream keys the client refuses, the end of a replay, the end-of-stream mark,
//! the broker's settings, and where a line on standard output goes.
//!
//! The subject is the transport, so most checks drive the connected broker directly; the one
//! about a pipe's routing drives the service through the harness.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use ruststream::testing::InProcess;
use ruststream::{
    ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Positioned, Publisher, Seekable,
    Seeker, Subscribe, Subscriber,
};
use ruststream_sea_file::{
    FileBroker, FilePosition, FileStream, SEQUENCE_HEADER, SeaFileError, StdioBroker,
};

/// The file the brokers here are built with; nothing is opened at it.
const PATH: &str = "/var/lib/orders/orders.ss";

/// How long a replay may take to report the end of a two-message file before the test says it is
/// waiting for more.
const REPLAY_BOUND: Duration = Duration::from_secs(5);

/// A stream key the client refuses: the characters it takes are letters, digits, `.`, `_` and
/// `-`.
const REFUSED_KEY: &str = "orders/eu";

#[tokio::test]
async fn a_stream_key_the_file_refuses_is_refused_in_process() {
    let connected = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let published = connected
        .publisher()
        .publish(OutgoingMessage::new(REFUSED_KEY, b"{}".as_slice()), None)
        .await
        .expect_err("the file takes no such stream key");
    assert!(
        matches!(published, SeaFileError::Invalid(ref why) if why.contains(REFUSED_KEY)),
        "got {published:?}",
    );
    let subscribed = connected
        .subscribe_stream(FileStream::new(REFUSED_KEY))
        .await
        .expect_err("the file reads no such stream key");
    assert!(
        matches!(subscribed, SeaFileError::Invalid(_)),
        "got {subscribed:?}"
    );
    connected.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_stream_key_a_pipe_refuses_is_refused_in_process() {
    let connected = StdioBroker::new()
        .connect_in_process()
        .await
        .expect("connects");
    let published = connected
        .publisher()
        .publish(OutgoingMessage::new(REFUSED_KEY, b"{}".as_slice()), None)
        .await
        .expect_err("a line takes no such stream key");
    assert!(
        matches!(published, SeaFileError::Invalid(_)),
        "got {published:?}"
    );
    let subscribed = connected
        .subscribe(REFUSED_KEY)
        .await
        .expect_err("standard input reads no such stream key");
    assert!(
        matches!(subscribed, SeaFileError::Invalid(_)),
        "got {subscribed:?}"
    );
    connected.shutdown().await.expect("shutdown");
}

/// A pipe drops an empty line, so the publisher refuses a message that would become one, in
/// process as on a real pipe.
#[tokio::test]
async fn an_empty_line_is_refused_in_process() {
    let connected = StdioBroker::new()
        .connect_in_process()
        .await
        .expect("connects");
    let refused = connected
        .publisher()
        .publish(OutgoingMessage::new("lines", b"".as_slice()), None)
        .await
        .expect_err("an empty message must not be transmitted");
    assert!(
        matches!(refused, SeaFileError::Invalid(ref why) if why.contains("empty")),
        "got {refused:?}",
    );
    connected.shutdown().await.expect("shutdown");
}

/// The broker's settings are checked the way `connect` checks them: an interval that is not a
/// positive multiple of 1024 is refused rather than ignored.
#[tokio::test]
async fn an_invalid_beacon_interval_is_refused_in_process() {
    let refused = FileBroker::new(PATH)
        .beacon_interval(1000)
        .connect_in_process()
        .await
        .expect_err("an interval that is not a multiple of 1024 must be refused");
    assert!(
        matches!(refused, SeaFileError::Invalid(ref why) if why.contains("beacon interval")),
        "got {refused:?}",
    );
}

/// A replay reads what the file holds and completes at its end, delivering everything before it.
#[tokio::test]
async fn a_replay_completes_at_the_end_of_the_file_in_process() {
    let connected = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let publisher = connected.publisher();
    for body in [b"one".as_slice(), b"two".as_slice()] {
        publisher
            .publish(OutgoingMessage::new("orders", body), None)
            .await
            .expect("publish");
    }

    let mut replay = connected
        .subscribe_stream(FileStream::new("orders").replay())
        .await
        .expect("replay opens");
    let mut stream = pin!(replay.stream());
    let mut read = Vec::new();
    while let Some(next) = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay ends at the end of the file rather than waiting")
    {
        let message = next.expect("delivery is ok");
        read.push((message.position(), message.payload().to_vec()));
    }
    assert_eq!(
        read,
        [
            (FilePosition::sequence(1), b"one".to_vec()),
            (FilePosition::sequence(2), b"two".to_vec()),
        ],
    );
    connected.shutdown().await.expect("shutdown");
}

/// A replay that has read the whole file can still be moved, as on a file: the seek replays the
/// log again from the position it names.
#[tokio::test]
async fn a_seek_after_a_replay_ended_replays_again_in_process() {
    let connected = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let publisher = connected.publisher();
    for body in [b"one".as_slice(), b"two".as_slice()] {
        publisher
            .publish(OutgoingMessage::new("orders", body), None)
            .await
            .expect("publish");
    }

    let mut replay = connected
        .subscribe_stream(FileStream::new("orders").replay())
        .await
        .expect("replay opens");
    let seeker = replay.seeker();
    let mut stream = pin!(replay.stream());
    let mut read = Vec::new();
    while let Some(next) = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay ends at the end of the file rather than waiting")
    {
        read.push(next.expect("delivery is ok").payload().to_vec());
    }
    assert_eq!(read, [b"one".to_vec(), b"two".to_vec()]);

    seeker
        .seek(FilePosition::sequence(2))
        .await
        .expect("a replay that reached the end can be moved");
    let again = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay moves on")
        .expect("the replay delivers again")
        .expect("delivery is ok");
    assert_eq!(again.payload(), b"two".as_slice());
    let end = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay ends again rather than waiting");
    assert!(end.is_none(), "got {end:?}");
    connected.shutdown().await.expect("shutdown");
}

/// A refused seek leaves a replay where it was, as the file's reader does: what it had not yet
/// delivered still comes, then the end.
#[tokio::test]
async fn a_refused_seek_leaves_a_replay_where_it_was_in_process() {
    let connected = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let publisher = connected.publisher();
    for body in [b"one".as_slice(), b"two".as_slice()] {
        publisher
            .publish(OutgoingMessage::new("orders", body), None)
            .await
            .expect("publish");
    }

    let mut replay = connected
        .subscribe_stream(FileStream::new("orders").replay())
        .await
        .expect("replay opens");
    let seeker = replay.seeker();
    let mut stream = pin!(replay.stream());
    let first = stream
        .next()
        .await
        .expect("the replay delivers")
        .expect("delivery is ok");
    assert_eq!(first.payload(), b"one".as_slice());

    seeker
        .seek(FilePosition::sequence(9))
        .await
        .expect_err("no message carries that sequence");
    let second = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay moves on")
        .expect("the replay goes on after a refused seek")
        .expect("delivery is ok");
    assert_eq!(second.payload(), b"two".as_slice());
    let end = tokio::time::timeout(REPLAY_BOUND, stream.next())
        .await
        .expect("the replay ends rather than waiting");
    assert!(end.is_none(), "got {end:?}");
    connected.shutdown().await.expect("shutdown");
}

/// A live subscription that read the end-of-stream mark cannot be moved: on a file the client's
/// consumer ends with the stream it tailed, so the seek is refused rather than replayed.
#[tokio::test]
async fn a_seek_after_the_end_of_stream_mark_is_refused_in_process() {
    let connected = FileBroker::new(PATH)
        .end_with_eos()
        .connect_in_process()
        .await
        .expect("connects");
    let mut tail = connected
        .subscribe_stream(FileStream::new("orders"))
        .await
        .expect("subscription opens");
    let seeker = tail.seeker();
    connected
        .publisher()
        .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
        .await
        .expect("publish");
    connected.shutdown().await.expect("shutdown");

    let refused = seeker
        .seek(FilePosition::beginning())
        .await
        .expect_err("a subscription that ended at the mark cannot be moved");
    assert!(
        matches!(refused, SeaFileError::Seek { ref stream, .. } if stream == "orders"),
        "got {refused:?}",
    );
    let mut stream = pin!(tail.stream());
    let delivered = stream
        .next()
        .now_or_never()
        .expect("the queued message is ready")
        .expect("the stream is open")
        .expect("delivery is ok");
    assert_eq!(delivered.payload(), b"one".as_slice());
    assert!(
        matches!(stream.next().now_or_never(), Some(None)),
        "the mark ends the subscription after what was queued",
    );
}

/// A live subscription ends at the end-of-stream mark the broker's shutdown writes, and only
/// there: without the setting it goes on waiting, as it does on a file.
#[tokio::test]
async fn the_end_of_stream_mark_ends_a_live_subscription_in_process() {
    for (end_with_eos, ends) in [(true, true), (false, false)] {
        let broker = FileBroker::new(PATH);
        let broker = if end_with_eos {
            broker.end_with_eos()
        } else {
            broker
        };
        let connected = broker.connect_in_process().await.expect("connects");
        let mut tail = connected
            .subscribe_stream(FileStream::new("orders"))
            .await
            .expect("subscription opens");
        connected.shutdown().await.expect("shutdown");

        let mut stream = pin!(tail.stream());
        let next = stream.next().now_or_never();
        if ends {
            assert!(
                matches!(next, Some(None)),
                "the mark must end the subscription"
            );
        } else {
            assert!(next.is_none(), "without the mark nothing ends it");
        }
    }
}

/// A delivery reports its sequence the way each transport numbers it: a stream file from one, a
/// pipe from zero.
#[tokio::test]
async fn each_transport_numbers_its_messages_as_its_client_does() {
    let file = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let mut from_file = file.subscribe("orders").await.expect("opens");
    file.publisher()
        .publish(OutgoingMessage::new("orders", b"{}".as_slice()), None)
        .await
        .expect("publish");
    let filed = pin!(from_file.stream())
        .next()
        .await
        .expect("delivery")
        .expect("delivery is ok");
    assert_eq!(filed.headers().get_str(SEQUENCE_HEADER), Some("1"));

    let pipe = StdioBroker::new()
        .loopback()
        .connect_in_process()
        .await
        .expect("connects");
    let mut from_pipe = pipe.subscribe("orders").await.expect("opens");
    pipe.publisher()
        .publish(OutgoingMessage::new("orders", b"{}".as_slice()), None)
        .await
        .expect("publish");
    let piped = pin!(from_pipe.stream())
        .next()
        .await
        .expect("delivery")
        .expect("delivery is ok");
    assert_eq!(piped.headers().get_str(SEQUENCE_HEADER), Some("0"));

    file.shutdown().await.expect("shutdown");
    pipe.shutdown().await.expect("shutdown");
}

/// A header crosses the in-process file in the envelope a stream file writes, so a value comes
/// back the way the file gives it back: trimmed, and as text.
#[tokio::test]
async fn a_header_crosses_the_envelope_the_file_writes() {
    let connected = FileBroker::new(PATH)
        .connect_in_process()
        .await
        .expect("connects");
    let mut subscriber = connected.subscribe("orders").await.expect("opens");
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "  acme  ");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish");
    let delivered = pin!(subscriber.stream())
        .next()
        .await
        .expect("delivery")
        .expect("delivery is ok");
    assert_eq!(delivered.headers().get_str("x-tenant"), Some("acme"));
    connected.shutdown().await.expect("shutdown");
}

/// What a pipeline stage writes and what it reads, driven through the harness.
mod a_pipe {
    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
    struct Job {
        id: u64,
    }

    #[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
    #[outgoing(name = "lines")]
    struct Line {
        job: u64,
    }

    /// Answers each job with a line on standard output.
    #[subscriber("jobs", publish)]
    async fn stage(job: &Job) -> Line {
        Line { job: job.id }
    }

    /// Reads the stream key the stage writes, on the same process's standard input.
    #[subscriber("lines")]
    async fn tee(line: &Line) -> HandlerOutcome {
        let _ = line.job;
        HandlerOutcome::ack()
    }

    fn app(broker: StdioBroker) -> impl App {
        RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(broker, |b| {
            b.include(stage).out_retry(Publish).to("jobs.retry");
            b.include(tee).out_retry(Publish).to("lines.retry");
        })
    }

    /// Standard output reaches the next process, not this one: the line is written, and the
    /// stage's own subscription on that key reads nothing.
    #[tokio::test]
    async fn a_line_the_service_writes_goes_downstream() -> Result<(), Box<dyn std::error::Error>> {
        let tb = TestApp::start(app(StdioBroker::new())).await?;
        tb.broker::<StdioBroker>()
            .message(&Job { id: 1 })
            .to("jobs")
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .published::<Line>("lines")
            .assert_called_once()
            .with(&Line { job: 1 });
        tb.broker::<StdioBroker>()
            .subscriber("lines")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    /// Under loopback the same line comes back to this process's own standard input.
    #[tokio::test]
    async fn a_line_comes_back_under_loopback() -> Result<(), Box<dyn std::error::Error>> {
        let tb = TestApp::start(app(StdioBroker::new().loopback())).await?;
        tb.broker::<StdioBroker>()
            .message(&Job { id: 2 })
            .to("jobs")
            .publish()
            .await?;

        tb.broker::<StdioBroker>()
            .subscriber("lines")
            .assert_called_once()
            .with(&Line { job: 2 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
