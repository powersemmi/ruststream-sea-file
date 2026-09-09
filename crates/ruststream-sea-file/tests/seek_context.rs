//! The file form's context keys and reply destinations, driven through the `TestApp` harness on
//! the in-process transport.
//!
//! Every handler here is an ordinary service handler: it names `FileStream` as its subscription
//! and reads the transport's keys, exactly as it would against a stream file. Nothing in this
//! file knows it is a test - the harness supplies the input, drives the reaction to a standstill,
//! and records what happened, so there is no collector, no channel and no clock anywhere.
//!
//! A stream key on this transport is a name inside the broker: the file is named once, in
//! `FileBroker::new(path)`, and standard output is named nowhere. Both ways of resolving a reply
//! destination therefore behave here the way they do on a subject broker, and the last two tests
//! pin them.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_sea_file::file::prelude::*;
use ruststream_sea_file::testing::{FileTestBroker, FileTestPublish};
use serde::{Deserialize, Serialize};

/// The producer's cursor contract: an entry carrying `resume_at` asks the consumer to skip
/// forward to that position once it has been handled.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Job {
    id: u64,
    resume_at: Option<u64>,
}

impl Job {
    const fn plain(id: u64) -> Self {
        Self {
            id,
            resume_at: None,
        }
    }
}

/// What the audit trail records: which job was seen, and where the subscription sat when it
/// arrived. Publishing it is how the handler's view of the context leaves the handler. It
/// declares no destination of its own, so the subscriber that produces it names one.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Seen {
    id: u64,
    at: u64,
}

/// Turns the key's position into the sequence the audit trail records. A delivery off a retained
/// log always sits at a sequence; the other forms only ever name a place to seek to.
fn sequence_of(at: FilePosition) -> u64 {
    match at {
        FilePosition::Sequence(sequence) => sequence,
        other => panic!("a delivery must report a sequence, got {other:?}"),
    }
}

// --8<-- [start:handler]
/// Reads both per-delivery keys, and skips forward when the producer marks a region poisoned.
/// The audit reply carries the position the `Position` key reported, so a test can read it.
#[subscriber(
    FileStream::new("jobs"),
    start_at(FilePosition::beginning()),
    publish("audit")
)]
async fn work(job: &Job, Ctx(at): Ctx<Position>, Ctx(seeker): Ctx<SeekHandle>) -> Seen {
    if let Some(resume_at) = job.resume_at {
        let _ = seeker.seek(FilePosition::sequence(resume_at)).await;
    }
    Seen {
        id: job.id,
        at: sequence_of(at),
    }
}
// --8<-- [end:handler]

/// A receipt belongs on the `receipts` stream key wherever it is produced, so the type carries
/// the destination and every mounting of a handler returning one publishes it there.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

/// The declared form: the clause says only that the return value is published.
#[subscriber("checkout", publish)]
async fn checkout(job: &Job) -> Receipt {
    Receipt { id: job.id }
}

/// The mount-site form: `Seen` declares no destination, so this subscriber names one, and the
/// same type reaches a different key from the audit handler above.
#[subscriber("review", publish("reviewed"))]
async fn review(job: &Job) -> Seen {
    Seen {
        id: job.id,
        at: job.resume_at.unwrap_or_default(),
    }
}

/// The batch counterpart: the seek handle is subscription-scoped, so it rides the batch context,
/// while the target rides the elements themselves.
#[subscriber(FileStream::new("digest"), start_at(FilePosition::beginning()))]
async fn digest(batch: &[Job], ctx: &mut Context<'_, FileBatchContext>) -> Vec<HandlerOutcome> {
    if let Some(resume_at) = batch.iter().find_map(|job| job.resume_at) {
        let _ = ctx
            .context(SeekHandle)
            .seek(FilePosition::sequence(resume_at))
            .await;
    }
    batch.iter().map(|_| HandlerOutcome::ack()).collect()
}

/// Appends `jobs` to the broker's retained log before any subscription opens, the way a producer
/// that ran earlier would have left them in a stream file.
async fn record(
    broker: &FileTestBroker,
    stream: &str,
    jobs: impl IntoIterator<Item = Job>,
) -> Result<(), Box<dyn std::error::Error>> {
    let publisher = broker.publisher();
    for job in jobs {
        publisher.message(&job).to(stream).publish().await?;
    }
    Ok(())
}

/// The run every seeking test replays: the second job marks what follows it poisoned and names
/// where to resume - position 3, the fourth entry.
fn poisoned_run() -> Vec<Job> {
    vec![
        Job::plain(1),
        Job {
            id: 2,
            resume_at: Some(3),
        },
        Job::plain(3),
        Job::plain(4),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_reads_its_position_off_the_delivery_context()
-> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("seek-context", "0.1.0")).with_broker(
        FileTestBroker::new(),
        |b| {
            // The audit reply through the policy the mount site names, rather than through the
            // broker's default: the same route the other tests take by omission.
            b.include(work).out(Reply, FileTestPublish);
        },
    );
    let tb = TestApp::start(app).await?;

    for id in [1, 2] {
        tb.message(&Job::plain(id)).to("jobs").publish().await?;
    }

    tb.broker::<FileTestBroker>()
        .subscriber("jobs")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    // Each delivery reported its own place in the retained log, in order.
    assert_eq!(
        tb.broker::<FileTestBroker>()
            .published::<Seen>("audit")
            .decoded(),
        vec![Seen { id: 1, at: 0 }, Seen { id: 2, at: 1 }],
    );
    Ok(())
}

// --8<-- [start:test]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_repositions_its_own_subscription_through_the_seek_key()
-> Result<(), Box<dyn std::error::Error>> {
    // A run recorded before the service exists, the way an earlier producer would have left it.
    let broker = FileTestBroker::new();
    record(&broker, "jobs", poisoned_run()).await?;

    let app = RustStream::new(AppInfo::new("seek-context", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
    });
    let tb = TestApp::start(app).await?;
    // The subscription opens at the start of the retained log, so the whole recorded run replays
    // into it; nothing else is published.
    tb.settle().await?;

    tb.broker::<FileTestBroker>()
        .subscriber("jobs")
        .assert_called(3)
        .settled(HandlerOutcome::ack());
    assert_eq!(
        tb.broker::<FileTestBroker>()
            .published::<Seen>("audit")
            .decoded(),
        vec![
            Seen { id: 1, at: 0 },
            Seen { id: 2, at: 1 },
            // Job 3 sat at position 2 and was skipped; job 4 arrives from the seek target.
            Seen { id: 4, at: 3 },
        ],
    );
    Ok(())
}
// --8<-- [end:test]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_body_reaches_the_seek_handle_through_the_batch_context()
-> Result<(), Box<dyn std::error::Error>> {
    let broker = FileTestBroker::new();
    record(&broker, "digest", poisoned_run()).await?;

    let app = RustStream::new(AppInfo::new("batch-context", "0.1.0")).with_broker(broker, |b| {
        b.include(digest.batch(nonzero!(4)));
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    // Two batches: the whole recorded run, then the one the seek repositioned onto. A batch is
    // settled before the reposition takes effect, so the skipped region is part of the first
    // batch and absent from the second - the seek governs what comes after it, not what it saw.
    tb.broker::<FileTestBroker>()
        .subscriber("digest")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    let seen: Vec<u64> = tb
        .broker::<FileTestBroker>()
        .subscriber("digest")
        .received::<Job>()
        .into_iter()
        .map(|job| job.id)
        .collect();
    assert_eq!(seen, vec![1, 2, 3, 4, 4]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_that_declares_a_key_is_published_there()
-> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        FileTestBroker::new(),
        |b| {
            b.include(checkout);
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<FileTestBroker>()
        .message(&Job::plain(7))
        .to("checkout")
        .publish()
        .await?;

    tb.broker::<FileTestBroker>()
        .subscriber("checkout")
        .assert_called_once();
    tb.broker::<FileTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_declaring_no_key_takes_the_one_the_subscriber_names()
-> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("reply-destination", "0.1.0")).with_broker(
        FileTestBroker::new(),
        |b| {
            b.include(review).out(Reply, FileTestPublish);
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<FileTestBroker>()
        .message(&Job::plain(3))
        .to("review")
        .publish()
        .await?;

    tb.broker::<FileTestBroker>()
        .subscriber("review")
        .assert_called_once();
    tb.broker::<FileTestBroker>()
        .published::<Seen>("reviewed")
        .assert_called_once()
        .with(&Seen { id: 3, at: 0 });
    Ok(())
}
