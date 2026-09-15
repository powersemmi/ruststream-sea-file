//! Where a stdio registration's deferred copy actually goes, on the real transport.
//!
//! A pipe settles nothing, so `retry_after` on this transport is a line the service writes to its
//! standard output under the destination the mount site named. The in-process stand records that
//! line, which proves the wiring; what it cannot prove is that the copy survives the client's line
//! format and reaches a reader of that stream key. Here it does: the service and the reader are
//! two brokers on the process's own pipes, with
//! [`loopback`](ruststream_sea_file::StdioBroker::loopback) joining the output back to the input,
//! so the copy takes the client's real publish and the client's real consume on its way.
//!
//! One test in this binary, and it owns the process's pipes: shutting this transport down ends
//! every stdio consumer and producer in the process, so a second test beside it would be torn down
//! by it.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscribe, Subscriber};
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};

/// How long the handler asks the runtime to hold the message back. Short, because the clock here
/// is the real one.
const RETRY_DELAY: Duration = Duration::from_millis(150);

/// How long the copy is given to come round.
const RECV_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Job {
    id: u64,
}

/// Asks for a pause before another attempt, which on a pipe means a copy sent downstream.
#[subscriber("jobs")]
async fn defer(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deferred_copy_leaves_under_the_name_the_mount_site_gave_it() {
    // The next stage of the pipeline, reading the key the mount site sends copies to. It opens
    // first, because a line nobody is reading when it is written is a line nobody reads.
    let downstream = StdioBroker::new()
        .loopback()
        .connect()
        .await
        .expect("stdio attaches");
    let mut copies = downstream
        .subscribe("jobs.retry")
        .await
        .expect("subscription opens");

    let app = RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(
        StdioBroker::new().loopback(),
        |b| {
            b.include(defer).out_retry(Publish).to("jobs.retry");
        },
    );
    let running = app.start().await.expect("the service starts");

    downstream
        .publisher()
        .message(&Job { id: 3 })
        .to("jobs")
        .publish()
        .await
        .expect("the job reaches the stage");

    let copy = {
        let mut stream = pin!(copies.stream());
        tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the deferred copy arrives")
            .expect("the stream stays open")
            .expect("the copy is ok")
    };
    assert_eq!(
        serde_json::from_slice::<Job>(copy.payload()).expect("the copy decodes"),
        Job { id: 3 },
        "the copy carries the message the handler deferred",
    );

    running.shutdown().await.expect("the service stops");
    let _ = downstream.shutdown().await;
}
