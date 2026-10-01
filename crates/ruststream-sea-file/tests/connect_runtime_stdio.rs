//! A stdio subscription opened from another runtime keeps delivering once that runtime has
//! stopped: the task that reads the client's consumer runs on the runtime the broker connected on.
//!
//! One test in this binary, and it owns the process's pipes: shutting this transport down ends
//! every stdio consumer and producer in the process, so a second test beside it would be torn down
//! by it.

use std::pin::pin;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscribe, Subscriber,
};
use ruststream_sea_file::{ConnectedStdioBroker, StdioBroker, StdioSubscriber};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// How long the line is given to come round.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Opens `name` from a current-thread runtime on a thread of its own, and stops that runtime
/// before handing the broker and the subscriber back.
async fn subscribe_elsewhere(
    connected: ConnectedStdioBroker,
    name: &'static str,
) -> (ConnectedStdioBroker, StdioSubscriber) {
    let (done, opened) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the caller's runtime builds");
        let subscriber = runtime
            .block_on(connected.subscribe(name))
            .expect("the subscription opens");
        // Stopped before the subscription is read, so a task left on it is gone by then.
        drop(runtime);
        let _ = done.send((connected, subscriber));
    });
    opened
        .await
        .expect("the caller's thread hands the subscription back")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_outlives_the_runtime_that_opened_it() {
    let connected = StdioBroker::new()
        .loopback()
        .connect()
        .await
        .expect("stdio attaches");
    let (connected, mut subscriber) = subscribe_elsewhere(connected, "jobs").await;

    connected
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"line".as_slice()), None)
        .await
        .expect("the line is written");

    let line = {
        let mut stream = pin!(subscriber.stream());
        timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the subscription delivers after the runtime that opened it stopped")
            .expect("the stream stays open")
            .expect("the line is ok")
    };
    assert_eq!(line.payload(), b"line");

    drop(subscriber);
    let _ = connected.shutdown().await;
}
