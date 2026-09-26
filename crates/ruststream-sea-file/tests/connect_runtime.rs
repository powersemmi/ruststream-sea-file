//! A stream file subscription opened from another runtime keeps delivering once that runtime has
//! stopped: the tasks behind it, this crate's driver and the client's reader, run on the runtime
//! the broker connected on.
//!
//! This is the shape of a caller on a thread of its own, with a current-thread runtime that ends
//! when the thread's work does, while the connection it used lives on.

mod common;

use std::pin::pin;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_sea_file::{ConnectedFileBroker, FileBroker, FileStream, FileSubscriber};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// How long a delivery is given to arrive.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Opens `descriptor` from a current-thread runtime on a thread of its own, and stops that runtime
/// before handing the broker and the subscriber back.
async fn subscribe_elsewhere(
    connected: ConnectedFileBroker,
    descriptor: FileStream,
) -> (ConnectedFileBroker, FileSubscriber) {
    let (done, opened) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the caller's runtime builds");
        let subscriber = runtime
            .block_on(connected.subscribe_stream(descriptor))
            .expect("the subscription opens");
        // Stopped before the subscription is read, so a task left on it is gone by then.
        drop(runtime);
        let _ = done.send((connected, subscriber));
    });
    opened
        .await
        .expect("the caller's thread hands the subscription back")
}

async fn publish(connected: &ConnectedFileBroker, stream: &str, body: &[u8]) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(stream, body), None)
        .await
        .expect("the publish appends");
}

async fn next_body(subscriber: &mut FileSubscriber) -> Vec<u8> {
    let mut stream = pin!(subscriber.stream());
    timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the subscription delivers after the runtime that opened it stopped")
        .expect("the stream stays open")
        .expect("the delivery is ok")
        .payload()
        .to_vec()
}

#[test]
fn a_live_subscription_outlives_the_runtime_that_opened_it() {
    common::on_a_file(async {
        let path = common::tmp_path("connect-runtime-live");
        let connected = FileBroker::new(path.clone())
            .connect()
            .await
            .expect("the file opens");

        let (connected, mut subscriber) =
            subscribe_elsewhere(connected, FileStream::new("orders")).await;
        publish(&connected, "orders", b"live").await;

        assert_eq!(next_body(&mut subscriber).await, b"live");
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        let _ = std::fs::remove_file(&path);
    });
}

#[test]
fn a_replay_outlives_the_runtime_that_opened_it() {
    common::on_a_file(async {
        let path = common::tmp_path("connect-runtime-replay");
        let connected = FileBroker::new(path.clone())
            .connect()
            .await
            .expect("the file opens");
        publish(&connected, "orders", b"retained").await;

        let (connected, mut subscriber) =
            subscribe_elsewhere(connected, FileStream::new("orders").replay()).await;

        assert_eq!(next_body(&mut subscriber).await, b"retained");
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        let _ = std::fs::remove_file(&path);
    });
}
