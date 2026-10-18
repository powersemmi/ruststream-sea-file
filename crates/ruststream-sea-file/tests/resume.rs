//! A service that resumes at a stored sequence, with the file broker in process and against a
//! real stream file.
//!
//! The file keeps no record of how far a service read, so a service stores the first sequence it
//! has not handled and opens its subscription there on the next start. After a run that handled
//! every message, that sequence is one past the last message the file holds.

#![cfg(feature = "testing")]

mod common;

use std::fmt::Display;

use ruststream::testing::{InProcess, TestApp};
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

/// The file the in-process runs are built with; nothing is opened at it.
const PATH: &str = "/var/lib/shop/orders.ss";

/// How many orders the file holds before the service starts.
const RECORDED: u64 = 3;

#[derive(Debug, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Invoices each order once, across restarts.
#[subscriber(FileStream::new("orders"))]
async fn invoice(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "invoiced");
    HandlerOutcome::ack()
}

/// The service, opened at the first sequence the previous run did not invoice.
fn app(file: FileBroker, resume_at: u64) -> RustStream {
    RustStream::new(AppInfo::new("invoicing", "0.1.0")).with_broker(file, |b| {
        b.include(invoice.start_at(FilePosition::sequence(resume_at)));
    })
}

fn invoiced(tb: &TestApp<()>) -> Vec<u64> {
    tb.broker::<FileBroker>()
        .subscriber("orders")
        .received::<Order>()
        .into_iter()
        .map(|order| order.id)
        .collect()
}

/// The body both modes run: the service resumes one past the last recorded order, so it invoices
/// the next order appended and none of the recorded ones.
async fn the_service_resumes_one_past_the_last_message(tb: TestApp<()>) {
    tb.broker::<FileBroker>()
        .message(&Order { id: RECORDED + 1 })
        .publish()
        .await
        .expect("publish drives the delivery");
    tb.settle().await.expect("the service settles");

    assert_eq!(invoiced(&tb), [RECORDED + 1]);
    tb.shutdown().await.expect("shutdown");
}

/// What a sequence further ahead than the next one gets in both modes: the service does not
/// start, and the error names the subscription whose position was refused.
fn assert_refused(refused: &impl Display) {
    let refused = refused.to_string();
    assert!(
        refused.contains("seek") && refused.contains("orders"),
        "{refused}"
    );
}

/// Records the orders on a clone of the service's broker before the service starts, the way an
/// earlier producer would have left them.
async fn record_in_process(file: &FileBroker) {
    let _connected = file
        .clone()
        .connect_in_process()
        .await
        .expect("the file connects in process");
    let publisher = file.publisher();
    for id in 1..=RECORDED {
        publisher
            .message(&Order { id })
            .publish()
            .await
            .expect("the order is recorded");
    }
}

/// Records the orders on the stream file with a producer run of its own, which numbers them from
/// one.
async fn record_on_the_file(path: &str) {
    let shop =
        RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(FileBroker::new(path), |_| {});
    let shop = TestApp::start_live(shop)
        .await
        .expect("the producer starts on the file");
    for id in 1..=RECORDED {
        shop.broker::<FileBroker>()
            .message(&Order { id })
            .publish()
            .await
            .expect("the order is recorded");
    }
    shop.shutdown().await.expect("the producer stops");
}

#[tokio::test]
async fn the_service_resumes_one_past_the_last_message_in_process() {
    let file = FileBroker::new(PATH);
    record_in_process(&file).await;
    let tb = TestApp::start(app(file, RECORDED + 1))
        .await
        .expect("the service starts at the next sequence");
    the_service_resumes_one_past_the_last_message(tb).await;
}

#[test]
fn the_service_resumes_one_past_the_last_message_on_a_stream_file() {
    common::on_a_file(async {
        let path = common::tmp_path("resume-next");
        record_on_the_file(&path).await;
        let tb = TestApp::start_live(app(FileBroker::new(&path), RECORDED + 1))
            .await
            .expect("the service starts at the next sequence");
        the_service_resumes_one_past_the_last_message(tb).await;
        let _ = std::fs::remove_file(&path);
    });
}

#[tokio::test]
async fn a_sequence_further_ahead_stops_the_service_at_startup_in_process() {
    let file = FileBroker::new(PATH);
    record_in_process(&file).await;
    let Err(refused) = TestApp::start(app(file, RECORDED + 2)).await else {
        panic!("the file has not written the sequence after the next one");
    };
    assert_refused(&refused);
}

#[test]
fn a_sequence_further_ahead_stops_the_service_at_startup_on_a_stream_file() {
    common::on_a_file(async {
        let path = common::tmp_path("resume-ahead");
        record_on_the_file(&path).await;
        let Err(refused) = TestApp::start_live(app(FileBroker::new(&path), RECORDED + 2)).await
        else {
            panic!("the file has not written the sequence after the next one");
        };
        assert_refused(&refused);
        let _ = std::fs::remove_file(&path);
    });
}
