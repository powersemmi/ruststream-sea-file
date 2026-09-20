//! What the in-process stands do with the buffer the framework hands them on the publish path.
//!
//! Both stands declare `Take`, because a recorded delivery keeps its payload; the two real
//! publishers lend theirs, so there is nothing to hand over on those. Content equality cannot
//! tell a hand-over from a copy, so the payload is followed by the address it was written at.

#![cfg(feature = "testing")]

use ruststream::testing::TestableBroker;
use ruststream::{Broker, BytesMut, OutgoingMessage, Publisher};
use ruststream_sea_file::testing::{FileTestBroker, StdioTestBroker};

/// A payload buffer of the kind the publish path produces, with the address it was written at.
fn body() -> (BytesMut, *const u8) {
    let body = BytesMut::from(&br#"{"id":7}"#[..]);
    let at = body.as_ptr();
    (body, at)
}

#[tokio::test]
async fn the_file_stand_records_the_buffer_it_was_handed() {
    let connected = FileTestBroker::new().connect().await.expect("connects");
    let publisher = connected.publisher();
    let (body, written_at) = body();

    publisher
        .publish(OutgoingMessage::produced("orders", body), None)
        .await
        .expect("the stand accepts the publish");

    assert_eq!(
        connected
            .published("orders")
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the delivery must carry the buffer the publish wrote, not a copy of it",
    );
}

#[tokio::test]
async fn the_stdio_stand_records_the_buffer_it_was_handed() {
    let connected = StdioTestBroker::new().connect().await.expect("connects");
    let publisher = connected.publisher();
    let (body, written_at) = body();

    publisher
        .publish(OutgoingMessage::produced("orders", body), None)
        .await
        .expect("the stand accepts the publish");

    assert_eq!(
        connected
            .published("orders")
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the delivery must carry the buffer the publish wrote, not a copy of it",
    );
}
