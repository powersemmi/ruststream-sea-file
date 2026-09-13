//! Conformance: every suite this crate's capabilities justify, run against a real stream file in
//! the temp directory and against the in-process stand-in.
//!
//! Both forms, because either one alone leaves a hole. The file is what ships, so it is what the
//! contract is really about; the stand-in is what a service's own tests run on, so a contract it
//! quietly fails is one a test author is misled by. Where the two disagree the disagreement is the
//! finding, and the stand-in is what gets fixed.
//!
//! The routing suite is the exception, and only because of how it is built: it drives the broker
//! through `TestableBroker`, which the stand-in implements and a stream file cannot.
//!
//! Not run: `request_reply`, `transactions` and `owned_transactions`, for the plain reason that
//! neither transport implements those capabilities. The stdio form is not driven through the
//! suites either: its shutdown ends every stdio consumer and producer in the process by the
//! client's design, so one suite would take the rest of the binary's tests down with it. Its
//! round trip is covered over a real pipe in `integration_sea.rs`.

#![cfg(feature = "testing")]

mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::conformance::{capabilities, harness};
use ruststream_sea_file::testing::FileTestBroker;
use ruststream_sea_file::{FileBroker, FileStream};

fn tmp_path(name: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir()
        .join(format!(
            "ruststream-sea-{name}-{}-{}.ss",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

/// Nothing the broker or the descriptor contributes to a document may carry a password.
///
/// Neither transport authenticates: a stream file is opened by path and a pipe by being the
/// process's own, so there is no credential to plant and the scan looks for a string the
/// configuration never held. It runs anyway, as the guard it is: the document reports the path
/// and the stream key on purpose, and a field added later that serialized the broker's
/// configuration wholesale would be caught here rather than after it shipped.
#[test]
fn the_document_carries_no_credential() {
    harness::describes_without_credentials(
        &FileBroker::new(tmp_path("credentials")),
        &FileStream::new("orders"),
        "hunter2",
    );
}

#[test]
fn sea_test_broker_passes_conformance_suite() {
    common::rt().block_on(async {
        harness::run_suite(FileTestBroker::new).await;
    });
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_lifecycle() {
    common::rt().block_on(async {
        let path = tmp_path("lifecycle");
        harness::lifecycle(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same ladder in process, including the publisher that outlives the shutdown: a stand-in
/// that kept accepting publishes through a dead handle would teach a service's tests that the
/// aliasing case is harmless, which against a real file it is not.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn sea_test_broker_passes_lifecycle() {
    common::rt().block_on(async {
        harness::lifecycle(
            FileTestBroker::new,
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The address the descriptor reports, held to its promise against real stream files: a publish
/// there must reach the subscription that reported it. This is what a `retry_after` rests on here,
/// because neither transport settles a delivery.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_redelivery_address() {
    common::rt().block_on(async {
        let path = tmp_path("redelivery-address");
        harness::redelivery_address(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same promise in process, because a service's own retry tests run here: a stand-in that
/// reported an address nothing arrived at would pass a registration the file refuses.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn sea_test_broker_passes_redelivery_address() {
    common::rt().block_on(async {
        harness::redelivery_address(
            FileTestBroker::new,
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The batch contract against real stream files: the suite opens its subscription at a size
/// smaller than the run, so a batch coming back longer than the mount site asked for fails here.
/// Neither client batches on the wire, so what this pins is the client-side assembly.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_batch_suite() {
    common::rt().block_on(async {
        let path = tmp_path("batches");
        capabilities::batches(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same contract on the in-process transport, which batches the same way, so a batch handler
/// under the harness sees what it would see against a file.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn sea_test_broker_passes_batch_suite() {
    common::rt().block_on(async {
        capabilities::batches(
            FileTestBroker::new,
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_seeking_suite() {
    common::rt().block_on(async {
        let path = tmp_path("seeking");
        capabilities::seeking(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The seeking contract in process, on the retained log the stand-in exists to reproduce: pinned
/// captured positions, a seek forward skipping what is queued, and live delivery afterwards. This
/// is the capability a service is most likely to write tests around, so the stand-in owes it the
/// same answers a file gives.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn sea_test_broker_passes_seeking_suite() {
    common::rt().block_on(async {
        capabilities::seeking(
            FileTestBroker::new,
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}
