//! Conformance: the routing suite against the in-process transport, and the lifecycle plus
//! seeking suites against real stream files in the temp directory - no external broker
//! exists to need.

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
