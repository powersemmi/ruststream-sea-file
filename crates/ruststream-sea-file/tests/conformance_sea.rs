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
