//! Shared test runtime for the file transport's tests.
//!
//! `sea-streamer-file` manages producers through a process-wide singleton whose dispatcher
//! task is spawned onto the first tokio runtime that touches it. A per-test runtime (what
//! `#[tokio::test]` creates) would die at the end of that first test and take the dispatcher
//! with it, hanging every later flush/end in the process. All tests therefore run on one
//! shared multi-thread runtime.

use std::future::Future;
use std::sync::LazyLock;

use tokio::runtime::Runtime;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime builds")
});

// Every test binary compiles its own copy of this module, and none of them uses every helper.
#[allow(dead_code)]
pub(crate) fn rt() -> &'static Runtime {
    &RT
}

/// Runs a test body that opens stream files on the shared runtime.
#[allow(dead_code)]
pub(crate) fn on_a_file<F>(body: F)
where
    F: Future<Output = ()>,
{
    RT.block_on(body);
}

/// A stream file of this test run's own, in the temp directory.
///
/// The process id and a counter keep two runs - and two tests inside one run - off each other's
/// files, so a stale file from an interrupted run can never be read as this one's.
#[allow(dead_code)]
pub(crate) fn tmp_path(name: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

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
