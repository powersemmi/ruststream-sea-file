//! Shared test runtime.
//!
//! `sea-streamer-file` manages producers through a process-wide singleton whose dispatcher
//! task is spawned onto the first tokio runtime that touches it. A per-test runtime (what
//! `#[tokio::test]` creates) would die at the end of that first test and take the dispatcher
//! with it, hanging every later flush/end in the process. All tests therefore run on one
//! shared multi-thread runtime.

use std::sync::LazyLock;

use tokio::runtime::Runtime;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime builds")
});

pub(crate) fn rt() -> &'static Runtime {
    &RT
}
