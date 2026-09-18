//! In-process stands for the two transports, behind the `testing` feature.
//!
//! A service's handlers are unit-tested with the framework's
//! [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) harness, which
//! supplies the input, drives the reaction to a standstill and records what happened. The stands
//! here are what it drives: [`FileTestBroker`] for stream files and [`StdioTestBroker`] for
//! pipelines. Every suite in this crate runs locally, on temp files and in-process pipes, with no
//! broker to start.
//!
//! # The mount site does not change
//!
//! Nothing here is named in a service's routes. A service keeps the descriptor and the policy it
//! ships with - [`FileStream`](crate::FileStream) resolves against the file stand,
//! [`FilePublish`](crate::FilePublish) and [`StdioPublish`](crate::StdioPublish) pair against
//! their own - so the wiring a test covers is the wiring that runs, and there is deliberately no
//! harness-only policy to swap in.
//!
//! ```
//! use ruststream::testing::TestApp;
//! use ruststream_sea_file::file::prelude::*;
//! use ruststream_sea_file::testing::FileTestBroker;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Outgoing, Serialize, Deserialize)]
//! struct Job {
//!     id: u64,
//! }
//!
//! #[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
//! struct Seen {
//!     id: u64,
//! }
//!
//! #[subscriber(FileStream::new("jobs"), publish("audit"))]
//! async fn work(job: &Job) -> Seen {
//!     Seen { id: job.id }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let app = RustStream::new(AppInfo::new("jobs", "0.1.0"))
//!     .with_broker(FileTestBroker::new(), |b| {
//!         b.include(work);
//!     });
//! let tb = TestApp::start(app).await.expect("startup failed");
//!
//! tb.broker::<FileTestBroker>()
//!     .message(&Job { id: 7 })
//!     .to("jobs")
//!     .publish()
//!     .await
//!     .expect("publish");
//!
//! tb.broker::<FileTestBroker>()
//!     .published::<Seen>("audit")
//!     .assert_called_once()
//!     .with(&Seen { id: 7 });
//! # }
//! ```
//!
//! # What each stand reproduces
//!
//! [`FileTestBroker`] keeps a retained, positioned log in memory - no file, no server. That is
//! the one transport property a stream file's handlers are written against: a delivery reports a
//! [`FilePosition`](crate::FilePosition), the subscription hands out a
//! [`FileSeeker`](crate::FileSeeker), and [`FileContext`](crate::FileContext) and
//! [`FileBatchContext`](crate::FileBatchContext) build off its deliveries unchanged. A seeking
//! service therefore mounts on it with no edit at all, and a batch handler sees batches of the
//! size its mount site asked for. Settlement answers what a file answers: `ack` and `nack` report
//! [`AckError::Unsupported`](ruststream::AckError::Unsupported). Positions answer what a file
//! answers too: the log is numbered from one per stream key, and a seek to a position the log has
//! not reached is refused and ends the subscription, so a test cannot pass on a resume the file
//! would refuse.
//!
//! [`StdioTestBroker`] is a separate type because a pipe answers differently. It addresses no
//! retry copy, so a registration that names no destination refuses to start here exactly as it
//! refuses against a real pipeline, and it offers no seeking. Its publish side records what the
//! service wrote to standard output, under the stream key the line would carry, so a test reads
//! the copies back with `published::<T>("jobs.retry")`. One stand for both transports would let a
//! registration start under a test that a pipe refuses.
//!
//! # What they leave to the real transport
//!
//! Files and beacons, the end-of-stream mark, the header envelope, the line format and the
//! durability a restart depends on. A [`replay`](crate::FileStream::replay) opens at the start of
//! the retained log here, but nothing in process writes an end-of-stream mark, so the
//! subscription does not complete the way a finished file's does. A test on a stand must not
//! conclude that a payload survives a shell pipeline or that a `.ss` file holds particular bytes;
//! those are covered against real files and real pipes by this repository's own suites.
//!
//! How far the resemblance goes is measured rather than claimed. The framework's contract
//! suites (the lifecycle ladder, seeking and batching) run against these stands as well as
//! against a real stream file, so a contract the file keeps and a stand quietly broke fails here
//! rather than in a service's tests.

mod broker;
mod router;
mod stdio;
mod subscriber;

pub use broker::{ConnectedFileTestBroker, FileTestBroker, FileTestPublisher};
pub use stdio::{
    ConnectedStdioTestBroker, StdioTestBroker, StdioTestPublisher, StdioTestSubscriber,
};
pub use subscriber::{FileTestMessage, FileTestSubscriber};

// The in-process half of `FileSeeker`, which lives at the crate root: one seeker type serves both
// transports, so the key a handler reads it by yields the same value under either.
pub(crate) use subscriber::LogSeeker;
