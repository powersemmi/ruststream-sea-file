//! In-process test support, behind the `testing` feature.
//!
//! [`FileTestBroker`] is a handler-stub transport that reproduces the crate's routing over a
//! retained, positioned log in memory - no file, no server - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so application
//! handlers can be unit-tested with the [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! The log is retained and positioned because that is the one transport property a stream file's
//! handlers are written against: a delivery reports a [`FilePosition`](crate::FilePosition), the
//! subscription hands out a [`FileSeeker`](crate::FileSeeker), and the file form's
//! [`FileContext`](crate::FileContext) and [`FileBatchContext`](crate::FileBatchContext) build off
//! its deliveries unchanged - so a service that seeks mounts on this broker with no edit at all,
//! and [`FileStream`](crate::FileStream) resolves here the way it resolves against a file.
//!
//! Settlement answers what a stream file answers: `ack` and `nack` report
//! [`AckError::Unsupported`](ruststream::AckError::Unsupported), so a handler that reads the
//! answer behaves under the harness the way it behaves in production.
//!
//! [`StdioTestBroker`] is the stand for the other transport, and it is a separate type because
//! the two transports answer differently. A pipe carries a message to the next process and never
//! back to this one, so a stdio subscription addresses none of its retry copies and the mount
//! site names the destination; a stream file's stream key is both ends of it and names nothing.
//! One stand for both would let a registration start under a test that a pipe refuses. A stdio
//! subscription does not seek here either, which is the other thing a pipe cannot do.
//!
//! Nothing here is named at a mount site. A service's routes file keeps the descriptor and the
//! policy it ships with - [`FilePublish`](crate::FilePublish) against the file stand,
//! [`StdioPublish`](crate::StdioPublish) against the stdio one - so the wiring a test covers is
//! the wiring that runs. There is deliberately no harness-only policy to swap in.
//!
//! How far the resemblance goes is measured rather than asserted: the framework's own contract
//! suites - the lifecycle ladder, seeking and batching - run against this broker as well as
//! against a real stream file, so a contract the file keeps and this broker quietly broke would
//! fail here rather than in a service's test.
//!
//! Everything beyond that is left to the real transport: files and beacons, end-of-stream marks,
//! the header envelope and the durability a restart depends on. Those are verified end to end
//! against real stream files instead.

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
