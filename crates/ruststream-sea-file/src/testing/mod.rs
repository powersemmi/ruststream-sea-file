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
//! Everything beyond that is left to the real transport: files and beacons, end-of-stream marks,
//! the header envelope, `AckError::Unsupported`, and the durability a restart depends on. Those are
//! verified end to end against real stream files instead.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedFileTestBroker, FileTestBroker, FileTestPublish, FileTestPublisher};
pub use subscriber::{FileTestMessage, FileTestSubscriber};

// The in-process half of `FileSeeker`, which lives at the crate root: one seeker type serves both
// transports, so the key a handler reads it by yields the same value under either.
pub(crate) use subscriber::LogSeeker;
