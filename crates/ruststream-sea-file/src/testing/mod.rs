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
//! Nothing here is named at a mount site. A service's routes file keeps the descriptor and the
//! policy it ships with - [`FilePublish`](crate::FilePublish),
//! [`StdioPublish`](crate::StdioPublish) - and both pair against this broker, so the wiring a test
//! covers is the wiring that runs. There is deliberately no harness-only policy to swap in.
//!
//! How far the resemblance goes is measured rather than asserted: the framework's own contract
//! suites - the lifecycle ladder, seeking and batching - run against this broker as well as
//! against a real stream file, so a contract the file keeps and this broker quietly broke would
//! fail here rather than in a service's test.
//!
//! Everything beyond that is left to the real transport: files and beacons, end-of-stream marks,
//! the header envelope and the durability a restart depends on. Those are verified end to end
//! against real stream files instead.
//!
//! One difference is left, and it is temporary rather than intended. Acknowledgement succeeds
//! here, where both real transports report `AckError::Unsupported`, and `nack(requeue = true)`
//! re-queues, where neither transport can redeliver at all. The honest version is written and
//! measured (see the reverted commit on this history); what holds it back is
//! `conformance::harness::run_suite`, whose scenarios settle every delivery with
//! `expect("ack failed")`, so a broker answering `Unsupported` fails the routing suite it must
//! pass. The framework's other suites and the `TestApp` harness are indifferent - a handler's
//! settlement is recorded from the handler's own decision, before the broker is asked - so
//! `run_suite` adapting to a transport that cannot settle is the whole of what is needed.
//!
//! Until then: do not read an ack's success here as evidence that the transport records progress,
//! and do not build a test on a redelivery. Neither survives contact with a stream file or a pipe;
//! what resumes a subscription is the descriptor's start position or a captured
//! [`FilePosition`](crate::FilePosition).

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedFileTestBroker, FileTestBroker, FileTestPublisher};
pub use subscriber::{FileTestMessage, FileTestSubscriber};

// The in-process half of `FileSeeker`, which lives at the crate root: one seeker type serves both
// transports, so the key a handler reads it by yields the same value under either.
pub(crate) use subscriber::LogSeeker;
