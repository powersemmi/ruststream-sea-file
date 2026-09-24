//! The brokers' in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected brokers, their subscribers, their publishers and their deliveries each carry this
//! transport as a variant of their own, so a service's descriptors, publish policies and seeking
//! handlers run against it unchanged. It has no configuration of its own: it reads the production
//! broker's settings, and a message crosses it through the same conversions a real one goes
//! through (the stream key check, the header envelope, the stdio publisher's refusal of an empty
//! line), so it never succeeds where a stream file or a pipe fails.
//!
//! What it models. For a stream file: one retained log per stream key, numbered from one, a live
//! subscription that starts at the tip, a replay that reads what the log holds and completes at its
//! end, positions and seeking with the file's refusals, and the end-of-stream mark
//! [`end_with_eos`](crate::FileBroker::end_with_eos) writes on shutdown. For a pipe: standard input
//! is what the test feeds in, standard output is what the service writes, and what the service
//! writes comes back to its own subscriptions only under
//! [`loopback`](crate::StdioBroker::loopback). What belongs to the real transport and is left to
//! the live mode: the bytes a `.ss` file holds, beacons, durability across a restart, the line
//! format a shell pipeline carries, and the process-wide reach of a stdio shutdown.

mod file;
mod pipe;
mod queue;

pub(crate) use file::{FileQueue, LogSeeker, MemoryFile};
pub(crate) use pipe::{MemoryPipe, PipeQueue};
pub(crate) use queue::Release;
