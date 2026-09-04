//! How this crate answers a batch handler.
//!
//! Neither client reads several entries at a time: `sea-streamer`'s consumer yields exactly one
//! message per `next()`, on a stream file and on standard input alike, and exposes no count to
//! translate a batch size into. A batch handler must still run, so both subscribers assemble
//! their batches on the client with the framework's own
//! [`BufferedSubscriber`](ruststream::BufferedSubscriber) and delegate
//! [`BatchSubscriber`](ruststream::BatchSubscriber) to it - which is what honours the size the
//! mount site named, per subscription.
//!
//! What is this crate's own choice is the deadline that closes a partial batch.

use std::time::Duration;

/// How long a batch waits for further deliveries after its first one before going out short.
///
/// Every transport here is local - a file on disk, a pipe, an in-process log - and the file
/// client resets its read backoff the moment a burst starts, so deliveries that already exist
/// land far inside this window. What the deadline actually bounds is the latency of a batch at
/// an idle tail, where waiting longer would buy nothing but delay.
pub(crate) const BATCH_MAX_WAIT: Duration = Duration::from_millis(10);
