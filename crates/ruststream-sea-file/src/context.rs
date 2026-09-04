//! The file transport's typed contexts and their compile-time keys.
//!
//! A stream file is a replayable log, so a handler running on it can reposition its own
//! subscription. The delivery's position and the subscription's reposition handle travel as
//! fields of a context the runtime builds per delivery, and handlers read them by key -
//! [`Ctx`](ruststream::runtime::Ctx) on the attribute path, `ctx.context(..)` on the manual one.
//!
//! Standard input has no retained log, so nothing here applies to it: its deliveries carry no
//! reposition handle, which is what makes a stdio mount of a seeking handler a compile error.

use ruststream::{BuildBatchContext, BuildContext, ContextField, Field};

use crate::message::FilePosition;
use crate::subscriber::{FileMessage, FileSeeker};

/// The file transport's per-delivery context: this delivery's position in the stream file, and
/// the subscription's own seeker.
///
/// The runtime builds one per delivery, and handlers read its fields by key - [`Position`] and
/// [`SeekHandle`]. A body that repositions a stream file names this type as its context axis and
/// needs nothing else.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
/// use ruststream::Seeker;
/// use ruststream_sea_file::{FileContext, FilePosition, SeekHandle};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<Job, (), (), FileContext> for Replayer {
///     async fn handle(
///         &self,
///         job: &Job,
///         _outs: &(),
///         ctx: &mut Context<'_, FileContext>,
///     ) -> Result<(), HandlerOutcome> {
///         if job.id == 0 && ctx.context(SeekHandle).seek(FilePosition::end()).await.is_err() {
///             return Err(HandlerOutcome::retry());
///         }
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug)]
pub struct FileContext {
    position: FilePosition,
    seeker: FileSeeker,
}

impl BuildContext<FileMessage> for FileContext {
    fn build(msg: &FileMessage) -> Self {
        Self {
            position: ruststream::Positioned::position(msg),
            // A clone of the subscription's pre-minted handle: reference-count bumps only,
            // nothing allocated per delivery.
            seeker: msg.seeker().clone(),
        }
    }
}

/// The file transport's subscription-scoped batch context: the subscription's own seeker, shared
/// by every delivery of the batch.
///
/// The runtime builds one per dispatched batch from the batch's first delivery, and a batch body
/// reads it by key - [`SeekHandle`]. Per-delivery data (a [`Position`]) has no place here: a
/// batch spans many deliveries, so the position a body reacts to rides the elements themselves,
/// and keeping this a separate type from [`FileContext`] is what rejects a batch body asking for
/// per-delivery fields at compile time.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
/// use ruststream::Seeker;
/// use ruststream_sea_file::{FileBatchContext, FilePosition, SeekHandle};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// struct Replayer;
///
/// impl Handle<[Job], (), (), FileBatchContext> for Replayer {
///     async fn handle(
///         &self,
///         batch: &[Job],
///         _outs: &(),
///         ctx: &mut Context<'_, FileBatchContext>,
///     ) -> Result<(), Vec<HandlerOutcome>> {
///         if batch.iter().any(|job| job.id == 0)
///             && ctx.context(SeekHandle).seek(FilePosition::end()).await.is_err()
///         {
///             return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
///         }
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug)]
pub struct FileBatchContext {
    seeker: FileSeeker,
}

impl BuildBatchContext<FileMessage> for FileBatchContext {
    fn build(first: &FileMessage) -> Self {
        Self {
            seeker: first.seeker().clone(),
        }
    }
}

// The in-process transport delivers under the same contexts and the same keys: it reports a
// position and hands out a `FileSeeker` too, so a service that seeks mounts on the test broker
// with no edit at all. Standard input implements neither, which is what keeps a seeking handler
// off it at compile time.
#[cfg(feature = "testing")]
impl BuildContext<crate::testing::FileTestMessage> for FileContext {
    fn build(msg: &crate::testing::FileTestMessage) -> Self {
        Self {
            position: ruststream::Positioned::position(msg),
            seeker: msg.seeker().clone(),
        }
    }
}

#[cfg(feature = "testing")]
impl BuildBatchContext<crate::testing::FileTestMessage> for FileBatchContext {
    fn build(first: &crate::testing::FileTestMessage) -> Self {
        Self {
            seeker: first.seeker().clone(),
        }
    }
}

/// The key reading this delivery's [`FilePosition`] out of [`FileContext`].
///
/// The value is the pinned form: seeking back to it redelivers exactly this message.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber(FileStream::new("jobs"))]
/// async fn audit(job: &Job, Ctx(at): Ctx<Position>) -> HandlerOutcome {
///     println!("job {} sits at {at:?}", job.id);
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Position;

impl ContextField for Position {
    type Context = FileContext;
    type Value = FilePosition;

    fn read(self, src: &FileContext) -> FilePosition {
        src.position
    }
}

impl Field<FileContext> for Position {
    type Value<'a> = FilePosition;

    fn get(self, src: &FileContext) -> FilePosition {
        src.position
    }
}

/// The key reading the subscription's [`FileSeeker`] out of [`FileContext`] or
/// [`FileBatchContext`]: the reposition handle, resolved once when the subscription opens and
/// carried by every delivery.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, poisoned: bool }
///
/// /// Skips to the live tail when the producer marks a poisoned region.
/// #[subscriber(FileStream::new("jobs"))]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if job.poisoned && seeker.seek(FilePosition::end()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeekHandle;

impl ContextField for SeekHandle {
    type Context = FileContext;
    type Value = FileSeeker;

    fn read(self, src: &FileContext) -> FileSeeker {
        src.seeker.clone()
    }
}

impl Field<FileContext> for SeekHandle {
    type Value<'a> = &'a FileSeeker;

    fn get(self, src: &FileContext) -> &FileSeeker {
        &src.seeker
    }
}

impl Field<FileBatchContext> for SeekHandle {
    type Value<'a> = &'a FileSeeker;

    fn get(self, src: &FileBatchContext) -> &FileSeeker {
        &src.seeker
    }
}
