//! The file transport's typed contexts and their compile-time keys.
//!
//! A stream file is a replayable log, so a handler running on it can reposition its own
//! subscription. The delivery's position and the subscription's reposition handle travel as
//! fields of a context the runtime builds per delivery, and handlers read them by key -
//! [`Ctx`](ruststream::runtime::Ctx) on the attribute path, `ctx.context(..)` on the manual one.
//!
//! Standard input has no retained log, so nothing here applies to it: its deliveries carry no
//! reposition handle, which is what makes a stdio mount of a seeking handler a compile error.
//!
//! The [Position](https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/position/),
//! [Seek](https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/seek/) and
//! [FileBatchContext](https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/batch-context/)
//! pages show them in a service.

use ruststream::{BuildBatchContext, BuildContext, ContextField, Field};

use crate::message::FilePosition;
use crate::subscriber::{FileMessage, FileSeeker};

/// The file transport's per-delivery context: this delivery's position in the stream file, and
/// the subscription's own seeker.
///
/// The runtime builds one per delivery, and handlers read its fields by key - [`Position`] and
/// [`SeekHandle`]. A handler that reads both, or that reads them on the manual path, names this
/// type as its context.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber(FileStream::new("jobs"))]
/// async fn work(job: &Job, ctx: &mut Context<'_, FileContext>) -> HandlerOutcome {
///     if job.id == 0 && ctx.context(SeekHandle).seek(FilePosition::end()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     tracing::info!(job.id, position = ?ctx.context(Position), "worked");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/jobs.ss"), |b| {
///             b.include(work);
///         })
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
/// The batch settles first, so a seek from its body applies to the batches after it:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
///     resume_at: Option<u64>,
/// }
///
/// #[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
/// async fn drain(jobs: &[Job], ctx: &mut Context<'_, FileBatchContext>) -> HandlerOutcome {
///     tracing::info!(jobs = jobs.len(), "drained");
///     let Some(resume_at) = jobs.iter().find_map(|job| job.resume_at) else {
///         return HandlerOutcome::ack();
///     };
///     if let Err(error) = ctx
///         .context(SeekHandle)
///         .seek(FilePosition::sequence(resume_at))
///         .await
///     {
///         tracing::warn!(resume_at, %error, "seek refused");
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("worker", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/jobs.ss"), |b| {
///             b.include(drain.batch(nonzero!(3)));
///         })
/// }
/// ```
///
/// A batch body that asks for a per-delivery position does not compile: [`Position`] is a key of
/// [`FileContext`] alone.
///
/// ```compile_fail,E0277
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber(FileStream::new("jobs"))]
/// async fn drain(jobs: &[Job], ctx: &mut Context<'_, FileBatchContext>) -> HandlerOutcome {
///     let first: FilePosition = ctx.context(Position);
///     tracing::info!(jobs = jobs.len(), ?first, "drained");
///     HandlerOutcome::ack()
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
///     tracing::info!(job.id, position = ?at, "audited");
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
///
/// Standard input keeps no log to move within, so a handler that reads the key does not compile
/// against [`StdioBroker`](crate::StdioBroker):
///
/// ```compile_fail,E0277
/// use ruststream_sea_file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber("jobs")]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if job.id == 0 && seeker.seek(FilePosition::end()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
///         b.include(work);
///     })
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
