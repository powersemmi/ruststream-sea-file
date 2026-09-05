//! File and stdio stream implementation of the `RustStream` broker contract, built on
//! `sea-streamer`.
//!
//! Two transports in one crate run a service against a persistent stream with no external
//! broker, over
//! [`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
//! [`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio):
//!
//! - [`FileBroker`] - a persistent, replayable stream on disk: survives restarts, records
//!   and replays, and repositions on demand - [`FileSubscriber`] implements the
//!   framework's `Seekable` capability with pinned captured positions (a sequence rewind is
//!   inclusive) plus beginning/end/timestamp forms, and publishes them to handlers through
//!   the [`Position`] and [`SeekHandle`] context keys.
//! - [`StdioBroker`] - standard input and output as one stream, so a service becomes a stage
//!   of a shell pipeline.
//!
//! Each transport has a module of its own ([`mod@file`], [`stdio`]) holding that transport's
//! types, its `Publish` policy and its prelude. A service globs the prelude of the transport it
//! runs on, or the [crate-level one](prelude) when it spans both.
//!
//! Both transports serve batch handlers. Neither client reads several entries at a time, so the
//! batches are assembled on the client and the size a mount site names in `batch(n)` is honoured
//! there; nothing at the mount site says which of the two it is.
//!
//! Scope and limits: the client keeps no consumer positions (its resumable mode is
//! unimplemented upstream), so acknowledgement reports
//! [`AckError::Unsupported`](ruststream::AckError::Unsupported) on both transports - resume
//! explicitly via the descriptor's start position or a captured [`FilePosition`]. Payloads
//! are plain bytes with no header space, so user headers travel in a text-safe envelope
//! applied only when headers are present - a file written without headers stays readable as
//! a plain payload stream by other tools. The file transport does not build on Windows (an
//! upstream constraint).

#![forbid(unsafe_code)]

mod batching;
mod context;
mod error;
pub mod file;
mod message;
pub mod prelude;
pub mod stdio;
mod stream;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;
mod wire;

pub use context::{FileBatchContext, FileContext, Position, SeekHandle};
pub use error::SeaFileError;
pub use file::{ConnectedFileBroker, FileBroker, FilePublish, FilePublisher};
pub use message::{FilePosition, SEQUENCE_HEADER, SeaMessage};
pub use stdio::{ConnectedStdioBroker, StdioBroker, StdioPublish, StdioPublisher, StdioSubscriber};
pub use stream::FileStream;
pub use subscriber::{FileMessage, FileSeeker, FileSubscriber};
