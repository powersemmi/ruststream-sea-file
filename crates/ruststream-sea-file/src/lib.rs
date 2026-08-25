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
//!   inclusive) plus beginning/end/timestamp forms.
//! - [`StdioBroker`] - standard input and output as one stream, so a service becomes a stage
//!   of a shell pipeline.
//!
//! Each transport is a *form*, with a module of its own ([`mod@file`], [`stdio`]) carrying
//! that form's types, its `Publish` policy alias and a prelude of its own. A service on one
//! form globs that form's prelude and never writes a form-specific policy name at an include
//! site; a service spanning both globs the [crate-level prelude](prelude) and reaches the
//! aliases as `file::Publish` and `stdio::Publish`.
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

pub use error::SeaFileError;
pub use file::{ConnectedFileBroker, FileBroker, FilePublish, FilePublisher};
pub use message::{FilePosition, SEQUENCE_HEADER, SeaMessage};
pub use stdio::{ConnectedStdioBroker, StdioBroker, StdioPublish, StdioPublisher, StdioSubscriber};
pub use stream::FileStream;
pub use subscriber::{FileSeeker, FileSubscriber};
