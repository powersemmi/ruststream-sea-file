//! File and stdio stream implementation of the `RustStream` broker contract, built on
//! `sea-streamer`.
//!
//! The gap this crate fills: running a service against a persistent stream without an
//! external broker. Two transports in one crate, over
//! [`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
//! [`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio):
//!
//! - [`FileBroker`] - a persistent, replayable stream on disk: survives restarts, records
//!   and replays, and repositioning is first-class - [`FileSubscriber`] implements the
//!   framework's `Seekable` capability with pinned captured positions (a sequence rewind is
//!   inclusive) plus beginning/end/timestamp forms.
//! - [`StdioBroker`] - standard input and output as one stream, so a service becomes a stage
//!   of a shell pipeline; something no network broker can offer.
//!
//! Honest scope: the client keeps no consumer positions (its resumable mode is
//! unimplemented upstream), so acknowledgement reports
//! [`AckError::Unsupported`](ruststream::AckError::Unsupported) on both transports - resume
//! explicitly via the descriptor's start position or a captured [`FilePosition`]. Payloads
//! are plain bytes with no header space, so user headers travel in a text-safe envelope
//! applied only when headers are present - a file written without headers stays readable as
//! a plain payload stream by other tools. The file transport does not build on Windows (an
//! upstream constraint).

#![forbid(unsafe_code)]

mod error;
mod file;
mod message;
mod stdio;
mod stream;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;
mod wire;

pub use error::SeaFileError;
pub use file::{ConnectedFileBroker, FileBroker, FilePublish, FilePublisher};
pub use message::{FilePosition, SEQUENCE_HEADER, SeaMessage};
pub use stdio::{ConnectedStdioBroker, StdioBroker, StdioPublish, StdioPublisher, StdioSubscriber};
pub use stream::{FileStream, Start};
pub use subscriber::{FileSeeker, FileSubscriber};
