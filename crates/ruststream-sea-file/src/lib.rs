#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod batching;
mod context;
pub mod envelope;
mod error;
pub mod file;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
mod stdin;
pub mod stdio;
mod stream;
mod subscriber;

pub use context::{FileBatchContext, FileContext, Position, SeekHandle};
pub use envelope::{Envelope, Framed, NoOp, Rs};
pub use error::SeaFileError;
pub use file::{ConnectedFileBroker, FileBroker, FilePublish, FilePublisher};
pub use message::{FilePosition, SEQUENCE_HEADER, SeaMessage};
pub use stdio::{ConnectedStdioBroker, StdioBroker, StdioPublish, StdioPublisher, StdioSubscriber};
pub use stream::{FileEnvelopeExt, FileStream};
pub use subscriber::{FileMessage, FileSeeker, FileSubscriber};
