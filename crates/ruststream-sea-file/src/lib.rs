#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod batching;
mod context;
mod error;
pub mod file;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
pub mod stdio;
mod stream;
mod subscriber;
mod wire;

pub use context::{FileBatchContext, FileContext, Position, SeekHandle};
pub use error::SeaFileError;
pub use file::{ConnectedFileBroker, FileBroker, FilePublish, FilePublisher};
pub use message::{FilePosition, SEQUENCE_HEADER, SeaMessage};
pub use stdio::{ConnectedStdioBroker, StdioBroker, StdioPublish, StdioPublisher, StdioSubscriber};
pub use stream::FileStream;
pub use subscriber::{FileMessage, FileSeeker, FileSubscriber};
