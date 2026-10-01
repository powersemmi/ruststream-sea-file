//! The imports a service spanning both transport forms writes every time, in one glob.
//!
//! The framework's prelude, this crate's broker, descriptor, position, seeker and policy types,
//! the seeking capability traits and context keys, and the [`mod@file`] and [`stdio`] modules. A
//! service on one form globs that form's prelude instead, where the policy is named by concept
//! (`file::Publish`, `stdio::Publish`); a mount site that spans both forms writes the prefixed
//! names, since the two forms would claim the same one here.
//!
//! # Examples
//!
//! ```
//! use ruststream_sea_file::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Debug, Outgoing, Serialize)]
//! struct Receipt {
//!     order: u64,
//! }
//!
//! #[subscriber(
//!     FileStream::new("orders"),
//!     start_at(FilePosition::beginning()),
//!     publish("receipts")
//! )]
//! async fn record(order: &Order) -> Receipt {
//!     Receipt { order: order.id }
//! }
//!
//! #[subscriber("orders", publish("receipts"))]
//! async fn tee(order: &Order) -> Receipt {
//!     Receipt { order: order.id }
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
//!             b.include(record).out_reply(FilePublish);
//!         })
//!         .with_broker(StdioBroker::new(), |b| {
//!             b.include(tee)
//!                 .out_reply(StdioPublish)
//!                 .out_retry(StdioPublish)
//!                 .to("orders.retry");
//!         })
//! }
//! ```

pub use ruststream::prelude::*;
pub use ruststream::{Positioned, Seeker};

pub use crate::{
    FileBatchContext, FileBroker, FileContext, FilePosition, FilePublish, FileSeeker, FileStream,
    Position, SeekHandle, StdioBroker, StdioPublish,
};
pub use crate::{file, stdio};

// No bare `Publish` here: the two forms disagree on what it names, so a mixed mount site goes
// through `FilePublish` / `StdioPublish` (or `file::Publish` / `stdio::Publish` by path) - do not
// add one.
