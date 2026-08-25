//! The imports a service spanning both transport forms writes every time, in one glob.
//!
//! The framework's prelude, this crate's broker, descriptor, position, seeker and policy types,
//! the seeking capability traits, and the [`mod@file`] and [`stdio`] modules. A service on one
//! form globs that form's prelude instead; here the two policies are written `file::Publish` and
//! `stdio::Publish`.
//!
//! # Examples
//!
//! ```
//! use ruststream_sea_file::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
//! async fn record(order: &Order) -> HandlerResult {
//!     println!("recorded order {}", order.id);
//!     HandlerResult::Ack
//! }
//!
//! #[subscriber("orders")]
//! async fn tee(order: &Order) -> HandlerResult {
//!     println!("teed order {}", order.id);
//!     HandlerResult::Ack
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
//!             b.after_startup(file::Publish, async move |_publisher| Ok::<_, std::io::Error>(()));
//!             b.include(record);
//!         })
//!         .with_broker(StdioBroker::new(), |b| {
//!             b.after_startup(stdio::Publish, async move |_publisher| Ok::<_, std::io::Error>(()));
//!             b.include(tee);
//!         })
//! }
//! ```

pub use ruststream::prelude::*;
pub use ruststream::{Positioned, Seeker};

pub use crate::{
    FileBroker, FilePosition, FilePublish, FileSeeker, FileStream, StdioBroker, StdioPublish,
};
pub use crate::{file, stdio};

// No bare `Publish` here: the two forms disagree on what it names, so a mixed file goes through
// `file::Publish` / `stdio::Publish` - do not add one.
