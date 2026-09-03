//! The imports a service spanning both transport forms writes every time, in one glob.
//!
//! The framework's prelude, this crate's broker, descriptor, position, seeker and policy types,
//! the seeking capability traits and context keys, and the [`mod@file`] and [`stdio`] modules. A
//! service on one form globs that form's prelude instead.
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
//! async fn record(order: &Order) -> HandlerOutcome {
//!     println!("recorded order {}", order.id);
//!     HandlerOutcome::ack()
//! }
//!
//! #[subscriber("orders")]
//! async fn tee(order: &Order) -> HandlerOutcome {
//!     println!("teed order {}", order.id);
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
//!             b.after_startup(FilePublish, async move |_publisher| Ok::<_, std::io::Error>(()));
//!             b.include(record);
//!         })
//!         .with_broker(StdioBroker::new(), |b| {
//!             b.after_startup(StdioPublish, async move |_publisher| Ok::<_, std::io::Error>(()));
//!             b.include(tee);
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

// The policies keep their prefixed names. The bare `Publish` is the framework's slot capability
// trait, which arrives through the glob above; re-exporting a policy under that name would win
// over the glob and silently shadow the trait - do not add one.
