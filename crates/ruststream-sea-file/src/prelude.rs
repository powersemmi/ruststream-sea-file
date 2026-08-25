//! The imports a service spanning both transports writes every time, in one glob.
//!
//! Most services run on one form and glob that form's prelude instead: [`file::prelude`] or
//! [`stdio::prelude`]. This one is for a service that mounts on both, and differs from them in
//! two ways.
//!
//! It carries **no bare `Publish`**. The two forms each name their own policy `Publish`, and at
//! crate level there is no honest answer to which one that should be, so the form modules come
//! in instead: write `file::Publish` and `stdio::Publish` where the forms differ. Globbing the
//! two form preludes together would collide on exactly that name, which rustc reports as
//! `E0659` at the `use` line - the signal to come here.
//!
//! Its capability manifest is the **union** of the forms', so it is a weaker statement than a
//! form prelude's: it says a capability exists somewhere in this crate, not that it works on
//! the transport a given handler runs over. The file form can seek and report positions and the
//! stdio form can do neither, so a service mixing them should read [`file::prelude`] and
//! [`stdio::prelude`] to see which of its handlers may call what.
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
//! // The policies are told apart by their form, not by a transport-specific type name.
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

// The framework's prelude stops short of brokers on purpose, because which broker a service
// runs on is the one thing every service states for itself. Importing *this* prelude is that
// statement: the broker is named by the crate path on the `use` line, so the choice is still
// written down, and the framework's glob can ride along instead of being repeated underneath
// it. One import then serves a service file.
pub use ruststream::prelude::*;

// The union of the two form manifests, which is why it is the weaker statement: `Seeker` and
// `Positioned` are the file form's, and the stdio form has neither. A form prelude says what the
// transport under a handler can do; this says only that something in the crate can.
pub use ruststream::{Positioned, Seeker};

// The forms themselves, which is how a mixed service reaches `file::Publish` and
// `stdio::Publish` without either shadowing the other.
pub use crate::{file, stdio};

// The shared surface, all of it unambiguously named, so a mixed service still writes the broker
// and descriptor types directly. Only the policy alias needs the form path.
pub use crate::{
    FileBroker, FilePosition, FilePublish, FileSeeker, FileStream, StdioBroker, StdioPublish,
};

// Deliberately absent, each for its own reason:
//
// - a bare `Publish`: see the note above - the forms disagree, and the disagreement is the point.
// - `testing`: broker-author and test-harness tooling behind a feature gate, not the surface a
//   service writes against, so a test module names it and says by that import what it is doing.
// - the connected brokers, the live publishers and the subscribers (`ConnectedFileBroker`,
//   `FilePublisher`, `FileSubscriber`, and their stdio counterparts): the runtime produces these
//   from the forms above, and a service never spells one out.
// - `SeaMessage` and `SEQUENCE_HEADER`: message-level machinery, absent for the same reason the
//   framework's prelude leaves `OutgoingMessage` out - code working at that layer names it
//   explicitly and says by that import which layer it is working at.
// - `SeaFileError`: a service names errors where it handles them, not at the top of every file.
// - `Seekable`: implemented, but on `FileSubscriber` - the subscriber side, which the runtime's
//   plumbing consumes. A service names the seeker type in `Seek<FileSeeker>` and calls `Seeker`
//   on what it gets back; it never names this trait, so it is not in the manifest.
// - `DescribeServer`, and the contract traits `Subscribe` and `DefaultPublish`: machinery of the
//   broker contract rather than capabilities a handler calls, and `DescribeServer` sits on the
//   unconnected broker forms for AsyncAPI generation to read.
