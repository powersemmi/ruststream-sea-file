//! The imports a service on stream files writes every time, in one glob.
//!
//! `use ruststream_sea_file::prelude::*;` brings in the framework's own prelude and this
//! crate's user-facing surface on top of it: the two brokers, the file subscription
//! descriptor, the position type its `start_at` clause and its seeker speak in, and the
//! publish policies. A service file that mounts handlers on a stream file or on standard
//! input needs nothing else from either crate.
//!
//! It is also this broker's capability manifest. The framework's capability traits are
//! optional, and the ones a service writes down are those it names in a bound or whose
//! methods it calls on a value it was handed; the glob carries exactly those, which here
//! means repositioning a live subscription and reading a delivery's position. What a service
//! can call is then visible from the import rather than from a table somewhere else. The
//! capability traits come from the framework, so they are the same items whichever broker
//! crate re-exports them: a service on two brokers can glob both preludes and let the
//! compiler check the overlap instead of hand-picking imports.
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
//! // Everything this handler needs came from the one glob, the `Seeker` trait behind
//! // `seek` included - which is the capability manifest doing its job.
//! #[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
//! async fn handle(order: &Order, Seek(seeker): Seek<FileSeeker>) -> HandlerResult {
//!     if order.id == 0 {
//!         let _ = seeker.seek(FilePosition::end()).await;
//!     }
//!     HandlerResult::Ack
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
//!             b.include(handle);
//!         })
//! }
//! ```

// The framework's prelude stops short of brokers on purpose, because which broker a service
// runs on is the one thing every service states for itself. Importing *this* prelude is that
// statement: the broker is named by the crate path on the `use` line, so the choice is still
// written down, and the framework's glob can ride along instead of being repeated underneath
// it. One import then serves a service file.
pub use ruststream::prelude::*;

// The capability manifest: the capability traits a service writes down, which are the ones it
// names in a bound and the ones whose methods it calls on a value it was handed. Both here are
// the second kind - `Seeker::seek` on the seeker bound by a `Seek<..>` parameter, and
// `Positioned::position` on a delivered message. The transports implement no capability a
// service names in a bound - a stream file has no transaction and no request-reply - so this
// list is an inventory rather than a convenience, and a capability gained later is added here in
// the same change.
pub use ruststream::{Positioned, Seeker};

pub use crate::{
    FileBroker, FilePosition, FilePublish, FileSeeker, FileStream, StdioBroker, StdioPublish,
};

// Deliberately absent, each for its own reason:
//
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
