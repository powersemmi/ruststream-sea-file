File and stdio transports for the `RustStream` messaging framework: a service runs on a
replayable stream on disk or as a stage of a shell pipeline, with no server anywhere.

Two transports share this crate, over
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio).

* [`FileBroker`] is a log kept in one `.ss` stream file. It survives restarts, replays what it
  recorded, and repositions on demand, which makes this crate the framework's reference
  implementation of seeking.
* [`StdioBroker`] is the process's own standard input and output as one stream, so
  `producer | service | consumer` works with ordinary command-line tools.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

# A service

A handler names its subscription and the composition root names the file:

```
use ruststream_sea_file::file::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/tmp/orders.ss"),
        |b| {
            b.include(handle);
        },
    )
}
```

`cargo run -- run` starts it. Nothing is installed and nothing listens: the transport is the
file. The same service on a pipeline replaces the glob with [`stdio::prelude`] and the broker
with [`StdioBroker`], and its subscription becomes a plain stream key.

# The two forms

Each transport owns a module holding its broker, its `Publish` policy and its prelude -
[`mod@file`] and [`stdio`]. A service globs the prelude of the form it runs on and imports
nothing else from this crate. A service that spans both globs [`prelude`] instead and writes the
prefixed [`FilePublish`] and [`StdioPublish`], since the two forms would claim the same policy
name.

Both brokers are constructed synchronously and do no I/O until the runtime connects them:

```text
FileBroker::new(path)     configuration only, synchronous
  .connect()   ->  ConnectedFileBroker    the open file; subscriptions and publishers
  .shutdown()  ->  ()                     flushed and closed
```

A publisher handed out before the shutdown shares the broker's connection, so afterwards it
returns [`SeaFileError::NotConnected`] rather than succeeding against a closed file. Shutting
down [`StdioBroker`] ends every stdio consumer and producer in the process, not only the ones
that broker opened: the transport is process-wide by the client's design.

# What the transports answer

| Capability | Answer |
| --- | --- |
| `Subscribe` | Both. A stream key is a subscription, so `#[subscriber("key")]` needs no descriptor. |
| `Positioned` | Both. Every delivery reports the sequence it sits at, per stream key: from one on a stream file, from zero on a pipe, as each client counts. |
| `Seekable` | The stream file only. Standard input keeps no log to move within, so a handler that reads [`SeekHandle`] does not compile against [`StdioBroker`]. |
| `BatchSubscriber` | Both, assembled on the client: neither client reads more than one entry at a time. |
| `Partitioned` | Neither. A stream file is one ordered log and the client writes every message to shard zero. |
| `RequestReply` | Neither. There is no reply address on a file or on a pipe. |
| `TransactionalPublisher` | Neither. Each publish appends and flushes on its own; there is no multi-write unit to commit. |
| `DescribeServer` | Both, as an in-process server: the file's path, or `stdio`. |

`ack` and `nack` report `AckError::Unsupported` on both transports. The client records no
consumer positions, and its resumable mode is unimplemented upstream, so nothing tracks how far a
service read. You resume explicitly: store a [`FilePosition`] read off a delivery and open the
next run with `start_at(..)`, or replay the file from its beginning. Seek to a position the stream
has reached: a sequence past the last one written, and an instant later than every message the
file holds, are both refused, and the subscription ends with the refusal.

# Headers

The client's payloads are plain bytes with no header space, so headers travel in the payload
itself, and only when a message has any. A message published without headers is written verbatim,
so a file recorded that way stays readable as a plain payload stream by any `sea-streamer`
consumer, and a file written by another tool stays readable here. The one exception is a payload
that itself begins with `rs1:`, which is enveloped so that no reader takes it for an envelope. A message with headers is
written as `rs1:` followed by base64 of a length-prefixed header block and the payload; the form
is text-safe because the stdio transport is line-oriented UTF-8. The stdio publisher also
envelopes a payload that has no headers when a line would not carry it as it is: one that is not
UTF-8, holds a newline, or begins or ends with whitespace. So binary, multi-line and padded
payloads survive a shell pipeline intact.

Every delivery carries its sequence number in the [`SEQUENCE_HEADER`] header.

# The generated document

The `asyncapi` feature fills in what this crate knows about the two transports. Each broker is
one server, and neither reports a host: the file broker reports the path of the stream file and
the stdio broker its protocol name, because there is nothing to connect to over a network.

```json
"servers": {
  "file": { "protocol": "file", "description": "/var/lib/ruststream/orders.ss" },
  "pipe": { "protocol": "stdio" }
}
```

A subscription opened through [`FileStream`] describes its channel with the stream key it reads.
The specification lists no binding for a file transport and its protocol keys are a closed list,
so the description travels in this crate's own `x-ruststream-file` extension. A subscription
opened by a bare stream key carries no descriptor and describes nothing, which is every stdio
subscription and a file subscription mounted without [`FileStream`]. The path of the file is not
repeated on the channel: the server is where a reader looks for it.

A channel the service publishes to carries the same extension, filled from the destination the
mount site resolved: a reply's stream key, an `Out` slot's, the dead-letter one. One stream key is
both ends of the file, so a channel is described the same way whether the service reads it or
appends to it. A stdio publish describes nothing, the way a stdio subscription does.

# Testing

A test hands the harness the app `main` runs. `TestApp::start` connects each broker in process,
with no file and no pipe, and the test addresses the broker by its own type:

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize, Outgoing, PartialEq)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[subscriber(FileStream::new("orders"), publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

/// The app `main` runs, and the one the tests hand the harness.
pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileBroker::new("/var/lib/orders.ss"), |b| {
            b.include(confirm);
        })
}

pub async fn confirms_an_order() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(app()).await?;
    tb.broker::<FileBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await?;

    tb.broker::<FileBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 });
    tb.broker::<FileBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order: 1 });
    tb.shutdown().await?;
    Ok(())
}
# }
# #[cfg(feature = "testing")]
# fn main() {
#     tokio::runtime::Builder::new_multi_thread()
#         .enable_all()
#         .build()
#         .unwrap()
#         .block_on(demo::confirms_an_order())
#         .unwrap();
# }
# #[cfg(not(feature = "testing"))]
# fn main() {}
```

The mode is the `testing` feature of this crate, enabled in the service's `[dev-dependencies]`.
In process, a stream file is a log in memory. It is new and empty for each broker value, and the
broker's clones share it, as they share the file they connect. It keeps what the file keeps: each
stream key numbered from one, a live subscription that starts at the tip, a replay that completes
at the end of the log, positions and seeking with the file's refusals, and the end-of-stream mark
of [`end_with_eos`](FileBroker::end_with_eos). A test that needs a recorded log connects a clone
of the service's broker and publishes to it before the service starts.

A pipe in process is standard input the test feeds and standard output the service writes. A line
the service writes is recorded under its stream key, and it reaches the service's own
subscriptions only under [`loopback`](StdioBroker::loopback), as on a real pipe. So a stdio test
reads what the service wrote with `published::<T>(key)`.

Both refuse what the real transport refuses: a stream key the client does not take, an empty line
on a pipe, a beacon interval that is not a multiple of 1024. A header crosses the same envelope a
file or a line carries.

`TestApp::start_live(app())` runs the same test against a real stream file. The real transport is
where the bytes of a `.ss` file, its beacons, durability across a restart and the line format of a
shell pipeline are exercised. Two brokers opened on one path share a file on disk and do not share
it in process, and a stdio shutdown in process ends only that broker's pipe.

# Operations

There is no authentication, no TLS and no connection setting to tune: the transport is a local
file or the process's own pipes. What is worth knowing before shipping:

* The file transport does not build on Windows, an upstream constraint of the file client.
* [`beacon_interval`](FileBroker::beacon_interval) must be a positive multiple of 1024 bytes;
  denser beacons make seeking finer-grained and the file larger.
* [`end_with_eos`](FileBroker::end_with_eos) marks a file finished. A live subscription ends
  only on that mark. A replay ends on it or at the end of the file, after delivering every
  message before it.
* A stream file grows without bound. Retention, rotation and disk budget are the operator's.
* The stdio publisher rejects a message with no payload and no headers with
  [`SeaFileError::Invalid`], because the client's line format silently drops empty lines.

# Where things are

* [`mod@file`]: the stream file - descriptors, replay, positions and seeking, batches,
  publishing, delayed redelivery.
* [`stdio`]: the pipeline - the line format, where a deferred copy goes, loopback.
* [Testing](#testing): the production app under `TestApp`, in process or against a real file.
* [`prelude`]: the one glob for a service that spans both forms.

The framework concepts every broker shares are on docs.rs:
[subscribers](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#subscribers),
[publishing](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#publishing),
[routing](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing),
[context](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#context-and-state),
[typed headers](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#typed-headers),
[lifecycle](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#lifecycle),
[middleware](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#middleware),
[failure policy](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#failure-policy),
[codecs](https://docs.rs/ruststream/latest/ruststream/codec/index.html),
[testing](https://docs.rs/ruststream/latest/ruststream/testing/index.html) and the
[CLI](https://docs.rs/ruststream/latest/ruststream/runtime/cli/index.html). Installation, the
tutorial and the list of brokers are on the site:
<https://powersemmi.github.io/ruststream/>.

# Cargo features

Both are off by default; the crate needs neither to run a service.

* `testing`: the in-process mode of both brokers, which the framework's `TestApp` harness
  connects in place of the file and the pipe.
* `asyncapi`: what a [`FileStream`] subscription contributes to the generated document.
