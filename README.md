<h1 align="center">ruststream-sea-file</h1>

<p align="center">
  <i>The file and stdio transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: persistent replayable streams on disk, and services that compose with shell pipelines.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sea-file"><img src="https://img.shields.io/crates/v/ruststream-sea-file.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sea-file"><img src="https://img.shields.io/crates/dr/ruststream-sea-file" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sea-file"><img src="https://img.shields.io/docsrs/ruststream-sea-file" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-sea-file/">Documentation</a></b>
</p>

---

`ruststream-sea-file` implements the RustStream broker contract over [`sea-streamer-file`](https://crates.io/crates/sea-streamer-file) and [`sea-streamer-stdio`](https://crates.io/crates/sea-streamer-stdio). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

There is no server anywhere in this crate: a broker is a `.ss` stream file on disk (durable, replayable, shared between processes) or the process's own standard input and output (a service as a stage of a shell pipeline). That makes it the zero-infrastructure entry point to the framework, and the reference implementation of the `Seekable` capability.

## Features

- **Lazy startup contract.** `FileBroker::new(path)` and `StdioBroker::new()` are synchronous and do no I/O; the runtime connects once at startup, so both compose with `#[ruststream::app]`. The file broker creates the file by default (`existing_only()` opts out), can finish it with an end-of-stream mark on shutdown (`end_with_eos()`), and tunes the density of its in-place index (`beacon_interval(bytes)`).
- **Replayable subscriptions.** `FileStream::new(key)` follows the live tail; where reading begins is the framework's `start_at(..)` clause with a `FilePosition` (everything retained, a timestamp, a captured position). `.replay()` reads a finished file and completes the stream when it ends - one pass over a recorded log, rather than a subscription that waits for more.
- **The `Seekable` capability.** `FileSubscriber` mints a `FileSeeker`; positions are `FilePosition::{beginning, end, sequence, timestamp}`. Captured positions (`Positioned::position`) carry the framework's pinned semantics: seeking to one redelivers exactly that message. A handler reaches both through the transport's context keys - `Ctx<Position>` for where this delivery sat, `Ctx<SeekHandle>` for the live subscription handle - and `start_at(..)` chooses where a subscription opens.
- **Batches on both transports.** A `&[T]` handler mounts with `.batch(nonzero!(n))` and is handed at most that many messages at once. Neither client reads several entries at a time, so a batch is filled on this side of the wire, out of the framework's own buffer, and a partial one goes out on a short deadline; nothing at the mount site says which side filled it.
- **Headers without breaking the file format.** A text-safe envelope is applied only when a message actually carries headers; payloads published without headers stay verbatim, so stream files remain readable by any `sea-streamer` consumer and existing files remain readable by this crate.
- **Stdio pipelines.** `StdioBroker` turns stdin into subscriptions and stdout into the publisher: `producer | service | consumer` in a shell. Binary payloads survive the line-oriented transport through the same envelope. `loopback()` wires stdout back into stdin for self-contained tests.
- **Acknowledgement is unsupported.** The transport keeps no consumer positions, so `ack` reports `AckError::Unsupported` instead of reporting success; resume explicitly from a captured `FilePosition`.
- **Delayed retries through a deferred copy.** With no settlement to lean on, `HandlerOutcome::retry_after(delay)` works the one way it can: the registration names the publisher the copy leaves through with `b.include(reconcile).out_retry(Publish)`, and the runtime republishes the message to the stream key the subscription reads once the delay is over. A `.replay()` subscription and the stdio form say up front that nothing addresses them, so a registration that binds the position over one refuses to start instead of losing every delayed message.
- **No per-message settings.** An append to a stream file and a line on standard output take a stream key and a payload and nothing else, so this crate adds no step to the publish builder and a handler body that publishes keeps the framework prelude and the plain `Out<impl Publisher, Marker>` bound.
- **In-process test broker** (feature `testing`). `FileTestBroker` serves the same routing over a retained, positioned log in memory - no file, no pipe - so a service that reads positions or seeks mounts on it unedited and runs under the framework's `TestApp` harness.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
# The in-process broker the test below runs on, and a runtime to run it under.
ruststream-sea-file = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

The file transport is not supported on Windows (an upstream constraint of the file client).

## Write a service

```rust
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, PartialEq, Serialize, Deserialize)]
struct Confirmation {
    id: u64,
}

#[subscriber(
    FileStream::new("orders"),
    start_at(FilePosition::beginning()),
    publish("confirmations")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileBroker::new("/var/lib/svc/orders.ss"), |b| {
            // The reply position of this registration, named with the transport's own policy.
            b.include(confirm).out_reply(Publish);
        })
}
```

Each transport has a prelude of its own, and it is the mount site's vocabulary: `file::prelude` and `stdio::prelude` each alias that form's policy to `Publish`, so the composition root names the concept and not the transport. A service that spans both globs `ruststream_sea_file::prelude`, where the policies keep their prefixed names (`FilePublish`, `StdioPublish`) because a bare one would be ambiguous there. A handler body needs neither glob: it imports `ruststream::prelude` and reaches for this crate's names only when it reads a position or seeks.

## Test it

Handlers run against `FileTestBroker`, an in-process stand-in with the same routing, the same positions and the same seeker over a log in memory, so a service mounts on it unedited. `TestApp` starts the app, publishes into it, and drives the reaction to a standstill before the assertions read it:

```rust
use ruststream::testing::TestApp;
use ruststream_sea_file::testing::FileTestBroker;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_order_is_confirmed() -> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileTestBroker::new(), |b| {
            b.include(confirm).out_reply(Publish);
        });
    let tb = TestApp::start(app).await?;

    tb.publish("orders", &Order { id: 1 }).await?;

    tb.broker::<FileTestBroker>()
        .subscriber("orders")
        .assert_called_once();
    tb.broker::<FileTestBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 1 });
    Ok(())
}
```

What the stand-in leaves out is what only a real transport can answer: the end-of-stream mark, the header envelope, `AckError::Unsupported`, durability across a restart. Those are covered by the suite that runs against stream files and pipes, and `just test` exercises it on temp files, needing no external broker.

## Layout

```
ruststream-sea-file/
├── crates/
│   └── ruststream-sea-file/    the published crate
│       └── examples/           runnable services on both transports
├── docs/                       the documentation site
├── .github/workflows/          CI (fmt, clippy, tests, security scans)
└── justfile                    the local gates
```

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
