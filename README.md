<h1 align="center">ruststream-sea-file</h1>

<p align="center">
  <i>The file and stdio transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: persistent replayable streams on disk, and services that compose with shell pipelines.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-sea-file` implements the RustStream broker contract over [`sea-streamer-file`](https://crates.io/crates/sea-streamer-file) and [`sea-streamer-stdio`](https://crates.io/crates/sea-streamer-stdio). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

There is no server anywhere in this crate: a broker is a `.ss` stream file on disk (durable, replayable, shared between processes) or the process's own standard input and output (a service as a stage of a shell pipeline). That makes it the zero-infrastructure entry point to the framework - and the reference implementation of the `Seekable` capability.

## Features

- **Lazy startup contract.** `FileBroker::new(path)` and `StdioBroker::new()` are synchronous and do no I/O; the runtime connects once at startup, so both compose with `#[ruststream::app]`. The file broker creates the file by default (`existing_only()` opts out), can finish it with an end-of-stream mark on shutdown (`end_with_eos()`), and tunes the beacon interval (`beacon_interval(n)`).
- **Replayable subscriptions.** `FileStream::new(key)` follows the live tail; where reading begins is the framework's `start_at(..)` clause with a `FilePosition` (everything retained, a timestamp, a captured position). `.replay()` reads a finished file and completes the stream when it ends - batch-style processing of a recorded log.
- **The `Seekable` capability.** `FileSubscriber` mints a `FileSeeker`; positions are `FilePosition::{beginning, end, sequence, timestamp}`. Captured positions (`Positioned::position`) carry the framework's pinned semantics: seeking to one redelivers exactly that message. The `start_at(..)` clause and the `Seek` handler parameter work out of the box.
- **Headers without breaking the file format.** A text-safe envelope is applied only when a message actually carries headers; payloads published without headers stay verbatim, so stream files remain readable by any `sea-streamer` consumer and existing files remain readable by this crate.
- **Stdio pipelines.** `StdioBroker` turns stdin into subscriptions and stdout into the publisher: `producer | service | consumer` in a shell. Binary payloads survive the line-oriented transport through the same envelope. `loopback()` wires stdout back into stdin for self-contained tests.
- **Honest acknowledgement.** The transport keeps no consumer positions, so `ack` reports `AckError::Unsupported` instead of pretending; resume explicitly from a captured `FilePosition`.
- **In-process test broker** (feature `testing`). `FileTestBroker` reproduces core routing with no file at all and implements `ruststream::testing::TestableBroker`.

## Status

Implemented and verified: the framework's conformance, lifecycle, and seeking suites plus the replay and stdio integration tests run in CI on temp files and in-process pipes - this crate needs no external broker, which is the point. Design and scope are tracked in [powersemmi/ruststream#193](https://github.com/powersemmi/ruststream/issues/193).

## Write a service

```rust
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_sea_file::{FileBroker, FilePosition, FileStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(FileBroker::new("/var/lib/svc/orders.ss"), |b| {
            b.include(handle);
        })
}
```

## Seek

The subscription opens at a chosen position with the `start_at` clause, and a handler
repositions its own subscription through the injected seeker:

```rust
use ruststream::Seeker;
use ruststream::runtime::{HandlerResult, Seek};
use ruststream::subscriber;
use ruststream_sea_file::{FilePosition, FileSeeker, FileStream};

#[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
async fn replay(job: &Job, Seek(seeker): Seek<FileSeeker>) -> HandlerResult {
    if job.id == 999 {
        // Skip the poisoned region: jump to the live tail.
        if seeker.seek(FilePosition::end()).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    HandlerResult::Ack
}
```

## Test it

Everything runs locally: `just test` covers the unit, conformance, lifecycle, seeking, replay, and stdio suites on temp files and in-process pipes. The `testing` feature offers the in-process `FileTestBroker` for handler tests with no filesystem at all.

## Layout

```
ruststream-sea-file/
├── crates/
│   └── ruststream-sea-file/    the published crate
│       └── examples/           runnable file_* examples
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
