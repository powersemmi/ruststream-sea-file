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

`ruststream-sea-file` runs a RustStream service over a stream file or a shell pipeline, through
[`sea-streamer-file`](https://crates.io/crates/sea-streamer-file) and
[`sea-streamer-stdio`](https://crates.io/crates/sea-streamer-stdio). There is no server: a broker
is a `.ss` file on disk, durable and replayable, or the process's own standard input and output.
It is the zero-infrastructure way into the framework. Handlers, routing, codecs and middleware come
from the framework; this crate is the transport.

## Features

- **Stream files:** subscriptions follow the live tail, or replay a finished file once.
- **Seeking:** start at the beginning, the end, a sequence or a timestamp, and a handler moves its
  subscription while the service runs.
- **Stdio pipelines:** `producer | service | consumer` in a shell, binary payloads included.
- **Files other tools read:** a payload published without headers stays verbatim, so any
  `sea-streamer` consumer reads the file.
- **Batches** assembled on the client.
- **Delayed retries** through a copy the runtime republishes, with retry caps and dead letters.
- **AsyncAPI** for both transports, behind the `asyncapi` feature.
- **Tests without files:** `TestApp` runs the service's own app with `FileBroker` and
  `StdioBroker` in process.

The transport keeps no consumer positions, so acknowledgement reports `AckError::Unsupported`;
a service resumes from a captured position. The file transport does not run on Windows.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-sea-file = { version = "0.7", features = ["testing"] }
```

## Write a service

```rust
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, Serialize, Deserialize)]
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
            b.include(confirm).out_reply(Publish);
        })
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`. Each
transport has its own prelude (`file`, `stdio`).

## Test it

`TestApp` runs the service's own app with `FileBroker` in process, with no file.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<FileBroker>()
    .message(&Order { id: 1 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<FileBroker>()
    .subscriber("orders")
    .assert_called_once();
tb.broker::<FileBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { id: 1 });
```

`TestApp::start_live(app())` runs the same test against a real stream file (`just test-brokers`).

## Documentation

- This crate: <https://docs.rs/ruststream-sea-file>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
