---
sidebar_position: 1
description: "Install ruststream-sea-file, run a first service on a stream file, look inside the file, test the service and run the same handler in a pipeline."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-sea-file`, runs a first service on a stream file and moves the same handler into a shell pipeline.
Neither transport needs a server.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sea-file = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-sea-file` holds both transports.
Its `testing` feature adds the [in-process transports](../components/testing/index.md) that tests run on, and `asyncapi` adds what the crate contributes to the [AsyncAPI document](../components/asyncapi/index.md).
Both features are off by default.
The stream file client builds on Unix systems only, so the crate does not build on Windows.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders under the stream key `orders` of the file `/tmp/orders.ss`.
It acknowledges an order with an amount and drops one without.
[`FileBroker::new`](../components/broker/file-broker.md) only records the path.
The file opens when the service starts, and startup creates it when it is missing.
[`FileStream::new`](../components/subscriptions/file-stream.md) names the stream key, and [`start_at(FilePosition::beginning())`](../components/positions/start-at.md) reads the key from the start of the file instead of from its end.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

It reads every order the file holds, then waits for the next one.
Another service appends an order by publishing it under `orders` on the same path, as [An event log on disk](event-log.md) shows.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops the service.

## Inside the file

The file client ships a decoder that prints a stream file one message per line:

```bash
cargo install sea-streamer-file --features=executables --bin ss-decode
ss-decode --file /tmp/orders.ss
```

Each line is `[timestamp | stream key | sequence | shard] payload`, for example `[2026-10-18T09:12:03.120 | orders | 1 | 0] {"id":1,"amount_cents":1250}`.
One file holds every stream key, and each key is numbered from one.
A new file also holds one message under the key `ruststream-internal`, which the crate writes when it creates the file.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`FileBroker` runs in process there, so the tests touch no file:

```bash
cargo test
```

[Testing a file or pipeline service](testing.md) walks through the harness and what the in-process transports model.

## The same handler in a pipeline

The other transport reads standard input and writes standard output.
The handler stays as it is; the prelude, the broker and the subscription change.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started_pipe.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started_pipe.rs#service"
```

</TabItem>
</Tabs>

[`StdioBroker::new`](../components/broker/stdio-broker.md) takes no settings, and the subscription names the stream key alone.
A stage names the key its deferred copies go to with [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md), and it does not start without one.
The publish policy is called `Publish` in both preludes, so code that publishes reads the same on either transport.

Feed the stage a line:

```bash
echo '[orders] {"id":1,"amount_cents":1250}' | cargo run -- run
```

The square brackets name the stream key.
The log goes to standard error, because standard output carries the stage's messages to the next process.
[Shell pipelines](pipelines.md) covers the line format and chained stages.

## Coming from sea-streamer

A service on `sea-streamer-file` writes its own consumer loop and producer.
Here is where each part goes in RustStream:

| sea-streamer | RustStream |
|---|---|
| `FileStreamer::connect` with `FileConnectOptions` | [`FileBroker::new`](../components/broker/file-broker.md) and its settings, applied at startup |
| `create_consumer` and a `next()` loop | a `#[subscriber]` handler on a [`FileStream`](../components/subscriptions/file-stream.md) |
| decoding the message payload | the handler's parameter type, decoded by the codec |
| `create_producer` and `send_to` | a reply or an `Out` parameter, sent through [`Publish`](../components/publishing/file-publish.md) |
| `set_auto_stream_reset` on the consumer options, then `seek_to` | [`start_at`](../components/positions/start-at.md) at the mount site, or the [seek handle](../components/positions/seek.md) in the handler ([Resuming and seeking](resume-and-seek.md)) |
| `StdioStreamer` | [`StdioBroker`](../components/broker/stdio-broker.md) |
