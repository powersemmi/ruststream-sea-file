---
sidebar_position: 6
description: "Test a stream file or pipeline service in process with TestApp: the production app, replies, delays, and the same test on a real stream file."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a file or pipeline service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), on an [in-process transport](../components/testing/index.md) or on a real stream file.

The service under test is a checkout.
An order arrives under `orders`, and the handler replies with a receipt under `receipts`.
An order without an amount is dropped.
An order over the fraud-check limit is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retry-after.md).
After three deliveries an order goes to `orders.review`, the [dead-letter key](../components/settlement/dead-letters.md).
The app is built by `service(file)`, so a test chooses the file and `main` uses the production one.
The service is the example file's own, [`examples/guide/testing.rs`](https://github.com/powersemmi/ruststream-sea-file/blob/main/crates/ruststream-sea-file/examples/guide/testing.rs).
The tests need the `testing` feature of `ruststream-sea-file` in `[dev-dependencies]`.

## An order and its receipt

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`FileBroker`](../components/broker/file-broker.md) to a log in memory.
The path is never opened, so tests run side by side in one process.
The test addresses the broker by its production type, `tb.broker::<FileBroker>()`.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## An order without an amount

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was appended.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
`retry_after` appends a copy of the order under `orders` when the delay ends.
The copy carries the retry count in a header, and `published` shows it under `orders` next to the test's own order.
The third delivery is the last one the mount site allows, so the order goes to `orders.review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process transports model

The in-process stream file is a log in memory, new for each broker value and shared by its clones.
It keeps what a file keeps:

- Each stream key is numbered from one, and a live subscription starts at the tip.
- A [replay](../components/subscriptions/replay.md) reads what the log holds and completes at its end.
- [Positions](../components/positions/index.md) and [seeks](../components/positions/seek.md) resolve against the log with the file's refusals.
- [`end_with_eos`](../components/broker/end-with-eos.md) ends every subscription at shutdown.

A log the service reads at startup is recorded on a clone of its broker before `TestApp::start`, as the [Resuming and seeking](resume-and-seek.md) tests do.

The in-process pipe is standard input the test feeds and standard output the service writes.
`tb.broker::<StdioBroker>().message(..)` is a line arriving under its key, and `published::<T>(key)` reads a line the stage wrote.
A stage reads its own output only under [`loopback`](../components/broker/loopback.md), as on a real pipe.
Both transports refuse what the real ones refuse: a stream key the client does not take, an empty line on a pipe, a beacon interval that is not a multiple of 1024.

The bytes of a `.ss` file, what a file keeps across runs, and the line format on a real pipe show only on the real transport.

## The same test on a stream file

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) runs the same app on a real stream file in the temp directory.
The transport is the local file system, so the live test needs no server and runs with the others in `cargo test`.
The file is named after the process, so nothing an earlier run appended is read, and the test removes it at the end.
The assertions are the in-process ones, unchanged.
A pipeline stage is tested live as a child process with piped standard input and output, as [Shell pipelines](pipelines.md#testing-the-stage) shows.
