---
title: Testing
description: "TestApp::start connects FileBroker to a log in memory and StdioBroker to a pipe in memory, with the production app unchanged."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`FileBroker`](../broker/file-broker.md) to a log in memory and [`StdioBroker`](../broker/stdio-broker.md) to a pipe in memory, with the production app unchanged.

A test hands the harness the app `main` runs and addresses the broker by its production type, `tb.broker::<FileBroker>()` or `tb.broker::<StdioBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode.
No file is opened and no pipe is attached, so tests run side by side in one process.

A stream file in process is a log in memory, new and empty for each broker value and shared by the broker's clones.
It keeps what the file keeps: each stream key numbered from one, a live subscription that starts at the tip, a [replay](../subscriptions/replay.md) that completes at the end of the log, [positions](../positions/index.md) and [seeking](../positions/seek.md) with the file's refusals, and the end-of-stream mark.
A test publishes into the log, and `published::<T>(key)` reads what the service appended, retry copies and dead letters included.

A pipe in process is standard input the test feeds and standard output the service writes.
A message the test publishes is a line arriving under its key.
A line the service writes is recorded under its key, and `published::<T>(key)` reads it the way the next stage would.
It reaches the service's own subscriptions only under [`loopback`](../broker/loopback.md), as on a real pipe.

Both refuse what the real transport refuses: a stream key the client does not take, an empty line on a pipe, a beacon interval that is not a multiple of 1024.
A header crosses the same [envelope](../headers/index.md) a file or a line carries.
A publish through the harness returns once every handler it woke has settled, so the assertions read finished state; `tb.advance(..)` on a paused clock brings a [`retry_after`](../settlement/retry-after.md) copy back.
What only the real transport shows, such as the bytes of a `.ss` file or what a file keeps across runs, is for a [live test](./live.md).
The guide [Testing a file or pipeline service](../../guides/testing.md) tests a whole service this way.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-sea-file = { version = "0.7", features = ["testing"] }
```

A file service answers an order with a receipt.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_testing.rs#file"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_testing.rs#file"
```

</TabItem>
</Tabs>

A pipeline stage answers a `jobs` line with a `results` line, which the test reads off the stage's standard output.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_testing.rs#stdio"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_testing.rs#stdio"
```

</TabItem>
</Tabs>
