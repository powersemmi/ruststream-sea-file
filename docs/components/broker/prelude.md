---
sidebar_position: 9
title: Prelude
description: "Glob file::prelude on a stream file, stdio::prelude on a pipeline, and the crate prelude only in a service that uses both."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

Glob [`file::prelude`](api:ruststream_sea_file::file::prelude) on a stream file, [`stdio::prelude`](api:ruststream_sea_file::stdio::prelude) on a pipeline, and the crate [`prelude`](api:ruststream_sea_file::prelude) only in a service that uses both.

Each glob carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
`use ruststream_sea_file::file::prelude::*;` adds [`FileBroker`](./file-broker.md), the descriptor [`FileStream`](../subscriptions/file-stream.md), the positions [`FilePosition`](../positions/index.md), the context keys [`Position`](../positions/position.md) and [`SeekHandle`](../positions/seek.md), and the publish policy as `Publish`.
`use ruststream_sea_file::stdio::prelude::*;` adds [`StdioBroker`](./stdio-broker.md) and its policy as `Publish`.

`Publish` is the name every broker crate gives its publish policy, so a mount site reads the same on every transport.
Moving a service from one transport to the other changes the glob and the broker, and leaves the mount sites alone.

The two transports would claim the same name `Publish`, so the crate prelude carries neither.
A service on both brokers globs it and writes the prefixed [`FilePublish`](../publishing/file-publish.md) and [`StdioPublish`](../publishing/stdio-publish.md).

## Example

One service records orders from a stream file and answers orders read on standard input.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_broker_prelude.rs#mixed"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_broker_prelude.rs#mixed"
```

</TabItem>
</Tabs>
