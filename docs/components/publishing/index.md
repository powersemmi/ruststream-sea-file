---
title: Publish
description: "Publish is each form's one policy and its default: replies, slots and lifecycle hooks publish through it with no per-message setting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publish

`Publish` is each form's one policy and its default: replies, slots and lifecycle hooks publish through it with no per-message setting.

Each form's prelude names its own policy `Publish`: [`FilePublish`](./file-publish.md) on a stream file, [`StdioPublish`](./stdio-publish.md) on a pipe.
A service that uses both brokers globs the crate's [prelude](../broker/prelude.md) and writes the two names in full.
The policy holds no connection; the runtime pairs it with the connected broker at startup.
A registration that names no policy for its reply publishes through `Publish`, so the bare `reply` clause needs nothing at the mount site.
`.out(Ledger, Publish)` wires an `Out` slot, and `after_startup(Publish, ..)` hands a lifecycle hook a publisher.
A reply goes to the stream key its type declares with `#[outgoing(name = "receipts")]`.
A reply type that declares no key is published where the subscriber says, with `reply("receipts")`.
A publish carries a stream key and a payload, and its headers ride inside the payload in an [envelope](../headers/index.md).
The policy has no per-message setting, so the publish builder gains no step from this crate and a handler body keeps the plain bound `Out<impl Publisher, Marker>`.
Each publish is written on its own: two publishes of one handler are two separate writes.

## Example

The service answers each order with a receipt and writes an audit line through the ledger slot.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_publish.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_publish.rs#reply"
```

</TabItem>
</Tabs>
