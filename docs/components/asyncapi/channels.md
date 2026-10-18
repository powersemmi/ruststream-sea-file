---
sidebar_position: 2
title: Channels
description: "A FileStream subscription and a file publish describe their channel with the stream key; bare keys and stdio describe nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A [`FileStream`](api:ruststream_sea_file::FileStream) subscription and a file publish describe their channel with the stream key; bare keys and stdio describe nothing.

A subscription opened through `FileStream` reports the stream key it reads, as `{ "x-ruststream-file": { "streamKey": "orders" } }` in the channel's bindings.
A subscription opened by a bare stream key, `#[subscriber("orders")]`, carries no descriptor, so its channel has no bindings.
That is every stdio subscription, and a file subscription mounted without `FileStream`.

A channel the service publishes to on a stream file carries the same extension, with the destination the mount site resolved: a reply's stream key, an `Out` slot's, the dead-letter one.
One stream key is both ends of the file, so a channel reads the same whether the service reads it or appends to it.
A stdio publish describes nothing, as a stdio subscription does.
The path of the file stays on the [server](./server.md).

## Example

The channels `orders` and `receipts` carry their stream keys.
The stdio channels `lines` and `lines.out` have no bindings.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
