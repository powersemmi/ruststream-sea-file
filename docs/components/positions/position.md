---
sidebar_position: 2
title: Position
description: "Ctx<Position> binds the delivery's place in the file; seeking back to it redelivers exactly that message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Position

`Ctx<Position>` binds the delivery's place in the file; seeking back to it redelivers exactly that message.

[`Position`](api:ruststream_sea_file::Position) is a context key of the stream file form.
`Ctx(at): Ctx<Position>` binds a [`FilePosition`](./index.md), and for a delivery it is always `FilePosition::Sequence(n)`: the message's number under its key, counted from one.
The value is pinned: a [seek](./seek.md) to it delivers that same message again, then the rest of the log in order.
Stored, it is the checkpoint a later run hands to [`start_at`](./start-at.md).
The same number travels in the delivery's `stream-sequence` header ([SEQUENCE_HEADER](../headers/sequence-header.md)), which each element of a batch carries too.
In the manual form the body names [`FileContext`](api:ruststream_sea_file::FileContext) as its context, `impl Handle<Order, Reply, (), FileContext>`, and reads `ctx.context(Position)`.

## Example

Each invoice records the sequence of the order it was made from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_position_key.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_position_key.rs#handler"
```

</TabItem>
</Tabs>
