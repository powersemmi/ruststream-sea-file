---
sidebar_position: 1
title: SEQUENCE_HEADER
description: "Every delivery carries its sequence number in the stream-sequence header, from one on a stream file and from zero on a pipe."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SEQUENCE_HEADER

Every delivery carries its sequence number in the `stream-sequence` header, from one on a stream file and from zero on a pipe.

[`SEQUENCE_HEADER`](api:ruststream_sea_file::SEQUENCE_HEADER) is the header's name, `stream-sequence`.
The value is the number in decimal, as each client counts.
A stream file numbers each stream key from one and keeps counting across restarts, so the number names the message's place in the file.
A pipe numbers the lines of each key from zero, unless a line's header names a sequence of its own.
The reader adds the header to every delivery, so the handler reads it with `ctx.headers()` like any other header.
On a stream file [`Ctx<Position>`](../positions/position.md) gives the same place as a [`FilePosition`](../positions/index.md), the form [`start_at`](../positions/start-at.md) and a seek take.

## Example

The service numbers each order with the sequence it sits at in the file.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_sequence_header.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_sequence_header.rs#handler"
```

</TabItem>
</Tabs>
