---
title: FilePosition
description: "FilePosition names a place in a stream key's log: the beginning, the tip, a sequence number, or an instant."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FilePosition

[`FilePosition`](api:ruststream_sea_file::FilePosition) names a place in a stream key's log: the beginning, the tip, a sequence number, or an instant.

A stream file keeps its log, so a subscription opens at a place in it with [`start_at`](./start-at.md), and a handler moves it with a [seek](./seek.md).
`FilePosition::beginning()` is the first message the file retains under the key.
`FilePosition::end()` is the tip, where the next append lands.
`FilePosition::sequence(n)` is message number `n` of the key, and the message itself is delivered again.
A stream key is numbered from one, and the numbering carries on when a later run appends to the same file.
`FilePosition::timestamp(millis)` is the first message stamped strictly later than that instant, in milliseconds since the Unix epoch.
A delivery reports its own place as a sequence, which [`Ctx<Position>`](./position.md) binds.
A sequence the file has not written yet, and an instant later than every message the file holds, are refused.
A position read off a delivery, the beginning and the tip are always within the file.

## Example

The revenue report is rebuilt from the first order the file retains on every startup, then follows the orders appended after.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_position.rs#positions"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_position.rs#positions"
```

</TabItem>
</Tabs>
