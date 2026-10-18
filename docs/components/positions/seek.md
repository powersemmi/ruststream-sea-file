---
sidebar_position: 3
title: Seek
description: "Ctx<SeekHandle> moves the running subscription; what was queued is discarded and a position past the tip is refused."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Seek

`Ctx<SeekHandle>` moves the running subscription; what was queued is discarded and a position past the tip is refused.

`Ctx(seeker): Ctx<SeekHandle>` binds a [`FileSeeker`](api:ruststream_sea_file::FileSeeker) for the subscription the delivery came from ([`SeekHandle`](api:ruststream_sea_file::SeekHandle)).
Its `seek(position)` takes a [`FilePosition`](./index.md), and the next delivery comes from there.
Deliveries already queued from the old place are discarded.
Seek to a position the file has reached.
The sequence the file writes next is the tip, and a seek there succeeds.
A sequence further ahead, and an instant later than every message the file holds, are refused with an error.
A live subscription then ends, and a [replay](../subscriptions/replay.md) stays where it was.
A position read with [`Ctx<Position>`](./position.md), the beginning and the tip are always within the file.
A seek after the broker has shut down returns an error.
A batch body reaches the same handle through [`FileBatchContext`](./batch-context.md).
In the manual form the body names [`FileContext`](api:ruststream_sea_file::FileContext) as its context and reads `ctx.context(SeekHandle)`.
A position that has to hold across a restart belongs in [`start_at`](./start-at.md).

## Example

Job 2 says the jobs up to sequence 4 cannot be processed, so the subscription jumps past job 3.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_seek.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_seek.rs#handler"
```

</TabItem>
</Tabs>
