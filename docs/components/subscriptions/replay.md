---
sidebar_position: 2
title: replay
description: "replay reads the whole retained file once and completes at its end or at the end-of-stream mark, instead of following writes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# replay

[`replay`](api:ruststream_sea_file::FileStream::replay) reads the whole retained file once and completes at its end or at the end-of-stream mark, instead of following writes.

`FileStream::new("orders").replay()` turns the subscription into one pass over what the file holds under its key.
It starts at the first message the file retains and delivers every message in order.
It completes at the end of the file, or at the end-of-stream mark a writer left with [`end_with_eos`](../broker/end-with-eos.md), and a message appended after that point is not read.
That suits a job over a finished log: an audit, a migration, an index rebuilt from scratch.
[`start_at(FilePosition::beginning())`](../positions/start-at.md) on a plain [descriptor](./file-stream.md) reads the same messages and then goes on following new writes.
A seek from a handler still moves a replay, one that has reached the end included, and the replay goes on from the new position ([Seek](../positions/seek.md)).
A seek to a position past the tip is refused, and the replay stays where it was.
A [`retry_after`](../settlement/retry-after.md) copy reaches a replay while the replay is still short of the end, and is left unread once the replay has passed it.
A service that only reads a finished file opens it with [`existing_only`](../broker/existing-only.md), so a wrong path stops it at startup instead of creating an empty file.
The guide [An event log on disk](../../guides/event-log.md) totals a finished file with a replay.

## Example

The audit reads the three orders of a finished day and completes; an order appended later is not read.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_replay.rs#replay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_replay.rs#replay"
```

</TabItem>
</Tabs>
