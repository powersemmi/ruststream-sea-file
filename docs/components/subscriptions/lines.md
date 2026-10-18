---
sidebar_position: 3
title: Lines
description: "A stdio subscription reads the lines of its stream key; a line with no key in its header reaches every subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lines

A stdio subscription reads the lines of its stream key; a line with no key in its header reaches every subscription.

`#[subscriber("jobs")]` on [`StdioBroker`](../broker/stdio-broker.md) reads the lines of standard input whose header names the key `jobs`.
A line is a header in square brackets, then the payload: `[timestamp | stream_key | sequence | shard_id] payload`.
Every field of the header is optional, so `[jobs] {"id":7}` names the key alone.
A line under another key is passed over, so a stage takes the keys it subscribed to out of a shared stream.
A line with no header names no key, and the client hands it to every subscription of the stage, whatever key the subscription reads.
The client parses each header before the service sees the line.
A stage stops on a line it cannot read: input that is not UTF-8, or a leading `[...]` that is not a header, a JSON array among them.
So a producer writes a header in front of a payload that opens with `[`.
A stdio subscription reads lines as they arrive.
A delivery reports the sequence its header names, or the count of its key's lines from zero when the header names none.
A reply is a line on standard output under the key its type declares, and the next stage reads it.
Every registration on a pipe names where a deferred copy goes, with [`out_retry(Publish).to(key)`](../settlement/out-retry.md).

## Example

The stage answers each `jobs` line with a `results` line for the next stage.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_lines.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_lines.rs#stage"
```

</TabItem>
</Tabs>
