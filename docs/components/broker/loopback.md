---
sidebar_position: 7
title: loopback
description: "loopback sends a stage's own output back to its input, so a stdio service runs in one process; it is a test aid."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# loopback

[`loopback`](api:ruststream_sea_file::stdio::StdioBroker::loopback) sends a stage's own output back to its input, so a stdio service runs in one process; it is a test aid.

Without it, a line the service writes reaches the next process of the pipeline, and the service's own subscriptions never read it.
With it, the line also reaches every subscription of the service on the same stream key.
So two stages of a pipeline run in one process, and a test drives both without a shell.
A subscription under `loopback` reads the service's output as well as standard input, so the end of standard input leaves it open until shutdown.

In a [`TestApp`](api:ruststream::testing::TestApp) test the pipe is held in memory and follows the same rule: the service reads its own output only under `loopback`.
Without it, a test reads what the service wrote with `published::<T>(key)`.
A service ships without `loopback`: a stage that works only under it stops working in a real pipeline.

## Example

The counting stage and the totalling stage run in one process.
The count the first one writes under `counts` comes back to the second.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_broker_loopback.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_broker_loopback.rs#stage"
```

</TabItem>
</Tabs>
