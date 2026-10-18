---
sidebar_position: 2
title: out_retry
description: "On a pipe a deferred copy goes to the next stage, so the mount site names it with out_retry(Publish).to(key) or the service does not start."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

On a pipe a deferred copy goes to the next stage, so the mount site names it with `out_retry(Publish).to(key)` or the service does not start.

[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) names the publisher the runtime's deferred copies leave through, and `.to(key)` names the stream key they go to.
Standard output reaches the next process in the pipeline and never this one's standard input.
So a copy cannot come back to the stage that deferred it, and the stage says where it goes.
Every registration on [`StdioBroker`](../broker/stdio-broker.md) names it, because a handler may answer [`retry_after`](./retry-after.md) on any delivery.
A registration that names none stops the service at startup, with an error that names the subscription and the `.to(..)` step that fixes it.
The copy leaves once the delay is over, and a downstream stage reads it under that key, for example to feed it back to the start of the pipeline.
On a stream file the copy needs no destination ([retry_after](./retry-after.md)), and `out_retry(Publish)` there only replaces the publisher.
The position is a slot, so `.transform(..)` runs on the copy; the copy carries the delivery's own bytes, so no codec encodes it.

## Example

The stage hands every job it cannot run now to `jobs.retry` after five seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_out_retry.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_out_retry.rs#stage"
```

</TabItem>
</Tabs>
