---
sidebar_position: 1
title: retry_after
description: "On a stream file a retry_after copy is appended under the key the subscription reads, so it comes back with nothing named at the mount site."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

On a stream file a `retry_after` copy is appended under the key the subscription reads, so it comes back with nothing named at the mount site.

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) asks for a later delivery.
A stream file cannot hold a message back, so the runtime does it.
It settles the delivery, waits out the delay and publishes a copy of the message, payload and headers, through the form's `Publish`.
One stream key is both ends of the file, so the copy is appended under the key the subscription reads, and the subscription reads it as the next message.
This holds for a [`FileStream`](../subscriptions/file-stream.md) and for a bare stream key alike.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) incremented, and the handler reads it like any other header.
The copy waits in the service's memory, so a process that exits before the delay is over loses it.
A [replay](../subscriptions/replay.md) reads the copy while the replay is still short of the end of the file, and leaves it unread once it has passed that point.
[`out_retry`](./out-retry.md) replaces the publisher the copy leaves through, and a transform on that position runs on the copy.

## Example

The first attempt finds the card processor busy; the copy comes back 30 seconds later and is charged.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_retry_after.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_retry_after.rs#handler"
```

</TabItem>
</Tabs>
