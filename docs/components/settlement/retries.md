---
sidebar_position: 3
title: Retries
description: "max_attempts caps how many deliveries a message gets, counted in the framework's retry-count header that every copy carries."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

`max_attempts` caps how many deliveries a message gets, counted in the framework's retry-count header that every copy carries.

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) follows `include` at the mount site; `n` counts the first delivery too.
Neither transport counts redeliveries, so the count is [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), which the runtime writes on every copy.
A handler reads the header off its context to know which attempt it runs.
Under a cap, an immediate [`retry`](api:ruststream::runtime::HandlerOutcome::retry) travels as a copy too, because the count lives in the header.
The delivery that reaches the cap is rejected and no copy follows it.
A cap with no [dead-letter key](./dead-letters.md) beside it writes the spent delivery nowhere.
A registration mounted by a bare stream key takes the cap the same way as one mounted by a [`FileStream`](../subscriptions/file-stream.md).
The guide [Retries and dead letters](../../guides/retries.md) caps a service's retries on a stream file and on a pipe.

## Example

The printer is jammed, so every attempt asks for another; the third delivery is the last.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_max_attempts.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_max_attempts.rs#handler"
```

</TabItem>
</Tabs>
