---
title: Acknowledgement
description: "ack and nack report Unsupported on both brokers: nothing records how far a service read, so it resumes from a stored position."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

`ack` and `nack` report `Unsupported` on both brokers: nothing records how far a service read, so it resumes from a stored position.

A handler settles a delivery as on any broker, with [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) or another outcome.
The runtime passes the settlement to the transport, which answers [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs a warning naming the subscription.
A stream file keeps no record of a reader, and the file client's resumable mode is not implemented.
So a subscription opens where its descriptor says on every start, whatever the previous run acknowledged.
A service that resumes stores the sequence it reached, read with [`Ctx<Position>`](../positions/position.md), and opens the next run there with [`start_at`](../positions/start-at.md).
A pipe has no log to go back to: a line read is gone, whatever the handler answers.
What a handler asks for beyond `ack` still happens, because the runtime does it: [`retry_after`](./retry-after.md) on a stream file, [`out_retry`](./out-retry.md) on a pipe, [a cap](./retries.md) and [a dead-letter key](./dead-letters.md) on both.

## Example

The service invoices every order the file holds; acknowledged orders are read again on the next start.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_acknowledgement.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_acknowledgement.rs#handler"
```

</TabItem>
</Tabs>
