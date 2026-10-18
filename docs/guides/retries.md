---
sidebar_position: 5
description: "Retry a message later on a stream file or a pipe, cap the deliveries and move what keeps failing to a dead-letter stream key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a message later, caps its deliveries and moves what keeps failing to a [dead-letter key](../components/settlement/dead-letters.md).

Neither transport holds a message back or counts its deliveries.
The framework does both: a later delivery is a copy the runtime publishes once a delay has passed, and the count travels in a header of that copy.

The example is a shipping service that ships an order once its payment has cleared.

## The subscriber

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#ship"
```

</TabItem>
</Tabs>

`Payments` is the service's client of the payment service, kept in the app's state.
While an order's payment has not cleared, `ship` answers [`retry_after`](../components/settlement/retry-after.md) and the service asks again two seconds later.

`retry_after` settles the delivery, waits out the delay and appends a copy of the order under the key the subscription reads.
The subscription reads the copy as a new message, after everything that was already in the file.
So the orders behind a waiting one are not held up.
The copy carries the retry-count header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
The copy waits in the service's memory until the delay is over, so a process that stops before then loses it.
A [replay](../components/subscriptions/replay.md) reads a copy only while it is still short of the end of the file.

## The cap and the dead-letter key

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) counts every delivery of one order, the first included.
The third delivery is not retried: [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) appends it under `orders.dead`, payload and headers as they arrived.
On a stream file the dead-letter key is one more stream key of the same file.
A cap with no dead-letter key beside it writes the spent order nowhere.

## Reading the dead letters

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` reads `orders.dead` like any other key.
The retry-count header of a dead letter holds the number of deliveries the order got, 3 here.
An operator who inspects the dead letters later reads them with a [replay](../components/subscriptions/replay.md) instead.

## Retries on a pipe

A stage's standard output reaches the next process and never its own input, so a copy cannot come back to the stage that deferred it.
On [`StdioBroker`](../components/broker/stdio-broker.md) the mount site names the key the copies go to, `b.include(ship).out_retry(Publish).to("orders.retry")`, and a later stage reads them ([out_retry](../components/settlement/out-retry.md)).
The pipeline stage of [Getting started](getting-started.md#the-same-handler-in-a-pipeline) is mounted this way.

A stage that names no key does not start, because any handler may answer `retry_after`.
The cap and the dead-letter key work on a pipe as they do on a file; a dead letter is a line under its key for a downstream stage.

## Testing the delay

The tests in the example file run the app in process on a paused clock, so `tb.advance(RECHECK)` lets the two seconds pass at once.
One test clears the payment between two deliveries, and the order ships on the second.
The dead-letter test runs both in process and on a real stream file with `TestApp::start_live`, where the delays pass on the real clock.
[Testing a file or pipeline service](testing.md) covers the harness.
