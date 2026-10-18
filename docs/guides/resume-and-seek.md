---
sidebar_position: 3
description: "Resume a stream file service where its last run stopped with a stored sequence, start at a moment in time, and move a running subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Resuming and seeking

This guide makes a service resume where its last run stopped, and moves a running subscription to another place in the file.

A stream file keeps no record of how far a reader got.
`ack` and `nack` report `Unsupported` on this transport ([Acknowledgement](../components/settlement/index.md)), so a restarted subscription opens wherever its mount site says.
A service that must not handle an order twice stores its own checkpoint.

The example is an invoicing service that reads the shop's orders and invoices each one once, across restarts.

## The checkpoint

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/checkpoint.rs" reference="examples/guide/resume.rs#checkpoint"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/checkpoint.rs" reference="examples/manual/guide/resume.rs#checkpoint"
```

</TabItem>
</Tabs>

The checkpoint is the sequence of the last order the service invoiced, in a file of its own.
`Invoicing` is the app's state, and the handler takes the checkpoint from it.
Any store the service already has works the same way: a database row, a key in a cache.

## Resuming at the checkpoint

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/resume.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/resume.rs#service"
```

</TabItem>
</Tabs>

`invoicing` reads the checkpoint before the service starts and passes it to [`start_at`](../components/positions/start-at.md) at the mount site.
The first run has no checkpoint and reads the file from the beginning.
[`FilePosition::sequence(n)`](../components/positions/index.md) delivers message `n` itself, so a run opened at the stored sequence gets the order the last run ended on once more.
`invoice` reads the delivery's sequence with [`Ctx<Position>`](../components/positions/position.md) and drops an order at or below the checkpoint.
Every other order is invoiced, and its sequence becomes the new checkpoint.

The checkpoint is written after the invoice.
A run that stops between the two invoices that order again on the next start, so the invoice itself must tolerate a repeat.
Writing the checkpoint first gives the opposite trade: an order is never invoiced twice, and one can be lost.

[`FilePosition::timestamp(ms)`](api:ruststream_sea_file::FilePosition::timestamp) opens at the first message written after a moment, in milliseconds since the Unix epoch.
It suits a service that rebuilds its view from a point in time instead of from a stored sequence.

## Moving a running subscription

A handler moves its own subscription with [`Ctx<SeekHandle>`](../components/positions/seek.md).
`seek(FilePosition::end())` skips the backlog and goes on from the tip; `seek(FilePosition::sequence(n))` goes back to message `n` or forward past a run of messages known to fail.
The deliveries already queued from the old place are discarded, and the next delivery comes from the new one.
A seek to a sequence past the next one the file writes is refused with an error: a live subscription then ends, and a [replay](../components/subscriptions/replay.md) stays where it was.
A batch handler reaches the same handle through [`FileBatchContext`](../components/positions/batch-context.md).

## Testing the resume

The in-process tests record five orders before the service starts and store a checkpoint of three.
The service reads orders three to five, drops the third and invoices the other two.
The restart test runs the service twice on one real file with [`TestApp::start_live`](../components/testing/live.md): the second run opens at the order the first one ended on and drops it.
[Testing a file or pipeline service](testing.md) covers both modes.
