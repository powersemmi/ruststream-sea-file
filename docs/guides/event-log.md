---
sidebar_position: 2
description: "Record a service's events in a stream file, follow them live from another subscription and total a finished file in one pass."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# An event log on disk

This guide records a shop's events in a stream file, follows them as they are written and totals a finished file in one pass.

A stream file is a log: a publish appends a message under a stream key, and every reader of the key sees it in order.
The example is a shop that records a receipt for each order, and an audit job that totals the receipts of a finished file.

## Recording and following

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/event_log.rs#record"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/event_log.rs#record"
```

</TabItem>
</Tabs>

`checkout` answers each order with a `Receipt`, and the reply is appended to the same file under `receipts`, the key the type declares.
One file holds every stream key, so the path is named once, in `FileBroker::new`.
A publish appends the message and flushes it, so the bytes reach the operating system before the call returns.
The file is synced to the device when the broker [shuts down](../components/broker/shutdown.md).

`notify` follows `receipts` from the end of the file and reads each receipt as `checkout` appends it.
Another process that opens the same path follows the key the same way.
A subscription with no [position](../components/positions/start-at.md) starts at the end, so a restarted `notify` reads only the receipts written after it started.

Each stream key is numbered from one.
The receipt keeps the order's sequence, which [`Ctx<Position>`](../components/positions/position.md) hands the handler, so a reader can [move back](resume-and-seek.md) to that order later.
A later run that appends to the same file carries the numbering on: after a restart the next order is number three, not one.

The file client caps a stream file at 16 GiB.
A publish that reaches the cap fails, and so does every later publish to that file.
[Running in production](production.md) covers what that means for a long-lived file.

## A one-pass job

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/bin/audit.rs" reference="examples/guide/event_log.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/bin/audit.rs" reference="examples/manual/guide/event_log.rs#audit"
```

</TabItem>
</Tabs>

[`replay()`](../components/subscriptions/replay.md) reads what the file holds under `receipts` from the first message and completes at the end of the file.
A receipt appended after that point is not read, so the job does its pass and has nothing more to wait for.
[`existing_only`](../components/broker/existing-only.md) makes startup fail when the file is missing, so a mistyped path stops the job instead of creating an empty file.

`total` takes a slice, and [`batch(nonzero!(64))`](../components/subscriptions/batches.md) at the mount site caps a batch at 64 receipts.
A batch takes what the file already holds and does not wait to fill up; one outcome settles the whole batch.

A writer marks a file finished with [`end_with_eos`](../components/broker/end-with-eos.md), which appends an end-of-stream mark at shutdown.
A replay completes at the mark, and a live subscription such as `notify` ends on it.

## Testing the log

The tests in the example file run the shop and the audit in process: an order gets its receipt and `notify` reads it, and the audit reads three recorded receipts in one batch and ignores a fourth.
The audit's receipts are recorded on a clone of its broker before it starts, the way the shop would have left them.
The restart test runs the shop twice on one real file with `TestApp::start_live` and checks that the second run numbers its order three.
[Testing a file or pipeline service](testing.md) covers both modes.
