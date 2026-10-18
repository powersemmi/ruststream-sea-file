---
sidebar_position: 4
title: Batches
description: "A slice handler takes a batch assembled on the client, up to the size batch(n) names; a partial batch leaves 10 ms after its first delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler takes a batch assembled on the client, up to the size `batch(n)` names; a partial batch leaves 10 ms after its first delivery.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
Both clients read one message at a time, so the crate assembles each batch on the client, on a stream file and on a pipe alike.
A batch takes what the transport already holds, up to `n`, and does not wait to fill up.
A batch that is not full goes out 10 ms after its first delivery, so at a quiet tail a batch is often one message.
A `Vec<HandlerOutcome>` settles message `i` with outcome `i`, and a single `HandlerOutcome` settles the whole batch.
A batch body on a stream file reaches the seek handle through [`FileBatchContext`](../positions/batch-context.md).
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` settles the whole batch and `Err(outcomes)` settles message by message.

## Example

The handler sums a batch of readings and settles each one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_batches.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_batches.rs#batch"
```

</TabItem>
</Tabs>

## The batch size

Ten readings recorded in the file arrive as batches of four, four and two.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_batches.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_batches.rs#mount"
```

</TabItem>
</Tabs>
