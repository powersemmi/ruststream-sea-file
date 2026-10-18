---
sidebar_position: 4
title: FileBatchContext
description: "A batch body reaches the seek handle through FileBatchContext, which holds no position; each element carries its own sequence."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FileBatchContext

A [batch body](../subscriptions/batches.md) reaches the seek handle through [`FileBatchContext`](api:ruststream_sea_file::FileBatchContext), which holds no position; each element carries its own sequence.

A batch spans many deliveries, so it has no single place in the file.
The batch context holds what every delivery of the batch shares: the subscription's [`SeekHandle`](./seek.md).
A batch body takes `ctx: &mut Context<'_, FileBatchContext>` and reads the handle with `ctx.context(SeekHandle)`.
The batch settles first, so a seek from its body applies to the batches after it.
The deliveries the current batch already holds stay in it.
Each element's own sequence is in its `stream-sequence` header ([SEQUENCE_HEADER](../headers/sequence-header.md)).
[`Position`](./position.md) is a key of the per-delivery [`FileContext`](api:ruststream_sea_file::FileContext), which a single-message handler reads.
In the manual form the body is an `impl Handle<[T], (), (), FileBatchContext>`.

## Example

Job 1 says the jobs up to sequence 5 cannot be processed: the first batch holds jobs 1 to 3, and the next one opens at job 5.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_batch_seek.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_batch_seek.rs#batch"
```

</TabItem>
</Tabs>
