---
sidebar_position: 1
title: FileBroker
description: "FileBroker::new records the path of a stream file; the runtime opens it at startup and creates it when it is missing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FileBroker

[`FileBroker::new`](api:ruststream_sea_file::file::FileBroker::new) records the path of a stream file; the runtime opens it at startup and creates it when it is missing.

A stream file holds one log per stream key, in the `.ss` format of [`sea-streamer-file`](https://docs.rs/sea-streamer-file).
Each stream key is numbered from one, and the numbering carries on when a later run appends to the same file.
A service that reads a file another service writes requires it with [`existing_only`](./existing-only.md) instead.

A publish appends the message under its stream key and flushes it, so the bytes reach the operating system before the call returns.
The file is synced to the device when the broker [shuts down](./shutdown.md).
The file client caps a stream file at 16 GiB by default, and a publish past that size fails.

The file client builds on Unix systems only, so the file transport does not build on Windows.
Two more settings shape the file itself: [`end_with_eos`](./end-with-eos.md) marks it finished at shutdown, and [`beacon_interval`](./beacon-interval.md) sets how densely it is indexed.

## Example

The service confirms each order on the stream key `receipts` of the same file.
The test starts it on a path where no file exists yet, and the file is there once startup returns.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_broker.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_broker.rs#broker"
```

</TabItem>
</Tabs>
