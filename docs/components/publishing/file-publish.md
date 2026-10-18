---
sidebar_position: 1
title: FilePublish
description: "A file publish appends under the destination's stream key and flushes, so a live subscriber or another reader sees it once the call returns."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FilePublish

A file publish appends under the destination's stream key and flushes, so a live subscriber or another reader sees it once the call returns.

[`FilePublish`](api:ruststream_sea_file::file::FilePublish), `Publish` in [`file::prelude`](api:ruststream_sea_file::file::prelude), pairs into a [`FilePublisher`](api:ruststream_sea_file::file::FilePublisher) on the file the broker opened.
A publish appends the message under its stream key and flushes it to the file, so the bytes have reached the operating system when the call returns.
A live subscription of that key reads the message next, and so does another process that reads the same file.
The file is synced to the storage device when the broker shuts down.
A stream key is a place inside the file, so the destination is the key alone and the path is named once, in [`FileBroker::new`](api:ruststream_sea_file::file::FileBroker::new).
The file client caps a stream file at 16 GiB: a publish that reaches the cap fails with [`SeaFileError::Publish`](../broker/errors.md), and every later publish to that file fails too.
A publisher kept from before the [shutdown](../broker/shutdown.md) returns [`SeaFileError::NotConnected`](api:ruststream_sea_file::SeaFileError::NotConnected).
A hook registered with `after_startup` runs once the subscriptions are open, and a hook that returns an error stops the startup.

## Example

The service leaves a record under `lifecycle` each time it starts.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_publish.rs#recorder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_publish.rs#recorder"
```

</TabItem>
</Tabs>
