---
sidebar_position: 5
title: size_limit
description: "size_limit caps the stream file at a size the operator chose; without it the file grows without limit."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# size_limit

[`size_limit`](api:ruststream_sea_file::file::FileBroker::size_limit) caps the stream file at a size the operator chose; without it the file grows without limit.

The cap counts every byte of the file, the bytes an earlier run wrote included.
The publish that reaches the cap fails with [`SeaFileError::Publish`](./errors.md), and every later publish to that file returns that error at once.
Publishes in flight at that moment each return, and each one reported done is in the file whole.

At the cap the file client behaves in two ways a reader of the file notices.
A message that fills the cap exactly is written whole, and its publish still fails: a reader of the file reads a message whose publish reported an error.
A message larger than the space left is cut at the cap, and a reader stops before it.

A file already larger than the cap fails startup with [`SeaFileError::Invalid`](./errors.md), which names the size and the cap.
Brokers opened on one path in one process share the file's writer, and with it the cap of the first one to connect.

In a [`TestApp`](api:ruststream::testing::TestApp) test the stream file is a log in memory and has no size, so the cap shows only against a real file, with [`TestApp::start_live`](../testing/live.md).

## Example

The recorder's log may take 64 GiB of the disk.
The tests cap a file at 1 KiB and publish until a publish fails, and start the recorder on a file an earlier run left larger than its cap.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_broker_size_limit.rs#capped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_broker_size_limit.rs#capped"
```

</TabItem>
</Tabs>
