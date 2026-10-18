---
sidebar_position: 1
title: FileStream
description: "FileStream names one stream key of the file and is the descriptor the generated document describes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FileStream

[`FileStream`](api:ruststream_sea_file::FileStream) names one stream key of the file and is the descriptor the generated document describes.

[`FileStream::new(key)`](api:ruststream_sea_file::FileStream::new) goes where a stream key goes, as the first argument of `#[subscriber]`.
It reads what the bare key reads: the messages appended under that key once the service has started.
The file itself is named once, by `FileBroker::new(path)`, so every descriptor of a service reads the same file.
Where reading begins is a [position](../positions/index.md), set with [`start_at`](../positions/start-at.md).
The descriptor adds two things to the bare key.
[`replay()`](./replay.md) reads the retained file once and completes, a reading mode a position cannot express.
Under the `asyncapi` feature the subscription's channel carries its stream key in the `x-ruststream-file` extension of the generated document, as [Channels](../asyncapi/channels.md) shows; a bare key describes nothing there.
A `retry_after` copy is appended under the descriptor's key, so it comes back to the same subscription with nothing named at the mount site ([retry_after](../settlement/retry-after.md)).
An empty key or a key the file does not take stops the service at startup.
The manual form passes the descriptor to [`subscriber(..)`](api:ruststream::runtime::subscriber) where a key would go.

## Example

Two orders were in the file when the service started; the service ships the third, the first appended after it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_stream.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_stream.rs#descriptor"
```

</TabItem>
</Tabs>
