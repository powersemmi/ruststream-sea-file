---
sidebar_position: 2
title: StdioPublish
description: "A stdio publish writes one line to standard output under its stream key; an empty message is refused because a pipe drops empty lines."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StdioPublish

A stdio publish writes one line to standard output under its stream key; an empty message is refused because a pipe drops empty lines.

[`StdioPublish`](api:ruststream_sea_file::stdio::StdioPublish), [`Publish`](./index.md) in [`stdio::prelude`](../broker/prelude.md), pairs into a [`StdioPublisher`](api:ruststream_sea_file::stdio::StdioPublisher).
A publish writes one line in the [line format](../subscriptions/lines.md), with the message's stream key in its header.
The next stage of the pipeline reads that line.
This process's own subscriptions do not, because standard output never reaches the same process's standard input; [`loopback`](../broker/loopback.md) changes that for a test.
A payload a text line would change, such as binary data or a payload with a newline, leaves in an envelope and arrives whole ([Payloads](../headers/payloads.md)).
A message with no payload and no headers would be an empty line, which the line format drops.
So the publisher refuses it with [`SeaFileError::Invalid`](../broker/errors.md), and nothing is written.

## Example

The stage answers each word with the same word in capitals, as a line under `shouts`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_publish.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_publish.rs#stage"
```

</TabItem>
</Tabs>
