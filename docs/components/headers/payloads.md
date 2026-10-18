---
sidebar_position: 2
title: Payloads
description: "A pipe carries binary, multi-line and padded payloads intact: the stdio publisher envelopes any payload a text line would change."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Payloads

A pipe carries binary, multi-line and padded payloads intact: the stdio publisher envelopes any payload a text line would change.

A line on a pipe is UTF-8 text: the reader splits its input at newlines and trims the space around each payload.
So the stdio publisher [envelopes](./index.md) a payload with no headers in four cases: it is not UTF-8, it holds a newline, it begins or ends with whitespace, or it begins with `rs1:` or `rs2:`.
Any other payload leaves as it is, so a plain JSON message stays a readable line.
The next stage opens the envelope and hands its handler the bytes that were published.
A stream file holds bytes, so the file publisher envelopes only a message with headers and a payload that begins with `rs1:` or `rs2:`.
A handler that keeps bytes as they are takes a [`Deserialized`](api:ruststream::runtime::Deserialized) input and answers with a [`Serialized`](api:ruststream::runtime::Serialized) type, so no codec touches them.

## Example

The stage keeps the first kilobyte of each binary image as its thumbnail, for the next stage.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_payloads.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_payloads.rs#stage"
```

</TabItem>
</Tabs>
