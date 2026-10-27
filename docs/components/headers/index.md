---
title: Headers
description: "Headers travel inside the payload: a message with headers is written as rs1: or rs2: plus base64, one without is written as it is."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

Headers travel inside the payload: a message with headers is written as `rs1:` or `rs2:` plus base64, one without is written as it is.

A stream file entry and a line on a pipe carry a stream key and a payload, with no room for headers.
So a message with headers is written as an [envelope](../../architecture/envelope.md): a prefix, then base64 of a header block followed by the payload.
The prefix `rs1:` marks a block of `name: value` lines.
It is used whenever every header reads back unchanged from a line, which keeps the message readable by earlier releases of this crate.
The prefix `rs2:` marks a block where each name and value is length-prefixed.
It is used when a value is not UTF-8, holds a line break, or begins or ends with whitespace, so a value travels byte for byte.
The envelope is text, so it fits on a line of a pipe.
A message with no headers is written as it is, so a file recorded that way stays readable by any `sea-streamer` consumer, and a file another tool wrote is readable here.
A payload that itself begins with `rs1:` or `rs2:` is enveloped, so no reader takes it for an envelope.
This is the default envelope; a broker, a subscription or a publish policy can choose another on [Envelopes](./envelopes.md).
A handler reads headers with [`Headers<T>`](api:ruststream::runtime::Headers) or `ctx.headers()` as on any broker, and the delivery parses them the first time they are read.
Every delivery also carries its position in [`SEQUENCE_HEADER`](./sequence-header.md).

## Example

The service confirms each order under the tenant header it arrived with, and the receipt carries the same header.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_headers.rs#handler"
```

</TabItem>
</Tabs>
