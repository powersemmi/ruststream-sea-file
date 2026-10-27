---
sidebar_position: 3
title: Envelopes
description: "An envelope is the format a record carries headers in: Rs by default, Framed for a codec document per message, NoOp for the payload alone, or the service's own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelopes

An envelope is the format a record carries headers in: [`Rs`](api:ruststream_sea_file::envelope::Rs) by default, [`Framed`](api:ruststream_sea_file::envelope::Framed) for a codec document per message, [`NoOp`](api:ruststream_sea_file::envelope::NoOp) for the payload alone, or the service's own.

The envelope is chosen like a codec, and the most specific level wins.
The broker sets the default with `FileBroker::new(path).envelope(..)` or `StdioBroker::new().envelope(..)`.
A subscription on a stream file reads in its own with `FileStream::<NoOp>::new("readings")`, and a publish policy writes in its own with `Publish.envelope(..)`.
The choice is a type, so it costs nothing per message.

- `Rs` writes a message with headers as `rs1:` or `rs2:` plus base64, and a message without them as it is. This is the format of every earlier release, described on [Envelope](../../architecture/envelope.md).
- `Framed::new(codec)` writes every message as one document of the codec, an object with the fields `headers` and `payload`. With `JsonCodec` on a pipe, each line is a JSON object any line tool reads.
- `NoOp` writes the payload as it is. A publish that carries a header fails and names the header and the destination, and so does a payload a line on a pipe cannot carry.

A record that is not in the subscription's envelope reads as a payload with no headers.
A delivery parses its headers the first time something reads them, so a handler that reads only the payload never parses the envelope.
A test addresses a broker by the type its envelope gives it, `tb.broker::<FileBroker<NoOp>>()`; a broker with an envelope of the service's own is registered once with `ruststream::register_testable_broker!`.
The generated AsyncAPI document reports the media type of `Framed` records beside the channel.

## Example

The plant service reads the bare records a sensor gateway writes and sends its alerts as JSON documents, while the stream file keeps the default envelope.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_envelopes.rs#builtin"
```

```rust reference="examples/sea_envelopes.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_envelopes.rs#builtin"
```

```rust reference="examples/manual/sea_envelopes.rs#app"
```

</TabItem>
</Tabs>

## An envelope of the service's own

The pipe of the same service carries a feed whose lines hold the plant, a tab, and the payload. The envelope finds the payload behind the tab and parses the plant only when the handler reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_envelopes.rs#custom"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_envelopes.rs#custom"
```

</TabItem>
</Tabs>
