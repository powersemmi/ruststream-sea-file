---
title: AsyncAPI
description: "With asyncapi each broker is a server and each FileStream channel carries its stream key in the x-ruststream-file extension."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI

With the `asyncapi` feature each broker is a server of the generated document, and each [`FileStream`](../subscriptions/file-stream.md) channel carries its stream key in the `x-ruststream-file` extension.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds what it knows about the two transports.
A [server](./server.md) reports the path of the stream file, or the protocol name `stdio`.
A [channel](./channels.md) on a stream file reports the stream key behind it.

The specification lists no binding for a file transport or for stdio, and its protocol keys are a closed list.
So the stream key travels in this crate's own extension, `x-ruststream-file`, which carries no binding version.
Everything comes from the service's own declarations: the brokers, the descriptors and the destinations the mount sites resolved.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-sea-file = { version = "0.7", features = ["asyncapi"] }
```

The service registers its stream file under the label `file` and its pipeline under `pipe`, the keys of their servers in the document.
`cargo run --example sea_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_asyncapi.rs#app"
```

</TabItem>
</Tabs>
