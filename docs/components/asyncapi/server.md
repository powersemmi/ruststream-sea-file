---
sidebar_position: 1
title: Server
description: "The file broker reports the path of its stream file and the stdio broker its protocol name; neither reports a host."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The file broker reports the path of its stream file and the stdio broker its protocol name; neither reports a host.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Both transports run inside the process, with nothing to connect to over a network, so a server has no `host`.
The [file broker](../broker/file-broker.md)'s `protocol` is `file`, and its `description` is the path of the stream file, where a reader finds the log.
The [stdio broker](../broker/stdio-broker.md)'s `protocol` is `stdio`, and it reports nothing else.
Neither server carries a `protocolVersion` or bindings.

## Example

The stream file and the pipeline, registered under the labels `file` and `pipe`, come out as these two servers:

```json
"servers": {
  "file": { "protocol": "file", "description": "/var/lib/ruststream/orders.ss" },
  "pipe": { "protocol": "stdio" }
}
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_asyncapi.rs#servers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_asyncapi.rs#servers"
```

</TabItem>
</Tabs>
