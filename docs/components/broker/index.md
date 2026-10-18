---
title: Broker
description: "Two brokers and no server: FileBroker keeps a log in one .ss stream file, StdioBroker makes the process's pipes a stream."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Broker

Two brokers and no server: [`FileBroker`](./file-broker.md) keeps a log in one `.ss` stream file, and [`StdioBroker`](./stdio-broker.md) makes the process's standard input and output a stream.

A [stream file](./file-broker.md) is a log on disk.
It keeps what was written across restarts, and a subscriber can replay it or move back to an earlier message.
A [stdio](./stdio-broker.md) service is one stage of a shell pipeline: it reads lines on standard input and writes lines on standard output.
Neither transport has a host to connect to, so nothing is installed and nothing listens.

Inside one transport, messages are grouped by stream key, a name such as `orders`.
A stream key in `#[subscriber("orders")]` is a subscription on both brokers.
A publish names the stream key it goes to.

Both brokers follow the same lifecycle.
`new` only records the configuration and does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
At startup the runtime connects the broker: the stream file is opened, or the process's pipes are attached.
Every subscription and every publisher of the service opens against that connected form, [`ConnectedFileBroker`](api:ruststream_sea_file::file::ConnectedFileBroker) or [`ConnectedStdioBroker`](api:ruststream_sea_file::stdio::ConnectedStdioBroker).
At [shutdown](./shutdown.md) the runtime closes it.

A service on one transport imports that transport's [prelude](./prelude.md).
A failure of either broker is a [`SeaFileError`](./errors.md).

## Example

The service reads the stream key `orders` of the file `/var/lib/orders/orders.ss` from its start.
`cargo run --example file_service --features testing -- run` starts it, and creates the file when it is missing.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_service.rs#app"
```

</TabItem>
</Tabs>
