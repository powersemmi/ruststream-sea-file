---
sidebar_position: 7
title: Shutdown
description: "Shutdown flushes and closes the stream file; a publisher kept from before returns NotConnected, and a stdio shutdown ends every stdio stream."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown flushes and closes the stream file; a publisher kept from before returns [`SeaFileError::NotConnected`](api:ruststream_sea_file::SeaFileError::NotConnected), and a stdio shutdown ends every stdio stream of the process.

The file broker's shutdown waits for the publishes in flight, then ends the file's writer.
The writer writes the [end-of-stream mark](./end-with-eos.md) when the broker asks for one, and syncs the file to the device.
When shutdown returns, the file is closed, so an `after_shutdown` hook of the app can move, copy or remove it.
Brokers opened on one path in one process share the file's writer: the first of them to shut down ends it, and a publish through the others fails.

A publisher taken before the shutdown, such as the one a startup hook received, still points at the closed broker.
A publish through it returns `NotConnected` and never succeeds against the closed file.

The stdio transport belongs to the whole process.
Shutting a [`StdioBroker`](./stdio-broker.md) down ends every stdio subscription and publisher in the process, not only the ones that broker opened.

## Example

Once the broker has closed the file, the app's `after_shutdown` hook moves it aside, and the next run starts a new one.
The test runs the hook's function after the harness shut the broker down, and the moved file opens as a whole stream file.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_broker_shutdown.rs#hooks"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_broker_shutdown.rs#hooks"
```

</TabItem>
</Tabs>
