---
sidebar_position: 7
description: "Run a stream file or pipeline service in production: the file's path and settings, what a publish writes, the size cap, shutdown and logs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

A stream file is the service's own storage, so the facts a networked broker keeps on its servers are the service's here: where the file lives, when its bytes reach the device and how large it grows.

## The file

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

The deployment names the path in an environment variable.
Startup creates the file when it is missing.
A service that only reads a file another process writes opens it with [`existing_only`](../components/broker/existing-only.md), so a wrong path stops it at startup.
[`beacon_interval`](../components/broker/beacon-interval.md) sets how far apart the file's index entries sit.
A seek reads forward from the nearest beacon, so closer beacons seek in finer steps and take more room; the client's default is one every 1 MiB.
The interval must be a positive multiple of 1024, and any other value stops the service at startup.
The file client builds on Unix systems only.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`app` builds the service on the production `broker`, and the tests start that same `app`.

## What a publish writes

A publish appends the message under its stream key and flushes it, so the bytes reach the operating system before the call returns.
A live reader in this process or another one sees the message from then on.
The file is synced to the device when the broker [shuts down](../components/broker/shutdown.md).
Between those points the operating system decides when the bytes reach the device.

The file client caps a stream file at 16 GiB.
A publish that reaches the cap fails with [`SeaFileError::Publish`](../components/broker/errors.md), and so does every later publish to that file.
A long-lived log therefore moves to a new file before it reaches the cap: the service starts on a new path, and readers of the old file finish it with a [replay](../components/subscriptions/replay.md).

## Shutdown

The broker's shutdown waits for the publishes in flight, writes the [end-of-stream mark](../components/broker/end-with-eos.md) when the broker asks for one, and syncs the file.
When shutdown returns the file is closed, so an `after_shutdown` hook can move or copy it.
[`end_with_eos`](../components/broker/end-with-eos.md) suits a writer whose file is complete when it stops, such as a daily log: a replay completes at the mark, and a live reader ends on it.
A publisher kept from before the shutdown returns [`SeaFileError::NotConnected`](../components/broker/errors.md) and never writes to the closed file.

## A pipeline stage

A stage's standard output carries its messages, so its log goes to standard error through `tracing`.
A line on standard output from anywhere else in the process reaches the next stage as a message.
The stdio transport belongs to the whole process: shutting a [`StdioBroker`](../components/broker/stdio-broker.md) down ends every stdio subscription and publisher in it.

## The AsyncAPI document

Under the `asyncapi` feature the file broker is a [server](../components/asyncapi/server.md) of the generated document with the protocol `file`, and its description is the path of the stream file.
The document is shared with the people who read it, so the path is too.

## Testing the configuration

The tests start the production `app` under [`TestApp::start`](../components/testing/index.md), so the settings are checked the way startup checks them, without a file.
A beacon interval that is not a multiple of 1024 fails the start in the test as it does on a real file.
