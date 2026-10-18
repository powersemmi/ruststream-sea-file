---
sidebar_position: 5
title: StdioBroker
description: "StdioBroker reads standard input and writes standard output, so a service runs as one stage of a shell pipeline."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# StdioBroker

[`StdioBroker`](api:ruststream_sea_file::stdio::StdioBroker) reads standard input and writes standard output, so a service runs as one stage of a shell pipeline.

`StdioBroker::new` takes no settings.
At startup the runtime attaches the process's own standard input and output, over [`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio).
Standard input is where every subscription reads, and standard output is where every publish writes.
So `producer | service | consumer` connects three processes with nothing in between.

Each line is one message in the client's format, `[timestamp | stream_key | sequence | shard_id] payload`, with every field in the brackets optional.
The stream key is part of the line, so one stage reads several keys from one input.
A subscription reads the lines of its own stream key.

A line the service writes goes to the next process, never back to this one.
So a delayed retry has no address on this transport, and every registration names where its deferred copies go with `.out_retry(Publish).to(key)`.
A registration that names none fails startup, and the error names the subscription.
[`loopback`](./loopback.md) sends the output back to the input, for tests that run several stages in one process.

When standard input ends, every subscription ends after the lines it has already read.
The guide [Shell pipelines](../../guides/pipelines.md#when-the-input-ends) shows a stage that then shuts down and exits.

The transport belongs to the whole process.
Shutting the broker down ends every stdio subscription and publisher in the process.
The guide [Shell pipelines](../../guides/pipelines.md) feeds a stage, reads its output and chains stages.

## Example

The stage reads text under `lines` and writes a word count under `counts`.

```sh
echo '[2024-01-01T00:00:00 | lines | 1] {"body":"one two three"}' \
    | cargo run --example stdio_broker --features testing -- run
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/stdio_broker.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/stdio_broker.rs#stage"
```

</TabItem>
</Tabs>
