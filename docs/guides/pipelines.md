---
sidebar_position: 4
description: "Run a RustStream service as one stage of a shell pipeline: the line format, feeding a stage, reading its output and chaining stages."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shell pipelines

This guide runs a service as one stage of a shell pipeline, `producer | stage | consumer`.

[`StdioBroker`](../components/broker/stdio-broker.md) reads the process's standard input as a stream and writes its standard output.
A subscription reads the lines of one stream key, and a publish writes a line for the next process.

The example is a pricing stage: it reads orders and answers each one with a receipt.

## The stage

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#stage"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#stage"
```

</TabItem>
</Tabs>

`price` reads the lines under `orders` and replies with a `Receipt`, which leaves on standard output under `receipts`.
The stage needs [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md) even though `price` never retries: on a pipe a deferred copy can only go to a later stage, so every registration names the key, and the stage does not start without it.

## The line

A line is a header in square brackets, then the payload ([Lines](../components/subscriptions/lines.md)):

```text
[timestamp | stream_key | sequence | shard_id] payload
```

Every field of the header is optional, so a producer usually writes the key alone:

```bash
echo '[orders] {"id":7,"quantity":3,"unit_cents":250}' | ./pricing run
```

A line under another key is passed over, so several stages can take their own keys out of one stream.
A line with no header names no key, and the client hands it to every subscription of the stage.
So a tool that writes plain JSON lines feeds every handler at once:

```bash
jq -c '.[]' orders.json | ./pricing run
```

The client parses each header before the service sees the line, and a line it cannot parse stops the whole process.
Two kinds of line do this: input that is not UTF-8, and a leading `[...]` that is not a header, such as a JSON array.
A producer of such payloads writes a header in front of each one.

## The output

The stage writes one line per publish, with a header the client fills in:

```text
[2026-10-18T15:08:01 | receipts | 0] {"order":7,"total_cents":750}
```

The timestamp is the time of writing, to the second, and the sequence counts each key's lines from zero.
A tool that reads plain JSON takes the payload after the header:

```bash
./pricing run < orders.txt | sed 's/^\[[^]]*\] //' | jq .total_cents
```

A payload that a text line would change leaves in an [envelope](../components/headers/payloads.md): a binary payload, one with a newline or padding, and every message with headers.
An envelope is a line starting with `rs1:` or `rs2:`, meant for the next RustStream stage, which opens it and hands its handler the bytes and headers that were published.

The log goes to standard error through `tracing`, so it never mixes with the messages.
A line printed to standard output with `println!` reaches the next stage as a line with no key, and every subscription there reads it.

## Chaining stages

```bash
./intake run < orders.txt | ./pricing run | ./billing run
```

Each stage reads the keys it subscribes to and writes its replies for the next one.
Headers and binary payloads cross every pipe whole, because each stage opens the envelopes the previous one wrote.
A stage that retries later sends its copies down the pipe under the key its mount site names, and a later stage reads them, as [Retries and dead letters](retries.md) shows.

## Testing the stage

The example file tests the stage in process: `TestApp::start` runs it, `tb.broker::<StdioBroker>().message(..)` injects an order line, and `published::<Receipt>("receipts")` reads the line it wrote.

The live tests spawn the built stage with piped standard input and output.
They check that it answers `orders` lines and lines with no key, passes over another key, and stops on a JSON array line and on input that is not UTF-8.
[`loopback`](../components/broker/loopback.md) feeds a stage its own output, so two stages run in one process and one test drives both.
