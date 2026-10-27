---
title: Path
sidebar_position: 4
description: "The way of one message through a RustStream service, on a stream file and on a pipe: the reader, the channel, the handler, the reply and the write."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one message that is answered, on a stream file and on a pipe",
  "text": "The diagram as text",
  "fileLane": "Stream file",
  "pipeLane": "Pipe",
  "fRead": ["Client consumer", "reads the file"],
  "fDriver": ["Driver task", "stamps the generation"],
  "fChannel": "Channel of 64",
  "fStream": ["FileSubscriber", "drops old generations"],
  "fBatch": ["Batching", "under batch(n)"],
  "fDecode": ["FileContext,", "then decode"],
  "fHandler": ["Handler", "#[subscriber] fn"],
  "fPublish": ["FilePublisher", "envelope if headers"],
  "fAppend": ["Append, flush", "into the file"],
  "pRead": ["Reader thread", "parses the line"],
  "pTask": ["Dispatch", "by stream key"],
  "pChannel": "Queue per subscription",
  "pStream": ["StdioSubscriber", "opens a line per poll"],
  "pBatch": ["Batching", "under batch(n)"],
  "pDecode": ["Decode"],
  "pHandler": ["Handler", "#[subscriber] fn"],
  "pPublish": ["StdioPublisher", "envelope if needed"],
  "pWrite": ["stdout thread", "writes the line"]
};

# Path

The way of one message through a RustStream service, on a stream file and on a pipe: the reader,
the channel, the handler, the reply and the write.

<Diagram name="path" words={words} />

On a file, the subscription's task reads ahead of the handler. It takes each message off the client,
keeps the bytes it read, and lets the [envelope](envelope.md) find the payload in them. Then it
queues the delivery on a channel of up to 64. The handler reads from that channel, so the next
message is out of the file while the current one is handled.

On a pipe, the process's reader thread reads ahead instead. It queues each line for the
subscriptions of its key, with no bound, and the subscription lets the envelope find the payload
when the handler asks for the next message.

On both transports the headers are parsed the first time something reads them, together with the
message's number in the [`stream-sequence`](../components/headers/sequence-header.md) header. A
handler that reads only the payload never parses them.

On a file, every delivery carries the generation the reader was in when it read it. A
[seek](seeking.md) opens a new generation, and the subscription drops what was queued under the old
one. A delivery also holds a shared reference to the subscription's seeker, so building its
context per message only counts references.

The handler's reply is encoded into a buffer the dispatch loop reuses. The publisher hands those
bytes to the client as they are when the envelope leaves them alone. The default envelope wraps them
only when the message has headers, or on a pipe when a line would change the payload. A file publish returns once the append is flushed to the
file. A reply written to the file the subscription reads is passed over by its reader, because it
sits under another stream key.

The delivery settles after the reply. On both transports an ack and a nack report that they are
unsupported, as [Settlement](settlement.md) draws.
