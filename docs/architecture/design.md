---
title: Design
sidebar_position: 2
description: "Where ruststream-sea-file sits: between the RustStream runtime and the two sea-streamer clients, a column for the stream file and one for the pipe."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the sea-streamer clients and what they speak to, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, codec, settlement, retries",
  "crate": "ruststream-sea-file",
  "fileSubscriber": ["FileSubscriber", "driver task"],
  "stdioSubscriber": ["StdioSubscriber", "reader task"],
  "filePublisher": "FilePublisher",
  "stdioPublisher": "StdioPublisher",
  "fileBroker": ["FileBroker", "ConnectedFileBroker"],
  "stdioBroker": ["StdioBroker", "ConnectedStdioBroker"],
  "envelope": ["Header envelope", "headers inside the payload"],
  "client": "sea-streamer clients",
  "fileClient": "sea-streamer-file",
  "stdioClient": "sea-streamer-stdio",
  "disk": "A .ss file on disk",
  "pipes": "stdin and stdout",
  "inProcess": ["Under testing, a broker's link can be a file or a pipe", "held in memory instead of the client"],
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-sea-file` sits: between the RustStream runtime and the two sea-streamer clients,
a column for the stream file and one for the pipe.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over two clients of
[sea-streamer](https://github.com/SeaQL/sea-streamer): `sea-streamer-file` for a stream file and
`sea-streamer-stdio` for the process's standard input and output. The runtime above it is the same
for every broker. What is specific to a file or a pipe is in this crate: the
[descriptor](../components/subscriptions/file-stream.md), [positions and
seeking](../components/positions/index.md), [what an ack means](../components/settlement/index.md),
[where a retry copy goes](../components/settlement/retry-after.md) and how headers travel.

A connected broker holds one link to its transport. For a file it is the client's streamer, one
producer that every publish appends through, and the runtime `connect` ran on. For stdio it is the
client's streamer and that runtime. Every clone of a broker shares the link, so connecting twice
connects once, and a publisher handed out before connect finds it on its first publish.

Every task of a connection runs on the runtime `connect` ran on. A subscription opened from another
runtime therefore keeps delivering after that runtime stops.

A file subscription is a driver task that owns the client's consumer, because the client's seek
needs the consumer to itself and cannot be interrupted. A stdio subscription is a reader task. Both
hand their messages over a channel of 64, and both clients read one message at a time, so a
[batch](../components/subscriptions/batches.md) is assembled in the crate. A partial batch leaves
10 milliseconds after its first message.

Neither transport takes per-message settings, so a publish has no options. The clients take a
payload and no headers, so headers travel inside the payload, in the [envelope](envelope.md).

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).
