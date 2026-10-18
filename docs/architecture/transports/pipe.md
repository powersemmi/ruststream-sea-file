---
title: Pipe
sidebar_position: 3
description: "The stdio client reads standard input on one thread and writes standard output on another, one line per message, for the whole process."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "How the stdio client carries lines in from the stage before and out to the stage after",
  "text": "The diagram as text",
  "upstream": "The stage before",
  "stdin": ["stdin thread", "reads and parses a line"],
  "dispatch": ["Dispatch by stream key:", "a line with no key", "goes to every consumer"],
  "consumers": ["StdioConsumer", "one per subscription"],
  "publisher": ["StdioPublisher", "envelope if needed"],
  "stdout": ["stdout thread", "numbers, stamps"],
  "line": ["[time | key | seq]", "payload"],
  "downstream": "The stage after",
  "process": ["One stdin and one stdout thread per process:", "a shutdown ends every stdio stream in it"],
  "lines": "lines"
};

# Pipe

The stdio client reads standard input on one thread and writes standard output on another, one
line per message, for the whole process.

<Diagram name="pipe" words={words} />

A line is `[timestamp | stream_key | sequence | shard_id] payload`, and every field in the
brackets is optional. The timestamp reads `YYYY-MM-DDTHH:MM:SS`, with fractions of a second
allowed. A line that starts with `[WORD]` is read as the stream key `WORD`.

The first subscription starts the stdin thread, which reads a line, parses its fields and hands it
to the consumers of its key. A line with no key goes to every consumer, so a stray line on
standard output of the stage before reaches every subscription of this one. When the stage starts,
the crate holds standard input until its first consumer exists, so a line already waiting is not
lost.

The first publish starts the stdout thread. It numbers each key from zero, stamps the time of
writing to the second, and prints the line. It skips an empty payload, so the
[publisher](../../components/publishing/stdio-publish.md) refuses an empty message instead of
reporting a line that never left. A payload a line would change goes inside the
[envelope](../envelope.md).

The dashed arrow is [`loopback`](../../components/broker/loopback.md): the stdout thread also hands each line to this process's own
consumers. It runs a stdio service in one process, as a test does.

Both threads belong to the process, not to a broker. A [shutdown](../../components/broker/shutdown.md)
ends every stdio consumer and producer the process holds. When standard input ends, the stdin
thread stops and the subscriptions stay open. A line that is not UTF-8, or one whose brackets do not
parse, aborts the process.

The model is `sea-streamer-stdio`'s own
([the client](https://docs.rs/sea-streamer-stdio/1.0.0-rc.2/sea_streamer_stdio/)). How a
subscription reads its key is on [Lines](../../components/subscriptions/lines.md).
