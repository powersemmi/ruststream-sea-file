---
title: Pipe
sidebar_position: 3
description: "The crate reads standard input on one thread and the stdio client writes standard output on another, one line per message, for the whole process."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "How a stage takes lines in from the stage before and sends lines out to the stage after",
  "text": "The diagram as text",
  "upstream": "The stage before",
  "stdin": ["Reader thread", "reads and parses a line"],
  "dispatch": ["Dispatch by stream key:", "a line with no key", "goes to every subscription"],
  "subscriptions": ["StdioSubscriber", "one queue each"],
  "publisher": ["StdioPublisher", "envelope if needed"],
  "stdout": ["stdout thread", "numbers, stamps"],
  "line": ["[time | key | seq]", "payload"],
  "downstream": "The stage after",
  "process": ["One reader and one stdout thread per process:", "a shutdown ends every stdio stream in it"],
  "lines": "lines"
};

# Pipe

The crate reads standard input on one thread and the stdio client writes standard output on
another, one line per message, for the whole process.

<Diagram name="pipe" words={words} />

A line is `[timestamp | stream_key | sequence | shard_id] payload`, and every field in the
brackets is optional. The timestamp reads `YYYY-MM-DDTHH:MM:SS`, with fractions of a second
allowed. A line that starts with `[WORD]` is read as the stream key `WORD`.

The first subscription starts the reader thread. It reads a line, parses its fields as the
client's format defines them and queues it for the subscriptions of its key. A line with no key
goes to every subscription, so a stray line on standard output of the stage before reaches every
subscription of this one. The thread starts after the first subscription exists, so a line
already waiting when the stage starts is not lost. A line that is not UTF-8, or one whose brackets
are not a header, is passed over with a warning, and the thread reads on.

When standard input ends, the thread ends every subscription after the lines it has already
queued, and a subscription opened later ends at once. The stage then exits the way
[Shell pipelines](../../guides/pipelines.md#when-the-input-ends) shows.

The first publish starts the stdout thread. It numbers each key from zero, stamps the time of
writing to the second, and prints the line. It skips an empty payload, so the
[publisher](../../components/publishing/stdio-publish.md) refuses an empty message instead of
reporting a line that never left. A payload a line would change goes inside the
[envelope](../envelope.md).

The dashed arrow is [`loopback`](../../components/broker/loopback.md): each line the process
writes is also queued for its own subscriptions, under the header the stdout thread gave it. It
runs a stdio service in one process, as a test does. A subscription under `loopback` reads the
process's own output, so the end of standard input leaves it open.

Both threads belong to the process, not to a broker. A [shutdown](../../components/broker/shutdown.md)
ends every stdio subscription and producer the process holds, after each subscription has
delivered the lines it already read.

The line format and the stdout thread are `sea-streamer-stdio`'s own
([the client](https://docs.rs/sea-streamer-stdio/1.0.0-rc.2/sea_streamer_stdio/)). How a
subscription reads its key is on [Lines](../../components/subscriptions/lines.md).
