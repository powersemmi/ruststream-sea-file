---
title: Writer
sidebar_position: 2
description: "One writer task per stream file frames each message and places the beacons; every publish is flushed, and the file is synced at shutdown."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "How a publish reaches a stream file, and when the file is flushed and synced",
  "text": "The diagram as text",
  "publisher": ["FilePublisher", "envelope, then send_to"],
  "producer": ["The client's producer", "queues the message"],
  "task": ["Writer task", "one per file, per process"],
  "sink": ["Message sink", "frames it, places beacons"],
  "file": ["The stream file"],
  "flush": ["flush() after", "every publish:", "to the OS"],
  "sync": ["sync_all() once,", "at shutdown:", "to the device"],
  "quota": ["The client stops a file at 16 GiB:", "that publish and every later one fail"]
};

# Writer

One writer task per stream file frames each message and places the beacons; the crate flushes
after every publish and the file is synced to the device at shutdown.

<Diagram name="writer" words={words} />

The [`FilePublisher`](../../components/publishing/file-publish.md) hands the stream key and the
payload to the client's producer. The producer queues it for the file's writer task, which numbers
the message within its key, frames it and inserts a beacon wherever the next interval falls.

The bytes collect in a buffer until something asks for them. The crate asks after every publish:
`publish` returns once a flush has handed the message to the operating system. A live reader of
the same file, in this process or another one, sees the message from then on.

The file is synced to the device once, when the producer ends. The crate ends it at
[shutdown](../../components/broker/shutdown.md), after the end-of-stream mark when one is set.
A message published since the last sync survives a crash of the process, not a power loss.

Every producer of one file in a process shares one writer task, which counts the producer handles.
Opening a producer on a file that already holds messages scans back to the last beacon and forward
to the end, to learn where each key's numbering stands. That scan grows with the file.

The client stops a file at 16 GiB unless told otherwise, and this crate leaves the limit at that
default. A publish that reaches the cap fails, and every later publish to that file fails too.
