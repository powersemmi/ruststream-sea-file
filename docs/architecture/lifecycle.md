---
title: Lifecycle
sidebar_position: 3
description: "FileBroker::new records a path, connect opens the file, shutdown ends its writer; StdioBroker attaches the process's pipes on first use."
---

import {Diagram} from '@site/src/components/Diagrams';

export const file = {
  "title": "The file broker's three states as types, and what each transition does to the stream file",
  "text": "The diagram as text",
  "states": "Broker",
  "transport": "On the file",
  "unconnected": ["FileBroker::new", "(path)"],
  "connected": ["ConnectedFileBroker"],
  "closed": ["Closed = ()", "nothing to report"],
  "config": ["Records the path", "and settings, no I/O"],
  "open": ["Opens the file or", "creates it; primes", "a fresh one"],
  "subscriptions": ["Each subscription:", "a consumer, or a", "replay reader"],
  "publishes": ["Each publish:", "append and flush"],
  "shutdown": ["Waits for publishes,", "writes the end mark", "if set, flushes,", "syncs to the device"],
  "afterShutdown": ["A publisher kept from before answers NotConnected,", "and a seek through an old seeker is refused"],
  "connect": "connect(self)",
  "close": "shutdown(self)"
};

export const stdio = {
  "title": "The stdio broker's three states as types, and what each transition does to the process's pipes",
  "text": "The diagram as text",
  "states": "Broker",
  "transport": "On the pipes",
  "unconnected": ["StdioBroker::new()"],
  "connected": ["ConnectedStdioBroker"],
  "closed": ["Closed = ()", "nothing to report"],
  "config": ["Records loopback,", "touches no pipe"],
  "reading": ["First subscription:", "the stdin thread"],
  "writing": ["First publish:", "the stdout thread"],
  "shutdown": ["Ends every stdio", "stream of the process"],
  "afterShutdown": ["A publisher kept from before answers NotConnected"],
  "connect": "connect(self)",
  "close": "shutdown(self)"
};

# Lifecycle

`FileBroker::new` records a path, connect opens the file, each subscription opens a reader, and
shutdown ends the writer; `StdioBroker` attaches the process's pipes on first use.

<Diagram name="lifecycle" words={file} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker when
the app starts. Connect creates the file when it is missing, or fails under
[`existing_only`](../components/broker/existing-only.md). It checks the beacon interval, opens the
client's streamer and one producer, and writes the priming message into a file that holds nothing
yet.

Each subscription opens its reader on the runtime `connect` ran on: a consumer at the tip for a
live subscription, or a reader of the whole file for a replay. A publish appends and flushes, as
[Writer](transports/writer.md) draws.

Shutdown marks the connection closed first, so a publish that starts after it fails. It then waits
for the publishes in flight and takes the producer out. The client ends the file's writer: it
writes the [end-of-stream mark](../components/broker/end-with-eos.md) when it is set, flushes, and
syncs the file to the device. The crate drops its producer only after that and asks the client
once more. A handle dropped late would otherwise end the writer of the next connection to the same
file.

After shutdown, a [publisher](../components/broker/shutdown.md) kept from before returns
`NotConnected`, and a seek through a seeker kept from before is refused. Neither reports a success
against the closed file.

## The stdio broker

<Diagram name="stdioLifecycle" words={stdio} />

Connect records whether the broker loops back and touches no pipe. The first subscription in the
process starts the client's stdin thread, and the first publish its stdout thread, as
[Pipe](transports/pipe.md) draws. Both threads serve the whole process, so a shutdown ends every
stdio consumer and producer in it, those of other brokers included.
