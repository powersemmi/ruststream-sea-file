---
title: In process
sidebar_position: 8
description: "Under the testing feature a connected broker can carry a stream file or a pipe held in memory, which the test harness drives with the production app unchanged."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One connected broker, two transports: the client through connect, a file or pipe in memory through connect_in_process",
  "text": "The diagram as text",
  "connected": ["ConnectedFileBroker", "ConnectedStdioBroker"],
  "live": ["The client: a .ss", "file, or the", "process's pipes"],
  "memory": ["A file or a pipe", "held in memory"],
  "coordinator": ["The harness counts", "deliveries in flight", "for settle()"],
  "liveOnly": ["Live only: the bytes", "of a .ss file, beacons,", "a restart, two brokers", "on one file, lines with", "no key, a shutdown", "of the whole process"],
  "emulated": ["In memory: a log per", "key from one, the tip,", "replay, seeks and their", "refusals, the end mark,", "loopback, the envelope"],
  "production": ["Without the testing feature, a link is the client", "alone: the same size, and no second branch"],
  "connect": "connect",
  "inProcess": "connect_in_process"
};

# In process

Under the `testing` feature a connected broker can carry a stream file or a pipe held in memory,
which the [test harness](../components/testing/index.md) drives with the production app unchanged.

<Diagram name="inProcess" words={words} />

`TestApp::start` connects each broker through `connect_in_process` instead of `connect`. The
connected broker, its subscriptions, its publishers and its deliveries are the same types, each
with the in-memory transport as a variant of its own. The service's descriptors, publish policies
and seeking handlers therefore run unchanged.

The in-memory transport reads the production broker's settings and nothing of its own. A message
crosses it through the same steps a real one goes through: the stream key check, the envelope, and
the refusal of an empty line on a pipe. So it refuses what a stream file or a pipe refuses.

The in-memory file keeps a log per stream key, numbered from one. A live subscription starts at the
tip, a replay reads the log and completes at its end, and a seek resolves against the log with the
file's refusals. Every clone of a broker shares one file, and a new broker starts with an empty
one. The in-memory pipe treats what the test injects as lines on standard input, and records what
the service writes. A line comes back to the service's own subscriptions only under `loopback`.

The harness counts the deliveries in flight on each transport, so `tb.settle()` returns once every
subscription has handled what reached it. What belongs to the real transport is tested
[live](../components/testing/live.md), on a stream file in a temporary directory or with a stage
started as a child process.

Without the `testing` feature the link has one variant. The compiler checks that it is exactly the
size of the client handles it wraps, so a production build carries no second transport and no
branch to one.
