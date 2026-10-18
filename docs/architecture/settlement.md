---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on a stream file and a pipe, which keep no consumer positions and acknowledge nothing."
---

import {Diagram} from '@site/src/components/Diagrams';

export const outcomes = {
  "title": "A handler's outcome on a stream file or a pipe: only a copy the runtime publishes brings a message back",
  "text": "The diagram as text",
  "outcome": ["The handler's outcome"],
  "ack": ["ack, drop:", "reported", "unsupported,", "logged"],
  "retry": ["retry():", "the requeue is", "unsupported,", "logged"],
  "later": ["retry_after(d):", "the runtime", "publishes", "a copy"],
  "resume": ["Nothing records how far a service read:", "a restart resumes from a position it stored"]
};

export const retries = {
  "title": "A retry on a stream file or a pipe: the attempt cap decides between a copy, the dead-letter key and nothing",
  "text": "The diagram as text",
  "outcome": ["retry_after(d), or retry() under a declared cap", "the retry-count header says which attempt"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "copy": ["Below the cap:", "a copy, after", "any delay"],
  "deadLetter": ["At the cap:", "a copy to the", "dead-letter", "key"],
  "dropped": ["At the cap,", "no dead letter:", "nothing is", "written"],
  "file": ["File: the same", "stream key, read", "by the subscription"],
  "pipe": ["Pipe: the key", "out_retry names,", "for the next stage"],
  "crash": ["The delayed copy is held in the process:", "a crash before the timer fires loses it"],
  "retries": "a retry",
  "below": "below",
  "atCap": "at the cap",
  "onFile": "file",
  "onPipe": "pipe"
};

# Settlement

What `ack`, `drop`, `retry` and [`retry_after`](../components/settlement/retry-after.md) become on
a stream file and a pipe, which keep no consumer positions and acknowledge nothing.

<Diagram name="outcomes" words={outcomes} />

Neither client records what a reader has handled. A delivery's
[`ack` and `nack`](../components/settlement/index.md) therefore report `AckError::Unsupported` at
once, and the runtime logs it. An acknowledged message is not marked anywhere, and a requeued one
does not come back. A service that restarts resumes from a
[position it stored itself](../components/positions/start-at.md).

A retry is the one outcome that brings a message back, and the runtime does all of it.

## Retries

<Diagram name="retries" words={retries} />

A `retry_after` outcome, and a `retry()` on a registration that declares
[`max_attempts`](../components/settlement/retries.md) or a
[dead-letter key](../components/settlement/dead-letters.md), take this path. Neither transport
counts its deliveries, so the attempt travels in the copy's retry-count header, and the first
delivery counts as one. The cap is checked first. At the cap, a declared dead letter gets a copy,
and without one the delivery is rejected and nothing is written.

Below the cap, the runtime drops the delivery and publishes a copy, after the delay of a
`retry_after`. On a stream file, a stream key is both ends: the subscription reports the key it
reads, and the copy is appended under it, so the mount site names nothing. A replay reads the copy
only while it is still short of the end of the file.

On a pipe, the copy leaves on standard output for the next stage, so the mount site names its key
with [`out_retry`](../components/settlement/out-retry.md). A registration on a pipe that names none
refuses to start.
