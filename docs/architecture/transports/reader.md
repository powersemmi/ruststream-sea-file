---
title: Reader
sidebar_position: 1
description: "The file client follows a stream file like tail -f, separates messages from beacons, and seeks by surveying the beacons and reading forward."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "How the file client reads a stream file, and how its seek finds a position",
  "text": "The diagram as text",
  "file": "The stream file",
  "source": ["Source task", "reads to the end, waits"],
  "messages": ["Message source", "steps over the beacons"],
  "consumer": ["Consumer", "the messages of its key"],
  "subscription": ["The subscription", "one message per next()"],
  "survey": ["Survey the beacons", "narrow the region"],
  "rewind": ["Rewind", "to that beacon"],
  "scan": ["Read forward", "to the target"],
  "replay": ["A replay: the crate reads the message source itself,", "so the end comes after the last message"],
  "bytes": "bytes",
  "next": "next()",
  "seek": "seek"
};

# Reader

The file client follows a stream file like `tail -f`, separates messages from beacons, and seeks by
surveying the beacons and reading forward.

<Diagram name="reader" words={words} />

A task reads the file to its end, then waits for the operating system to report a change. The OS
may report late, so the wait also times out, after 1 millisecond at first and up to 1024 as the
file stays quiet. A burst of writes resets the wait. One watcher serves each file in a process,
however many readers it has. A file removed from the disk ends the stream.

Above that task, a message source reassembles the frames, steps over the beacons and joins a
message a beacon split. The consumer keeps the messages of its stream keys and hands them out one
per call.

A live [subscription](../../components/subscriptions/file-stream.md) is that consumer, opened in
real-time mode and positioned at the tip. It waits for the next write instead of ending at the end
of the file.

A [replay](../../components/subscriptions/replay.md) reads the message source directly and skips
the other keys. The client's own replay reads ahead and reports the end while that read-ahead is
still undelivered, so the crate does not use it.

A seek to the beginning or the end moves straight there. A seek to a sequence number or an instant
surveys the beacons for the region that holds it, rewinds to that beacon, and reads forward to the
first message with that number or later, or strictly after that instant. Reading past the end of
the file refuses the seek, and the source goes back to where it was. A seek cannot be interrupted
halfway, so the crate runs it on a task of its own, as [Seeking](../seeking.md) draws.

Nothing in the file records how far a reader got, so a service that restarts resumes from a
[position it stored itself](../../components/positions/start-at.md).
