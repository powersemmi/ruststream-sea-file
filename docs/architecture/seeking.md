---
title: Seeking
sidebar_position: 5
description: "A handler moves its own file subscription through the seek handle: the driver task runs the client's seek, and what was queued from the old position is dropped."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A seek from a handler: the command, the client's seek, and the deliveries it discards",
  "text": "The diagram as text",
  "handler": ["Handler", "seeker.seek(position)"],
  "command": ["Command to the driver task", "outside any select!"],
  "map": ["FilePosition to the client's target", "a sequence inclusive, an instant in ns"],
  "clientSeek": ["The client's seek", "runs to completion"],
  "moved": ["Moved:", "a new generation"],
  "refused": ["Refused: a live", "subscription ends,", "a replay stays"],
  "dropped": ["Queued deliveries", "of the old generation", "are dropped"],
  "next": ["The next delivery", "from the new position"],
  "backlog": ["A full channel does not hold a seek back: the driver", "serves seeks while it waits for room"],
  "sends": "sends",
  "ok": "moved",
  "error": "refused"
};

# Seeking

A handler moves its own file subscription through the [seek handle](../components/positions/seek.md):
the driver task runs the client's seek, and what was queued from the old position is dropped.

<Diagram name="seeking" words={words} />

The client's seek needs the consumer to itself and cannot be interrupted halfway. So the
subscription's driver task owns the consumer, and a seek reaches it as a command. The driver runs
the seek to completion, outside any race with the next read, and answers the handler when it is
done.

A [`FilePosition`](../components/positions/index.md) becomes the client's target. The beginning and
the tip map directly. A sequence number is inclusive, so seeking to a position read off a delivery
delivers that message again. An instant in milliseconds becomes nanoseconds, and a value out of
range is refused before the client sees it.

A seek that moved the reader opens a new generation, and the driver publishes it before it answers.
The subscription drops every delivery stamped with an older one, so once `seek` returns, nothing
read at the old position reaches the handler.

A refused seek, such as a sequence past the last one written, leaves a replay where it was, with
what it had read still queued. A live subscription ends instead: the client reads forward looking
for the message and runs out of file. A replay that reached the end of the file can still seek,
and it delivers again from the new position.

Seeks are served while the channel is full. The handler that stopped draining the channel may be
the one waiting on a seek, so a backlog larger than the channel never blocks it. After shutdown,
every seek is refused.

The [in-process transport](in-process.md) moves a subscription the same way, with the same
refusals, inside the subscription's own poll.
