---
title: Stream file
sidebar_position: 0
description: "A .ss stream file is one log on disk for every stream key: a header, framed messages, a beacon at every interval, and an optional end-of-stream mark."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A stream file on disk, region by region, and one of its messages taken apart into its fields",
  "text": "The diagram as text",
  "file": "orders.ss",
  "header": ["Header, 128 bytes", "beacon interval"],
  "orders1": ["Message", "orders, number 1"],
  "receipts1": ["Message", "receipts, number 1"],
  "orders2": ["Message", "orders, number 2"],
  "beacon": ["Beacon", "every interval"],
  "continued": ["orders 2,", "continued"],
  "eos": ["End-of-stream mark", "when set"],
  "message": "One message",
  "key": ["Stream key", "its length, its name"],
  "meta": ["Shard, sequence,", "time in milliseconds"],
  "size": "Payload size",
  "payload": "Payload",
  "checksum": "Checksum",
  "end": "0x0D",
  "keys": ["Every stream key shares the file, each numbered", "from one, all on shard 0"]
};

# Stream file

A `.ss` stream file is one log on disk for every stream key: a header, framed messages, a beacon at
every interval, and an optional end-of-stream mark.

<Diagram name="streamFile" words={words} />

The file opens with a 128-byte header. It holds the bytes `Ss`, a format version, the file's
name, the time it was created and the beacon interval.

Messages follow in the order they were written, whatever their stream key. Each one carries its
key, a shard, a sequence number and a timestamp in milliseconds, then the payload and a checksum.
Numbers are big-endian, and a `0x0D` byte closes each frame, so a file with text payloads stays
readable in an editor.

A stream key is numbered from one. A later connection that appends to the same file carries the
numbering on. This client writes every message to shard zero.

A beacon sits at every multiple of the [beacon interval](../../components/broker/beacon-interval.md),
1 MiB by default. It records, for each stream key, the last message header and a running checksum,
so it works as an index inside the file. A reader that lands in the middle of the file aligns on
the next beacon, and a seek narrows its search by them. A message that crosses the interval is
split, and the beacon sits between its two parts.

[`end_with_eos`](../../components/broker/end-with-eos.md) writes an end-of-stream message at
shutdown, under the client's internal key with the payload `EOS`. A live subscription ends there,
and a replay completes there.

A file that holds nothing yet gets one message under the key `ruststream-internal` when the broker
connects. The client cannot open a live consumer on a file with no content, and no subscription
reads that key.

A stream key is 1 to 249 characters: ASCII letters and digits, `.`, `_` and `-`. The client stops
writing a file at 16 GiB: a publish that reaches the cap fails, and every later publish to that
file fails too.

The format is `sea-streamer-file`'s own
([the file format](https://github.com/SeaQL/sea-streamer/blob/main/sea-streamer-file/src/format.rs),
[the client](https://docs.rs/sea-streamer-file/1.0.0-rc.2/sea_streamer_file/)). How the client
reads a file is on [Reader](reader.md), and how it writes one on [Writer](writer.md).
