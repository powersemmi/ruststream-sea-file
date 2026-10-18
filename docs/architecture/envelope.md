---
title: Envelope
sidebar_position: 7
description: "Headers travel inside the payload: a message that needs them is written as rs1: or rs2: and base64, and one that does not is written as it is."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "How a publish writes a message's headers and payload into the bytes the client carries",
  "text": "The diagram as text",
  "message": ["A message to publish", "headers and a payload"],
  "check": ["Does it have headers, start with rs1: or rs2:,", "or, on a pipe, a payload a line would change?"],
  "plain": ["The payload is", "written as it is"],
  "lines": ["Does every header", "survive a text line?"],
  "rs1": ["rs1: base64(", "u32 length,", "name: value lines,", "payload)"],
  "rs2": ["rs2: base64(", "u32 length,", "length, name,", "length, value ..,", "payload)"],
  "decode": ["A reader opens either envelope; anything else", "reads as a payload without headers"],
  "yes": "yes",
  "no": "no"
};

# Envelope

Headers travel inside the payload: a message that needs them is written as `rs1:` or `rs2:` and
base64, and one that does not is written as it is.

<Diagram name="envelope" words={words} />

Neither client has room for headers: a message is a stream key and a payload. So the publisher
writes the headers into the payload, inside an envelope, and the subscription opens it again.

A message without headers is written as it is. A file written that way stays a plain stream of
payloads that any `sea-streamer` reader understands. A payload that begins with `rs1:` or `rs2:` is
enveloped anyway, or a reader would take it for an envelope.

A pipe carries UTF-8 text, one line per message, and the reader at the other end trims each line.
So on a pipe a payload that is not UTF-8, holds a line break, or begins or ends with whitespace is
enveloped too, and arrives whole.

Both envelopes are a prefix and base64, so either fits on a line. Inside, a big-endian length of
the header block comes first, then the block, then the payload. `rs1:` writes each header as a
`name: value` line, and earlier releases of this crate read it. It is used when every header reads
back unchanged from such a line. `rs2:` writes each name and value with its own length, so it
carries any bytes, and it is used for the rest.

A reader opens either envelope and adds the [`stream-sequence`](../components/headers/sequence-header.md) header. Anything that is not a
well-formed envelope reads as a payload with no headers. How a service reads and sets headers is on
[Envelope](../components/headers/index.md) in the components.
