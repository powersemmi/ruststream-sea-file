---
sidebar_position: 10
title: Errors
description: "SeaFileError is the crate's one error type: connect, subscribe, receive, publish and seek failures, a closed broker, an invalid input."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`SeaFileError`](api:ruststream_sea_file::SeaFileError) is the crate's one error type: connect, subscribe, receive, publish and seek failures, a closed broker, an invalid input.

Each variant names the step that failed, with the file path or the stream key it failed on.
The client's own error is its `source`, so the client's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the stream file could not be opened or created, or standard input and output could not be attached |
| `Subscribe` | a subscription could not be opened |
| `Receive` | an open subscription failed to read |
| `Publish` | the client refused to write a message |
| `Seek` | a subscription could not be moved to a position |
| `NotConnected` | a publisher was used after its broker [shut down](./shutdown.md) |
| `Invalid` | a stream key the client does not take, an empty message on standard output, or a [beacon interval](./beacon-interval.md) that is not a multiple of 1024 |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides what to do with the delivery.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

The stage writes each job's note to standard output as its bytes.
A pipe drops an empty line, so an empty note is refused with `Invalid`; the handler logs the error and finishes the job without the note.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sea_broker_errors.rs#refused"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sea_broker_errors.rs#refused"
```

</TabItem>
</Tabs>
