---
sidebar_position: 1
title: start_at
description: "start_at opens a subscription at a position on every startup; a stored sequence is how a service resumes on a stream file."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

`start_at` opens a subscription at a [position](./index.md) on every startup; a stored sequence is how a service resumes on a stream file.

It is a clause of the attribute, `start_at(FilePosition::beginning())`, or [`.start_at(position)`](api:ruststream::runtime::SubscriberSettings::start_at) among the mount-site settings.
The mount-site form takes a position the service learns only when it starts, such as a stored checkpoint.
A subscription with no `start_at` opens at the tip.
The file keeps no record of how far a service read, because `ack` and `nack` report `Unsupported` on this transport ([Acknowledgement](../settlement/index.md)).
So a service that resumes where it stopped stores the sequence it reached, read with [`Ctx<Position>`](./position.md), and opens the next run at the first sequence it did not handle.
`FilePosition::sequence(n)` delivers message `n` itself, so the stored sequence of the last handled message is the one to add one to.
The sequence the file writes next, one after its last message, opens the subscription at the tip, so a service that handled every message resumes there.
A sequence further ahead stops the service at startup.
The position applies on every startup: a subscription that starts at the beginning handles the whole retained file each time the service starts.

## Example

The previous run invoiced the orders up to sequence 3, so this run opens at sequence 4.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_start_at.rs#resume"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_start_at.rs#resume"
```

</TabItem>
</Tabs>
