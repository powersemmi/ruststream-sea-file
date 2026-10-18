---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a real stream file in a temp directory; no server is needed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a real stream file in a temp directory; no server is needed.

It connects the broker through its ordinary connect, so the file is created, written and read by the file client.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it, ten seconds at most.
[`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.
The file is the whole stand, so a live test runs on any machine, in the same `cargo test` as the rest.
Each run takes a path of its own, so nothing an earlier run appended is read, and removes the file at the end.

The live mode is for what the log in memory leaves out: the bytes of a `.ss` file, what a file keeps across two runs of a service, and a file that [`existing_only`](../broker/existing-only.md) refuses.
Two runs on one path show a service resuming at a [stored position](../positions/start-at.md), or reading again what it [acknowledged](../settlement/index.md).
A stdio stage is tested live as a child process with piped standard input and output, because a stdio broker in a test process would take that process's own pipes.

## Example

The two tests share one body: one in process, one on a stream file of the run's own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_testing_live.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_testing_live.rs#live"
```

</TabItem>
</Tabs>
