---
sidebar_position: 4
title: beacon_interval
description: "beacon_interval sets how far apart the file's beacons sit; denser beacons seek in finer steps and make the file larger."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# beacon_interval

[`beacon_interval`](api:ruststream_sea_file::file::FileBroker::beacon_interval) sets how far apart the file's beacons sit; denser beacons seek in finer steps and make the file larger.

A beacon is an index entry the file client writes into the stream file at a fixed interval of bytes.
Beacons are the file's index: a seek finds its place in the file by them, so closer beacons make seeking finer-grained.
Each beacon takes room in the file.
The client's default is one beacon every 1 MiB.

The interval is a number of bytes, a positive multiple of 1024.
Any other value fails startup with [`SeaFileError::Invalid`](./errors.md), in a [`TestApp`](../testing/index.md) test as on a real file.
The interval is written into the header of a file the broker creates.

## Example

The broker writes a beacon every 64 KiB.
The test reads the interval back out of the header of the file it created.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_broker_beacons.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_broker_beacons.rs#broker"
```

</TabItem>
</Tabs>
