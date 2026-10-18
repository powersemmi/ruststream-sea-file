---
sidebar_position: 2
title: existing_only
description: "existing_only makes startup fail when the stream file is missing, for a service that reads a file another process writes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# existing_only

[`existing_only`](api:ruststream_sea_file::file::FileBroker::existing_only) makes startup fail when the stream file is missing, for a service that reads a file another process writes.

Without the setting, startup creates a missing file and the service reads an empty log.
For a reader that is the wrong answer: a mistyped path, or a writer that has not run yet, leaves it with nothing to read and no error.
With the setting, startup fails with [`SeaFileError::Connect`](api:ruststream_sea_file::SeaFileError::Connect), which names the path, and no file is created.
A file that is there opens as usual.

In a [`TestApp`](api:ruststream::testing::TestApp) test the stream file is a log in memory, and it counts as the file the operator provisioned.
So the refusal shows only against a real file, with `TestApp::start_live`.

## Example

The auditor reads the orders the order service writes to its file.
The test starts the auditor on a missing path, and startup fails; once the order service has created the file, the auditor starts and reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_broker_existing_only.rs#reader"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_broker_existing_only.rs#reader"
```

</TabItem>
</Tabs>
