---
sidebar_position: 3
title: end_with_eos
description: "end_with_eos writes an end-of-stream mark at shutdown; a live subscription ends on it and a replay completes at it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# end_with_eos

[`end_with_eos`](api:ruststream_sea_file::file::FileBroker::end_with_eos) writes an end-of-stream mark at shutdown; a live subscription ends on it and a replay completes at it.

A live subscription follows the file and waits for the next write.
The mark is the one thing that ends it: a subscription that reads up to the mark delivers every message before it and then ends.
A replay reads the file once and completes at the mark, or at the end of the file when there is none.
A subscription that opens at the tip of a finished file starts after the mark, so it reads what is appended later.

The mark is how a recorder tells its readers that a log is complete.
After it, a seek on a live subscription is refused.

## Example

The recorder marks its file finished when it shuts down.
In the test an auditor reads the receipts from the start of that file, ends at the mark, and never reads a receipt appended after it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_broker_eos.rs#writer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_broker_eos.rs#writer"
```

</TabItem>
</Tabs>
