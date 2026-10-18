---
sidebar_position: 4
title: Dead letters
description: "dead_letter names the stream key a spent delivery is written to, with its payload, its headers and its delivery count."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

`dead_letter` names the stream key a spent delivery is written to, with its payload, its headers and its delivery count.

[`dead_letter(key)`](api:ruststream::runtime::Mounting::dead_letter) goes beside [`max_attempts(n)`](./retries.md).
The delivery that reaches the cap is published under that key instead of coming back.
It keeps its payload and the headers it arrived with, and its retry count says how many deliveries the message got.
On a stream file the key is one more stream inside the same file.
On a pipe it is a line under that key, which a downstream stage reads.
A service reads the dead letters with a subscription of their own, or a [replay](../subscriptions/replay.md) when an operator inspects them.

## Example

A label that fails three times is written under `labels.dead`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_dead_letter.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_dead_letter.rs#handler"
```

</TabItem>
</Tabs>
