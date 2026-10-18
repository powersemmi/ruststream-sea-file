---
title: Subscriptions
description: "A stream key in #[subscriber] is a subscription on both brokers; on a stream file it follows the live tail of that key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A stream key in `#[subscriber]` is a subscription on both brokers; on a stream file it follows the live tail of that key.

`#[subscriber("orders")]` reads the stream key `orders`.
A stream file holds many keys side by side, and a line on a pipe names its key in its header, so on both brokers the key is the whole subscription.
On a stream file the subscription opens at the tip and reads every message appended under its key from then on.
What the file held before the service started is read from a [position](../positions/index.md) or in a [replay](./replay.md).
A key is 1 to 249 characters of ASCII letters, digits, `.`, `_` and `-`; a subscription to any other key stops the service at startup with an error that names the key.
On a stream file, the descriptor [`FileStream`](./file-stream.md) opens the same subscription and adds the replay mode and a channel description in the generated document.
On a pipe the key is the only form, and [Lines](./lines.md) shows how a line names it.
The manual form passes the key to [`subscriber(name, body)`](api:ruststream::runtime::subscriber).

## Example

Every order appended under `orders` while the service runs is shipped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/file_subscriptions.rs#bare"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/file_subscriptions.rs#bare"
```

</TabItem>
</Tabs>
