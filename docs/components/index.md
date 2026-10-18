---
sidebar_position: 0
description: "The parts of ruststream-sea-file a service on a stream file or a shell pipeline uses, one page per component."
---

# Components

The parts of `ruststream-sea-file` a service uses, one page per component: the two
[brokers](./broker/index.md) ([`FileBroker`](./broker/file-broker.md) on a stream file and
[`StdioBroker`](./broker/stdio-broker.md) on a pipeline) and their settings, their
[subscriptions](./subscriptions/index.md), [positions](./positions/index.md) and seeking, their
[publish policies](./publishing/index.md), [acknowledgement and retries](./settlement/index.md), the
[headers](./headers/index.md) a message carries, the [AsyncAPI](./asyncapi/index.md) document, and
the in-process transport a [`TestApp`](./testing/index.md) test runs on.
