---
sidebar_position: 0
description: "The parts of ruststream-sea-file a service on a stream file or a shell pipeline uses, one page per component."
---

# Components

The parts of `ruststream-sea-file` a service uses, one page per component: the two brokers
([`FileBroker`](api:ruststream_sea_file::file::FileBroker) on a stream file and
[`StdioBroker`](api:ruststream_sea_file::stdio::StdioBroker) on a pipeline), their
subscriptions, their publish policies and the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.
