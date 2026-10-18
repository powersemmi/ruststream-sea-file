---
slug: /
sidebar_position: 0
description: "Guides to building services on a stream file or a shell pipeline with ruststream-sea-file, the file and stdio transport for RustStream."
---

# Guides

`ruststream-sea-file` is the file and stdio transport for
[RustStream](https://powersemmi.github.io/ruststream/latest/), built on
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio). It has two transports. A stream file
is a log kept in one `.ss` file on disk: it survives a restart, and a subscriber can replay it or
move back to an earlier position. Standard input and output make a service a stage of a shell
pipeline. Neither one needs a server.
