# ruststream-sea-file

**`ruststream-sea-file`** is the file and stdio transport for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio). A broker here is a log kept in one
`.ss` stream file on disk, or the process's own standard input and output. Neither one needs a
server.

A subscriber on a stream file can rewind the log. That is the framework's `Seekable` capability,
and this crate is its reference implementation. A service on standard input and output is a stage
of a shell pipeline.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

## Where to go next

A subscription is a stream key: `FileStream::new("orders")` on a stream file, the bare name on
standard input. A stream file replays what it recorded and repositions on demand, batches are
assembled on the client, and a delayed redelivery reaches a stream file under the stream key its
subscription reads. Neither transport settles a delivery, so a service resumes from a position it
stored itself. The crate's own reference is on docs.rs:

<div class="grid cards" markdown>

- :material-file-document-outline: **[The stream file](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/file/index.html)** - descriptors, replay, positions and seeking, batches, publishing, delayed redelivery.
- :material-console: **[The pipeline](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/stdio/index.html)** - the line format, where a deferred copy goes, loopback.
- :material-test-tube: **[Testing](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/testing/index.html)** - the in-process stand for each transport.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: installation, the tutorial, the list of brokers.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-sea-file)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This page is the entry point for the file and stdio transports, and everything else about them is
in the [crate's rustdoc](https://docs.rs/ruststream-sea-file). The framework concepts that apply
to every broker (writing subscribers, publishing, routing, codecs, middleware, observability, the
CLI) are in the
[RustStream rustdoc](https://docs.rs/ruststream/latest/ruststream/runtime/index.html), and the
framework's own entry pages are on the
[RustStream site](https://powersemmi.github.io/ruststream/).
