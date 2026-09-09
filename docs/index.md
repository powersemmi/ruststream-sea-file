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

<div class="grid cards" markdown>

- :material-file-document-outline: **[File and stdio guide](file.md)** - stream files, replay, seeking, headers, pipelines, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-sea-file)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the file and stdio transport only. The framework concepts that apply to every
broker (writing subscribers, publishing, routing, codecs, middleware, observability, the CLI) are
in the [RustStream documentation](https://powersemmi.github.io/ruststream/).
