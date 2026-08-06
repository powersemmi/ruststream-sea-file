# ruststream-sea-file

**`ruststream-sea-file`** is the file and stdio transport for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio). A broker here is either a `.ss` stream
file on disk (durable, replayable, shared between processes) or the process's own standard input
and output.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework. There is no server anywhere
in it, so it is the zero-infrastructure entry point to RustStream, and the reference implementation
of the `Seekable` capability.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-sea-file = "0.6"
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

This site documents the file and stdio transport only. Framework concepts that apply to every
broker (writing subscribers, publishing, routing, codecs, middleware, observability, the CLI) live
in the [RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover
what is specific to this transport and link back to the framework docs where the two meet.
