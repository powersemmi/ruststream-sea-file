<h1 align="center">ruststream-sea-file</h1>

<p align="center">
  <i>The file and stdio transport for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: persistent replayable streams on disk, and services that compose with shell pipelines.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sea-file/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-sea-file` will implement the [RustStream](https://github.com/powersemmi/ruststream) broker contract over [`sea-streamer-file`](https://crates.io/crates/sea-streamer-file) and [`sea-streamer-stdio`](https://crates.io/crates/sea-streamer-stdio). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Status

**Not implemented yet.** This repository is a scaffold: the workspace, CI, and release plumbing are in place, and the crate is an empty stub. The implementation will target the `ruststream` 0.6 line; the design and scope are tracked in [powersemmi/ruststream#193](https://github.com/powersemmi/ruststream/issues/193).

## Planned surface

- `FileBroker`: a persistent, replayable stream on disk that survives restarts and supports record-and-replay workflows.
- `StdioBroker`: standard input and output as one stream, so a service becomes a stage of a shell pipeline.
- Start-position and resumable-mode subscription descriptors; repositioning built on the core `Seekable` capability (powersemmi/ruststream#186).
- A header envelope applied only when headers are present, so files written without headers stay readable as plain payload streams.
- Acknowledgement as position commit for the file transport; reported as unsupported for standard input.

The broker contract (lazy startup, the typed connect/shutdown lifecycle, and the optional capability traits) is defined by [`ruststream`](https://crates.io/crates/ruststream) and verified by `ruststream::conformance`, with the suite run against a real broker before release.

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
