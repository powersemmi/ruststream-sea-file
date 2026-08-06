# File and stdio

`ruststream-sea-file` supplies two transports that need no server: a persistent, replayable stream
file on disk, and the process's own standard input and output. Both implement the same broker
contract as any network broker, so framework concepts (writing subscribers, routing, codecs,
middleware) carry over unchanged - see the
[RustStream documentation](https://powersemmi.github.io/ruststream/) for those.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-sea-file = "0.6"
serde = { version = "1", features = ["derive"] }
```

The file transport does not build on Windows, an upstream constraint of the file client.

## The two brokers

`FileBroker::new(path)` records the path of a `.ss` stream file. `StdioBroker::new()` records
nothing at all. Both are synchronous and do no I/O, so both compose with the `#[ruststream::app]`
builder, and both follow the framework's ladder of consuming transitions:

```text
FileBroker::new(path)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedFileBroker      the open file; subscriptions and publishers
  .shutdown()  ->  ()                       flushed and closed
```

Because `shutdown` consumes the connected broker, publishing or subscribing after it does not
compile. A publisher handed out before the shutdown still aliases the connection and reports
`SeaFileError::NotConnected` once it is gone, rather than succeeding against a closed file.

`FileBroker` carries three settings, all applied on connect:

- `existing_only()` requires the file to exist instead of creating it.
- `end_with_eos()` writes an end-of-stream mark on shutdown, so replay consumers of the finished
  file complete instead of waiting for more data.
- `beacon_interval(bytes)` sets the density of the file's in-place index (a positive multiple of
  1024). Denser beacons make seeking finer-grained at the cost of file size.

Shutting down the stdio broker is globally destructive by the client's design: every stdio consumer
and producer in the process ends, which is what shutting down a process-wide transport means.

## Subscriptions

`FileStream::new(key)` is the subscription descriptor for one stream key in the file. It sits
inline in the `#[subscriber(..)]` decorator, and a plain descriptor follows the live tail:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:handler"
```

Mount it on the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

On the stdio broker a subscription is a string-literal stream key, resolved through the framework's
`Subscribe` capability: `#[subscriber("jobs")]` consumes the `jobs` key off standard input.

### Replay mode

`FileStream::new(key).replay()` reads the retained file from its start and completes the stream at
the end of the file instead of following live writes. That is batch processing of a recorded log:
the subscription ends on its own once the file is exhausted, which a live subscription never does.
Pair it with a writer that called `end_with_eos()`, so the reader sees the end-of-stream mark.

Replay is the one reading mode the position API cannot express. Everything else about where a
subscription begins is the framework's seek surface.

## Seeking

`FileSubscriber` implements the framework's `Seekable` capability, and this crate is its reference
implementation. Positions are `FilePosition`:

| Position | Meaning |
| --- | --- |
| `FilePosition::beginning()` | Everything retained in the file. |
| `FilePosition::end()` | The tip of the stream. |
| `FilePosition::sequence(n)` | A message sequence, redelivered inclusively. |
| `FilePosition::timestamp(millis)` | The earliest message strictly later than that instant, in milliseconds since the Unix epoch. |

A captured position (`Positioned::position` on a delivered message) carries the framework's pinned
semantics: seeking to it redelivers exactly that message, then the rest of the log in order. The
sequence rewind is inclusive, which is what makes that hold.

Where a subscription begins is the `start_at(..)` clause on the decorator, applied before the first
delivery. A handler repositions its own live subscription through the injected `Seek` parameter:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:seek"
```

Deliveries queued from before a seek are discarded, so the next message the handler sees comes from
the new position. See
[Seeking](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking) in the
framework docs for the capability itself.

Standard input has no retained log, so `StdioSubscriber` offers no repositioning at all.

## Publishing

A publisher is a policy plus the live connection. `FilePublish` pairs into `FilePublisher` and
writes into the stream file; `StdioPublish` pairs into `StdioPublisher` and writes lines to
standard output. Each is its broker's default publish policy, so a
`#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher replies through
it.

The file publisher flushes on every publish: the sink buffers, and live subscribers (and external
tails of the same file) observe the file, not the buffer.

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:publish"
```

The stdio publisher rejects an empty message with `SeaFileError::Invalid`, because the client's
line format silently drops empty lines.

## The header envelope

The client's payloads are plain bytes with no header space. User headers therefore travel in an
envelope, applied only when a message actually carries headers:

- A message published without headers is written verbatim. A file recorded that way stays readable
  as a plain payload stream by any `sea-streamer` consumer, and files written by other tools stay
  readable by this crate.
- A message with headers is written as `rs1:` followed by base64 of a length-prefixed header block
  and the payload. The encoding is text-safe because the stdio transport is line-oriented UTF-8.

The stdio publisher additionally envelopes a non-UTF-8 payload even when it carries no headers,
since the line format rejects binary. That is how binary payloads survive a shell pipeline intact.

Every delivery also exposes its sequence number in the `stream-sequence` header
(`SEQUENCE_HEADER`).

## Acknowledgement

The transport keeps no consumer positions, so `ack` and `nack` report `AckError::Unsupported`
instead of pretending to record progress. Resume is explicit: record a captured `FilePosition` and
open the next run with `start_at(..)`, or replay the file from the beginning.

## Stdio pipelines

`StdioBroker` turns the process into a stage of a shell pipeline: stdin is the subscription, stdout
is the publisher, and `producer | service | consumer` works with ordinary command-line tools.

```rust
--8<-- "crates/ruststream-sea-file/examples/stdio_pipeline.rs:pipeline"
```

Lines follow the client's `[timestamp | stream_key | seq] payload` format, so the stream key is
part of the line and one process can carry several keys:

```text
echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' | ./pipeline run
```

`StdioBroker::new().loopback()` wires this process's stdout back into its own stdin, which makes a
stdio service testable in one process with no external commands.

## Testing

Everything in this crate runs locally: the framework's conformance, lifecycle, and seeking suites
plus the replay and stdio integration tests exercise temp files and in-process pipes, with no
external broker to start.

The `testing` feature ships `FileTestBroker`, an in-process transport that reproduces the crate's
core routing with no file at all. It follows the same ladder as the real brokers, and its connected
form implements `ruststream::testing::TestableBroker`, so it drives the `TestApp` harness: inject
traffic with `broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

`FileTestBroker` routes by exact address match and simulates none of the file semantics: replay,
seeking, the end-of-stream mark, and the envelope are covered against real stream files instead.
