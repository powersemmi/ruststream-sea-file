# File and stdio

`ruststream-sea-file` supplies two transports that need no server: a persistent, replayable stream
file on disk, and the process's own standard input and output. Both implement the same broker
contract as any network broker, so framework concepts (writing subscribers, routing, codecs,
middleware) carry over unchanged - see the
[RustStream documentation](https://powersemmi.github.io/ruststream/) for those.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

The file transport does not build on Windows, an upstream constraint of the file client.

## Capabilities

Which of the framework's optional capability traits this crate implements, and how:

| Capability | Implemented | Notes |
| --- | --- | --- |
| `Subscribe` | Yes | Both connected brokers resolve a string-literal stream key, so `#[subscriber("key")]` works without a descriptor. See [Subscriptions](#subscriptions). |
| `Seekable` + `Positioned` | Yes | `FileSubscriber` mints a `FileSeeker`, and a file delivery reports a `FilePosition`; both reach handlers through the `Position` and `SeekHandle` context keys. This crate is the framework's reference implementation of the capability. `StdioSubscriber` is not seekable: standard input has no retained log. See [Seeking](#seeking). |
| `Partitioned` | No | The transport has no partition or key concept; a stream file is a single ordered log. |
| `BatchSubscriber` | Yes, on the client | Neither client reads several entries at a time, nor offers a count to translate a batch size into, so both subscribers assemble their batches through the framework's own buffer. A mount site names `batch(n)` and gets batches of at most that size either way. See [Batches](#batches). |
| `RequestReply` | No | Neither transport has a reply-address concept, and stdio is one-directional per stream. |
| `TransactionalPublisher` | No | A stream file has no atomic multi-write unit; each publish appends and flushes on its own. |
| `OwnedTransactions` | No | Same reason: there is no transaction to own. |
| `DescribeServer` | Yes | Both brokers report an in-process server spec (`file` with the path, `stdio`), which is what AsyncAPI generation reads. |

Acknowledgement is not a capability trait, and this transport reports it as unsupported: the client
keeps no consumer positions (its resumable mode is unimplemented upstream), so `ack` and `nack`
return `AckError::Unsupported` rather than claiming progress that nothing records. Resume is
explicit instead. See [Acknowledgement](#acknowledgement).

## The two brokers

Each transport has a module of its own holding that transport's types, its `Publish` policy and
its prelude. A service on stream files opens with `use ruststream_sea_file::file::prelude::*;`
and one on a pipeline with `use ruststream_sea_file::stdio::prelude::*;`, and needs nothing else
from either crate. A service that spans both globs `ruststream_sea_file::prelude` and writes
`FilePublish` and `StdioPublish` where the two forms differ.

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

The same descriptor is what the macro-free path passes to the mount constructor -
`subscriber(FileStream::new("orders"), body)` - so both spellings name the subscription the same
way.

On the stdio broker a subscription is a string-literal stream key, resolved through the framework's
`Subscribe` capability: `#[subscriber("jobs")]` consumes the `jobs` key off standard input.

### Replay mode

`FileStream::new(key).replay()` reads the retained file from its start and completes the stream at
the end of the file instead of following live writes. That is batch processing of a recorded log:
the subscription ends on its own once the file is exhausted, which a live subscription never does.
Pair it with a writer that called `end_with_eos()`, so the reader sees the end-of-stream mark.

Replay is the one reading mode the position API cannot express. Everything else about where a
subscription begins is the framework's seek surface.

### Batches

A handler taking `&[T]` consumes a batch:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:batch"
```

and the mount site names how large one may be - the whole of what it has to say about batching:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:mount"
```

Neither client reads several entries at a time - a `sea-streamer` consumer yields one message per
call, off a stream file and off standard input alike, and exposes no count to translate that size
into. So both subscribers assemble their batches on the client, out of the framework's own buffer.
Nothing at the mount site says which of the two it is, and the contract is the same either way: a
batch never carries more than the size it was opened with, and carries fewer whenever that is all
the transport had. The run above records ten readings and settles them in batches of at most four.

A partial batch goes out 10 ms after its first delivery. That deadline belongs to this crate rather
than to the mount site: every transport here is local, and the file client reads a burst greedily
once one starts, so deliveries that already exist land far inside the window. What it bounds is how
long a batch waits at an idle tail.

## Seeking

`FileSubscriber` implements the framework's `Seekable` capability, and this crate is its reference
implementation. Positions are `FilePosition`:

| Position | Meaning |
| --- | --- |
| `FilePosition::beginning()` | Everything retained in the file. |
| `FilePosition::end()` | The tip of the stream. |
| `FilePosition::sequence(n)` | A message sequence, redelivered inclusively. |
| `FilePosition::timestamp(millis)` | The earliest message strictly later than that instant, in milliseconds since the Unix epoch. |

A captured position carries the framework's pinned semantics: seeking to it redelivers exactly that
message, then the rest of the log in order. The sequence rewind is inclusive, which is what makes
that hold.

Where a subscription begins is the `start_at(..)` clause on the decorator, applied before the first
delivery. A handler repositions its own live subscription through the transport's context keys,
which the runtime resolves at compile time:

| Key | Reads | Available on |
| --- | --- | --- |
| `Position` | this delivery's `FilePosition` | `FileContext` |
| `SeekHandle` | the subscription's `FileSeeker` | `FileContext`, `FileBatchContext` |

`FileContext` is the per-delivery context: a handler names it as its context type, or takes the
keys as parameters with the `Ctx` extractor and names nothing at all.

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:seek"
```

`FileBatchContext` is the counterpart for a [batch](#batches). A batch spans many deliveries, so it
carries the seek handle and no position; a batch body names it (`ctx: &mut Context<'_,
FileBatchContext>`) and reads the handle with `ctx.context(SeekHandle)`. Per-delivery positions ride
the elements instead - every delivery carries its sequence in the `stream-sequence` header.

Both context types belong to the file transport's own delivery type, so a handler that reads either
key does not compile against `StdioBroker`: standard input has no retained log, and offers no
repositioning at all.

Deliveries queued from before a seek are discarded, so the next message the handler sees comes from
the new position. See
[Seeking](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking) in the
framework docs for the capability itself.

## Publishing

A publisher is a policy plus the live connection. `FilePublish` pairs into `FilePublisher` and
writes into the stream file; `StdioPublish` pairs into `StdioPublisher` and writes lines to
standard output. Each is its broker's default publish policy, so a
`#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher replies through
it.

A service writes two vocabularies, in two kinds of file. A mount site globs a transport prelude and
names a policy by concept - `Publish`, whichever form it is on - so moving a service between forms
changes the glob and leaves the composition root alone. A handler body globs `ruststream::prelude`
instead and bounds an injected publisher by the framework's capability traits, never by a broker
type. The prefixed `FilePublish` and `StdioPublish` are for the one place the concept name is
ambiguous: a mount site that spans both forms.

The file publisher flushes on every publish: the sink buffers, and live subscribers (and external
tails of the same file) observe the file, not the buffer.

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:publish"
```

The stdio publisher rejects an empty message with `SeaFileError::Invalid`, because the client's
line format silently drops empty lines.

### Per-message arguments

A handler publishes through the framework's builder: `publisher.message(&value).publish()`, with
`.to(key)` where the value's own `#[derive(Outgoing)]` leaves the stream key to the call. This
transport adds no per-message step of its own; the stream key and the payload are all it carries,
and the builder supplies both. Opaque bytes go the same way, as a value whose type declares itself
already serialized (`#[derive(Outgoing, Serialized)] struct Frame(Vec<u8>)`), so no codec runs on
them.

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

Your own handlers are tested with the framework's `TestApp` harness, against the `testing`
feature's `FileTestBroker`. It follows the same ladder as the real brokers, its connected form
implements `ruststream::testing::TestableBroker`, and it routes over a **retained, positioned log** -
the one transport property a stream file's handlers are written against. So a seeking service
mounts on it with no edit at all: `FileStream` resolves here, a delivery reports a `FilePosition`,
the subscription hands out a `FileSeeker`, and `FileContext` and `FileBatchContext` build off its
deliveries the way they build off a file's. It batches the same way too, so a batch handler sees
batches of the size its mount site asked for here as well.

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:handler"
```

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:test"
```

The harness supplies the input, drives the reaction to a standstill, and records what happened, so
a test needs no waiting and no collector of its own. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)
for the assertion surface.

What the in-process broker deliberately leaves alone is everything a file is for: files and
beacons, the end-of-stream mark that completes a replay, the header envelope, timing, and the
`AckError::Unsupported` the real transport reports. Those are covered against real stream files by
this repo's own suite, which needs no server either.
