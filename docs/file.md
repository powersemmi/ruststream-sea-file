# File and stdio

`ruststream-sea-file` runs a RustStream service with no server anywhere: on a persistent,
replayable stream file on disk, or on the process's own standard input and output. Subscribers,
routing, codecs and middleware come from the framework, and the
[RustStream documentation](https://powersemmi.github.io/ruststream/) covers them; this page is about
the two transports.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

The file transport does not build on Windows: the file client underneath it is Unix-only.

## Capabilities

Which of the framework's optional capability traits this crate implements, and what each gives you:

| Capability | Implemented | Notes |
| --- | --- | --- |
| `Subscribe` | Yes | You name a stream key as a string literal on either transport, and `#[subscriber("key")]` needs no descriptor. See [Subscriptions](#subscriptions). |
| `Seekable` + `Positioned` | `Positioned` on both, `Seekable` on the file | Every delivery reports the sequence it sits at. A handler on a stream file also moves its own subscription, through the `Position` and `SeekHandle` context keys, and this crate is the framework's reference implementation of that capability. A stdio subscription does not move: standard input keeps no log to move within. See [Seeking](#seeking). |
| `Partitioned` | No | A stream file is one ordered log with no shards. |
| `BatchSubscriber` | Yes, on the client | You name `batch(n)` at the mount site and get batches of at most `n` on either transport. See [Batches](#batches). |
| `RequestReply` | No | Neither transport has a reply address. |
| `TransactionalPublisher` | No | A stream file has no atomic multi-write unit: each publish appends and flushes on its own. |
| `OwnedTransactions` | No | Same reason: there is no transaction to own. |
| `DescribeServer` | Yes | The generated AsyncAPI document names an in-process server: `file` with the path, or `stdio`. |

`ack` and `nack` return `AckError::Unsupported` on both transports: the client records no consumer
positions, and its resumable mode is unimplemented upstream. See
[Acknowledgement](#acknowledgement).

## The two brokers

Each transport has a module of its own with that transport's types, its `Publish` policy and its
prelude. A service on stream files opens with `use ruststream_sea_file::file::prelude::*;` and one
on a pipeline with `use ruststream_sea_file::stdio::prelude::*;`, and imports nothing else from
this crate.

A service that spans both globs `ruststream_sea_file::prelude` and writes the prefixed
`FilePublish` and `StdioPublish`, since the two forms would claim the same policy name.

`FileBroker::new(path)` records the path of a `.ss` stream file. `StdioBroker::new()` records
nothing at all. Opening and closing happen in the transitions that follow:

```text
FileBroker::new(path)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedFileBroker      the open file; subscriptions and publishers
  .shutdown()  ->  ()                       flushed and closed
```

A publisher handed out before the shutdown shares the broker's connection: once the broker is
closed it returns `SeaFileError::NotConnected`, never a silent success.

`FileBroker` takes three optional settings, all applied when it connects:

- `existing_only()` requires the file to exist instead of creating it.
- `end_with_eos()` writes an end-of-stream mark on shutdown, so a replay consumer of the finished
  file completes instead of waiting for more data.
- `beacon_interval(bytes)` sets how far apart the file's beacons sit, in bytes; the value must be a
  positive multiple of 1024. A beacon summarises the streams written before it and is what makes
  the file seekable, so denser beacons make seeking finer-grained and the file larger.

Shutting down the stdio broker ends every stdio consumer and producer in the process, not only the
ones this broker opened.

## Subscriptions

`FileStream::new(key)` is the subscription descriptor for one stream key in the file. It sits
inline in the `#[subscriber(..)]` attribute, and a plain descriptor follows the live tail:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:handler"
```

Mount it on the broker:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

The manual path takes the same descriptor: `subscriber(FileStream::new("orders"), body)`.

On the stdio broker a subscription is the stream key itself: `#[subscriber("jobs")]` consumes the
`jobs` key off standard input.

### Replay mode

`FileStream::new(key).replay()` reads the file from its start and ends the subscription at the end
of the file, instead of following live writes. That is how you process a recorded log in one pass.
A file written with `end_with_eos()` ends with the mark a replay stops on.

Replay is the one reading mode a position cannot express.

### Batches

A handler taking `&[T]` consumes a batch:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:batch"
```

and the mount site names how large one may be:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:mount"
```

Both clients read one entry at a time, so a batch is assembled on the client side. A batch never
holds more than the size the mount site named, and holds fewer whenever that is all the transport
had.

A partial batch goes out 10 ms after its first delivery. The deadline is fixed here: a mount site
cannot change it. What it bounds is how long a batch waits at an idle tail.

## Seeking

A subscription on a stream file moves to any position in the retained log. A position is a
`FilePosition`:

| Position | Meaning |
| --- | --- |
| `FilePosition::beginning()` | The start of the retained file. |
| `FilePosition::end()` | The tip of the stream. |
| `FilePosition::sequence(n)` | Message number `n`, redelivered inclusively. |
| `FilePosition::timestamp(millis)` | The first message strictly later than that instant; `millis` is milliseconds since the Unix epoch. |

A position read off a delivery is pinned: seeking back to it redelivers exactly that message, then
the rest of the log in order.

The `start_at(..)` clause on the attribute says where a subscription begins, before its first
delivery. A running handler moves its own subscription through two context keys:

| Key | Reads | Available on |
| --- | --- | --- |
| `Position` | this delivery's `FilePosition` | `FileContext` |
| `SeekHandle` | the handle that moves the subscription | `FileContext`, `FileBatchContext` |

A handler takes those keys as parameters with the `Ctx` extractor and names no context type at all.

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:seek"
```

A [batch](#batches) body declares `ctx: &mut Context<'_, FileBatchContext>` and reads the handle
with `ctx.context(SeekHandle)`. A batch spans many deliveries, so this context holds no position;
each element's own sequence is in its `stream-sequence` header.

A handler that reads either key does not compile against `StdioBroker`: standard input keeps no log
to move within.

Deliveries queued from before a seek are discarded, so the next message the handler sees comes from
the new position. See
[Seeking](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking) in the
framework docs for the capability itself.

## Publishing

`FilePublish` is the policy that constructs the publisher `FilePublisher`, which appends to the
stream file; `StdioPublish` constructs `StdioPublisher`, which writes lines to standard output. You
specify the policy when you register the handler, and at startup it instantiates the publisher on
the connected broker.

Each policy is its broker's default, so a reply goes through it when the mount site names no other.
You name another policy at the mount site with `.out`, marker first and policy second:
`b.include(handle).out(Reply, Publish);` names the policy for the reply, and an injected `Out` slot
takes one the same way under its own marker.

A destination on this transport is a stream key inside the broker. The file itself is named once,
in `FileBroker::new(path)`, and the stdio form writes to standard output. You can therefore declare
the key on the reply type with `#[outgoing(name = "results")]` and write the bare `publish` clause
on the subscriber. A reply whose type declares no key is published where the subscriber says:
`publish("results")`.

Each transport prelude exports its policy as `Publish`, so a mount site names the concept: a service
that moves between the two forms changes the glob, not the policy name.

The file publisher flushes on every publish, so a live subscriber - or an external reader of the
same file - sees a message as soon as the call returns.

Publishing from a startup hook names the same policy:

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:publish"
```

The stdio publisher returns `SeaFileError::Invalid` for a message with no payload and no headers,
because the client's line format silently drops empty lines.

### Per-message arguments

A handler publishes through the framework's builder: `publisher.message(&value).publish()`, with
`.to(key)` where the value's own `#[derive(Outgoing)]` leaves the stream key to the call. A message
here has a stream key and a payload, and those are the builder's own arguments.

Opaque bytes go the same way, as a value whose type declares itself already serialized
(`#[derive(Outgoing, Serialized)] struct Frame(Vec<u8>)`), so no codec runs on them.

Brokers with a protocol field per message - a priority, a QoS, an ordering key - add a step to that
builder for it. These two transports have none: an append to a stream file and a line on standard
output carry a key and a payload and nothing else. So this crate adds no step, and a handler body
that publishes keeps the framework prelude alone and the plain bound `Out<impl Publisher, Marker>`.

## The header envelope

The client's payloads are plain bytes with no header space, so headers are written into the payload
itself. The envelope appears only when a message has headers:

- A message published without headers is written verbatim. A file recorded that way stays readable
  as a plain payload stream by any `sea-streamer` consumer, and a file written by another tool
  stays readable here.
- A message with headers is written as `rs1:` followed by base64 of a length-prefixed header block
  and the payload. The form is text-safe because the stdio transport is line-oriented UTF-8.

The stdio publisher also envelopes a non-UTF-8 payload that has no headers, so binary survives a
shell pipeline intact.

Every delivery has its sequence number in the `stream-sequence` header (`SEQUENCE_HEADER`).

## Acknowledgement

You resume explicitly on this transport: store a `FilePosition` read off a delivery and open the
next run with `start_at(..)`, or replay the file from the beginning. `ack` and `nack` return
`AckError::Unsupported`, so nothing else records how far you read.

### Retrying after a delay

`HandlerOutcome::retry_after(delay)` has no transport to lean on here, so the framework's own
fallback is the whole mechanism: it drops the delivery and, once the delay is over, publishes a
copy of the message with an incremented retry-count header. You wire the publisher it uses in the
composition root:

```rust
--8<-- "crates/ruststream-sea-file/tests/redelivery.rs:retry_via"
```

The copy goes to the stream key the subscription reads, so a live subscription on a stream file
gets its message back. A replay does not: it reads the region the file already held and completes,
and a copy written afterwards would never reach it. So a scope that wires `retry_via` over a
`FileStream::replay()` subscription refuses to start, naming the subscription, rather than dropping
every delayed message at runtime. Stdio refuses for the same reason: what you publish goes to the
next process in the pipeline, not back into your own standard input.

Without `retry_via`, `retry_after` degrades to an immediate requeue, which on these transports
means the delay is lost.

## Stdio pipelines

`StdioBroker` makes the process a stage of a shell pipeline: standard input is the subscription,
standard output is the publisher, and `producer | service | consumer` works with ordinary
command-line tools.

```rust
--8<-- "crates/ruststream-sea-file/examples/stdio_pipeline.rs:pipeline"
```

Lines follow the client's `[timestamp | stream_key | seq] payload` format, so the stream key is
part of the line and one process serves several keys:

```text
echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' | ./pipeline run
```

`StdioBroker::new().loopback()` sends this process's standard output back into its own standard
input, so a stdio service runs under test in one process with no external commands.

## Testing

Every suite in this crate runs locally, on temp files and in-process pipes, with no broker to
start.

You test your own handlers with the framework's `TestApp` harness, against `FileTestBroker` from
the `testing` feature. It keeps a retained, positioned log in memory: the one transport property a
stream file's handlers are written against.

A seeking service therefore mounts on it with no edit at all: `FileStream` resolves here, a
delivery reports a `FilePosition`, and `FileContext` and `FileBatchContext` build off its
deliveries the way they build off a file's. Batches work the same way, so a batch handler sees
batches of the size its mount site asked for.

The mount site needs no edit either. `Publish` - both forms of it, `FilePublish` and
`StdioPublish` - pairs against this broker, so a routes file keeps the policy it ships with and
`.out(Reply, Publish)` reads the same under the harness as it does in production. There is no
harness-only policy to swap in.

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:handler"
```

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:test"
```

The harness supplies the input, runs the reaction until nothing is left in flight, and records what
happened, so a test needs no waiting and no collector of its own. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)
for the assertion surface.

How far the resemblance goes is measured, not claimed. The framework's contract suites - the
lifecycle ladder, seeking, batching - run against the in-process broker as well as against a real
stream file, so a contract the file keeps and the in-process broker quietly broke fails in this
repo rather than in your service's tests.

The in-process broker settles the way a file does: `ack` and `nack` return
`AckError::Unsupported` on both, so a handler that reads the answer behaves the same either way.
What it leaves alone is everything a file is for: files and beacons, the end-of-stream mark, the
header envelope, and timing. Those are verified against real stream files by this repo's own suite.
