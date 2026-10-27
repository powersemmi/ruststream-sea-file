File and stdio transports for the [RustStream](https://github.com/powersemmi/ruststream)
messaging framework, over [`sea-streamer-file`](https://docs.rs/sea-streamer-file) and
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio).

[`FileBroker`] keeps a log in one `.ss` stream file: it survives restarts, replays what it
recorded and repositions on demand. [`StdioBroker`] makes the process's standard input and output
one stream, so a service runs as a stage of a shell pipeline. Neither needs a server. A service
resumes from a position it stored, because `ack` and `nack` report `AckError::Unsupported` on both
transports.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

```
use ruststream_sea_file::file::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/var/lib/orders/orders.ss"),
        |b| {
            b.include(handle);
        },
    )
}
```

`cargo run -- run` starts it. [`FileBroker::new`] only records the path; the runtime opens the
file at startup and closes it when the service stops. The same service on a pipeline globs
[`stdio::prelude`], mounts on [`StdioBroker`], and subscribes by the bare stream key.

# Where things are

The [guides][guides] take a service from installation to production, in this order:
[getting started][g-start], [an event log on disk][g-event-log], [resuming and
seeking][g-resume], [shell pipelines][g-pipelines], [retries and dead letters][g-retries],
[testing][g-testing] and [running in production][g-production]. The
[architecture][architecture] pages show how a stream file and a pipe carry messages and where
the crate sits between them and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [Broker][c-broker]: the two forms, each a module with its broker, its policy and its prelude:
  [`mod@file`] and [`stdio`].
- [FileBroker][c-file-broker]: [`FileBroker`], connected into [`ConnectedFileBroker`].
- [StdioBroker][c-stdio-broker]: [`StdioBroker`], connected into [`ConnectedStdioBroker`].
- [existing_only][c-existing-only]: [`FileBroker::existing_only`], a file that must already exist.
- [end_with_eos][c-end-with-eos]: [`FileBroker::end_with_eos`], the end-of-stream mark at
  shutdown.
- [beacon_interval][c-beacon-interval]: [`FileBroker::beacon_interval`], how densely the file is
  indexed.
- [size_limit][c-size-limit]: [`FileBroker::size_limit`], an opt-in cap on the file's size.
- [loopback][c-loopback]: [`StdioBroker::loopback`], a stage's output sent back to its input.
- [Shutdown][c-shutdown]: the file closed, and [`SeaFileError::NotConnected`] for a publisher
  used afterwards.
- [Prelude][c-prelude]: [`file::prelude`], [`stdio::prelude`], and [`prelude`] for a service on
  both.
- [Errors][c-errors]: [`SeaFileError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a stream key in `#[subscriber]`, on both brokers.
- [FileStream][c-file-stream]: [`FileStream`], one stream key of the file.
- [replay][c-replay]: [`FileStream::replay`], the retained file read once.
- [Lines][c-lines]: the lines of a stream key on standard input, through [`StdioSubscriber`].
- [Batches][c-batches]: a slice handler over batches assembled on the client.

## Positions

- [FilePosition][c-positions]: [`FilePosition`], a place in a stream key's log.
- [start_at][c-start-at]: opening a subscription at a position on every startup.
- [Position][c-position]: [`Position`], the delivery's place in the file, read from
  [`FileContext`].
- [Seek][c-seek]: [`SeekHandle`] and [`FileSeeker`], moving a running subscription.
- [FileBatchContext][c-batch-context]: [`FileBatchContext`], a batch handler's context.

## Publishing

- [Publish][c-publishing]: [`file::Publish`](struct@file::Publish) and [`stdio::Publish`](struct@stdio::Publish), each form's one policy.
- [FilePublish][c-file-publish]: [`FilePublish`](struct@FilePublish) and its live [`FilePublisher`].
- [StdioPublish][c-stdio-publish]: [`StdioPublish`](struct@StdioPublish) and its live [`StdioPublisher`].

## Settlement

- [Acknowledgement][c-settlement]: `ack` and `nack` on a transport that records no positions.
- [Retries][c-retries]: `max_attempts`, counted in the framework's retry-count header.
- [retry_after][c-retry-after]: a copy appended under the key the subscription reads.
- [out_retry][c-out-retry]: where a deferred copy goes on a pipe.
- [Dead letters][c-dead-letters]: `dead_letter`, the stream key a spent delivery goes to.

## Headers

- [Headers][c-headers]: the envelope headers travel in, inside the payload.
- [Envelopes][c-envelopes]: [`Envelope`] and its formats [`Rs`], [`Framed`] and [`NoOp`], chosen
  on the broker, a [`FileStream`] or a publish policy.
- [Payloads][c-payloads]: binary, multi-line and padded payloads on a pipe.
- [SEQUENCE_HEADER][c-sequence-header]: [`SEQUENCE_HEADER`], the sequence of every delivery.

## Tooling

- [AsyncAPI][c-asyncapi]: what the two brokers contribute to the generated document (feature
  `asyncapi`): the [server][c-server] and the [channels][c-channels].
- [Testing][c-testing]: the in-process stream file and pipe
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the brokers to (feature `testing`), and [Live][c-live] runs against a real stream file.

# Cargo features

All are off by default; the crate needs none of them to run a service. `testing` adds the
in-process mode of both brokers, which `TestApp::start` connects in place of the file and the
pipe; a service enables it in its `[dev-dependencies]`. `json`, `cbor` and `msgpack` make the core's codec
of that name a [`Framed`] document codec, and with `testing` add the in-process mode of a broker
framed by it. `asyncapi` adds
what a [`FileStream`] subscription and a publish contribute to the generated document.

[guides]: https://powersemmi.github.io/ruststream-sea-file/latest/
[g-start]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/getting-started/
[g-event-log]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/event-log/
[g-resume]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/resume-and-seek/
[g-pipelines]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/pipelines/
[g-retries]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/retries/
[g-testing]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-sea-file/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-sea-file/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-sea-file/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/
[c-file-broker]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/file-broker/
[c-stdio-broker]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/stdio-broker/
[c-existing-only]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/existing-only/
[c-end-with-eos]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/end-with-eos/
[c-beacon-interval]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/beacon-interval/
[c-size-limit]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/size-limit/
[c-loopback]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/loopback/
[c-shutdown]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-sea-file/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/
[c-file-stream]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/file-stream/
[c-replay]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/replay/
[c-lines]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/lines/
[c-batches]: https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/batches/
[c-positions]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/
[c-start-at]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/start-at/
[c-position]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/position/
[c-seek]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/seek/
[c-batch-context]: https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/batch-context/
[c-publishing]: https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/
[c-file-publish]: https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/file-publish/
[c-stdio-publish]: https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/stdio-publish/
[c-settlement]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/
[c-retries]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/retries/
[c-retry-after]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/retry-after/
[c-out-retry]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/out-retry/
[c-dead-letters]: https://powersemmi.github.io/ruststream-sea-file/latest/components/settlement/dead-letters/
[c-headers]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/
[c-envelopes]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/envelopes/
[c-payloads]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/payloads/
[c-sequence-header]: https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/sequence-header/
[c-asyncapi]: https://powersemmi.github.io/ruststream-sea-file/latest/components/asyncapi/
[c-server]: https://powersemmi.github.io/ruststream-sea-file/latest/components/asyncapi/server/
[c-channels]: https://powersemmi.github.io/ruststream-sea-file/latest/components/asyncapi/channels/
[c-testing]: https://powersemmi.github.io/ruststream-sea-file/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-sea-file/latest/components/testing/live/
