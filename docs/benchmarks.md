# Benchmarks

A layer between the stream file and your code costs time on every message: the subscription stream,
the decode, the settlement, the dispatch. This page says how much, measured against the same work
written by hand on `sea-streamer-file`.

Every scenario runs three times over in one process, as three loops that differ in one thing each:
what carries the messages.

- **Raw client** - `sea-streamer-file` driven directly: its streamer, its consumer, its producer.
- **This crate** - the crate's own types, hand-driven: the broker, the subscription descriptor, the
  stream it yields, the delivery's payload and its settlement, and the publisher. No handler and no
  runtime.
- **RustStream service** - what a user writes: a `#[subscriber]` handler, the app, the runtime.

Everything else is held equal - the same stream file, the same consumer options, the same decode
into the same type, the payload bytes, the tokio runtime and the build. The procedure is the
framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

The best of three interleaved rounds, with the slowest round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "RustStream service", "adapterOverhead": "Crate overhead", "overhead": "Framework overhead", "indistinguishable": "indistinguishable", "brokerBound": "storage-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "storage": "Storage", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

The two overhead columns answer different questions. `Crate overhead` is what this crate's own
consumer and publisher cost over the library they wrap, which is what this repository is
responsible for. `Framework overhead` is the whole service against the same raw client, so the
distance between the two columns is what the runtime adds over this transport in particular.

A negative figure means that column was faster than the raw client. On the replay row it is, and
the reason is the shape of the code rather than the speed of it: a hand-written loop reads a
message and handles it in the same task, while this crate's subscription reads ahead on a task of
its own, so the next body is out of the file before the current one is done. That head start
belongs to the crate rather than to the library, and the runtime's own task widens it.

There are two scenarios because a stream file is read two ways. A replay reads a file written in
full before the subscription opened, and nothing paces it: this is where the cost of dispatch shows
in full rather than inside a wait. A live tail follows a file still being appended to, and there
every delivery waits for an append to reach the disk.

A row reported as `indistinguishable` is one whose two ends differ by less than the spread between
runs of either. A figure below the run-to-run noise would read as precision that was never
measured, so none is published.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sea-file/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

There is no broker in this row and no network. The transport is a file, so the filesystem takes the
server's place, and the numbers belong to it as much as to this crate: the same code on a slower
device publishes slower figures.

The `storage-bound` mark on a row is decided against the round trip published above, not guessed. A
durable append is what this transport makes a delivery wait on, so it is timed on its own outside
every loop, and a row is marked when one append costs at least half of what a message cost. Such a
row is a lower bound on the cost of dispatch, never a measurement of it.

The state of the page cache matters more than any of it. A replay served from memory and one served
from the device differ by far more than the cost this page is about, so every run writes its stream
file immediately before reading it back, and what is published is the first of the two.

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one subscription, one stream key, a small body and one file. It measures what a delivery
costs in this crate, not how fast a disk can be read, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last handler returns, in
all three loops alike. Nothing is acknowledged: the transport keeps no consumer positions, so a
settlement reports that it is unsupported rather than pretending, and the raw client has no
settlement to make at all.

The replay scenario departs from the framework's procedure in one point, and does so on purpose:
its subscription opens after the messages were written, because a replay is exactly that. The live
tail is the scenario that attaches first and is fed afterwards.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe points the runs at a directory under `target/`, runs every scenario through all three
loops, removes the stream files and rewrites `docs/benchmarks/results.json` with what it measured.
It takes about a quarter of an hour, it writes tens of gigabytes through that directory, and it
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds, up to a ceiling of two million messages - about a gibibyte
of stream file - past which a run would be measuring the device rather than this crate.
