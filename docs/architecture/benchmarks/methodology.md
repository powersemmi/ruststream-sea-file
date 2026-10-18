---
title: Methodology
description: "How ruststream-sea-file measures itself: the three loops, the two ways of reading a stream file, the storage-bound arithmetic and the code count."
---

# Methodology

How `ruststream-sea-file` measures itself: the three loops, the two ways of reading a stream file,
the storage-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on a stream file.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `sea-streamer-file` driven directly: its streamer, its consumer, its producer.
- **Broker crate.** This crate and nothing above it: [`FileBroker`](../../components/broker/file-broker.md)
  connects, a [`FileStream`](../../components/subscriptions/file-stream.md) opens a subscription,
  and a loop pulls deliveries off its stream, decodes them and settles them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the stream file, the consumer options, the decode into
the same struct, the payload bytes, the tokio runtime with four worker threads, and the binary.
The raw client has no settlement to make, and the crate's reports `AckError::Unsupported` at once,
so no loop waits on one.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The two scenarios

- **Replay.** The whole file is written first, then each loop reads it from the beginning. The
  file holds 8192 bodies more than the window counts, so no loop reaches its end.
- **Live tail.** The subscription opens first, and each loop appends through its own layer while
  it reads. Each append is flushed to the file before the next one, as the crate's publisher does,
  and the writer runs at most 32 768 messages ahead of the reader.

## The run

- **A fresh stream file per run.** It sits under `target/bench-streams` and is removed when the
  run ends, so a run never reads what an earlier one left.
- **The window opens at the first delivery and closes when the last handler returns**, in all
  three loops. Opening the file, creating the consumer and writing the recorded region are outside
  it.
- **The message count comes from a probe** of 100 000 messages, which measures the raw loop's rate,
  so a measured run lasts at least five seconds on any machine. The count stops at two million
  messages, about a gibibyte of stream file.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.
- **The page cache is warm.** Every run writes its file just before it reads it back, so the
  figures are the filesystem's, not the device's.

A row is `storage-bound` when the raw client spent the run waiting on appends. That is arithmetic:
a probe times 50 000 appends, each flushed to the file, after 5000 as warm-up. A live tail waits for
one append per delivery and a replay for none. The row is flagged when that wait reaches half the
measured time per message. The time of one append is published with the machine, so the arithmetic
can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
`FileBroker` over a stream file of its own, built without the `testing` feature. The service runs
on a single-threaded runtime. Another thread appends the messages through the client after the
service has started, and that work is outside the measured region.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

Five runs of one binary gave the same per-message figures within half a percent, except the
allocations of a reply, which moved between 70 and 74 with how often the service waited on the
writer. A run's total moved by up to 2.7 percent of instructions. So a run against a saved
baseline fails on six percent more instructions in a scenario. Allocations are held to a floor
each scenario declares, on every run.

## Running it yourself

```bash
just bench
```

The recipe runs both comparison scenarios through all three loops, removes the stream files and
rewrites `docs/benchmarks/results.json`. It wants the machine to itself, and it writes several
gibibytes through `target/bench-streams`, one file of up to about a gibibyte at a time. It clears
`RUSTFLAGS`, so the binary is not tied to the CPU it was built on. Both recipes build the
benchmarks the way a release is built, with fat link-time optimization across crates, so the
numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes about a minute and needs valgrind; the recipe installs the benchmark runner
itself. `just bench-code 5000` measures over five thousand deliveries instead of a thousand.
`just bench-code --save-baseline=main` on `main` records a baseline, and
`just bench-code --baseline=main` on a branch compares with it.
