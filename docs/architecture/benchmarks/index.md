---
title: Benchmarks
description: "What ruststream-sea-file costs on every message: throughput against the raw sea-streamer-file client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {seaFile} from '@site/src/components/Benchmarks/seaFile';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} against the client)", "indistinguishable": "indistinguishable", "brokerBound": "storage-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "Each loop against the raw client's best round: right of zero is slower, left is faster", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The transport", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} against no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "filesystem": "Filesystem", "file_size": "Stream file size", "page_cache": "Page cache", "round_trip": "One append", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-sea-file` costs on every message: throughput against the raw `sea-streamer-file`
client, and instructions and allocations of its code.

## Against a raw client

The first chart runs two scenarios three ways on a stream file on this machine's disk. `Raw client`
drives the `sea-streamer-file` streamer, consumer and producer directly. `Broker crate` drives this
crate's subscription and publisher with a loop of its own and no runtime. `RustStream` is the
service a user writes, a `#[subscriber]` handler in an app. The first gap is what this crate costs
over the client it wraps, and the second is what the runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round: a dot right of zero is slower, and
a dot left of it is faster. A hollow dot is a loop whose difference from the raw client is smaller
than the spread of the rounds. The table under the chart prints the best round with the median in
parentheses.

The two scenarios are the two ways a [stream file](../transports/index.md) is read. A replay reads
a file written in full before the subscription opened, and nothing paces it. A live tail follows a
file still being appended to, and every delivery waits for an append.

On the replay row both layers come out faster than the raw client. The crate's [subscription
reads ahead](../path.md) on a task of its own, so the next message is out of the file while the
current one is handled. The hand-written raw loop reads and handles in one task.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`FileBroker`](../../components/broker/file-broker.md) over a stream file of its own. The count
includes the framework's dispatch, this crate's code and the part of the client that runs on that
thread. The client's writer and the file reads and writes on tokio's blocking threads are not
counted. `Cold start` is what starting the service and taking the first message cost once.

## Results

<Results entry={seaFile} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={seaFile} labels={machine} />

The transport is a file, so the filesystem takes the place of a broker's server, and the numbers
belong to it as much as to this crate. `One append` is the time of one append flushed to the file,
measured on its own.

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
