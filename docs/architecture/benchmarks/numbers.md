---
title: Numbers
description: "What the benchmark numbers of ruststream-sea-file measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-sea-file` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one stream file. A row is not comparable with a row another broker crate
publishes: the transports do different work per message.

The file lives on the same machine, read from a warm page cache. With no network and no device
wait, the same cost per message is a larger share of the time than it would be on a slower disk.
Read the percentage as an upper bound on what a deployed service pays.

A negative figure on the replay row is not a faster client. The crate reads ahead on a task of its
own, and the raw loop reads and handles in one task. It is a difference in the shape of the two
loops, not a measurement of the crate's cost.

A `storage-bound` row is one where the raw client waited on appends for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the part of the client that
runs on the service's thread. The same count costs more time where it misses the cache, so a count
is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Opening the file, the connect, the subscription and the first allocations behind them are the cold
start, paid once.

A reply is appended to the same file the subscription reads. The reader passes over it on the way
to the next message, and the reply's row counts that reading too.

## What they do not mean

A row is one subscription reading one stream key of one file, with a small body. It measures what a
delivery costs inside this crate, not how fast a disk can be read.

Several stream keys in one file, a pool of worker lanes and a [batch
handler](../../components/subscriptions/batches.md) in the comparison each answer a different
question, and the comparison measures none of them. The code chart counts a batch handler and a
reply over the same file.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
