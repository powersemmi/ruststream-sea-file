// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Replying: the handler returns a value, the runtime encodes it and hands it to the crate's
//! default publisher, which appends it to the same stream file under the key the reply type
//! declares and flushes the append. The reply lands in the file the subscription reads, so the
//! subscription's reader passes over it on the way to the next order, and that is counted too.

mod common;

use std::hint::black_box;

use common::{INPUT, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sea_file::file::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(FileStream::new(INPUT), start_at(FilePosition::beginning()), publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// Five runs allocated 143,943 to 147,620 blocks over the longest run: how often the service
// waits on the writer's thread moves the count, since a wait that parks allocates and one that
// finds its answer ready does not. The floor is the highest of them plus that spread, rounded up
// and stated over a thousand deliveries, so an unchanged tree passes. Two allocations more per
// delivery exceed it; one more stays inside the spread.
#[library_benchmark(config = common::config_every(73_600, 1_000, 4_100))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
