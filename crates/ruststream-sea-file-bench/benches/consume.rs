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
//! Consuming a small JSON body: the subscription reads the stream file from its beginning, the
//! dispatcher decodes each body into a struct, the handler reads a field, and the runtime settles
//! the delivery. A stream file keeps no consumer positions, so the ack reports that settlement is
//! unsupported.

mod common;

use std::hint::black_box;

use common::{INPUT, Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sea_file::file::prelude::*;

#[subscriber(FileStream::new(INPUT), start_at(FilePosition::beginning()))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// Five runs allocated 65,293 blocks each over the longest run, nearly all of them in the
// client's reader. The floor is that plus 0.1 percent, stated over a thousand deliveries; one
// allocation more per delivery would exceed it by more than 1,900.
#[library_benchmark(config = common::config_every(32_532, 1_000, 295))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
