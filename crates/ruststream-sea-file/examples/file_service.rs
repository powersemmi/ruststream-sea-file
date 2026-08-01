//! A service over a persistent stream file: survives restarts, replays on demand, needs no
//! external broker.

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_sea_file::{FileBroker, FileStream, Start};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(FileStream::new("orders").start(Start::Beginning))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/tmp/orders.ss"),
        |b| {
            b.include(handle);
        },
    )
}
