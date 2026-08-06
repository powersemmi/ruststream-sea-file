//! A service over a persistent stream file: survives restarts, replays on demand, needs no
//! external broker.

// --8<-- [start:handler]
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_sea_file::{FileBroker, FilePosition, FileStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(FileStream::new("orders"), start_at(FilePosition::beginning()))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        FileBroker::new("/tmp/orders.ss"),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
