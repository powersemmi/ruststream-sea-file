//! `beacon_interval`: how far apart the stream file's beacons sit. The live test reads the
//! interval back out of the file's header.
//!
//! ```text
//! cargo run --example file_broker_beacons --features testing -- run
//! cargo test --example file_broker_beacons --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[subscriber(FileStream::new("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order read");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// A beacon every 64 KiB of the file: sixteen times as dense as the client's default of 1 MiB.
fn broker(path: &str) -> FileBroker {
    FileBroker::new(path).beacon_interval(64 * 1024)
}

fn service(path: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(path), |b| {
        b.include(handle);
    })
}
// --8<-- [end:broker]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service("/var/lib/orders/orders.ss")
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::SeaFileError;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, app, common, service};

    /// The beacon interval kept in the header of the stream file at `path`.
    ///
    /// The layout is the file client's: a two-byte mark and a version, the file name as a
    /// length-prefixed string, a millisecond timestamp, then the interval as a big-endian `u32`.
    fn beacon_interval_of(path: &str) -> Result<u32, Box<dyn Error>> {
        let header = fs::read(path)?;
        let at = 4 + usize::from(header[3]) + 8;
        Ok(u32::from_be_bytes(header[at..at + 4].try_into()?))
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_interval_that_is_not_a_multiple_of_1024_fails_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
            .register_broker(FileBroker::new("/var/lib/orders/orders.ss").beacon_interval(1000));

        let Err(failed) = TestApp::start(app).await else {
            panic!("an interval of 1000 bytes must be refused");
        };
        let mut cause: Option<&(dyn Error + 'static)> = Some(&failed);
        while let Some(err) = cause {
            if let Some(refused) = err.downcast_ref::<SeaFileError>() {
                assert!(matches!(refused, SeaFileError::Invalid(_)), "{refused}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with a SeaFileError: {failed:?}");
    }

    #[test]
    fn the_interval_reaches_the_file() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("beacons");
            TestApp::start_live(service(&path))
                .await?
                .shutdown()
                .await?;

            assert_eq!(beacon_interval_of(&path)?, 64 * 1024);
            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
