//! `size_limit`: a stream file capped at a size the operator chose. Without the setting a file
//! grows without limit. The live tests run on files in the temp directory.
//!
//! ```text
//! cargo run --example file_broker_size_limit --features testing -- run
//! cargo test --example file_broker_size_limit --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::num::NonZeroU64;

use ruststream::nonzero;
use serde::{Deserialize, Serialize};

use ruststream_sea_file::file::prelude::*;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;

#[subscriber(FileStream::new("readings"))]
async fn record(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.sensor, "reading recorded");
    HandlerOutcome::ack()
}

// --8<-- [start:capped]
/// A recorder whose log may take `limit` bytes of the disk and no more.
fn recorder(path: &str, limit: NonZeroU64) -> impl App<State = ()> {
    let broker = FileBroker::new(path).size_limit(limit);
    RustStream::new(AppInfo::new("recorder", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    recorder("/var/lib/readings/readings.ss", nonzero!(64 << 30))
}
// --8<-- [end:capped]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;

    use ruststream::nonzero;
    use ruststream::testing::TestApp;
    use ruststream_sea_file::SeaFileError;
    use ruststream_sea_file::file::prelude::*;

    use super::{Reading, common, recorder};

    /// Whether `err` or one of its sources is `SeaFileError::Invalid`.
    fn is_invalid(err: &(dyn Error + 'static)) -> bool {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if matches!(
                err.downcast_ref::<SeaFileError>(),
                Some(SeaFileError::Invalid(_))
            ) {
                return true;
            }
            cause = err.source();
        }
        false
    }

    /// Once a publish reaches the cap, it fails, and so does every publish after it.
    #[test]
    fn a_publish_past_the_cap_fails_and_so_does_every_later_one() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("size-limit-reached");
            let tb = TestApp::start_live(recorder(&path, nonzero!(1024))).await?;

            let mut sensor = 0;
            while tb
                .broker::<FileBroker>()
                .message(&Reading { sensor })
                .publish()
                .await
                .is_ok()
            {
                sensor += 1;
            }
            let later = tb
                .broker::<FileBroker>()
                .message(&Reading { sensor: 0 })
                .publish()
                .await;
            assert!(later.is_err(), "a publish after the cap was taken");
            assert!(
                fs::metadata(&path)?.len() <= 1024,
                "the file grew past its cap"
            );

            tb.shutdown().await?;
            fs::remove_file(&path)?;
            Ok(())
        })
    }

    /// A file an earlier run left larger than the cap does not open.
    #[test]
    fn a_file_already_past_the_cap_fails_startup() -> Result<(), Box<dyn Error>> {
        common::rt().block_on(async {
            let path = common::tmp_path("size-limit-past");
            let tb = TestApp::start_live(recorder(&path, nonzero!(1 << 20))).await?;
            for sensor in 0..20 {
                tb.broker::<FileBroker>()
                    .message(&Reading { sensor })
                    .publish()
                    .await?;
            }
            tb.shutdown().await?;

            let Err(failed) = TestApp::start_live(recorder(&path, nonzero!(256))).await else {
                panic!("a file past its cap must not open");
            };
            assert!(is_invalid(&failed), "{failed:?}");
            fs::remove_file(&path)?;
            Ok(())
        })
    }
}
