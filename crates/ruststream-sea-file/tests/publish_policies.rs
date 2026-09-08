//! Both transport forms' publish policies pair against the in-process broker.
//!
//! A routes file names the policy of the transport it runs on - `.out(Reply, Publish)` - and that
//! mount site has to run under the harness as written. A harness-only policy substituted at the
//! include site would mean the wiring a test covers is not the wiring that ships, which is the
//! whole reason the production policies pair here.
//!
//! Each form is written through the prelude its own mount site globs, because both forms name
//! their policy `Publish` and the same in-process broker pairs both: the stand-in is this crate's
//! only one, and only its name is file-specific.

#![cfg(feature = "testing")]

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u64,
}

/// The reply, which the declaration's `publish` clause carries to a destination the test reads.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Receipt {
    id: u64,
}

/// A service on stream files: its own descriptor, its own policy, mounted on the stand-in.
mod file_form {
    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;
    use ruststream_sea_file::testing::FileTestBroker;

    use super::{Order, Receipt};

    #[subscriber(FileStream::new("orders"), publish("receipts"))]
    async fn confirm(order: &Order) -> Receipt {
        Receipt { id: order.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_file_policy_pairs_against_the_in_process_broker()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = RustStream::new(AppInfo::new("policies", "0.1.0")).with_broker(
            FileTestBroker::new(),
            |b| {
                b.include(confirm).out(Reply, Publish);
            },
        );
        let tb = TestApp::start(app).await?;

        tb.message(&Order { id: 1 }).to("orders").publish().await?;

        tb.broker::<FileTestBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<FileTestBroker>()
                .published::<Receipt>("receipts")
                .decoded(),
            vec![Receipt { id: 1 }],
        );
        Ok(())
    }
}

/// A service on a shell pipeline: a bare stream key, its own policy, mounted on the same stand-in.
mod stdio_form {
    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;
    use ruststream_sea_file::testing::FileTestBroker;

    use super::{Order, Receipt};

    #[subscriber("jobs", publish("results"))]
    async fn work(order: &Order) -> Receipt {
        Receipt { id: order.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stdio_policy_pairs_against_the_in_process_broker()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = RustStream::new(AppInfo::new("policies", "0.1.0")).with_broker(
            FileTestBroker::new(),
            |b| {
                b.include(work).out(Reply, Publish);
            },
        );
        let tb = TestApp::start(app).await?;

        tb.message(&Order { id: 1 }).to("jobs").publish().await?;

        tb.broker::<FileTestBroker>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        // The reply is asserted as a decoded value, not as a line: the stand-in routes bytes and
        // does not apply the line format a real pipe would.
        assert_eq!(
            tb.broker::<FileTestBroker>()
                .published::<Receipt>("results")
                .decoded(),
            vec![Receipt { id: 1 }],
        );
        Ok(())
    }
}
