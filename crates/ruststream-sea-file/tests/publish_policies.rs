//! Both transport forms' publish policies pair against their broker in process.
//!
//! A routes file names the policy of the transport it runs on - `.out(Reply, Publish)` - and that
//! mount site runs under the harness as written, on the production broker connected in process.
//!
//! Each form is written through the prelude its own mount site globs, because both forms name
//! their policy `Publish`.

#![cfg(feature = "testing")]

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Order {
    id: u64,
}

/// The reply, which the declaration's `publish` clause carries to a destination the test reads.
#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq, Eq)]
struct Receipt {
    id: u64,
}

/// A service on stream files: its own descriptor, its own policy.
mod file_form {
    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;

    use super::{Order, Receipt};

    #[subscriber(FileStream::new("orders"), publish("receipts"))]
    async fn confirm(order: &Order) -> Receipt {
        Receipt { id: order.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_file_policy_pairs_against_the_in_process_broker()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = RustStream::new(AppInfo::new("policies", "0.1.0")).with_broker(
            FileBroker::new("/var/lib/policies/orders.ss"),
            |b| {
                b.include(confirm).out(Reply, Publish);
            },
        );
        let tb = TestApp::start(app).await?;

        tb.message(&Order { id: 1 }).to("orders").publish().await?;

        tb.broker::<FileBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(
            tb.broker::<FileBroker>()
                .published::<Receipt>("receipts")
                .decoded(),
            vec![Receipt { id: 1 }],
        );
        Ok(())
    }
}

/// A service on a shell pipeline: a bare stream key, its own policy.
mod stdio_form {
    use ruststream::testing::TestApp;
    use ruststream_sea_file::stdio::prelude::*;

    use super::{Order, Receipt};

    #[subscriber("jobs", publish("results"))]
    async fn work(order: &Order) -> Receipt {
        Receipt { id: order.id }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_stdio_policy_pairs_against_the_in_process_broker()
    -> Result<(), Box<dyn std::error::Error>> {
        let app = RustStream::new(AppInfo::new("policies", "0.1.0")).with_broker(
            StdioBroker::new(),
            |b| {
                // A pipe addresses none of its retry copies, so the destination is named here,
                // the way a service on a real pipeline names it.
                b.include(work)
                    .out(Reply, Publish)
                    .out_retry(Publish)
                    .to("jobs.retry");
            },
        );
        let tb = TestApp::start(app).await?;

        tb.message(&Order { id: 1 }).to("jobs").publish().await?;

        tb.broker::<StdioBroker>()
            .subscriber("jobs")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        // The reply is what the service wrote to its standard output, read back as a value.
        assert_eq!(
            tb.broker::<StdioBroker>()
                .published::<Receipt>("results")
                .decoded(),
            vec![Receipt { id: 1 }],
        );
        Ok(())
    }
}
