//! Headers on a stream file: a message with headers is written in an envelope inside its
//! payload, and one without is written as it is.
//!
//! ```text
//! cargo test --example sea_headers --features testing
//! cargo run --example sea_headers --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_sea_file::file::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
/// The header contract every order and receipt carries.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Tenant {
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders", headers = Tenant)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts", headers = Tenant)]
struct Receipt {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Receipts;

/// Confirms an order under the tenant it arrived with.
#[subscriber(FileStream::new("orders"))]
async fn confirm(
    order: &Order,
    Headers(tenant): Headers<Tenant>,
    Out(receipts): Out<impl Publisher, Receipts>,
) -> HandlerOutcome {
    tracing::info!(order.id, tenant = %tenant.tenant, "confirmed");
    let receipt = Receipt { order: order.id };
    if receipts
        .message(&receipt)
        .with_headers(&tenant)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The service on the file `main` hands it.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(file, |b| {
        b.include(confirm).out(Receipts, Publish).build();
    })
}
// --8<-- [end:handler]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/shop/orders.ss")))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_sea_file::file::prelude::*;
    use serde::Serialize;

    use super::{Order, Receipt, Tenant, app};

    fn acme() -> Tenant {
        Tenant {
            tenant: "acme".to_owned(),
        }
    }

    #[tokio::test]
    async fn the_tenant_reaches_the_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(FileBroker::new("/var/lib/shop/orders.ss"))).await?;

        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .with_headers(&acme())
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 })
            .with_header("tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }

    /// A message with no header contract.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "notes")]
    struct Note {
        text: String,
    }

    /// The bytes the stream file holds: an envelope for a message with headers, and the payload
    /// itself for one without.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_file_envelopes_only_what_has_headers() -> Result<(), Box<dyn Error>> {
        let path =
            env::temp_dir().join(format!("ruststream-sea-file-headers-{}.ss", process::id()));
        let path = path.to_string_lossy().into_owned();

        let tb = TestApp::start_live(app(FileBroker::new(&path))).await?;
        tb.broker::<FileBroker>()
            .message(&Order { id: 7 })
            .with_headers(&acme())
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .message(&Note {
                text: "plain".to_owned(),
            })
            .publish()
            .await?;
        tb.broker::<FileBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_header("tenant", "acme");
        tb.shutdown().await?;

        let bytes = fs::read(&path)?;
        let holds = |needle: &[u8]| bytes.windows(needle.len()).any(|window| window == needle);
        assert!(holds(b"rs1:"), "a message with headers is enveloped");
        assert!(
            !holds(br#"{"id":7}"#),
            "an enveloped payload is not written as it is"
        );
        assert!(
            holds(br#"{"text":"plain"}"#),
            "a message without headers is written as it is"
        );
        fs::remove_file(&path)?;
        Ok(())
    }
}
