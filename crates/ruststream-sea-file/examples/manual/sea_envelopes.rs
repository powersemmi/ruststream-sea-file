//! The service of `examples/sea_envelopes.rs` written without the macros: the subscription names
//! its envelope with `FileStream::new(..).envelope(NoOp)`, and the handlers are `Handle` impls.
//!
//! ```text
//! cargo test --example manual_sea_envelopes --features testing,json
//! cargo run --example manual_sea_envelopes --features testing,json -- run
//! ```

use std::borrow::Cow;
use std::process::ExitCode;

use ruststream::HeaderMap;
use ruststream::codec::JsonCodec;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sea_file::envelope::{Envelope, Medium, SealError};
use ruststream_sea_file::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:builtin]
/// A reading as the sensor gateway writes it: a bare JSON payload, no headers.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Reading {
    sensor: String,
    millivolts: u32,
}

/// An alert the dashboard reads as JSON lines, with the plant in a header.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "alerts", headers = Plant)]
struct Alert {
    sensor: String,
}

/// The header contract of an alert.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Plant {
    plant: String,
}

#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Reads the gateway's bare records and raises an alert on a low reading.
struct Watch;

impl<Entry> Handle<Reading, (), Outs<(Entry,)>> for Watch
where
    Entry: OutEntry<Alerts, Wire: Publisher>,
{
    async fn handle(
        &self,
        reading: &Reading,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if reading.millivolts >= 3000 {
            return Ok(());
        }
        let alert = Alert {
            sensor: reading.sensor.clone(),
        };
        let plant = Plant {
            plant: "north".to_owned(),
        };
        if outs
            .get(Alerts)
            .message(&alert)
            .with_headers(&plant)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:builtin]

// --8<-- [start:custom]
/// A feed whose lines hold the plant, a tab, and the payload.
#[derive(Debug, Clone, Copy, Default)]
struct PlantTab;

impl Envelope for PlantTab {
    /// Where the tab is, when the line has one.
    type Opened = Option<usize>;

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        _medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        let plant = headers.get("plant").unwrap_or_default();
        Ok(Cow::Owned([plant, b"\t", payload].concat()))
    }

    fn open(&self, record: &[u8]) -> Option<usize> {
        record.iter().position(|byte| *byte == b'\t')
    }

    fn payload<'r>(&self, record: &'r [u8], tab: &'r Option<usize>) -> &'r [u8] {
        tab.map_or(record, |tab| &record[tab + 1..])
    }

    fn headers(&self, record: &[u8], tab: &Option<usize>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(tab) = *tab {
            headers.insert(
                "plant",
                String::from_utf8_lossy(&record[..tab]).into_owned(),
            );
        }
        headers
    }
}

/// Counts the readings of each plant that arrive on standard input.
struct Tally;

impl Handle<Reading> for Tally {
    async fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let Headers(plant) = Headers::<Plant>::from_context(ctx).await?;
        tracing::info!(plant = %plant.plant, sensor = %reading.sensor, "counted");
        Ok(())
    }
}
// --8<-- [end:custom]

// --8<-- [start:app]
/// The plant service: the stream file keeps the default envelope, the readings subscription reads
/// bare records, the alerts go out as JSON documents, and the pipe reads the tab feed.
fn app(file: FileBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("plant", "0.1.0"))
        .with_broker(file, |b| {
            b.include(subscriber(FileStream::new("readings").envelope(NoOp), Watch).build())
                .out(Alerts, FilePublish.envelope(Framed::new(JsonCodec)))
                .build();
        })
        .with_broker(StdioBroker::new().envelope(PlantTab), |b| {
            b.include(subscriber("readings", Tally).build())
                .out_retry(StdioPublish)
                .to("readings.retry");
        })
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(|| app(FileBroker::new("/var/lib/plant/plant.ss")))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sea_file::prelude::*;
    use serde::Serialize;

    use super::{Alert, Plant, PlantTab, Reading, app};

    ruststream::register_testable_broker!(StdioBroker<PlantTab>);

    /// A reading as a test sends it, to the subscription the test names.
    #[derive(Serialize, Outgoing)]
    #[outgoing(headers = Plant)]
    struct Sample {
        sensor: String,
        millivolts: u32,
    }

    fn low() -> Reading {
        Reading {
            sensor: "boiler".to_owned(),
            millivolts: 1200,
        }
    }

    #[tokio::test]
    async fn a_low_reading_raises_an_alert_with_its_plant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(FileBroker::new("/var/lib/plant/plant.ss"))).await?;

        tb.broker::<FileBroker>()
            .message(&low())
            .to("readings")
            .publish()
            .await?;

        tb.broker::<FileBroker>()
            .published::<Alert>("alerts")
            .assert_called_once()
            .with(&Alert {
                sensor: "boiler".to_owned(),
            })
            .with_header("plant", "north");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_tab_feed_reaches_the_tally_with_its_plant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(FileBroker::new("/var/lib/plant/plant.ss"))).await?;

        tb.broker::<StdioBroker<PlantTab>>()
            .message(&Sample {
                sensor: "boiler".to_owned(),
                millivolts: 1200,
            })
            .to("readings")
            .with_headers(&Plant {
                plant: "south".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<StdioBroker<PlantTab>>()
            .subscriber("readings")
            .assert_called_once()
            .with(&low());
        tb.shutdown().await?;
        Ok(())
    }
}
