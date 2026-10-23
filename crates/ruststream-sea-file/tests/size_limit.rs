//! The size limit in process and on a real stream file: the same records reach the limit at the
//! same place, with the same outcome for each publish and the same messages left in the file.
//!
//! The in-process file counts each record in the bytes the client writes for it, beacons
//! included, so a test of a capped service does not succeed where the real file fails.

use std::num::NonZeroU64;
use std::time::Duration;

use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_sea_file::{ConnectedFileBroker, FileBroker};
use sea_streamer_file::{FileErr, FileId, MessageSource, StreamMode};
use sea_streamer_types::{Buffer as _, Message as _};

use crate::common;

/// How long a step on a local file may take before the test calls it stuck.
const STEP: Duration = Duration::from_secs(10);

/// A limit a few dozen records reach.
const LIMIT: NonZeroU64 = NonZeroU64::new(8 * 1024).unwrap();

/// One record: its stream key and the length of its payload.
type Record = (&'static str, usize);

/// What a run reports: whether each publish succeeded, and the records the file holds, by
/// their place in the list.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    accepted: Vec<bool>,
    held: Vec<usize>,
}

/// The payload of record `n`: its number first, so the file can be read back record by record.
fn payload(n: usize, len: usize) -> Vec<u8> {
    let mut bytes = format!("{n:04}").into_bytes();
    bytes.resize(len.max(4), b'.');
    bytes
}

fn broker(path: &str, beacon_interval: Option<u32>) -> FileBroker {
    let broker = FileBroker::new(path).size_limit(LIMIT);
    match beacon_interval {
        Some(bytes) => broker.beacon_interval(bytes),
        None => broker,
    }
}

async fn publish_all(connected: &ConnectedFileBroker, records: &[Record]) -> Vec<bool> {
    let publisher = connected.publisher();
    let mut accepted = Vec::new();
    for (n, (key, len)) in records.iter().enumerate() {
        let bytes = payload(n, *len);
        let result = publisher
            .publish(OutgoingMessage::new(key, &bytes[..]), None)
            .await;
        accepted.push(result.is_ok());
    }
    accepted
}

fn number(bytes: &[u8]) -> usize {
    std::str::from_utf8(&bytes[..4])
        .expect("a number")
        .parse()
        .expect("a number")
}

/// The records a real stream file holds whole, in file order.
async fn read_back(path: &str, records: &[Record]) -> Vec<usize> {
    let mut source = MessageSource::new(FileId::new(path), StreamMode::Replay)
        .await
        .expect("the file opens for a replay");
    let mut held = Vec::new();
    loop {
        match source.next().await {
            Ok(msg) => {
                let key = msg.message.stream_key();
                if records.iter().any(|(name, _)| key.name() == *name) {
                    held.push(number(msg.message.message().as_bytes()));
                }
            }
            Err(FileErr::NotEnoughBytes | FileErr::StreamEnded) => break,
            Err(e) => panic!("the file reads back: {e}"),
        }
    }
    held.sort_unstable();
    held
}

async fn on_a_real_file(records: &[Record], beacon_interval: Option<u32>) -> Outcome {
    let path = common::tmp_path("size-limit-agree");
    let connected = broker(&path, beacon_interval)
        .connect()
        .await
        .expect("connects");
    let accepted = publish_all(&connected, records).await;
    let _ = connected.shutdown().await;
    if accepted.contains(&false) {
        // The client reports the cap as soon as it hands its last write, which ends exactly at
        // the limit, to the runtime's file thread: the bytes reach the file a moment later.
        let size = || std::fs::metadata(&path).expect("the file is there").len();
        tokio::time::timeout(STEP, async {
            while size() < LIMIT.get() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the last write reaches the file");
    }
    let held = read_back(&path, records).await;
    std::fs::remove_file(&path).expect("the file is removed");
    Outcome { accepted, held }
}

async fn in_process(records: &[Record], beacon_interval: Option<u32>) -> Outcome {
    let connected = broker("/tmp/in-process.ss", beacon_interval)
        .connect_in_process()
        .await
        .expect("connects in process");
    let accepted = publish_all(&connected, records).await;
    let mut held: Vec<usize> = ["orders", "receipts", "a-much-longer-stream-key"]
        .iter()
        .flat_map(|key| connected.published(key))
        .map(|message| number(message.payload()))
        .collect();
    held.sort_unstable();
    Outcome { accepted, held }
}

/// Records of many sizes over keys of different lengths, written until long past the limit,
/// with a beacon every kilobyte so records are split by beacons on the way.
#[test]
fn records_split_by_beacons_reach_the_limit_at_the_same_place() {
    common::on_a_file(async {
        let keys = ["orders", "receipts", "a-much-longer-stream-key"];
        let records: Vec<Record> = (0..80)
            .map(|n| (keys[n % keys.len()], (n * 37) % 290 + 4))
            .collect();
        let live = on_a_real_file(&records, Some(1024)).await;
        assert!(
            live.accepted.contains(&false),
            "the records never reached the limit"
        );
        assert_eq!(in_process(&records, Some(1024)).await, live);
    });
}

/// The record that fills the limit exactly, and one a byte too large for the space left: the
/// first is in the file and its publish fails, the second is not, and every later publish fails
/// in both cases.
#[test]
fn the_record_at_the_limit_has_the_same_outcome() {
    common::on_a_file(async {
        // How many bytes are left after two records, measured on a real file.
        let path = common::tmp_path("size-limit-probe");
        let connected = broker(&path, None).connect().await.expect("connects");
        publish_all(&connected, &[("orders", 300)]).await;
        let first = std::fs::metadata(&path).expect("the file").len();
        publish_all(&connected, &[("orders", 300)]).await;
        let second = std::fs::metadata(&path).expect("the file").len();
        let _ = connected.shutdown().await;
        std::fs::remove_file(&path).expect("the file is removed");
        let framing = second - first - 300;
        let exact = usize::try_from(LIMIT.get() - second - framing).expect("a small size");

        for (last, held) in [(exact, vec![0, 1, 2]), (exact + 1, vec![0, 1])] {
            let records = [
                ("orders", 300),
                ("orders", 300),
                ("orders", last),
                ("orders", 4),
            ];
            let live = on_a_real_file(&records, None).await;
            assert_eq!(
                live,
                Outcome {
                    accepted: vec![true, true, false, false],
                    held,
                }
            );
            assert_eq!(in_process(&records, None).await, live);
        }
    });
}
