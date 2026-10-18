//! Standard input, read by this crate for every stdio subscription of the process.
//!
//! The client's own reader leaves its consumers open when standard input ends, and stops the
//! whole process on a line it cannot parse. So this crate reads standard input itself: one thread
//! per process reads it line by line, parses the client's line format, and hands each line to the
//! subscriptions of its stream key. The end of input ends every subscription that reads standard
//! input alone. The client still writes standard output.
//!
//! Each line costs what it cost under the client's reader: one buffer for the line and one shared
//! message, with the payload decoded by the subscription that reads it.

use std::collections::HashMap;
use std::io::{self, BufRead};
use std::str;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::thread;

use sea_streamer_stdio::{TIMESTAMP_FORMAT, TIMESTAMP_FORMAT_SUBSEC};
use sea_streamer_types::export::time::PrimitiveDateTime;
use sea_streamer_types::{
    MAX_STREAM_KEY_LEN, MessageHeader, SeqNo, ShardId, SharedMessage, StreamKey, Timestamp,
    is_valid_stream_key_char,
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::message::SeaMessage;

/// The key of a line whose header names none, as the client spells it. Such a line reaches every
/// subscription.
const BROADCAST: &str = "broadcast";

/// How much of a line a warning quotes.
const QUOTED: usize = 64;

/// The subscriptions of the process and what the reader has seen.
///
/// Why a static: standard input is one per process, and so is its reader. Every stdio broker of
/// the process shares both, as they shared the client's.
static INPUT: LazyLock<Mutex<Input>> = LazyLock::new(Mutex::default);

/// Opens a subscription of `key` on standard input, starting the reader on the first one.
///
/// A subscription of a [`loopback`](crate::StdioBroker::loopback) broker also reads what the
/// process writes, so it `outlives_input`: the end of standard input leaves it open, and only a
/// shutdown ends it. Any other subscription ends with the input, and one opened after the input
/// ended ends at once.
///
/// # Errors
///
/// Returns the error of starting the reader's thread.
pub(crate) fn subscribe(key: StreamKey, outlives_input: bool) -> io::Result<InputQueue> {
    let mut input = lock(&INPUT);
    if !input.reading {
        thread::Builder::new()
            .name("ruststream-sea-file-stdin".into())
            .spawn(|| read_lines(io::stdin().lock(), &INPUT))?;
        input.reading = true;
    }
    // Registered under the lock the reader takes for each line, so a line read from here on
    // reaches this subscription: none is read before it exists and handed to no one.
    Ok(input.open(&INPUT, key, outlives_input))
}

/// Ends every subscription of the process, after the lines each has already read: what a stdio
/// shutdown does.
pub(crate) fn end_all() {
    lock(&INPUT).subscriptions.clear();
}

/// Hands a line this process wrote back to its own subscriptions, as the line the next process
/// reads: the header the client wrote it under, and the payload.
pub(crate) fn loop_back(written: &MessageHeader, payload: Vec<u8>) {
    let meta = Meta {
        timestamp: Some(*written.timestamp()),
        key: Some(written.stream_key().clone()),
        sequence: Some(*written.sequence()),
        shard: Some(*written.shard_id()),
    };
    lock(&INPUT).dispatch(meta, payload, 0);
}

fn lock(input: &Mutex<Input>) -> MutexGuard<'_, Input> {
    // Each step under the lock leaves the registry whole, so a panic beside it loses nothing.
    input.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads `source` line by line into `input` until it ends, then ends the subscriptions that read
/// it alone.
fn read_lines(mut source: impl BufRead, input: &Mutex<Input>) {
    // The next line's buffer is as large as the last line, so lines of one size take one
    // allocation each and are never copied into a larger one.
    let mut capacity = 0;
    loop {
        let mut line = Vec::with_capacity(capacity);
        match source.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) => {
                warn!(
                    target: "ruststream_sea_file::stdio",
                    %error,
                    "standard input failed to read; the stdio subscriptions end here",
                );
                break;
            }
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        capacity = line.len() + 1;
        lock(input).take(line);
    }
    lock(input).end();
}

/// One subscription of standard input, in arrival order.
pub(crate) struct InputQueue {
    input: &'static Mutex<Input>,
    id: SubscriptionId,
    rx: mpsc::UnboundedReceiver<SharedMessage>,
}

impl InputQueue {
    /// The next line, or `None` once the subscription has ended.
    pub(crate) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<SeaMessage>> {
        self.rx
            .poll_recv(cx)
            .map(|line| line.map(|line| SeaMessage::new(&line)))
    }
}

impl Drop for InputQueue {
    fn drop(&mut self) {
        lock(self.input).close(self.id);
    }
}

type SubscriptionId = u64;

struct Subscription {
    id: SubscriptionId,
    key: StreamKey,
    outlives_input: bool,
    sender: mpsc::UnboundedSender<SharedMessage>,
}

/// The subscriptions of one standard input and the state its lines are numbered by.
#[derive(Default)]
struct Input {
    /// Whether the reader's thread was started; it runs until the input ends.
    reading: bool,
    /// Whether the reader saw the end of the input.
    ended: bool,
    next_id: SubscriptionId,
    subscriptions: Vec<Subscription>,
    /// The sequence the next line of each key and shard carries when its header names none,
    /// counted from zero as the client counts.
    sequences: HashMap<(StreamKey, ShardId), SeqNo>,
}

impl Input {
    fn open(
        &mut self,
        input: &'static Mutex<Self>,
        key: StreamKey,
        outlives_input: bool,
    ) -> InputQueue {
        let (sender, rx) = mpsc::unbounded_channel();
        let id = self.next_id;
        self.next_id += 1;
        // After the end of the input the sender is dropped here, so the subscription ends at
        // once.
        if outlives_input || !self.ended {
            self.subscriptions.push(Subscription {
                id,
                key,
                outlives_input,
                sender,
            });
        }
        InputQueue { input, id, rx }
    }

    fn close(&mut self, id: SubscriptionId) {
        self.subscriptions
            .retain(|subscription| subscription.id != id);
    }

    /// The end of the input: the subscriptions that read nothing else end, after their lines.
    fn end(&mut self) {
        self.ended = true;
        self.subscriptions
            .retain(|subscription| subscription.outlives_input);
    }

    /// One line off the input, without its newline. A line the format does not define is
    /// passed over with a warning, and the lines after it are read.
    fn take(&mut self, line: Vec<u8>) {
        let Ok(text) = str::from_utf8(&line) else {
            warn!(
                target: "ruststream_sea_file::stdio",
                bytes = line.len(),
                "a line of standard input is not UTF-8 and is passed over",
            );
            return;
        };
        let Some((meta, offset)) = parse(text) else {
            warn!(
                target: "ruststream_sea_file::stdio",
                line = %text.chars().take(QUOTED).collect::<String>(),
                "a line of standard input opens with a header the format does not define and is \
                 passed over",
            );
            return;
        };
        self.dispatch(meta, line, offset);
    }

    /// Numbers a line as the client does and hands it to every subscription of its key, or to
    /// every subscription when its header names no key.
    fn dispatch(&mut self, meta: Meta, bytes: Vec<u8>, offset: usize) {
        let broadcast = meta.key.is_none();
        let key = meta
            .key
            .unwrap_or_else(|| StreamKey::new(BROADCAST).expect("the client's key is valid"));
        let shard = meta.shard.unwrap_or_default();
        let next = self.sequences.entry((key.clone(), shard)).or_default();
        let sequence = if let Some(sequence) = meta.sequence {
            *next = sequence;
            sequence
        } else {
            let sequence = *next;
            *next += 1;
            sequence
        };
        let length = bytes.len() - offset;
        let message = SharedMessage::new(
            MessageHeader::new(
                key.clone(),
                shard,
                sequence,
                meta.timestamp.unwrap_or_else(Timestamp::now_utc),
            ),
            bytes,
            offset,
            length,
        );
        for subscription in &self.subscriptions {
            if broadcast || subscription.key == key {
                // A subscription dropped meanwhile closes itself under this lock next.
                let _ = subscription.sender.send(message.clone());
            }
        }
    }
}

/// The header fields a line names.
#[derive(Debug, Default, PartialEq, Eq)]
struct Meta {
    timestamp: Option<Timestamp>,
    key: Option<StreamKey>,
    sequence: Option<SeqNo>,
    shard: Option<ShardId>,
}

impl Meta {
    fn is_empty(&self) -> bool {
        self.timestamp.is_none()
            && self.key.is_none()
            && self.sequence.is_none()
            && self.shard.is_none()
    }
}

/// Reads the header of a line in the client's format, `[timestamp | stream_key | sequence |
/// shard_id] payload`, and returns it with the offset of the payload; `None` for a leading
/// `[...]` that is no header.
///
/// The rules are the client's: a line that does not open with a bracketed field is a payload
/// with no header; the fields come in that order and each may be left out, the sequence and the
/// shard only after a key; the payload starts after the spaces that follow the header and runs
/// to the end of the line.
fn parse(line: &str) -> Option<(Meta, usize)> {
    let Some((fields, rest)) = line
        .strip_prefix('[')
        .and_then(|inner| inner.split_once(']'))
        .filter(|(fields, _)| !fields.is_empty())
    else {
        return Some((
            Meta {
                timestamp: Some(Timestamp::now_utc()),
                ..Meta::default()
            },
            0,
        ));
    };
    let mut meta = Meta::default();
    for field in fields.split('|').map(str::trim) {
        if meta.is_empty()
            && let Some(timestamp) = parse_timestamp(field)
        {
            meta.timestamp = Some(timestamp);
        } else if meta.key.is_none() && is_stream_key(field) {
            meta.key = Some(StreamKey::new(field).ok()?);
        } else if meta.key.is_some()
            && meta.sequence.is_none()
            && let Ok(sequence) = field.parse()
        {
            meta.sequence = Some(sequence);
        } else if meta.sequence.is_some()
            && meta.shard.is_none()
            && let Ok(shard) = field.parse()
        {
            meta.shard = Some(ShardId::new(shard));
        } else {
            return None;
        }
    }
    let offset = line.len() - rest.trim_start().len();
    Some((meta, offset))
}

fn parse_timestamp(field: &str) -> Option<Timestamp> {
    PrimitiveDateTime::parse(field, &TIMESTAMP_FORMAT_SUBSEC)
        .or_else(|_| PrimitiveDateTime::parse(field, &TIMESTAMP_FORMAT))
        .ok()
        .map(PrimitiveDateTime::assume_utc)
}

fn is_stream_key(field: &str) -> bool {
    // The key's characters are ASCII, so its length in bytes is its length in characters.
    (1..=MAX_STREAM_KEY_LEN).contains(&field.len()) && field.chars().all(is_valid_stream_key_char)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use sea_streamer_types::{Buffer as _, Message as _};

    use super::*;

    fn key(name: &str) -> StreamKey {
        StreamKey::new(name).expect("a valid key")
    }

    /// The payload a parsed line carries.
    fn payload(line: &str) -> &str {
        let (_, offset) = parse(line).expect("the line parses");
        &line[offset..]
    }

    #[test]
    fn a_line_with_no_header_is_all_payload() {
        let (meta, offset) = parse("{\"id\": 1}").expect("a plain line parses");
        assert_eq!(offset, 0);
        assert!(meta.key.is_none() && meta.sequence.is_none());
        assert!(meta.timestamp.is_some(), "the client stamps it on arrival");
        assert_eq!(payload("[] x"), "[] x", "an empty bracket is no header");
    }

    #[test]
    fn every_field_of_a_header_is_read_in_order() {
        let line = "[2022-01-02T03:04:05 | jobs.1 | 123 | 4]   {\"id\": 1}";
        let (meta, _) = parse(line).expect("the line parses");
        assert_eq!(
            meta,
            Meta {
                timestamp: Some(Timestamp::from_unix_timestamp(1_641_092_645).unwrap()),
                key: Some(key("jobs.1")),
                sequence: Some(123),
                shard: Some(ShardId::new(4)),
            }
        );
        assert_eq!(payload(line), "{\"id\": 1}");
    }

    #[test]
    fn a_header_may_leave_fields_out() {
        let (meta, _) = parse("[2022-01-02T03:04:05.678] x").expect("a timestamp alone");
        assert_eq!(
            meta.timestamp,
            Some(Timestamp::from_unix_timestamp_nanos(1_641_092_645_678_000_000).unwrap())
        );
        assert!(meta.key.is_none());
        let (meta, _) = parse("[jobs | 7] x").expect("a key and a sequence");
        assert_eq!((meta.key, meta.sequence), (Some(key("jobs")), Some(7)));
        assert!(meta.timestamp.is_none());
    }

    #[test]
    fn a_bracket_that_is_no_header_is_refused() {
        for line in [
            "[ ]",
            "[1,2]",
            "[not a key] x",
            "[jobs | x] y",
            "[jobs | 1 | 2 | 3] z",
        ] {
            assert!(parse(line).is_none(), "{line:?} is no header");
        }
    }

    #[test]
    fn lines_are_numbered_per_key_unless_the_header_names_a_sequence() {
        let input: &'static Mutex<Input> = Box::leak(Box::default());
        let mut jobs = lock(input).open(input, key("jobs"), false);
        let mut chores = lock(input).open(input, key("chores"), false);

        read_lines(
            Cursor::new("[jobs] a\n[jobs | 10] b\n[jobs] c\n[chores] d\nplain\n"),
            input,
        );

        let read = |queue: &mut InputQueue| {
            let mut lines = Vec::new();
            while let Ok(line) = queue.rx.try_recv() {
                lines.push((line.sequence(), line.message().as_str().unwrap().to_owned()));
            }
            lines
        };
        assert_eq!(
            read(&mut jobs),
            [
                (0, "a".into()),
                (10, "b".into()),
                (10, "c".into()),
                (0, "plain".into())
            ]
        );
        assert_eq!(read(&mut chores), [(0, "d".into()), (0, "plain".into())]);
    }

    #[test]
    fn the_end_of_input_ends_the_subscriptions_that_read_it_alone() {
        use tokio::sync::mpsc::error::TryRecvError;

        let input: &'static Mutex<Input> = Box::leak(Box::default());
        let mut alone = lock(input).open(input, key("jobs"), false);
        let mut looped = lock(input).open(input, key("jobs"), true);

        read_lines(Cursor::new("[jobs] a\n"), input);

        assert!(alone.rx.try_recv().is_ok(), "the line read before the end arrives");
        assert_eq!(alone.rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert!(looped.rx.try_recv().is_ok());
        assert_eq!(looped.rx.try_recv().unwrap_err(), TryRecvError::Empty);

        let mut late = lock(input).open(input, key("jobs"), false);
        assert_eq!(late.rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        let mut late_looped = lock(input).open(input, key("jobs"), true);
        assert_eq!(late_looped.rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }
}
