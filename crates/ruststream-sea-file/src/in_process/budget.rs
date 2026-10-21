//! The size limit of the in-process stream file: each record counted in the bytes the file
//! client writes for it, against the limit the broker set.
//!
//! The client's format: a header, then each record as its key, shard, sequence, timestamp,
//! payload length, payload, checksum and a closing byte, with a beacon every `beacon_interval`
//! bytes that may split a record. A beacon carries a marker for some of the stream keys written
//! so far, taken in turn when they do not all fit.

use std::collections::BTreeSet;

/// The file header the client writes first.
const HEADER: u64 = 128;
/// A record's bytes besides its key and payload: the key's length, the shard, the sequence and
/// the timestamp (eight bytes each), the payload's length, the checksum and the closing byte.
const RECORD: u64 = 1 + 8 + 8 + 8 + 4 + 2 + 1;
/// A beacon's bytes besides its markers: two separators, the bytes of the split record that
/// remain, and the number of markers.
const BEACON: u64 = 1 + 4 + 1 + 1;
/// A marker's bytes besides its key: the key's length, shard, sequence, timestamp and running
/// checksum.
const MARKER: u64 = 1 + 8 + 8 + 8 + 2;
/// The largest marker, for the longest key a marker holds.
const MARKER_MAX: u64 = MARKER + u8::MAX as u64;

/// What the file does with one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fit {
    /// The record is written and the file has room left.
    Kept,
    /// The record is written whole and reaches the limit: its publish fails, as does every
    /// later one.
    KeptAtTheLimit,
    /// The record does not fit the room left: on disk it is cut at the limit, and its publish
    /// fails, as does every later one.
    Refused,
}

/// The bytes the file has written, against its limit.
#[derive(Debug)]
pub(crate) struct Budget {
    limit: u64,
    interval: u64,
    /// How many markers a beacon holds at most.
    markers: usize,
    offset: u64,
    keys: BTreeSet<String>,
    /// The markers written so far, which pick the key the next beacon starts from.
    marked: usize,
    full: bool,
}

impl Budget {
    /// A new file of `limit` bytes with a beacon every `interval` bytes, its header written.
    pub(crate) fn new(limit: u64, interval: u32) -> Self {
        let interval = u64::from(interval);
        let markers = usize::try_from(
            interval
                .checked_sub(BEACON)
                .map_or(0, |space| (space / MARKER_MAX).min(u64::from(u8::MAX)))
                / 2,
        )
        .unwrap_or(0);
        let mut budget = Self {
            limit,
            interval,
            markers,
            offset: 0,
            keys: BTreeSet::new(),
            marked: 0,
            full: false,
        };
        budget.write(HEADER);
        if budget.offset >= limit {
            budget.full = true;
        }
        budget
    }

    /// Counts a record of `payload` bytes under `key`.
    pub(crate) fn take(&mut self, key: &str, payload: usize) -> Fit {
        if self.full {
            return Fit::Refused;
        }
        self.keys.insert(key.to_owned());
        let left = self.limit - self.offset;
        let record = key.len() as u64 + payload as u64 + RECORD;
        let start = self.offset;
        let record_end = self.write(record);
        let taken = self.offset - start;
        if record_end - start > left {
            self.full = true;
            Fit::Refused
        } else if taken >= left {
            self.full = true;
            Fit::KeptAtTheLimit
        } else {
            Fit::Kept
        }
    }

    /// Writes `bytes`, with a beacon at every interval it crosses, and returns where the bytes
    /// themselves ended.
    fn write(&mut self, mut bytes: u64) -> u64 {
        let mut end = self.offset;
        while bytes > 0 {
            let chunk = (self.interval - self.offset % self.interval).min(bytes);
            self.offset += chunk;
            bytes -= chunk;
            end = self.offset;
            if self.offset.is_multiple_of(self.interval) {
                self.offset += self.beacon();
            }
        }
        end
    }

    /// The size of the next beacon, which marks the keys in turn.
    fn beacon(&mut self) -> u64 {
        let count = self.keys.len().min(self.markers);
        let size = if count == 0 {
            0
        } else {
            self.keys
                .iter()
                .cycle()
                .skip(self.marked % self.keys.len())
                .take(count)
                .map(|key| key.len() as u64 + MARKER)
                .sum()
        };
        self.marked += count;
        BEACON + size
    }
}

#[cfg(test)]
mod tests {
    use super::{Budget, Fit};

    #[test]
    fn a_record_is_its_key_and_payload_and_32_bytes() {
        // Room for one record of 100 bytes after the header: 128 + 6 + 100 + 32.
        let mut budget = Budget::new(266, 1 << 20);
        assert_eq!(budget.take("orders", 99), Fit::Kept);
        let mut budget = Budget::new(266, 1 << 20);
        assert_eq!(budget.take("orders", 100), Fit::KeptAtTheLimit);
        assert_eq!(budget.take("orders", 1), Fit::Refused);
        let mut budget = Budget::new(266, 1 << 20);
        assert_eq!(budget.take("orders", 101), Fit::Refused);
    }

    #[test]
    fn a_beacon_that_follows_a_record_counts_for_it() {
        // A record that ends on the interval is followed by a beacon with one marker:
        // 7 + 6 + 27 bytes.
        let mut budget = Budget::new(1024 + 40, 1024);
        assert_eq!(budget.take("orders", 1024 - 128 - 38), Fit::KeptAtTheLimit);
        let mut budget = Budget::new(1024 + 41, 1024);
        assert_eq!(budget.take("orders", 1024 - 128 - 38), Fit::Kept);
    }
}
