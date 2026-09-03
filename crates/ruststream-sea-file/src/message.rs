//! [`SeaMessage`]: a delivered message, shared by the file and stdio transports.

use std::future::{Future, ready};

use bytes::Bytes;
use ruststream::{AckError, HeaderMap, IncomingMessage, Positioned};
use sea_streamer_types::{Buffer as _, Message as _, SharedMessage};

use crate::wire;

/// Header exposing the message's sequence number within its stream.
pub const SEQUENCE_HEADER: &str = "stream-sequence";

/// A position in a stream file's retained log, accepted by
/// [`Seeker::seek`](ruststream::Seeker::seek).
///
/// Captured positions ([`Positioned::position`]) carry the pinned semantics the framework
/// defines: seeking to one redelivers exactly that message (the transport's sequence rewind
/// is inclusive). The other forms keep the transport's own semantics: `Beginning` replays
/// everything retained, `End` skips to the tip, and `Timestamp` resumes at the earliest
/// message strictly later than the instant (milliseconds since the Unix epoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePosition {
    /// Everything retained.
    Beginning,
    /// The tip of the stream.
    End,
    /// A captured message sequence (inclusive).
    Sequence(u64),
    /// Milliseconds since the Unix epoch (exclusive).
    Timestamp(u64),
}

// Constructor forms of the variants: the `start_at(..)` macro clause resolves the position's
// type from a constructor call, which a bare unit-variant path does not provide.
impl FilePosition {
    /// Everything retained: [`FilePosition::Beginning`].
    #[must_use]
    pub const fn beginning() -> Self {
        Self::Beginning
    }

    /// The tip of the stream: [`FilePosition::End`].
    #[must_use]
    pub const fn end() -> Self {
        Self::End
    }

    /// A message sequence, redelivered inclusively: [`FilePosition::Sequence`].
    #[must_use]
    pub const fn sequence(sequence: u64) -> Self {
        Self::Sequence(sequence)
    }

    /// Milliseconds since the Unix epoch, resuming strictly later:
    /// [`FilePosition::Timestamp`].
    #[must_use]
    pub const fn timestamp(millis: u64) -> Self {
        Self::Timestamp(millis)
    }
}

/// A message delivered by one of this crate's subscribers.
///
/// This is the whole delivery on standard input; a stream file wraps it in a
/// [`FileMessage`](crate::FileMessage), which adds the subscription's reposition handle.
///
/// The transport keeps no consumer positions (its resumable mode is unimplemented upstream),
/// so acknowledgement reports [`AckError::Unsupported`] rather than pretending; resume
/// explicitly via the descriptor's start position or a captured [`FilePosition`].
pub struct SeaMessage {
    payload: Bytes,
    headers: HeaderMap,
    stream: String,
    sequence: u64,
}

impl std::fmt::Debug for SeaMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaMessage")
            .field("stream", &self.stream)
            .field("sequence", &self.sequence)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl SeaMessage {
    pub(crate) fn new(message: &SharedMessage) -> Self {
        let (mut headers, payload) = wire::decode(message.message().as_bytes());
        let sequence = message.sequence();
        headers.insert(SEQUENCE_HEADER, sequence.to_string());
        Self {
            payload,
            headers,
            stream: message.stream_key().name().to_owned(),
            sequence,
        }
    }

    /// The stream key this message was published to.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

impl Positioned for SeaMessage {
    type Position = FilePosition;

    fn position(&self) -> FilePosition {
        FilePosition::Sequence(self.sequence)
    }
}

impl IncomingMessage for SeaMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}
