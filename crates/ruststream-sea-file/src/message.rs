//! [`SeaMessage`]: a delivered message, shared by the file and stdio transports.

use std::fmt;
use std::future::{Future, ready};
use std::sync::OnceLock;

use ruststream::{AckError, HeaderMap, IncomingMessage, Positioned, Str};
use sea_streamer_types::{MessageHeader, SharedMessage};

use crate::envelope::{Envelope, Rs};
#[cfg(feature = "testing")]
use crate::in_process::Release;

/// Header exposing the message's sequence number within its stream.
pub const SEQUENCE_HEADER: &str = "stream-sequence";

/// A position in a stream file's retained log: where `start_at(..)` opens a subscription, and
/// where [`Seeker::seek`](ruststream::Seeker::seek) moves a running one.
///
/// Captured positions ([`Positioned::position`]) carry the pinned semantics the framework
/// defines: seeking to one redelivers exactly that message (the transport's sequence rewind
/// is inclusive). The other forms keep the transport's own semantics: `Beginning` replays
/// everything retained, `End` skips to the tip, and `Timestamp` resumes at the earliest
/// message strictly later than the instant (milliseconds since the Unix epoch).
///
/// Seek to a position the stream has reached, or to the sequence it writes next, which is the
/// tip: a service that handled every message resumes there. A sequence further ahead, and an
/// instant later than every message it holds, are both refused - the transport reads forward
/// looking for the message and runs out of file - and the subscription ends with that refusal
/// rather than staying where it was. A captured position, the beginning and the tip are always
/// safe; a number a service worked out for itself is what needs the care.
///
/// The
/// [FilePosition](https://powersemmi.github.io/ruststream-sea-file/latest/components/positions/)
/// page shows it in a service.
///
/// # Examples
///
/// A handler reads its delivery's place in the file with `Ctx<Position>` and moves its own
/// subscription with `Ctx<SeekHandle>`:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Job {
///     id: u64,
///     poisoned: bool,
/// }
///
/// #[subscriber(FileStream::new("jobs"), start_at(FilePosition::beginning()))]
/// async fn work(
///     job: &Job,
///     Ctx(at): Ctx<Position>,
///     Ctx(seeker): Ctx<SeekHandle>,
/// ) -> HandlerOutcome {
///     if job.poisoned && seeker.seek(FilePosition::end()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     tracing::info!(job.id, position = ?at, "worked");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/jobs.ss"), |b| {
///             b.include(work);
///         })
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePosition {
    /// Everything retained.
    Beginning,
    /// The tip of the stream.
    End,
    /// A message sequence (inclusive). A stream key is numbered from one, and the numbering
    /// carries on when a later connection appends to the same file.
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
/// The delivery keeps the record as it was read. The subscription's [`Envelope`] located the
/// payload when the delivery was built, and parses the headers the first time they are read,
/// keeping them for the rest of the delivery.
///
/// The transport keeps no consumer positions (its resumable mode is unimplemented upstream),
/// so acknowledgement reports [`AckError::Unsupported`] rather than pretending; resume
/// explicitly via the descriptor's start position or a captured [`FilePosition`].
pub struct SeaMessage<Env: Envelope = Rs> {
    header: MessageHeader,
    record: Vec<u8>,
    opened: Env::Opened,
    headers: OnceLock<HeaderMap>,
    envelope: Env,
    /// Tells the test harness this delivery is done when it is dropped: the in-process transport
    /// counts its deliveries in flight. The field is there only with the `testing` feature.
    #[cfg(feature = "testing")]
    release: Option<Release>,
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// delivery exactly the fields a delivery off a file or a pipe needs.
#[cfg(not(feature = "testing"))]
const _: () = assert!(
    size_of::<SeaMessage>()
        == size_of::<(
            MessageHeader,
            Vec<u8>,
            <Rs as Envelope>::Opened,
            OnceLock<HeaderMap>,
        )>()
);

impl<Env: Envelope> fmt::Debug for SeaMessage<Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeaMessage")
            .field("stream", &self.stream())
            .field("sequence", &self.header.sequence())
            .field("record_len", &self.record.len())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> SeaMessage<Env> {
    /// Opens a record the client read: the envelope locates the payload, and leaves the headers
    /// for the first read of them.
    pub(crate) fn open(message: SharedMessage, envelope: &Env) -> Self {
        // The client hands over the only reference to the bytes it read, so they move here
        // without a copy; a record another reader still shares is copied.
        let (header, record) = message.to_owned_message().take();
        let opened = envelope.open(&record);
        Self {
            header,
            record,
            opened,
            headers: OnceLock::new(),
            envelope: envelope.clone(),
            #[cfg(feature = "testing")]
            release: None,
        }
    }

    /// The same delivery, counted in flight by the harness until it is dropped.
    #[cfg(feature = "testing")]
    pub(crate) fn released_by(mut self, release: Option<Release>) -> Self {
        self.release = release;
        self
    }

    /// The stream key this message was published to.
    #[must_use]
    pub fn stream(&self) -> &str {
        self.header.stream_key().name()
    }
}

impl<Env: Envelope> Positioned for SeaMessage<Env> {
    type Position = FilePosition;

    fn position(&self) -> FilePosition {
        FilePosition::Sequence(*self.header.sequence())
    }
}

impl<Env: Envelope> IncomingMessage for SeaMessage<Env> {
    fn payload(&self) -> &[u8] {
        self.envelope.payload(&self.record, &self.opened)
    }

    fn headers(&self) -> &HeaderMap {
        self.headers.get_or_init(|| {
            let mut headers = self.envelope.headers(&self.record, &self.opened);
            headers.insert(
                Str::from_static(SEQUENCE_HEADER),
                self.header.sequence().to_string(),
            );
            headers
        })
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Err(AckError::Unsupported))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sea_streamer_types::{ShardId, StreamKey, Timestamp};

    use super::*;
    use crate::envelope::{Medium, SealError};

    /// The default envelope, counting how often it parses headers.
    #[derive(Debug, Clone, Default)]
    struct Counting(Arc<AtomicUsize>);

    impl Envelope for Counting {
        type Opened = <Rs as Envelope>::Opened;

        fn seal<'p>(
            &self,
            headers: &HeaderMap,
            payload: &'p [u8],
            medium: Medium,
        ) -> Result<Cow<'p, [u8]>, SealError> {
            Rs.seal(headers, payload, medium)
        }

        fn open(&self, record: &[u8]) -> Self::Opened {
            Rs.open(record)
        }

        fn payload<'r>(&self, record: &'r [u8], opened: &'r Self::Opened) -> &'r [u8] {
            Rs.payload(record, opened)
        }

        fn headers(&self, record: &[u8], opened: &Self::Opened) -> HeaderMap {
            self.0.fetch_add(1, Ordering::Relaxed);
            Rs.headers(record, opened)
        }
    }

    fn delivery(envelope: &Counting, headers: &HeaderMap, payload: &[u8]) -> SeaMessage<Counting> {
        let record = envelope
            .seal(headers, payload, Medium::File)
            .expect("the default envelope seals")
            .into_owned();
        let length = record.len();
        let header = MessageHeader::new(
            StreamKey::new("orders").expect("the key is valid"),
            ShardId::new(0),
            7,
            Timestamp::now_utc(),
        );
        SeaMessage::open(SharedMessage::new(header, record, 0, length), envelope)
    }

    #[test]
    fn headers_are_parsed_once_and_only_when_read() {
        let envelope = Counting::default();
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let message = delivery(&envelope, &headers, b"{\"id\":1}");

        assert_eq!(message.payload(), b"{\"id\":1}");
        assert_eq!(
            envelope.0.load(Ordering::Relaxed),
            0,
            "the payload parsed the headers"
        );

        assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
        assert_eq!(message.headers().get_str(SEQUENCE_HEADER), Some("7"));
        assert_eq!(envelope.0.load(Ordering::Relaxed), 1);
    }
}
