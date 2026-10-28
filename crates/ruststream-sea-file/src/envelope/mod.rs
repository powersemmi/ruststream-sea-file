//! How a message's headers and payload travel in one record of a stream file or one line of a
//! pipe.
//!
//! Neither transport has room for headers: a stream file entry and a line carry a stream key and
//! a payload. An [`Envelope`] writes the headers and the payload into one record on publish and
//! finds them again on delivery. It is chosen like a codec, the most specific level winning: the
//! broker's default ([`FileBroker::envelope`](crate::FileBroker::envelope),
//! [`StdioBroker::envelope`](crate::StdioBroker::envelope)), then a subscription's
//! (the mount-site step [`FileEnvelopeExt::envelope`](crate::FileEnvelopeExt::envelope), or
//! [`FileStream::envelope`](crate::FileStream::envelope) on the manual path) or a publish policy's
//! ([`FilePublish::envelope`](crate::FilePublish::envelope),
//! [`StdioPublish::envelope`](crate::StdioPublish::envelope)). The choice is a type, so it costs
//! nothing per message.
//!
//! - [`Rs`], the default: the `rs1:` and `rs2:` envelope, applied only to a message that needs
//!   it, so a message with no headers is written as it is.
//! - [`Framed`]: one document of a codec per message, headers and payload inside it.
//! - [`NoOp`]: the payload as it is, with no headers.
//!
//! A delivery keeps the record as it was read. Its payload is found when the delivery is built;
//! its headers are parsed the first time something reads them, so a subscription that never reads
//! headers never parses them. The
//! [Envelopes](https://powersemmi.github.io/ruststream-sea-file/latest/components/headers/envelopes/)
//! page shows the three formats and an envelope of a service's own.

use std::borrow::Cow;
use std::error::Error as StdError;

use ruststream::HeaderMap;

mod framed;
mod noop;
mod rs;

pub use framed::{FrameCodec, Framed, FramedOpened};
pub use noop::NoOp;
pub use rs::Rs;

/// A format that writes a message's headers and payload into one record, and reads them back.
///
/// [`seal`](Self::seal) runs on publish. [`open`](Self::open) runs once per delivery and only
/// locates the payload; [`headers`](Self::headers) runs the first time the delivery's headers are
/// read, so its cost is paid only by a subscription that reads them. A delivery keeps a clone of
/// the envelope that opened it, so an envelope is cheap to clone: a unit struct, or a struct of
/// unit structs.
///
/// An envelope recognises its own records by a marker or an exact shape, writes a payload that
/// merely looks like one of them so no reader takes it for one, and reads a record it does not
/// recognise as a payload with no headers.
///
/// A service tests with an envelope of its own the way it tests with a built-in one: the test
/// registers the broker type it builds once, with `ruststream::register_testable_broker!`.
///
/// # Examples
///
/// Each line of a tab-separated feed holds the tenant, a tab, and the payload. The envelope finds
/// the payload behind the tab and parses the tenant only when a handler reads the headers:
///
/// ```
/// use std::borrow::Cow;
///
/// use ruststream::HeaderMap;
/// use ruststream_sea_file::envelope::{Envelope, Medium, SealError};
/// use ruststream_sea_file::stdio::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Clone, Copy, Default)]
/// struct TenantTab;
///
/// impl Envelope for TenantTab {
///     /// Where the tab is, when the line has one.
///     type Opened = Option<usize>;
///
///     fn seal<'p>(
///         &self,
///         headers: &HeaderMap,
///         payload: &'p [u8],
///         _medium: Medium,
///     ) -> Result<Cow<'p, [u8]>, SealError> {
///         let tenant = headers.get("tenant").unwrap_or_default();
///         Ok(Cow::Owned([tenant, b"\t", payload].concat()))
///     }
///
///     fn open(&self, record: &[u8]) -> Option<usize> {
///         record.iter().position(|byte| *byte == b'\t')
///     }
///
///     fn payload<'r>(&self, record: &'r [u8], tab: &'r Option<usize>) -> &'r [u8] {
///         tab.map_or(record, |tab| &record[tab + 1..])
///     }
///
///     fn headers(&self, record: &[u8], tab: &Option<usize>) -> Option<HeaderMap> {
///         let mut headers = HeaderMap::new();
///         if let Some(tab) = *tab {
///             headers.insert("tenant", String::from_utf8_lossy(&record[..tab]).into_owned());
///         }
///         Some(headers)
///     }
/// }
///
/// #[derive(Debug, Deserialize)]
/// struct Reading {
///     millivolts: u32,
/// }
///
/// #[subscriber("readings")]
/// async fn store(reading: &Reading, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
///     tracing::info!(reading.millivolts, tenant = %tenant.tenant, "recorded");
///     HandlerOutcome::ack()
/// }
///
/// #[derive(Debug, Deserialize)]
/// struct Tenant {
///     tenant: String,
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("readings", "0.1.0"))
///         .with_broker(StdioBroker::new().envelope(TenantTab), |b| {
///             b.include(store);
///         })
/// }
/// ```
pub trait Envelope: Clone + Send + Sync + 'static {
    /// What [`open`](Self::open) found in a record: what the delivery keeps beside the record to
    /// find the payload and, when they are read, the headers.
    type Opened: Send + Sync + 'static;

    /// The media type of the record this envelope writes, when the record has one of its own.
    ///
    /// The generated `AsyncAPI` document reports it beside the channel, while the message keeps
    /// the media type of its codec. An envelope that writes the payload as it is has none.
    const CONTENT_TYPE: Option<&'static str> = None;

    /// Writes a message's headers and payload into one record for `medium`.
    ///
    /// A payload that needs no envelope is returned borrowed, which costs nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when the message cannot be written in this format: a header the
    /// format has no room for, or a payload it cannot encode.
    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError>;

    /// Locates the payload in a record that was read, without parsing its headers.
    fn open(&self, record: &[u8]) -> Self::Opened;

    /// The payload of `record`, found where [`open`](Self::open) located it.
    fn payload<'r>(&self, record: &'r [u8], opened: &'r Self::Opened) -> &'r [u8];

    /// Parses the headers of `record`: an empty map for a record that carries none, `None` for a
    /// header block that does not parse.
    ///
    /// The delivery reads a `None` as no headers and logs it with the subscription and the
    /// envelope; the payload is delivered either way.
    fn headers(&self, record: &[u8], opened: &Self::Opened) -> Option<HeaderMap>;
}

/// What a sealed record travels on: it decides what the record must survive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Medium {
    /// An entry of a stream file, which keeps any bytes.
    File,
    /// A line on a pipe: UTF-8 text with no line break, and trimmed of whitespace at both ends.
    Line,
}

/// Why a message could not be sealed into a record.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SealError {
    /// The envelope has no room for headers, and the message carries one.
    #[error("the envelope carries no headers, and the message has the header '{name}'")]
    Header {
        /// The first header the message carries.
        name: String,
    },

    /// A line cannot carry the record as it is: it is not UTF-8, holds a line break, or begins
    /// or ends with whitespace.
    #[error(
        "a line cannot carry the record: it is not UTF-8 text, holds a line break, or begins or ends with whitespace"
    )]
    Line,

    /// The envelope writes header values as text, and this one is not UTF-8.
    #[error("the envelope writes header values as text, and the header '{name}' is not UTF-8")]
    NotText {
        /// The header whose value is not text.
        name: String,
    },

    /// The envelope's own encoding failed.
    #[error("the envelope failed to encode the message: {0}")]
    Encode(#[source] Box<dyn StdError + Send + Sync>),
}

/// The envelope a subscription or a publish policy uses when it names none: the broker's.
///
/// The default type parameter of [`FileStream`](crate::FileStream),
/// [`FilePublish`](struct@crate::FilePublish) and [`StdioPublish`](struct@crate::StdioPublish).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FromBroker;

/// The envelope a mount site resolves to, given the broker's.
///
/// [`FromBroker`] resolves to the broker's envelope; any [`Envelope`] resolves to itself.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an envelope",
    label = "an envelope or `FromBroker` goes here",
    note = "name one of `Rs`, `Framed::new(codec)`, `NoOp`, or a type implementing `Envelope`"
)]
pub trait EnvelopeChoice<BrokerEnvelope: Envelope>: Clone + Send + Sync + 'static {
    /// The envelope the choice resolves to.
    type Envelope: Envelope;

    /// The envelope itself, given the broker's.
    fn resolve(&self, broker: &BrokerEnvelope) -> Self::Envelope;
}

impl<BrokerEnvelope: Envelope> EnvelopeChoice<BrokerEnvelope> for FromBroker {
    type Envelope = BrokerEnvelope;

    fn resolve(&self, broker: &BrokerEnvelope) -> BrokerEnvelope {
        broker.clone()
    }
}

impl<BrokerEnvelope: Envelope, Chosen: Envelope> EnvelopeChoice<BrokerEnvelope> for Chosen {
    type Envelope = Self;

    fn resolve(&self, _broker: &BrokerEnvelope) -> Self {
        self.clone()
    }
}

/// Seals a message for `medium`, and refuses a record a line cannot carry: what reaches the
/// transport is what the reader at the other end reads back.
pub(crate) fn seal<'p, Env: Envelope>(
    envelope: &Env,
    headers: &HeaderMap,
    payload: &'p [u8],
    medium: Medium,
) -> Result<Cow<'p, [u8]>, SealError> {
    let record = envelope.seal(headers, payload, medium)?;
    if medium == Medium::Line && !fits_a_line(&record) {
        return Err(SealError::Line);
    }
    Ok(record)
}

/// Whether a line of the stdio client carries `record` unchanged.
///
/// The line is text. The client writes the record after the line's meta fields and ends it with
/// a newline, and the reader at the other end of the pipe splits on newlines and trims the record
/// it finds. So a record that is not UTF-8, holds a newline, or begins or ends with whitespace
/// would arrive cut into pieces or trimmed.
pub(crate) fn fits_a_line(record: &[u8]) -> bool {
    std::str::from_utf8(record).is_ok_and(|text| !text.contains('\n') && text.trim() == text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_refuses_a_record_it_would_change() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        for raw in [b"first\nsecond".as_slice(), b" padded", &[0xff, 0xfe]] {
            assert!(matches!(
                seal(&NoOp, &HeaderMap::new(), raw, Medium::Line),
                Err(SealError::Line)
            ));
            // A stream file keeps any bytes.
            assert_eq!(
                seal(&NoOp, &HeaderMap::new(), raw, Medium::File)
                    .expect("a file keeps any bytes")
                    .as_ref(),
                raw
            );
        }
    }

    #[test]
    fn a_mount_site_without_an_envelope_takes_the_brokers() {
        assert_eq!(
            format!("{:?}", EnvelopeChoice::<NoOp>::resolve(&FromBroker, &NoOp)),
            "NoOp"
        );
        assert_eq!(
            format!("{:?}", EnvelopeChoice::<Rs>::resolve(&NoOp, &Rs)),
            "NoOp"
        );
    }
}
