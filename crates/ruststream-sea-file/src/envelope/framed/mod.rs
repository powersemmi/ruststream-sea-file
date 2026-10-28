//! [`Framed`]: one document of a codec per message, headers and payload inside it.
//!
//! The document follows the convention of the `CloudEvents` formats: a JSON document carries a
//! payload that is JSON as a value and any other as base64, and any other codec carries it as a
//! byte string. Each form lives in a module of its own.

use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::str;

use bytes::Bytes;
use ruststream::HeaderMap;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
use ruststream::codec::Codec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;
use serde::Deserializer;
use serde::de::{MapAccess, Visitor};

use super::{Envelope, Medium, SealError};

mod binary;
#[cfg(feature = "json")]
mod json;

/// One document of a codec per message: `headers` and exactly one payload field, nothing else.
///
/// `headers` maps each header name to its value as text. The payload follows the convention of
/// the `CloudEvents` formats. With [`JsonCodec`](ruststream::codec::JsonCodec), a payload that is
/// a JSON text is embedded as a value in `payload`, byte for byte, and any other payload is a
/// base64 string in `payload_base64`. Each line on a pipe is then a JSON object any line tool
/// reads:
///
/// ```text
/// {"headers":{"event":"created"},"payload":{"id":7}}
/// {"headers":{"event":"created"},"payload_base64":"CAc="}
/// ```
///
/// A JSON document on a pipe is one line, so a JSON payload that holds a raw line break travels
/// as `payload_base64` there; in a stream file it is embedded. Any other codec
/// ([`CborCodec`](ruststream::codec::CborCodec), [`MsgpackCodec`](ruststream::codec::MsgpackCodec),
/// a codec of the service) carries the payload as a byte string in `payload`; such a document is
/// not text, so a pipe refuses it with [`SealError::Line`]. A header whose value is not UTF-8
/// fails the publish with [`SealError::NotText`].
///
/// Every message is framed, headers or not. A record that is not a document in exactly this shape
/// reads as a payload with no headers; a document whose header block does not parse reads as its
/// payload with no headers, and the delivery logs it. The generated `AsyncAPI` document reports
/// the codec's media type as the envelope's. The codec implements [`FrameCodec`].
///
/// Framing costs only the subscriptions and publishers that choose it. A JSON publish checks that
/// the payload is a JSON text, one pass over it, and a payload that is not one is base64-encoded.
/// A JSON delivery finds an embedded payload inside the record, with no copy, and decodes a base64
/// one into a buffer of its own; any other codec's delivery copies the payload out of the
/// document.
///
/// # Examples
///
/// A pipeline stage that reads and writes JSON lines:
///
/// ```
/// use ruststream::codec::JsonCodec;
/// use ruststream_sea_file::stdio::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Debug, Outgoing, Serialize)]
/// #[outgoing(name = "receipts")]
/// struct Receipt {
///     order: u64,
/// }
///
/// #[subscriber("orders", reply)]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("confirm", "0.1.0"))
///         .with_broker(StdioBroker::new().envelope(Framed::new(JsonCodec)), |b| {
///             b.include(confirm);
///         })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Framed<Format> {
    codec: Format,
}

impl<Format: FrameCodec> Framed<Format> {
    /// Frames every message as one document of `codec`.
    #[must_use]
    pub const fn new(codec: Format) -> Self {
        Self { codec }
    }
}

/// A codec [`Framed`] writes its documents in: where the payload goes in one, and how it is
/// found again.
///
/// The core's JSON, CBOR and msgpack codecs implement it under this crate's features of the same
/// name. The provided methods write the payload as a byte string in `payload`, the form of a
/// binary codec, so a codec of the service implements the trait with an empty body. JSON
/// overrides them with the `payload` / `payload_base64` pair of the `CloudEvents` JSON format.
///
/// # Examples
///
/// ```
/// use ruststream::BytesMut;
/// use ruststream::codec::{CborCodec, Codec, CodecError};
/// use ruststream_sea_file::envelope::FrameCodec;
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Serialize;
/// use serde::de::DeserializeOwned;
///
/// /// CBOR under the media type an analytics team registered for its documents.
/// #[derive(Debug, Clone, Copy, Default)]
/// struct AnalyticsCbor;
///
/// impl Codec for AnalyticsCbor {
///     const CONTENT_TYPE: &'static str = "application/vnd.acme.analytics+cbor";
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         CborCodec.encode(value)
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         CborCodec.decode(bytes)
///     }
/// }
///
/// impl FrameCodec for AnalyticsCbor {}
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("analytics", "0.1.0")).with_broker(
///         FileBroker::new("/var/lib/analytics.ss").envelope(Framed::new(AnalyticsCbor)),
///         |_b| {},
///     )
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a codec `Framed` writes documents in",
    note = "enable this crate's `json`, `cbor` or `msgpack` feature for the core's codec, or \
            implement `ruststream_sea_file::envelope::FrameCodec` for a codec of the service"
)]
pub trait FrameCodec: Codec + Clone + 'static {
    /// Writes the document of a message for `medium`. Every header value is UTF-8 by now.
    ///
    /// # Errors
    ///
    /// [`SealError::Encode`] when the codec cannot encode the document.
    fn frame(
        &self,
        headers: &HeaderMap,
        payload: &[u8],
        medium: Medium,
    ) -> Result<Vec<u8>, SealError> {
        let _ = medium;
        binary::frame(self, headers, payload)
    }

    /// Where the payload of a document is, or [`FramedOpened::foreign`] for a record that is no
    /// document.
    fn frame_payload(&self, record: &[u8]) -> FramedOpened {
        binary::payload(self, record)
    }

    /// The headers of a document; `None` for a header block that does not parse.
    fn frame_headers(&self, record: &[u8]) -> Option<HeaderMap> {
        binary::headers(self, record)
    }
}

#[cfg(feature = "cbor")]
impl FrameCodec for CborCodec {}

#[cfg(feature = "msgpack")]
impl FrameCodec for MsgpackCodec {}

/// What [`Framed`] found in a record: where its payload is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FramedOpened(Opened);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Opened {
    /// Not a document: the record is the payload.
    Foreign,
    /// A document whose payload sits inside the record, at this range.
    Within(Range<usize>),
    /// A document whose payload was decoded into a buffer of its own.
    Decoded(Vec<u8>),
}

impl FramedOpened {
    /// A record that is no document: the whole record is the payload, with no headers.
    #[must_use]
    pub const fn foreign() -> Self {
        Self(Opened::Foreign)
    }

    /// A document whose payload is the bytes of the record at `range`.
    #[must_use]
    pub const fn within(range: Range<usize>) -> Self {
        Self(Opened::Within(range))
    }

    /// A document whose payload was decoded out of the record.
    #[must_use]
    pub const fn decoded(payload: Vec<u8>) -> Self {
        Self(Opened::Decoded(payload))
    }
}

impl<Format: FrameCodec> Envelope for Framed<Format> {
    type Opened = FramedOpened;

    const CONTENT_TYPE: Option<&'static str> = Some(Format::CONTENT_TYPE);

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        // Checked before encoding: a serializer that met the value half way through the document
        // could not say which header it was.
        if let Some((name, _)) = headers
            .iter()
            .find(|(_, value)| str::from_utf8(value).is_err())
        {
            return Err(SealError::NotText {
                name: name.to_string(),
            });
        }
        self.codec.frame(headers, payload, medium).map(Cow::Owned)
    }

    fn open(&self, record: &[u8]) -> FramedOpened {
        self.codec.frame_payload(record)
    }

    fn payload<'r>(&self, record: &'r [u8], opened: &'r FramedOpened) -> &'r [u8] {
        match &opened.0 {
            Opened::Foreign => record,
            // A range a codec of the service reported past the record reads as the whole record.
            Opened::Within(range) => record.get(range.clone()).unwrap_or(record),
            Opened::Decoded(payload) => payload,
        }
    }

    fn headers(&self, record: &[u8], opened: &FramedOpened) -> Option<HeaderMap> {
        if opened.0 == Opened::Foreign {
            return Some(HeaderMap::new());
        }
        self.codec.frame_headers(record)
    }
}

/// Reads a header map of text names to text values.
fn text_headers<'de, D: Deserializer<'de>>(deserializer: D) -> Result<HeaderMap, D::Error> {
    deserializer.deserialize_map(TextHeadersVisitor)
}

struct TextHeadersVisitor;

impl<'de> Visitor<'de> for TextHeadersVisitor {
    type Value = HeaderMap;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map of header names to text values")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<HeaderMap, A::Error> {
        let mut headers = HeaderMap::new();
        while let Some((name, value)) = map.next_entry::<String, String>()? {
            headers.insert(name, Bytes::from(value.into_bytes()));
        }
        Ok(headers)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cbor")]
    use ruststream::codec::CborCodec;
    use ruststream::codec::JsonCodec;

    use super::*;

    const JSON: Framed<JsonCodec> = Framed::new(JsonCodec);

    fn created() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("event", "created");
        headers
    }

    /// Opens a record the way a delivery does, parsing its headers too.
    fn decode<Env: Envelope>(envelope: &Env, record: &[u8]) -> (HeaderMap, Vec<u8>) {
        let opened = envelope.open(record);
        (
            envelope.headers(record, &opened).unwrap_or_default(),
            envelope.payload(record, &opened).to_vec(),
        )
    }

    fn sealed<Env: Envelope>(envelope: &Env, payload: &[u8], medium: Medium) -> Vec<u8> {
        envelope
            .seal(&created(), payload, medium)
            .expect("the document seals")
            .into_owned()
    }

    #[test]
    fn a_json_payload_is_embedded_as_a_value_and_read_back_byte_exact() {
        let payload = br#"{"id": 7, "tags": ["a"]}"#;
        let record = sealed(&JSON, payload, Medium::Line);
        assert_eq!(
            str::from_utf8(&record).expect("a JSON document is text"),
            r#"{"headers":{"event":"created"},"payload":{"id": 7, "tags": ["a"]}}"#
        );
        let (headers, read) = decode(&JSON, &record);
        assert_eq!(headers.get_str("event"), Some("created"));
        assert_eq!(read, payload);
    }

    #[test]
    fn a_payload_that_is_not_json_travels_as_base64() {
        let record = sealed(&JSON, &[0x08, 0x07], Medium::File);
        assert_eq!(
            str::from_utf8(&record).expect("a JSON document is text"),
            r#"{"headers":{"event":"created"},"payload_base64":"CAc="}"#
        );
        let (headers, read) = decode(&JSON, &record);
        assert_eq!(headers.get_str("event"), Some("created"));
        assert_eq!(read, [0x08, 0x07]);
    }

    #[test]
    fn json_that_is_not_byte_exact_as_a_value_travels_as_base64() {
        // Surrounding whitespace would be lost in a value, and a line cannot hold a raw newline.
        for (payload, medium) in [
            (b" {\"id\":7}".as_slice(), Medium::File),
            (b"{\n\"id\":7}".as_slice(), Medium::Line),
            (b"plain text".as_slice(), Medium::File),
        ] {
            let record = sealed(&JSON, payload, medium);
            let text = str::from_utf8(&record).expect("a JSON document is text");
            assert!(text.contains("\"payload_base64\":"), "{text}");
            assert_eq!(decode(&JSON, &record).1, payload);
        }
        // A file keeps the newline, so the value is embedded there.
        let record = sealed(&JSON, b"{\n\"id\":7}", Medium::File);
        assert!(record.ends_with(b"\"payload\":{\n\"id\":7}}"));
    }

    #[test]
    fn a_json_null_payload_round_trips() {
        let record = sealed(&JSON, b"null", Medium::File);
        assert!(record.ends_with(b"\"payload\":null}"));
        assert_eq!(decode(&JSON, &record).1, b"null");
    }

    #[test]
    fn a_header_value_that_is_not_text_is_refused_by_name() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", Bytes::from_static(&[0xff, 0xfe]));
        let err = JSON
            .seal(&headers, b"{}", Medium::File)
            .expect_err("a JSON document holds header values as text");
        assert!(matches!(&err, SealError::NotText { name } if name == "x-binary"));
    }

    #[test]
    fn every_message_is_framed_so_a_lookalike_payload_stays_a_payload() {
        let lookalike = br#"{"headers":{"x":"y"},"payload":"z"}"#;
        let record = JSON
            .seal(&HeaderMap::new(), lookalike, Medium::File)
            .expect("a JSON document seals");
        let (read, payload) = decode(&JSON, &record);
        assert!(read.is_empty());
        assert_eq!(payload, lookalike);
    }

    #[test]
    fn a_record_that_is_not_a_document_reads_as_a_bare_payload() {
        for foreign in [
            b"plain text".as_slice(),
            br#"{"id":7}"#,
            br#"{"headers":{},"payload":1,"payload_base64":"AQ=="}"#,
            br#"{"headers":{},"payload":1,"extra":1}"#,
            br#"{"headers":{},"payload_base64":"not base64!"}"#,
            br#"{"headers":{}}"#,
            br#"{"payload":1}"#,
        ] {
            let (read, payload) = decode(&JSON, foreign);
            assert!(read.is_empty(), "{foreign:?}");
            assert_eq!(payload, foreign);
        }
    }

    #[test]
    fn a_malformed_header_block_reads_as_no_headers_before_the_payload() {
        let record = br#"{"headers":{"x":1},"payload":{"id":7}}"#;
        let (headers, payload) = decode(&JSON, record);
        assert!(headers.is_empty());
        assert_eq!(payload, br#"{"id":7}"#);
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn a_cbor_document_carries_header_values_as_text() {
        let record = sealed(&Framed::new(CborCodec), b"x", Medium::File);
        // A text string of seven bytes is headed 0x67 in CBOR; a byte string would be 0x47.
        assert!(record.windows(8).any(|window| window == b"\x67created"));
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn a_cbor_document_carries_the_payload_as_a_byte_string() {
        let cbor = Framed::new(CborCodec);
        let payload = br#"{"id":7}"#;
        let record = sealed(&cbor, payload, Medium::File);
        // A byte string of eight bytes is headed 0x48 in CBOR; a text string would be 0x68.
        let mut byte_string = vec![0x48];
        byte_string.extend_from_slice(payload);
        assert!(
            record
                .windows(9)
                .any(|window| window == byte_string.as_slice())
        );
        let (headers, read) = decode(&cbor, &record);
        assert_eq!(headers.get_str("event"), Some("created"));
        assert_eq!(read, payload);
    }

    #[test]
    fn the_envelope_reports_the_media_type_of_its_codec() {
        assert_eq!(
            <Framed<JsonCodec> as Envelope>::CONTENT_TYPE,
            Some("application/json")
        );
        assert_eq!(<super::super::Rs as Envelope>::CONTENT_TYPE, None);
    }

    /// A codec of the service implements the trait with an empty body and gets the byte-string
    /// form.
    #[cfg(feature = "cbor")]
    #[test]
    fn a_codec_of_the_service_frames_the_payload_as_a_byte_string() {
        use ruststream::BytesMut;
        use ruststream::codec::CodecError;
        use serde::Serialize;
        use serde::de::DeserializeOwned;

        #[derive(Debug, Clone, Copy)]
        struct Analytics;

        impl Codec for Analytics {
            const CONTENT_TYPE: &'static str = "application/vnd.acme.analytics+cbor";

            fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
                CborCodec.encode(value)
            }

            fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
                CborCodec.decode(bytes)
            }
        }

        impl FrameCodec for Analytics {}

        let framed = Framed::new(Analytics);
        let record = sealed(&framed, b"{\"id\":7}", Medium::File);
        let mut byte_string = vec![0x48];
        byte_string.extend_from_slice(b"{\"id\":7}");
        assert!(
            record
                .windows(9)
                .any(|window| window == byte_string.as_slice())
        );
        let (headers, payload) = decode(&framed, &record);
        assert_eq!(headers.get_str("event"), Some("created"));
        assert_eq!(payload, b"{\"id\":7}");
        assert_eq!(
            <Framed<Analytics> as Envelope>::CONTENT_TYPE,
            Some("application/vnd.acme.analytics+cbor")
        );
    }
}
