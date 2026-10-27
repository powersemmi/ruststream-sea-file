//! [`Framed`]: one document of a codec per message, headers and payload inside it.
//!
//! The document follows the convention of the `CloudEvents` formats: a JSON document carries a
//! payload that is JSON as a value and any other as base64, and a binary codec carries it as a
//! byte string. Each form lives in a module of its own.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
#[cfg(feature = "json")]
use std::ops::Range;

use bytes::Bytes;
use ruststream::HeaderMap;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
use ruststream::codec::Codec;
#[cfg(feature = "json")]
use ruststream::codec::JsonCodec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{Envelope, Medium, SealError};

mod binary;
#[cfg(feature = "json")]
mod json;

/// One document of a codec per message: `headers` and exactly one payload field, nothing else.
///
/// The payload follows the convention of the `CloudEvents` formats. With
/// [`JsonCodec`](ruststream::codec::JsonCodec), a payload that is a JSON text is embedded as a
/// value in `payload`, byte for byte, and any other payload is a base64 string in
/// `payload_base64`. Each line on a pipe is then a JSON object any line tool reads:
///
/// ```text
/// {"headers":{"event":"created"},"payload":{"id":7}}
/// {"headers":{"event":"created"},"payload_base64":"CAc="}
/// ```
///
/// A JSON document on a pipe is one line, so a JSON payload that holds a raw line break travels
/// as `payload_base64` there; in a stream file it is embedded. JSON header values are text, and a
/// header whose value is not UTF-8 fails the publish with [`SealError::NotText`]. With a binary
/// codec ([`CborCodec`](ruststream::codec::CborCodec),
/// [`MsgpackCodec`](ruststream::codec::MsgpackCodec)) the payload and the header values are
/// byte strings; such a document is not text, so a pipe refuses it with [`SealError::Line`].
///
/// Every message is framed, headers or not. A record that is not a document in exactly this shape
/// reads as a payload with no headers. The generated `AsyncAPI` document reports the codec's media
/// type as the envelope's. The codec is a [`DocumentCodec`], which the core's codecs are under
/// this crate's features of the same name (`json`, `cbor`, `msgpack`).
///
/// Framing costs only the subscriptions and publishers that choose it. A JSON publish checks that
/// the payload is a JSON text, one pass over it, and a payload that is not one is base64-encoded.
/// A JSON delivery finds an embedded payload inside the record, with no copy, and decodes a base64
/// one into a buffer of its own; a binary delivery copies the payload out of the document.
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

impl<Format: DocumentCodec> Framed<Format> {
    /// Frames every message as one document of `codec`.
    #[must_use]
    pub const fn new(codec: Format) -> Self {
        Self { codec }
    }
}

/// A codec [`Framed`] writes its documents in, and the form its documents carry the payload in.
///
/// The core's codecs implement it under this crate's features of the same name. A codec of a
/// service's own names the form of the documents it writes.
///
/// # Examples
///
/// A JSON codec under the vendor media type a contract was published with frames its documents
/// the way [`JsonCodec`](ruststream::codec::JsonCodec) does:
///
/// ```
/// use ruststream::BytesMut;
/// use ruststream::codec::{Codec, CodecError};
/// use ruststream_sea_file::envelope::{DocumentCodec, DocumentForm};
/// use ruststream_sea_file::file::prelude::*;
/// use serde::de::DeserializeOwned;
/// use serde::Serialize;
///
/// #[derive(Debug, Clone, Copy, Default)]
/// struct OrdersJson;
///
/// impl Codec for OrdersJson {
///     const CONTENT_TYPE: &'static str = "application/vnd.orders.v1+json";
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         let bytes =
///             serde_json::to_vec(value).map_err(|error| CodecError::Encode(Box::new(error)))?;
///         Ok(BytesMut::from(bytes.as_slice()))
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         serde_json::from_slice(bytes).map_err(|error| CodecError::Decode(Box::new(error)))
///     }
/// }
///
/// impl DocumentCodec for OrdersJson {
///     const FORM: DocumentForm = DocumentForm::Json;
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         FileBroker::new("/var/lib/orders.ss").envelope(Framed::new(OrdersJson)),
///         |_b| {},
///     )
/// }
/// ```
pub trait DocumentCodec: Codec + Clone + 'static {
    /// How the documents of this codec carry the payload.
    const FORM: DocumentForm;
}

/// How a [`Framed`] document carries the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DocumentForm {
    /// A JSON text as a value in `payload`, any other payload as base64 in `payload_base64`.
    #[cfg(feature = "json")]
    Json,
    /// A byte string in `payload`, read and written through the codec.
    Binary,
}

#[cfg(feature = "json")]
impl DocumentCodec for JsonCodec {
    const FORM: DocumentForm = DocumentForm::Json;
}

#[cfg(feature = "cbor")]
impl DocumentCodec for CborCodec {
    const FORM: DocumentForm = DocumentForm::Binary;
}

#[cfg(feature = "msgpack")]
impl DocumentCodec for MsgpackCodec {
    const FORM: DocumentForm = DocumentForm::Binary;
}

/// What [`Framed`] found in a record: the payload of a document, or a bare payload.
#[derive(Debug)]
pub struct FramedOpened(Opened);

#[derive(Debug)]
enum Opened {
    /// Not a document: the record is the payload.
    Foreign,
    /// A document whose payload sits inside the record, at this range.
    #[cfg(feature = "json")]
    InRecord(Range<usize>),
    /// A document whose payload was decoded into a buffer of its own.
    Decoded(Vec<u8>),
}

impl<Format: DocumentCodec> Envelope for Framed<Format> {
    type Opened = FramedOpened;

    const CONTENT_TYPE: Option<&'static str> = Some(Format::CONTENT_TYPE);

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        let record = match Format::FORM {
            #[cfg(feature = "json")]
            DocumentForm::Json => json::seal(headers, payload, medium)?,
            // A binary document is the same in a file and on a line; a line refuses it.
            DocumentForm::Binary => {
                let _ = medium;
                binary::seal(&self.codec, headers, payload)?
            }
        };
        Ok(Cow::Owned(record))
    }

    fn open(&self, record: &[u8]) -> FramedOpened {
        let opened = match Format::FORM {
            #[cfg(feature = "json")]
            DocumentForm::Json => json::open(record),
            DocumentForm::Binary => binary::open(&self.codec, record),
        };
        FramedOpened(opened.unwrap_or(Opened::Foreign))
    }

    fn payload<'r>(&self, record: &'r [u8], opened: &'r FramedOpened) -> &'r [u8] {
        match &opened.0 {
            Opened::Foreign => record,
            #[cfg(feature = "json")]
            Opened::InRecord(range) => &record[range.clone()],
            Opened::Decoded(payload) => payload,
        }
    }

    fn headers(&self, record: &[u8], opened: &FramedOpened) -> HeaderMap {
        if matches!(opened.0, Opened::Foreign) {
            return HeaderMap::new();
        }
        // `open` checked the shape of the headers already, so they parse.
        let parsed = match Format::FORM {
            #[cfg(feature = "json")]
            DocumentForm::Json => json::headers(record),
            DocumentForm::Binary => binary::headers(&self.codec, record),
        };
        parsed.unwrap_or_default()
    }
}

/// A header map, read one entry at a time: kept, or only checked for its shape without
/// keeping anything.
struct HeadersVisitor<Value> {
    keep: bool,
    value: PhantomData<Value>,
}

/// A header value as a form reads it, and the same value checked without being kept.
trait HeaderValue: for<'de> Deserialize<'de> {
    /// The value checked for its shape and dropped.
    type Checked: for<'de> Deserialize<'de>;

    /// The bytes the header map stores.
    fn into_bytes(self) -> Vec<u8>;
}

impl<'de, Value: HeaderValue> Visitor<'de> for HeadersVisitor<Value> {
    type Value = HeaderMap;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map of header names to their values")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<HeaderMap, A::Error> {
        let mut headers = HeaderMap::new();
        if !self.keep {
            while map.next_entry::<CheckedName, Value::Checked>()?.is_some() {}
            return Ok(headers);
        }
        while let Some((name, value)) = map.next_entry::<String, Value>()? {
            headers.insert(name, Bytes::from(value.into_bytes()));
        }
        Ok(headers)
    }
}

/// Reads a header map through [`HeadersVisitor`]: kept when `keep`, checked otherwise.
fn header_map<'de, Value, D>(deserializer: D, keep: bool) -> Result<HeaderMap, D::Error>
where
    Value: HeaderValue,
    D: Deserializer<'de>,
{
    deserializer.deserialize_map(HeadersVisitor::<Value> {
        keep,
        value: PhantomData,
    })
}

/// A header name, checked to be text and not kept.
struct CheckedName;

impl<'de> Deserialize<'de> for CheckedName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(TextVisitor).map(|()| Self)
    }
}

/// Accepts text and keeps nothing of it.
struct TextVisitor;

impl Visitor<'_> for TextVisitor {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("text")
    }

    fn visit_str<E>(self, _text: &str) -> Result<(), E> {
        Ok(())
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
            envelope.headers(record, &opened),
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
            br#"{"headers":{"x":1},"payload":1}"#,
            br#"{"payload":1}"#,
        ] {
            let (read, payload) = decode(&JSON, foreign);
            assert!(read.is_empty(), "{foreign:?}");
            assert_eq!(payload, foreign);
        }
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
}
