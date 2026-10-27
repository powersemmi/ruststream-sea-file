//! [`Framed`]: one document of a codec per message, headers and payload inside it.

use std::borrow::Cow;
use std::fmt;
use std::str;

use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Envelope, Medium, SealError};

/// One document of a codec per message: an object with exactly two fields, `headers` and
/// `payload`.
///
/// `headers` maps each header name to its value, and `payload` holds the payload. A value or a
/// payload that is UTF-8 is written as text, any other as the codec's bytes. With
/// [`JsonCodec`](ruststream::codec::JsonCodec) on a pipe, each line is a JSON object any line tool
/// reads:
///
/// ```text
/// {"headers":{"tenant":"acme"},"payload":"{\"id\":7}"}
/// ```
///
/// Every message is framed, headers or not. A record that is not such a document reads as a
/// payload with no headers. A pipe carries only a codec whose documents are text, so a binary
/// codec there fails the publish with [`SealError::Line`]. The generated `AsyncAPI` document
/// reports the codec's media type as the envelope's.
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
pub struct Framed<DocumentCodec> {
    codec: DocumentCodec,
}

impl<DocumentCodec: Codec> Framed<DocumentCodec> {
    /// Frames every message as one document of `codec`.
    #[must_use]
    pub const fn new(codec: DocumentCodec) -> Self {
        Self { codec }
    }
}

/// What [`Framed`] found in a record: the payload of a document, or a bare payload.
#[derive(Debug)]
pub struct FramedOpened(Option<Vec<u8>>);

impl<DocumentCodec> Envelope for Framed<DocumentCodec>
where
    DocumentCodec: Codec + Clone + 'static,
{
    type Opened = FramedOpened;

    const CONTENT_TYPE: Option<&'static str> = Some(DocumentCodec::CONTENT_TYPE);

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        _medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        let document = Document {
            headers: HeadersOut(headers),
            payload: Text(payload),
        };
        let encoded = self
            .codec
            .encode(&document)
            .map_err(|err| SealError::Encode(Box::new(err)))?;
        Ok(Cow::Owned(Vec::from(encoded)))
    }

    fn open(&self, record: &[u8]) -> FramedOpened {
        FramedOpened(
            self.codec
                .decode::<Located>(record)
                .ok()
                .map(|located| located.payload.0),
        )
    }

    fn payload<'r>(&self, record: &'r [u8], opened: &'r FramedOpened) -> &'r [u8] {
        opened.0.as_deref().unwrap_or(record)
    }

    fn headers(&self, record: &[u8], opened: &FramedOpened) -> HeaderMap {
        if opened.0.is_none() {
            return HeaderMap::new();
        }
        // `open` checked the shape of the headers already, so they parse.
        self.codec
            .decode::<Parsed>(record)
            .map(|parsed| parsed.headers.0)
            .unwrap_or_default()
    }
}

/// The document a message is written as.
#[derive(Serialize)]
struct Document<'a> {
    headers: HeadersOut<'a>,
    payload: Text<'a>,
}

/// A document read for its payload: the headers are checked for their shape and skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Located {
    #[serde(rename = "headers")]
    _headers: HeadersShape,
    payload: Blob,
}

/// A document read for its headers: the payload is skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parsed {
    headers: HeadersIn,
    #[serde(rename = "payload")]
    _payload: IgnoredAny,
}

/// Bytes written as text when they are UTF-8, and as the codec's bytes otherwise.
struct Text<'a>(&'a [u8]);

impl Serialize for Text<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match str::from_utf8(self.0) {
            Ok(text) => serializer.serialize_str(text),
            Err(_) => serializer.serialize_bytes(self.0),
        }
    }
}

/// The header map, written as a map of names to [`Text`] values.
struct HeadersOut<'a>(&'a HeaderMap);

impl Serialize for HeadersOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.0.iter() {
            map.serialize_entry(name, &Text(value))?;
        }
        map.end()
    }
}

/// Bytes read back from text, from the codec's bytes, or from a sequence of byte values (the
/// form a text codec writes bytes in).
struct Blob(Vec<u8>);

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BlobVisitor).map(Self)
    }
}

struct BlobVisitor;

impl<'de> Visitor<'de> for BlobVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("text or bytes")
    }

    fn visit_str<E>(self, text: &str) -> Result<Vec<u8>, E> {
        Ok(text.as_bytes().to_vec())
    }

    fn visit_string<E>(self, text: String) -> Result<Vec<u8>, E> {
        Ok(text.into_bytes())
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
        Ok(bytes.to_vec())
    }

    fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(bytes)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(bytes)
    }
}

/// A header map checked for its shape without keeping it: text names, [`Blob`] values.
struct HeadersShape;

impl<'de> Deserialize<'de> for HeadersShape {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_map(HeadersVisitor(false))
            .map(|_| Self)
    }
}

/// The header map, read back.
struct HeadersIn(HeaderMap);

impl<'de> Deserialize<'de> for HeadersIn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(HeadersVisitor(true)).map(Self)
    }
}

/// Reads a header map; `.0` says whether it keeps what it reads.
struct HeadersVisitor(bool);

impl<'de> Visitor<'de> for HeadersVisitor {
    type Value = HeaderMap;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map of header names to text or bytes")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<HeaderMap, A::Error> {
        let mut headers = HeaderMap::new();
        if !self.0 {
            while map.next_entry::<Name, Skipped>()?.is_some() {}
            return Ok(headers);
        }
        while let Some((name, value)) = map.next_entry::<String, Blob>()? {
            headers.insert(name, Bytes::from(value.0));
        }
        Ok(headers)
    }
}

/// A header name, checked to be text and not kept.
struct Name;

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(NameVisitor).map(|()| Self)
    }
}

struct NameVisitor;

impl Visitor<'_> for NameVisitor {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a header name")
    }

    fn visit_str<E>(self, _name: &str) -> Result<(), E> {
        Ok(())
    }
}

/// A header value, checked to be text or bytes and not kept.
struct Skipped;

impl<'de> Deserialize<'de> for Skipped {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SkippedVisitor).map(|()| Self)
    }
}

struct SkippedVisitor;

impl<'de> Visitor<'de> for SkippedVisitor {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("text or bytes")
    }

    fn visit_str<E>(self, _text: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_bytes<E>(self, _bytes: &[u8]) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element::<u8>()?.is_some() {}
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ruststream::codec::JsonCodec;

    use super::*;

    const JSON: Framed<JsonCodec> = Framed::new(JsonCodec);

    /// Opens a record the way a delivery does, parsing its headers too.
    fn decode(record: &[u8]) -> (HeaderMap, Vec<u8>) {
        let opened = JSON.open(record);
        (
            JSON.headers(record, &opened),
            JSON.payload(record, &opened).to_vec(),
        )
    }

    #[test]
    fn a_json_document_is_one_line_with_the_headers_and_the_payload_inside() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");
        let record = JSON
            .seal(&headers, b"{\"id\":7}", Medium::Line)
            .expect("a JSON document seals");
        assert_eq!(
            str::from_utf8(&record).expect("a JSON document is text"),
            r#"{"headers":{"tenant":"acme"},"payload":"{\"id\":7}"}"#
        );
        let (read, payload) = decode(&record);
        assert_eq!(read.get_str("tenant"), Some("acme"));
        assert_eq!(payload, b"{\"id\":7}");
    }

    #[test]
    fn bytes_that_are_not_text_round_trip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", Bytes::from_static(&[0x00, 0xff, b'\n']));
        let raw = [0xfe, 0x00, b'\r', 0x80];
        let record = JSON
            .seal(&headers, &raw, Medium::File)
            .expect("a JSON document seals");
        let (read, payload) = decode(&record);
        assert_eq!(read.get("x-binary"), Some([0x00, 0xff, b'\n'].as_slice()));
        assert_eq!(payload, raw);
    }

    #[test]
    fn every_message_is_framed_so_a_lookalike_payload_stays_a_payload() {
        let lookalike = br#"{"headers":{"x":"y"},"payload":"z"}"#;
        let record = JSON
            .seal(&HeaderMap::new(), lookalike, Medium::File)
            .expect("a JSON document seals");
        let (read, payload) = decode(&record);
        assert!(read.is_empty());
        assert_eq!(payload, lookalike);
    }

    #[test]
    fn a_record_that_is_not_a_document_reads_as_a_bare_payload() {
        for foreign in [
            b"plain text".as_slice(),
            br#"{"id":7}"#,
            br#"{"headers":{},"payload":"x","extra":1}"#,
            br#"{"headers":{"x":{"nested":1}},"payload":"x"}"#,
            br#"{"payload":"x"}"#,
        ] {
            let (read, payload) = decode(foreign);
            assert!(read.is_empty(), "{foreign:?}");
            assert_eq!(payload, foreign);
        }
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
