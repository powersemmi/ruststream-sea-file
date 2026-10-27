//! The byte-string form of a [`Framed`](super::Framed) document, the provided form of a
//! [`FrameCodec`]: the payload a byte string in `payload`, read and written through the codec.

use std::fmt;
use std::str;

use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{IgnoredAny, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{FramedOpened, text_headers};
use crate::envelope::SealError;

/// Writes one document through the codec.
pub(super) fn frame<Format: Codec>(
    codec: &Format,
    headers: &HeaderMap,
    payload: &[u8],
) -> Result<Vec<u8>, SealError> {
    let document = Document {
        headers: TextHeaders(headers),
        payload: ByteString(payload),
    };
    codec
        .encode(&document)
        .map(Vec::from)
        .map_err(|err| SealError::Encode(Box::new(err)))
}

/// Copies the payload out of a document; [`FramedOpened::foreign`] for a record that is not a
/// document in exactly this shape. The header block is skipped, not parsed.
pub(super) fn payload<Format: Codec>(codec: &Format, record: &[u8]) -> FramedOpened {
    codec.decode::<Located>(record).map_or_else(
        |_| FramedOpened::foreign(),
        |located| FramedOpened::decoded(located.payload.0),
    )
}

/// Parses the headers of a document; `None` for a header block that does not parse.
pub(super) fn headers<Format: Codec>(codec: &Format, record: &[u8]) -> Option<HeaderMap> {
    codec
        .decode::<Parsed>(record)
        .ok()
        .map(|parsed| parsed.headers)
}

/// The document a message is written as.
#[derive(Serialize)]
struct Document<'a> {
    headers: TextHeaders<'a>,
    payload: ByteString<'a>,
}

/// Bytes written as a byte string.
struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// The header map, written as a map of names to text values. The envelope checked every value
/// is UTF-8 before the document is written.
pub(super) struct TextHeaders<'a>(pub(super) &'a HeaderMap);

impl Serialize for TextHeaders<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.0.iter() {
            map.serialize_entry(name, str::from_utf8(value).unwrap_or_default())?;
        }
        map.end()
    }
}

/// A document read for its payload: the headers are skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Located {
    #[serde(rename = "headers")]
    _headers: IgnoredAny,
    payload: Bytes,
}

/// A document read for its headers: the payload is skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parsed {
    #[serde(deserialize_with = "text_headers")]
    headers: HeaderMap,
    #[serde(rename = "payload")]
    _payload: IgnoredAny,
}

/// A byte string, read back; nothing else is accepted.
struct Bytes(Vec<u8>);

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor).map(Self)
    }
}

struct BytesVisitor;

impl Visitor<'_> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
        Ok(bytes.to_vec())
    }

    fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(bytes)
    }
}
