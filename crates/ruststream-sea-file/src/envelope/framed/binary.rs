//! The binary form of a [`Framed`](super::Framed) document: the payload and the header values as
//! byte strings, read and written through the codec.

use std::fmt;

use ruststream::HeaderMap;
use serde::de::{IgnoredAny, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{DocumentCodec, HeaderValue, Opened, header_map};
use crate::envelope::SealError;

/// Writes one document through the codec.
pub(super) fn seal<Format: DocumentCodec>(
    codec: &Format,
    headers: &HeaderMap,
    payload: &[u8],
) -> Result<Vec<u8>, SealError> {
    let document = Document {
        headers: HeadersOut(headers),
        payload: ByteString(payload),
    };
    codec
        .encode(&document)
        .map(Vec::from)
        .map_err(|err| SealError::Encode(Box::new(err)))
}

/// Copies the payload out of a document; `None` for a record that is not a document in exactly
/// this shape.
pub(super) fn open<Format: DocumentCodec>(codec: &Format, record: &[u8]) -> Option<Opened> {
    codec
        .decode::<Located>(record)
        .ok()
        .map(|located| Opened::Decoded(located.payload.0))
}

/// Parses the headers of a document `open` recognised.
pub(super) fn headers<Format: DocumentCodec>(codec: &Format, record: &[u8]) -> Option<HeaderMap> {
    codec
        .decode::<Parsed>(record)
        .ok()
        .map(|parsed| parsed.headers)
}

/// The document a message is written as.
#[derive(Serialize)]
struct Document<'a> {
    headers: HeadersOut<'a>,
    payload: ByteString<'a>,
}

/// Bytes written as a byte string.
struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// The header map, written as a map of names to byte strings.
struct HeadersOut<'a>(&'a HeaderMap);

impl Serialize for HeadersOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.0.iter() {
            map.serialize_entry(name, &ByteString(value))?;
        }
        map.end()
    }
}

/// A document read for its payload: the headers are checked for their shape and skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Located {
    #[serde(rename = "headers", deserialize_with = "checked_headers")]
    _headers: (),
    payload: Bytes,
}

/// A document read for its headers: the payload is skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parsed {
    #[serde(deserialize_with = "kept_headers")]
    headers: HeaderMap,
    #[serde(rename = "payload")]
    _payload: IgnoredAny,
}

fn checked_headers<'de, D: Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
    header_map::<Bytes, D>(deserializer, false).map(drop)
}

fn kept_headers<'de, D: Deserializer<'de>>(deserializer: D) -> Result<HeaderMap, D::Error> {
    header_map::<Bytes, D>(deserializer, true)
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

/// A byte string checked and not kept.
struct CheckedBytes;

impl<'de> Deserialize<'de> for CheckedBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer
            .deserialize_bytes(CheckedBytesVisitor)
            .map(|()| Self)
    }
}

struct CheckedBytesVisitor;

impl Visitor<'_> for CheckedBytesVisitor {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string")
    }

    fn visit_bytes<E>(self, _bytes: &[u8]) -> Result<(), E> {
        Ok(())
    }
}

impl HeaderValue for Bytes {
    type Checked = CheckedBytes;

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}
