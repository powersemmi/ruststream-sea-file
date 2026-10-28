//! The JSON form of a [`Framed`](super::Framed) document: a payload that is a JSON text embedded
//! as a value in `payload`, any other payload as base64 in `payload_base64`.

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ruststream::HeaderMap;
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use ruststream::codec::JsonCodec;

use super::{FrameCodec, FramedOpened, text_headers};
use crate::envelope::{Medium, SealError};

impl FrameCodec for JsonCodec {
    fn frame(
        &self,
        headers: &HeaderMap,
        payload: &[u8],
        medium: Medium,
    ) -> Result<Vec<u8>, SealError> {
        seal(headers, payload, medium)
    }

    fn frame_payload(&self, record: &[u8]) -> FramedOpened {
        open(record).unwrap_or_else(FramedOpened::foreign)
    }

    fn frame_headers(&self, record: &[u8]) -> Option<HeaderMap> {
        headers(record)
    }
}

/// Writes one document. The payload is embedded only where it reads back byte for byte: a whole
/// JSON text, and on a line one that holds no line break.
fn seal(headers: &HeaderMap, payload: &[u8], medium: Medium) -> Result<Vec<u8>, SealError> {
    let embedded = is_json(payload) && !(medium == Medium::Line && payload.contains(&b'\n'));
    let mut out = Vec::with_capacity(payload.len().saturating_mul(4) / 3 + 64);
    out.extend_from_slice(b"{\"headers\":{");
    for (index, (name, value)) in headers.iter().enumerate() {
        let value = str::from_utf8(value).map_err(|_| SealError::NotText {
            name: name.to_string(),
        })?;
        if index > 0 {
            out.push(b',');
        }
        serde_json::to_writer(&mut out, name).map_err(|err| SealError::Encode(Box::new(err)))?;
        out.push(b':');
        serde_json::to_writer(&mut out, value).map_err(|err| SealError::Encode(Box::new(err)))?;
    }
    if embedded {
        out.extend_from_slice(b"},\"payload\":");
        out.extend_from_slice(payload);
    } else {
        out.extend_from_slice(b"},\"payload_base64\":\"");
        let start = out.len();
        let len = base64::encoded_len(payload.len(), true)
            .ok_or_else(|| SealError::Encode(Box::from("the payload is too large for base64")))?;
        out.resize(start + len, 0);
        BASE64
            .encode_slice(payload, &mut out[start..])
            .map_err(|err| SealError::Encode(Box::new(err)))?;
        out.push(b'"');
    }
    out.push(b'}');
    Ok(out)
}

/// Whether `payload` is one JSON text, with nothing around it that a value would lose.
fn is_json(payload: &[u8]) -> bool {
    serde_json::from_slice::<&RawValue>(payload).is_ok_and(|raw| raw.get().len() == payload.len())
}

/// A document read for its payload: the headers are skipped, not parsed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Located<'a> {
    #[serde(rename = "headers")]
    _headers: IgnoredAny,
    #[serde(borrow, default, deserialize_with = "present")]
    payload: Option<&'a RawValue>,
    #[serde(borrow, default)]
    payload_base64: Option<Cow<'a, str>>,
}

/// A document read for its headers: the payload is skipped.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Parsed {
    #[serde(deserialize_with = "text_headers")]
    headers: HeaderMap,
    #[serde(rename = "payload", default)]
    _payload: Option<IgnoredAny>,
    #[serde(rename = "payload_base64", default)]
    _payload_base64: Option<IgnoredAny>,
}

/// A present `payload` field, `null` included: `null` is a JSON text like any other.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// Finds the payload of a document: inside the record for an embedded value, decoded for base64.
/// `None` for a record that is not a document in exactly this shape.
fn open(record: &[u8]) -> Option<FramedOpened> {
    let located = serde_json::from_slice::<Located<'_>>(record).ok()?;
    match (located.payload, located.payload_base64) {
        (Some(raw), None) => {
            // The value is borrowed from the record, so its place there is a range of it.
            let start = raw.get().as_ptr().addr() - record.as_ptr().addr();
            Some(FramedOpened::within(start..start + raw.get().len()))
        }
        (None, Some(encoded)) => BASE64
            .decode(encoded.as_bytes())
            .ok()
            .map(FramedOpened::decoded),
        _ => None,
    }
}

/// Parses the headers of a document `open` recognised.
fn headers(record: &[u8]) -> Option<HeaderMap> {
    serde_json::from_slice::<Parsed>(record)
        .ok()
        .map(|parsed| parsed.headers)
}
