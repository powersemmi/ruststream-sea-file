//! The conditional header envelope.
//!
//! The client's payloads are plain bytes with no header space, so user headers travel in an
//! envelope - applied only when headers are present, so a file written without headers stays
//! readable as a plain payload stream by other tools. The envelope is text-safe
//! (`rs1:` + base64), because the stdio transport is line-oriented UTF-8.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use ruststream::HeaderMap;

const PREFIX: &str = "rs1:";

/// Encodes a payload with its headers. Headerless payloads pass through untouched;
/// `force_text` additionally envelopes a non-UTF-8 payload (the stdio transport rejects
/// binary lines).
pub(crate) fn encode(headers: &HeaderMap, payload: &[u8], force_text: bool) -> Vec<u8> {
    let needs_envelope =
        !headers.is_empty() || (force_text && std::str::from_utf8(payload).is_err());
    if !needs_envelope {
        return payload.to_vec();
    }
    let mut lines = String::new();
    for (name, value) in headers.iter() {
        lines.push_str(name);
        lines.push_str(": ");
        lines.push_str(&String::from_utf8_lossy(value));
        lines.push('\n');
    }
    let header_bytes = lines.as_bytes();
    let mut framed = Vec::with_capacity(4 + header_bytes.len() + payload.len());
    framed.extend_from_slice(&u32::try_from(header_bytes.len()).unwrap_or(0).to_be_bytes());
    framed.extend_from_slice(header_bytes);
    framed.extend_from_slice(payload);
    let mut out = String::with_capacity(PREFIX.len() + framed.len().div_ceil(3) * 4);
    out.push_str(PREFIX);
    BASE64.encode_string(&framed, &mut out);
    out.into_bytes()
}

/// Splits a payload back into headers and raw bytes; anything without the envelope prefix
/// reads as headerless.
pub(crate) fn decode(data: &[u8]) -> (HeaderMap, Bytes) {
    let enveloped = std::str::from_utf8(data)
        .ok()
        .and_then(|text| text.strip_prefix(PREFIX))
        .and_then(|encoded| BASE64.decode(encoded).ok());
    let Some(framed) = enveloped else {
        return (HeaderMap::new(), Bytes::copy_from_slice(data));
    };
    if framed.len() < 4 {
        return (HeaderMap::new(), Bytes::copy_from_slice(data));
    }
    let len = u32::from_be_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
    if framed.len() < 4 + len {
        return (HeaderMap::new(), Bytes::copy_from_slice(data));
    }
    let mut headers = HeaderMap::new();
    let text = String::from_utf8_lossy(&framed[4..4 + len]);
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    (headers, Bytes::copy_from_slice(&framed[4 + len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headerless_payloads_pass_through() {
        let (headers, payload) = decode(&encode(&HeaderMap::new(), b"raw bytes", false));
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), b"raw bytes");
        assert_eq!(encode(&HeaderMap::new(), b"raw bytes", false), b"raw bytes");
    }

    #[test]
    fn headers_round_trip_through_the_text_envelope() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("x-tenant", "acme");
        let encoded = encode(&headers, b"{\"id\":1}", false);
        assert!(
            std::str::from_utf8(&encoded).is_ok(),
            "envelope must be text-safe"
        );
        let (decoded, payload) = decode(&encoded);
        assert_eq!(decoded.get_str("content-type"), Some("application/json"));
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert_eq!(payload.as_ref(), b"{\"id\":1}");
    }

    #[test]
    fn force_text_envelopes_binary_payloads() {
        let raw = [0u8, 159, 146, 150, 255];
        let encoded = encode(&HeaderMap::new(), &raw, true);
        assert!(std::str::from_utf8(&encoded).is_ok());
        let (headers, payload) = decode(&encoded);
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), raw.as_slice());
    }
}
