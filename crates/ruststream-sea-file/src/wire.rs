//! The conditional header envelope.
//!
//! The client's payloads are plain bytes with no header space, so user headers travel in an
//! envelope - applied only when headers are present, so a file written without headers stays
//! readable as a plain payload stream by other tools. The envelope is text-safe (a prefix and
//! base64), because the stdio transport is line-oriented UTF-8.
//!
//! Two forms share that outer shape and differ in the header block. `rs1:` writes each header as
//! a `name: value` line, which every earlier release reads; it is used whenever every header
//! survives it. `rs2:` writes each name and value length-prefixed, which carries any bytes, and
//! is used only for a message with a header the line form would rewrite (a value that is not
//! UTF-8, holds a line break or begins or ends with whitespace).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use ruststream::HeaderMap;

/// The envelope whose header block is `name: value` lines.
const LINES: &str = "rs1:";
/// The envelope whose header block is length-prefixed names and values.
const FRAMED: &str = "rs2:";

/// Encodes a payload with its headers. Headerless payloads pass through untouched, unless the
/// reader would take them for something else: a payload that begins like an envelope is
/// enveloped itself on both transports. `force_text` marks a line of the stdio transport, which
/// additionally envelopes a payload a line cannot carry as it is.
pub(crate) fn encode(headers: &HeaderMap, payload: &[u8], force_text: bool) -> Vec<u8> {
    let needs_envelope = !headers.is_empty()
        || payload.starts_with(LINES.as_bytes())
        || payload.starts_with(FRAMED.as_bytes())
        || (force_text && !fits_a_line(payload));
    if !needs_envelope {
        return payload.to_vec();
    }
    let lines = headers.iter().all(|(name, value)| {
        !name.contains(':') && survives_a_line(name.as_bytes()) && survives_a_line(value)
    });
    let mut block = Vec::new();
    for (name, value) in headers.iter() {
        if lines {
            block.extend_from_slice(name.as_bytes());
            block.extend_from_slice(b": ");
            block.extend_from_slice(value);
            block.push(b'\n');
        } else {
            push_framed(&mut block, name.as_bytes());
            push_framed(&mut block, value);
        }
    }
    let mut framed = Vec::with_capacity(4 + block.len() + payload.len());
    framed.extend_from_slice(&u32::try_from(block.len()).unwrap_or(0).to_be_bytes());
    framed.extend_from_slice(&block);
    framed.extend_from_slice(payload);
    let prefix = if lines { LINES } else { FRAMED };
    let mut out = String::with_capacity(prefix.len() + framed.len().div_ceil(3) * 4);
    out.push_str(prefix);
    BASE64.encode_string(&framed, &mut out);
    out.into_bytes()
}

/// Whether a header name or value reads back unchanged from a `name: value` line: the reader
/// decodes the block as UTF-8, splits it into lines, splits each at its first colon and trims
/// both halves. A value may hold a colon; a name that holds one is checked by the caller.
fn survives_a_line(text: &[u8]) -> bool {
    std::str::from_utf8(text).is_ok_and(|text| !text.contains(['\n', '\r']) && text.trim() == text)
}

/// Appends `bytes` to a framed header block, after its length.
fn push_framed(block: &mut Vec<u8>, bytes: &[u8]) {
    block.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    block.extend_from_slice(bytes);
}

/// Whether a line of the stdio client carries `payload` unchanged.
///
/// The line is text. The client writes the payload after the line's meta fields and ends it with
/// a newline, and the reader at the other end of the pipe splits on newlines and trims the payload
/// it finds. So a payload that is not UTF-8, holds a newline, or begins or ends with whitespace
/// would arrive cut into pieces or trimmed.
fn fits_a_line(payload: &[u8]) -> bool {
    std::str::from_utf8(payload).is_ok_and(|text| !text.contains('\n') && text.trim() == text)
}

/// Splits a payload back into headers and raw bytes; anything that is not a well-formed envelope
/// reads as headerless.
pub(crate) fn decode(data: &[u8]) -> (HeaderMap, Bytes) {
    opened(data).unwrap_or_else(|| (HeaderMap::new(), Bytes::copy_from_slice(data)))
}

/// Opens an envelope of either form, or `None` for anything that is not one.
fn opened(data: &[u8]) -> Option<(HeaderMap, Bytes)> {
    let text = std::str::from_utf8(data).ok()?;
    let (lines, encoded) = match text.strip_prefix(LINES) {
        Some(encoded) => (true, encoded),
        None => (false, text.strip_prefix(FRAMED)?),
    };
    let framed = BASE64.decode(encoded).ok()?;
    let len = u32::from_be_bytes(framed.get(..4)?.try_into().ok()?) as usize;
    let block = framed.get(4..4usize.checked_add(len)?)?;
    let headers = if lines {
        lined_headers(block)
    } else {
        framed_headers(block)?
    };
    Some((headers, Bytes::copy_from_slice(&framed[4 + len..])))
}

/// The headers of an `rs1:` block: one `name: value` line each.
fn lined_headers(block: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for line in String::from_utf8_lossy(block).lines() {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    headers
}

/// The headers of an `rs2:` block: length-prefixed names and values, alternating. `None` for a
/// block that does not parse, so the message reads as headerless rather than half-decoded.
fn framed_headers(mut block: &[u8]) -> Option<HeaderMap> {
    let mut headers = HeaderMap::new();
    while !block.is_empty() {
        let name = take_framed(&mut block)?;
        let value = take_framed(&mut block)?;
        headers.insert(
            std::str::from_utf8(name).ok()?.to_owned(),
            Bytes::copy_from_slice(value),
        );
    }
    Some(headers)
}

/// Takes one length-prefixed field off the front of `block`.
fn take_framed<'block>(block: &mut &'block [u8]) -> Option<&'block [u8]> {
    let len = u32::from_be_bytes(block.get(..4)?.try_into().ok()?) as usize;
    let field = block.get(4..4usize.checked_add(len)?)?;
    *block = &block[4 + len..];
    Some(field)
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
    fn a_line_envelopes_what_the_line_format_would_cut_or_trim() {
        for raw in [
            b"first\nsecond".as_slice(),
            b" padded".as_slice(),
            b"padded\t".as_slice(),
            b"ends with a carriage return\r".as_slice(),
        ] {
            let encoded = encode(&HeaderMap::new(), raw, true);
            let line = std::str::from_utf8(&encoded).expect("envelope must be text-safe");
            assert!(!line.contains('\n'), "{line:?} would split the line");
            assert_eq!(line.trim(), line, "{line:?} would be trimmed");
            let (headers, payload) = decode(&encoded);
            assert!(headers.is_empty());
            assert_eq!(payload.as_ref(), raw);
        }
        // A line that survives the format is written as it is, so a pipeline stays readable.
        assert_eq!(
            encode(&HeaderMap::new(), b"{\"id\":1}", true),
            b"{\"id\":1}"
        );
        // A stream file is not a line: it keeps such payloads verbatim.
        assert_eq!(encode(&HeaderMap::new(), b" padded\n", false), b" padded\n");
    }

    #[test]
    fn a_payload_that_looks_like_an_envelope_is_enveloped() {
        // Four zero bytes in base64: without its own envelope this payload would decode as an
        // empty one, in either form.
        for raw in [b"rs1:AAAAAA==".as_slice(), b"rs2:AAAAAA==".as_slice()] {
            for force_text in [false, true] {
                let (headers, payload) = decode(&encode(&HeaderMap::new(), raw, force_text));
                assert!(headers.is_empty());
                assert_eq!(payload.as_ref(), raw);
            }
        }
    }

    #[test]
    fn headers_a_line_would_rewrite_round_trip_byte_for_byte() {
        let cases: [(&str, &[u8]); 5] = [
            ("x-binary", &[0x00, 0x80, b'\r', b'\n', 0xfe, 0xff, 0xc3]),
            ("x-multi-line", b"first\nsecond"),
            ("x-padded", b" padded\t"),
            ("x-carriage-return", b"value\r"),
            ("x-name:colon", b"value"),
        ];
        for (name, value) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(name, Bytes::copy_from_slice(value));
            headers.insert("x-plain", "plain");
            for force_text in [false, true] {
                let encoded = encode(&headers, b"payload", force_text);
                assert!(encoded.starts_with(FRAMED.as_bytes()), "{name}");
                assert!(
                    std::str::from_utf8(&encoded).is_ok(),
                    "envelope must be text-safe"
                );
                let (decoded, payload) = decode(&encoded);
                assert_eq!(decoded.get(name), Some(value), "{name}");
                assert_eq!(decoded.get_str("x-plain"), Some("plain"));
                assert_eq!(decoded.iter().count(), 2);
                assert_eq!(payload.as_ref(), b"payload");
            }
        }
    }

    /// The line form stays what a message whose headers survive it is written in, so a stream
    /// file or a pipeline read by an earlier release keeps its headers.
    #[test]
    fn headers_a_line_carries_keep_the_line_form() {
        let mut headers = HeaderMap::new();
        headers.insert("x-empty", "");
        headers.insert("x-url", "http://example.com:8080/a b");
        let encoded = encode(&headers, b"payload", false);
        assert!(encoded.starts_with(LINES.as_bytes()));
        let (decoded, _) = decode(&encoded);
        assert_eq!(decoded.get_str("x-empty"), Some(""));
        assert_eq!(
            decoded.get_str("x-url"),
            Some("http://example.com:8080/a b")
        );
    }

    #[test]
    fn a_malformed_framed_block_reads_as_headerless() {
        // A block that announces a name longer than itself.
        let mut framed = 8u32.to_be_bytes().to_vec();
        framed.extend_from_slice(&100u32.to_be_bytes());
        framed.extend_from_slice(b"name");
        let mut data = FRAMED.to_owned();
        BASE64.encode_string(&framed, &mut data);
        let (headers, payload) = decode(data.as_bytes());
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), data.as_bytes());
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
