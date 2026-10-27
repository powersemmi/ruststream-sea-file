//! [`Rs`]: the conditional `rs1:` / `rs2:` header envelope, the crate's default.
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

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use ruststream::HeaderMap;

use super::{Envelope, Medium, SealError, fits_a_line};

/// The envelope whose header block is `name: value` lines.
const LINES: &str = "rs1:";
/// The envelope whose header block is length-prefixed names and values.
const FRAMED: &str = "rs2:";

/// The crate's default envelope: `rs1:` or `rs2:` plus base64 of the headers and the payload,
/// written only for a message that needs it.
///
/// A message with no headers is written as it is, so a file recorded that way stays readable by
/// any `sea-streamer` consumer and a line stays readable by any line tool. A message with headers
/// is enveloped: `rs1:` when every header reads back unchanged from a `name: value` line, which
/// keeps it readable by every earlier release of this crate, and `rs2:` with length-prefixed
/// names and values otherwise. A payload that begins like an envelope is enveloped itself, and on
/// a pipe so is a payload a line cannot carry: one that is not UTF-8, holds a line break, or
/// begins or ends with whitespace. Every other record reads as a payload with no headers.
///
/// # Examples
///
/// A broker names the default to state it; a broker that names nothing writes the same records:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Debug, Deserialize)]
/// struct Tenant {
///     tenant: String,
/// }
///
/// #[subscriber(FileStream::new("orders"))]
/// async fn handle(order: &Order, Headers(meta): Headers<Tenant>) -> HandlerOutcome {
///     tracing::info!(order.id, tenant = %meta.tenant, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/orders.ss").envelope(Rs), |b| {
///             b.include(handle);
///         })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rs;

/// What [`Rs`] found in a record: an envelope, decoded, or a bare payload.
#[derive(Debug)]
pub struct RsOpened(Opened);

#[derive(Debug)]
enum Opened {
    /// Not an envelope: the record is the payload.
    Bare,
    /// An envelope, decoded: the header block and the payload behind it.
    Enveloped {
        decoded: Vec<u8>,
        /// Where the header block ends and the payload begins.
        block_end: usize,
        /// Whether the block is `name: value` lines, rather than length-prefixed fields.
        lines: bool,
    },
}

impl Envelope for Rs {
    type Opened = RsOpened;

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        Ok(encode(headers, payload, medium == Medium::Line))
    }

    fn open(&self, record: &[u8]) -> RsOpened {
        RsOpened(opened(record).unwrap_or(Opened::Bare))
    }

    fn payload<'r>(&self, record: &'r [u8], opened: &'r RsOpened) -> &'r [u8] {
        match &opened.0 {
            Opened::Bare => record,
            Opened::Enveloped {
                decoded, block_end, ..
            } => &decoded[*block_end..],
        }
    }

    fn headers(&self, _record: &[u8], opened: &RsOpened) -> Option<HeaderMap> {
        match &opened.0 {
            Opened::Bare => Some(HeaderMap::new()),
            Opened::Enveloped {
                decoded,
                block_end,
                lines: true,
            } => Some(lined_headers(&decoded[4..*block_end])),
            Opened::Enveloped {
                decoded, block_end, ..
            } => framed_headers(&decoded[4..*block_end]),
        }
    }
}

/// Encodes a payload with its headers. Headerless payloads pass through untouched, unless the
/// reader would take them for something else: a payload that begins like an envelope is
/// enveloped itself on both transports. `force_text` marks a line of the stdio transport, which
/// additionally envelopes a payload a line cannot carry as it is.
fn encode<'p>(headers: &HeaderMap, payload: &'p [u8], force_text: bool) -> Cow<'p, [u8]> {
    let needs_envelope = !headers.is_empty()
        || payload.starts_with(LINES.as_bytes())
        || payload.starts_with(FRAMED.as_bytes())
        || (force_text && !fits_a_line(payload));
    if !needs_envelope {
        return Cow::Borrowed(payload);
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
    Cow::Owned(out.into_bytes())
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

/// Opens an envelope of either form, or `None` for anything that is not one. The header block is
/// located but not parsed; a block that does not parse reads as no headers when it is read.
fn opened(record: &[u8]) -> Option<Opened> {
    let (lines, encoded) = match record.strip_prefix(LINES.as_bytes()) {
        Some(encoded) => (true, encoded),
        None => (false, record.strip_prefix(FRAMED.as_bytes())?),
    };
    let decoded = BASE64.decode(encoded).ok()?;
    let len = u32::from_be_bytes(decoded.get(..4)?.try_into().ok()?) as usize;
    let block_end = 4usize.checked_add(len)?;
    decoded.get(4..block_end)?;
    Some(Opened::Enveloped {
        decoded,
        block_end,
        lines,
    })
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
/// block that does not parse, so a half-decoded block is never read.
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

    /// Opens a record the way a delivery does, parsing its headers too.
    fn decode(record: &[u8]) -> (HeaderMap, Vec<u8>) {
        let opened = Rs.open(record);
        (
            Rs.headers(record, &opened).unwrap_or_default(),
            Rs.payload(record, &opened).to_vec(),
        )
    }

    #[test]
    fn headerless_payloads_pass_through() {
        let (headers, payload) = decode(&encode(&HeaderMap::new(), b"raw bytes", false));
        assert_eq!(headers, HeaderMap::new());
        assert_eq!(payload, b"raw bytes");
        assert_eq!(
            encode(&HeaderMap::new(), b"raw bytes", false).as_ref(),
            b"raw bytes"
        );
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
        assert_eq!(payload, b"{\"id\":1}");
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
            assert_eq!(payload, raw);
        }
        // A line that survives the format is written as it is, so a pipeline stays readable.
        assert_eq!(
            encode(&HeaderMap::new(), b"{\"id\":1}", true).as_ref(),
            b"{\"id\":1}"
        );
        // A stream file is not a line: it keeps such payloads verbatim.
        assert_eq!(
            encode(&HeaderMap::new(), b" padded\n", false).as_ref(),
            b" padded\n"
        );
    }

    #[test]
    fn a_payload_that_looks_like_an_envelope_is_enveloped() {
        // Four zero bytes in base64: without its own envelope this payload would decode as an
        // empty one, in either form.
        for raw in [b"rs1:AAAAAA==".as_slice(), b"rs2:AAAAAA==".as_slice()] {
            for force_text in [false, true] {
                let (headers, payload) = decode(&encode(&HeaderMap::new(), raw, force_text));
                assert!(headers.is_empty());
                assert_eq!(payload, raw);
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
                assert_eq!(payload, b"payload");
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
    fn a_malformed_framed_block_reads_as_no_headers_before_the_payload() {
        // A block that announces a name longer than itself, then the payload.
        let mut framed = 8u32.to_be_bytes().to_vec();
        framed.extend_from_slice(&100u32.to_be_bytes());
        framed.extend_from_slice(b"name");
        framed.extend_from_slice(b"payload");
        let mut data = FRAMED.to_owned();
        BASE64.encode_string(&framed, &mut data);
        let (headers, payload) = decode(data.as_bytes());
        assert!(headers.is_empty());
        assert_eq!(payload, b"payload");
    }

    #[test]
    fn force_text_envelopes_binary_payloads() {
        let raw = [0u8, 159, 146, 150, 255];
        let encoded = encode(&HeaderMap::new(), &raw, true);
        assert!(std::str::from_utf8(&encoded).is_ok());
        let (headers, payload) = decode(&encoded);
        assert_eq!(headers, HeaderMap::new());
        assert_eq!(payload, raw.as_slice());
    }
}
