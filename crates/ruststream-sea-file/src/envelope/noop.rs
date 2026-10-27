//! [`NoOp`]: the payload as it is, with no envelope.

use std::borrow::Cow;

use ruststream::HeaderMap;

use super::{Envelope, Medium, SealError};

/// No envelope: a record is the payload as it is, and carries no headers.
///
/// A file or a pipe written through it is readable by any tool, and any record reads as a
/// payload. A delivery carries no envelope headers: its headers hold only the transport's own
/// [`SEQUENCE_HEADER`](crate::SEQUENCE_HEADER). A publish that carries a header fails with
/// [`SeaFileError::Seal`](crate::SeaFileError::Seal) naming the header and the destination, and
/// so does a publish to a pipe whose payload a line cannot carry. The framework counts the
/// attempts of a retry in a header, so a `retry_after` copy through this envelope fails.
///
/// # Examples
///
/// A subscription reads a feed another tool writes, one reading per record:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct Reading {
///     millivolts: u32,
/// }
///
/// #[subscriber(FileStream::new("readings"))]
/// async fn on_reading(reading: &Reading) -> HandlerOutcome {
///     tracing::info!(reading.millivolts, "read");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("readings", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/readings.ss"), |b| {
///             b.include(on_reading.envelope(NoOp));
///         })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoOp;

impl Envelope for NoOp {
    type Opened = ();

    fn seal<'p>(
        &self,
        headers: &HeaderMap,
        payload: &'p [u8],
        _medium: Medium,
    ) -> Result<Cow<'p, [u8]>, SealError> {
        match headers.iter().next() {
            Some((name, _)) => Err(SealError::Header {
                name: name.to_string(),
            }),
            None => Ok(Cow::Borrowed(payload)),
        }
    }

    fn open(&self, _record: &[u8]) {}

    fn payload<'r>(&self, record: &'r [u8], (): &'r ()) -> &'r [u8] {
        record
    }

    fn headers(&self, _record: &[u8], (): &()) -> HeaderMap {
        HeaderMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_the_payload_as_it_is() {
        for raw in [
            b"rs1:AAAAAA==".as_slice(),
            b"{\"headers\":{},\"payload\":\"x\"}",
        ] {
            let sealed = NoOp
                .seal(&HeaderMap::new(), raw, Medium::File)
                .expect("a headerless message seals");
            assert_eq!(sealed.as_ref(), raw);
            NoOp.open(raw);
            assert_eq!(NoOp.payload(raw, &()), raw);
            assert!(NoOp.headers(raw, &()).is_empty());
        }
    }

    #[test]
    fn a_message_with_a_header_is_refused_by_name() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let err = NoOp
            .seal(&headers, b"{}", Medium::File)
            .expect_err("a header has no room in a bare payload");
        assert!(matches!(&err, SealError::Header { name } if name == "x-tenant"));
    }
}
