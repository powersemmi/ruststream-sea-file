//! The publish side of the stream file: the live [`FilePublisher`] and its policy
//! [`FilePublish`](struct@FilePublish).

use std::future::{Future, ready};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[cfg(feature = "testing")]
use bytes::Bytes;
#[cfg(feature = "testing")]
use ruststream::RawMessage;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{DefaultPublish, Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use sea_streamer_file::{FileErr, FileProducer};
use sea_streamer_types::{Producer as _, StreamErr, StreamKey};

use super::{Appender, ConnectedFileBroker, CoreCell, Link};
use crate::envelope::{self, Envelope, EnvelopeChoice, FromBroker, Medium, Rs};
use crate::error::{SeaFileError, box_err};
#[cfg(feature = "asyncapi")]
use crate::stream::channel_extension;

/// Publishes messages into the stream file.
///
/// Headers travel inside the record, in the publisher's [`Envelope`]: the policy's when it names
/// one, the broker's otherwise.
///
/// A publish carries no per-message settings: an append to a stream file takes a key and a
/// payload and nothing else, so [`Publisher::Options`] is the unit type and the publish builder
/// gains no step from this crate.
///
/// A destination is a stream key inside the broker, and the file itself is named once, in
/// [`FileBroker::new`](super::FileBroker::new). The
/// [FilePublish](https://powersemmi.github.io/ruststream-sea-file/latest/components/publishing/file-publish/)
/// page shows it in a service.
///
/// # Examples
///
/// A reply type declares its stream key, and the subscriber carries the bare `reply` clause:
///
/// ```
/// use ruststream_sea_file::file::prelude::*;
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
/// #[subscriber(FileStream::new("orders"), reply)]
/// async fn confirm(order: &Order) -> Receipt {
///     tracing::info!(order.id, "confirmed");
///     Receipt { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/orders.ss"), |b| {
///             b.include(confirm).out_reply(Publish);
///         })
/// }
/// ```
#[derive(Clone)]
pub struct FilePublisher<Env: Envelope = Rs> {
    pub(super) cell: CoreCell,
    pub(super) envelope: Env,
}

impl<Env: Envelope> std::fmt::Debug for FilePublisher<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePublisher").finish_non_exhaustive()
    }
}

impl<Env: Envelope> Publisher for FilePublisher<Env> {
    /// The append reads the bytes and keeps nothing: the client's `send_to` takes a slice, and
    /// an envelope that frames the payload writes a buffer of its own.
    type Payload = Lend;

    type Error = SeaFileError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&()>,
    ) -> Result<(), Self::Error> {
        let core = self.cell.get().ok_or(SeaFileError::NotConnected)?;
        core.ensure_open()?;
        let key = StreamKey::new(msg.name())
            .map_err(|e| SeaFileError::Invalid(format!("'{}': {e}", msg.name())))?;
        let record = envelope::seal(&self.envelope, msg.headers(), msg.payload(), Medium::File)
            .map_err(|source| SeaFileError::Seal {
                stream: msg.name().to_owned(),
                source,
            })?;
        match &core.link {
            Link::File(disk) => {
                let mut appender = disk.producer.lock().await;
                appender.append(msg.name(), &key, &record).await
            }
            .map_err(|err| match err {
                // The appender closed while this connection is open: another broker on the
                // same path shut down and ended the file's writer.
                SeaFileError::NotConnected if !core.closed.load(Ordering::Acquire) => {
                    SeaFileError::Publish {
                        stream: msg.name().to_owned(),
                        source: box_err(FileErr::ProducerEnded),
                    }
                }
                other => other,
            }),
            #[cfg(feature = "testing")]
            Link::InProcess(file) => {
                file.append(key, record.into_owned(), sent(&msg))
                    .map_err(|err| SeaFileError::Publish {
                        stream: msg.name().to_owned(),
                        source: box_err(err),
                    })
            }
        }
    }
}

impl Appender {
    /// Appends one message through an open producer, and records a failure that ended the
    /// client's writer, so a later publish reports it instead of reaching that writer.
    async fn append(
        &mut self,
        stream: &str,
        key: &StreamKey,
        payload: &[u8],
    ) -> Result<(), SeaFileError> {
        let failed = |source| SeaFileError::Publish {
            stream: stream.to_owned(),
            source,
        };
        let producer = match self {
            Self::Open(producer) => producer,
            Self::Failed(_, err) => return Err(failed(box_err(err.take()))),
            // A `shutdown` ran between the open check and the lock.
            Self::Closed => return Err(SeaFileError::NotConnected),
        };
        let mut cause = match append(producer, key, payload).await {
            Ok(()) => return Ok(()),
            Err(StreamErr::Backend(cause)) => cause,
            Err(other) => return Err(failed(box_err(other))),
        };
        let reported = cause.take();
        if let Self::Open(producer) = mem::replace(self, Self::Closed) {
            *self = Self::Failed(producer, cause);
        }
        Err(failed(box_err(reported)))
    }
}

/// Appends one message and flushes it to the file.
///
/// The flush is per publish: the sink buffers, and live subscribers (and external tails) observe
/// the file, not the buffer.
async fn append(
    producer: &mut FileProducer,
    key: &StreamKey,
    payload: &[u8],
) -> Result<(), StreamErr<FileErr>> {
    producer.send_to(key, payload)?.await?;
    producer.flush().await
}

/// The publish policy for [`FilePublisher`].
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream_sea_file::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Debug, Outgoing, Serialize)]
/// struct Started {
///     service: String,
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("recorder", "0.1.0"))
///         .with_broker(FileBroker::new("/tmp/recorder.ss"), |b| {
///             b.after_startup(FilePublish, async move |publisher| -> io::Result<()> {
///                 publisher
///                     .message(&Started { service: "recorder".into() })
///                     .to("lifecycle")
///                     .publish()
///                     .await
///                     .map_err(io::Error::other)
///             });
///         })
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[must_use]
pub struct FilePublish<Choice = FromBroker> {
    envelope: Choice,
}

/// The publish policy that writes in the broker's envelope, as a value: `out_reply(FilePublish)`.
// A unit struct cannot carry the envelope parameter, so the value the mount site names is this
// constant of the same name; the type and the value live in separate namespaces.
#[allow(non_upper_case_globals)]
pub const FilePublish: FilePublish = FilePublish {
    envelope: FromBroker,
};

impl<Choice> FilePublish<Choice> {
    /// Writes every message through this policy in `envelope`, whatever the broker's is.
    ///
    /// # Examples
    ///
    /// The receipts go out as JSON documents with their headers inside, while the orders the
    /// broker reads keep its default envelope:
    ///
    /// ```
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_sea_file::file::prelude::*;
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
    /// #[subscriber(FileStream::new("orders"), reply)]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { order: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(FileBroker::new("/var/lib/orders.ss"), |b| {
    ///             b.include(confirm)
    ///                 .out_reply(Publish.envelope(Framed::new(JsonCodec)));
    ///         })
    /// }
    /// ```
    pub fn envelope<Chosen: Envelope>(self, envelope: Chosen) -> FilePublish<Chosen> {
        FilePublish { envelope }
    }
}

impl<Env, Choice> PublishPolicy<ConnectedFileBroker<Env>> for FilePublish<Choice>
where
    Env: Envelope,
    Choice: EnvelopeChoice<Env>,
{
    type Live = FilePublisher<Choice::Envelope>;

    fn pair(
        self,
        connected: &ConnectedFileBroker<Env>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(FilePublisher {
            cell: Arc::clone(&connected.cell),
            envelope: self.envelope.resolve(&connected.envelope),
        }))
    }

    /// Names the stream key a publish through this policy appends under: the destination the
    /// mount site resolved, which on this transport is the stream key itself, and the media type
    /// of the envelope when it has one.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        channel_extension(channel, <Choice::Envelope as Envelope>::CONTENT_TYPE)
    }
}

impl<Env: Envelope> DefaultPublish for ConnectedFileBroker<Env> {
    type Policy = FilePublish;
}

/// The message a publisher sent, as the in-process file keeps it for the harness to read back.
#[cfg(feature = "testing")]
fn sent(msg: &OutgoingMessage<'_, &[u8]>) -> RawMessage {
    RawMessage::new(msg.name(), Bytes::copy_from_slice(msg.payload()))
        .with_headers(msg.headers().clone())
}
