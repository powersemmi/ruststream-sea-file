//! [`FileStream`]: the subscription descriptor for the file transport.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::IntoSource;
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::envelope::{Envelope, EnvelopeChoice, FromBroker};
use crate::error::SeaFileError;
use crate::file::ConnectedFileBroker;
use crate::subscriber::FileSubscriber;

/// A subscription descriptor for one stream key in the file.
///
/// A plain descriptor follows the live tail; where reading begins is the framework's
/// `start_at(..)` clause with a [`FilePosition`](crate::FilePosition) (or a live seek through
/// the [`SeekHandle`](crate::SeekHandle) context key). [`replay`](Self::replay) is the one
/// reading mode the position API cannot express: it reads the finished file and completes the
/// stream at its end instead of following live writes.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator, and [`IntoSource`], so the manual path's `subscriber(..)` constructor takes it
/// the way it takes a subject string. The
/// [FileStream](https://powersemmi.github.io/ruststream-sea-file/latest/components/subscriptions/file-stream/)
/// page shows it in a service.
///
/// # Examples
///
/// A replay of a recorded file:
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
/// #[subscriber(FileStream::new("orders").replay())]
/// async fn audit(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "audited");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("audit", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/orders.ss").existing_only(), |b| {
///             b.include(audit);
///         })
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct FileStream<Choice = FromBroker> {
    stream: String,
    replay: bool,
    envelope: Choice,
}

impl FileStream {
    /// Names the stream key.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            replay: false,
            envelope: FromBroker,
        }
    }
}

/// A [`FileStream`] whose type names its envelope: `FileStream::<NoOp>::new("readings")`.
///
/// `#[subscriber]` takes the descriptor's type from the constructor at the head of the
/// expression, so a subscription that reads in an envelope of its own names it there. The
/// envelope is its type's default value; [`FileStream::envelope`] after the constructor sets
/// another value of the same type.
///
/// # Examples
///
/// A feed another tool writes, one bare reading per record, in a file whose other keys carry
/// headers:
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
/// #[subscriber(FileStream::<NoOp>::new("readings"))]
/// async fn on_reading(reading: &Reading) -> HandlerOutcome {
///     tracing::info!(reading.millivolts, "read");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("readings", "0.1.0"))
///         .with_broker(FileBroker::new("/var/lib/plant.ss"), |b| {
///             b.include(on_reading);
///         })
/// }
/// ```
pub trait EnvelopedStream: Sized {
    /// Names the stream key, read in the envelope the type names.
    fn new(stream: impl Into<String>) -> Self;
}

impl<Env: Envelope + Default> EnvelopedStream for FileStream<Env> {
    fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            replay: false,
            envelope: Env::default(),
        }
    }
}

impl<Choice> FileStream<Choice> {
    /// Reads this stream key in `envelope`, whatever the broker's is.
    ///
    /// # Examples
    ///
    /// A feed another tool writes, one bare reading per record, in a file whose other keys carry
    /// headers, mounted without the macros:
    ///
    /// ```
    /// use ruststream::schemars::JsonSchema;
    /// use ruststream_sea_file::file::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Debug, Deserialize, JsonSchema)]
    /// #[schemars(crate = "ruststream::schemars")]
    /// struct Reading {
    ///     millivolts: u32,
    /// }
    ///
    /// struct OnReading;
    ///
    /// impl Handle<Reading> for OnReading {
    ///     async fn handle(
    ///         &self,
    ///         reading: &Reading,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> Result<(), HandlerOutcome> {
    ///         tracing::info!(reading.millivolts, "read");
    ///         Ok(())
    ///     }
    /// }
    ///
    /// fn app() -> impl App<State = ()> {
    ///     RustStream::new(AppInfo::new("readings", "0.1.0"))
    ///         .with_broker(FileBroker::new("/var/lib/plant.ss"), |b| {
    ///             b.include(
    ///                 subscriber(FileStream::new("readings").envelope(NoOp), OnReading).build(),
    ///             );
    ///         })
    /// }
    /// # let _ = app;
    /// ```
    pub fn envelope<Chosen: Envelope>(self, envelope: Chosen) -> FileStream<Chosen> {
        FileStream {
            stream: self.stream,
            replay: self.replay,
            envelope,
        }
    }

    pub(crate) const fn envelope_choice(&self) -> &Choice {
        &self.envelope
    }

    /// Replays the retained file from the beginning instead of following live writes; the
    /// subscription completes at the end of the file.
    ///
    /// Every message the file holds is delivered before the subscription completes. The end is
    /// the end-of-stream mark a writer left with
    /// [`end_with_eos`](crate::FileBroker::end_with_eos), or the end of the file when there is
    /// none.
    pub fn replay(mut self) -> Self {
        self.replay = true;
        self
    }

    /// The stream key this descriptor resolves.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub(crate) fn replay_value(&self) -> bool {
        self.replay
    }

    /// Where a deferred copy of a delayed message reaches this subscription again: the stream
    /// key, on both reading modes.
    ///
    /// A publisher on this broker appends under that key and the subscription reads the append.
    /// A replay reads the region the file already held and completes, so the copy reaches it
    /// while it is still short of that point and is left unread once it has passed it; the
    /// address is the same either way, and the delay is what decides.
    fn redelivery_address_value(&self) -> RedeliveryAddress {
        RedeliveryAddress::new(self.stream.clone())
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), SeaFileError> {
        if self.stream.is_empty() {
            return Err(SeaFileError::Invalid("stream key must be non-empty".into()));
        }
        Ok(())
    }
}

/// The extension key the file transport's channel description sits under.
#[cfg(feature = "asyncapi")]
const FILE_EXTENSION: &str = "x-ruststream-file";

/// What the document reports about a file channel: the stream key the channel resolves to.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileChannel<'a> {
    stream_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    envelope: Option<EnvelopeSpec>,
}

/// What the document reports about the envelope of a channel: the media type of its records.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EnvelopeSpec {
    content_type: &'static str,
}

#[cfg(feature = "asyncapi")]
impl EnvelopeSpec {
    /// The description of an envelope whose records have `content_type`.
    pub(crate) const fn new(content_type: &'static str) -> Self {
        Self { content_type }
    }
}

/// What a file channel adds to the generated `AsyncAPI` document: the stream key it resolves to,
/// and the media type of its envelope's records when they have one of their own.
///
/// The specification lists no binding for a file transport and its protocol keys are a closed
/// list, so an `x-` extension is the only lawful place for what a stream file knows. The path of
/// the file is the server's own description and is not repeated here.
///
/// Both ends of the transport answer through this: a subscription passes the key its descriptor
/// names, a publish policy passes the destination the mount site resolved. One stream key is both
/// ends of the file, so the two descriptions agree by construction.
#[cfg(feature = "asyncapi")]
pub(crate) fn channel_extension(stream_key: &str, envelope: Option<&'static str>) -> Bindings {
    let body = FileChannel {
        stream_key,
        envelope: envelope.map(EnvelopeSpec::new),
    };
    // A binding that fails to build is a binding the document goes without: a broker never holds
    // up a service over a description of itself.
    Binding::extension(FILE_EXTENSION, &body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

impl<Choice> IntoSource for FileStream<Choice> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl<Env, Choice> SubscriptionSource<ConnectedFileBroker<Env>> for FileStream<Choice>
where
    Env: Envelope,
    Choice: EnvelopeChoice<Env>,
{
    type Subscriber = FileSubscriber<Choice::Envelope>;
    // One stream key is both ends of the file: a publisher appends under it and this
    // subscription reads the append, so the runtime's deferred copies have an address.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &ConnectedFileBroker<Env>,
    ) -> Result<FileSubscriber<Choice::Envelope>, SeaFileError> {
        connected.subscribe_stream(self).await
    }

    /// The stream key, and the media type of the envelope the subscription reads in when its
    /// records have one.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        channel_extension(self.stream(), <Choice::Envelope as Envelope>::CONTENT_TYPE)
    }
}

impl<Env, Choice> RedeliveryAddressed<ConnectedFileBroker<Env>> for FileStream<Choice>
where
    Env: Envelope,
    Choice: EnvelopeChoice<Env>,
{
    fn redelivery_address(
        &self,
        _connected: &ConnectedFileBroker<Env>,
    ) -> impl Future<Output = Result<RedeliveryAddress, SeaFileError>> {
        // The descriptor already knows the answer; nothing is asked of the connection.
        ready(Ok(self.redelivery_address_value()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stream_keys_are_rejected_before_io() {
        assert!(FileStream::new("").validate().is_err());
    }

    #[test]
    fn replay_reads_the_retained_file() {
        assert!(FileStream::new("orders").replay().replay_value());
        assert!(!FileStream::new("orders").replay_value());
    }
}
