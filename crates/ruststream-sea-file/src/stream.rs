//! [`FileStream`]: the subscription descriptor for the file transport.

use std::future::{Future, ready};

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::IntoSource;
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};
#[cfg(feature = "testing")]
use ruststream::{Seekable, Seeker};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

#[cfg(feature = "testing")]
use crate::FilePosition;
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
/// the way it takes a subject string:
///
/// ```
/// use ruststream_sea_file::FileStream;
///
/// let live = FileStream::new("orders");
/// let batch = FileStream::new("orders").replay();
/// # let _ = (live, batch);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct FileStream {
    stream: String,
    replay: bool,
}

impl FileStream {
    /// Names the stream key.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            replay: false,
        }
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

    /// What this subscription adds to its channel in the generated `AsyncAPI` document.
    #[cfg(feature = "asyncapi")]
    fn channel_extension(&self) -> Bindings {
        channel_extension(self.stream())
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
}

/// What a file channel adds to the generated `AsyncAPI` document: the stream key it resolves to.
///
/// The specification lists no binding for a file transport and its protocol keys are a closed
/// list, so an `x-` extension is the only lawful place for what a stream file knows. The path of
/// the file is the server's own description and is not repeated here.
///
/// Both ends of the transport answer through this: a subscription passes the key its descriptor
/// names, a publish policy passes the destination the mount site resolved. One stream key is both
/// ends of the file, so the two descriptions agree by construction.
#[cfg(feature = "asyncapi")]
pub(crate) fn channel_extension(stream_key: &str) -> Bindings {
    let body = FileChannel { stream_key };
    // A binding that fails to build is a binding the document goes without: a broker never holds
    // up a service over a description of itself.
    Binding::extension(FILE_EXTENSION, &body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

impl IntoSource for FileStream {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedFileBroker> for FileStream {
    type Subscriber = FileSubscriber;
    // One stream key is both ends of the file: a publisher appends under it and this
    // subscription reads the append, so the runtime's deferred copies have an address.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &ConnectedFileBroker,
    ) -> Result<FileSubscriber, SeaFileError> {
        connected.subscribe_stream(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.channel_extension()
    }
}

impl RedeliveryAddressed<ConnectedFileBroker> for FileStream {
    fn redelivery_address(
        &self,
        _connected: &ConnectedFileBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, SeaFileError>> {
        // The descriptor already knows the answer; nothing is asked of the connection.
        ready(Ok(self.redelivery_address_value()))
    }
}

/// The descriptor resolves against the in-process transport too, so a service written on
/// `FileStream` mounts on [`FileTestBroker`](crate::testing::FileTestBroker) unchanged.
///
/// [`replay`](FileStream::replay) opens at the start of the retained log rather than at its tail.
/// Nothing in process writes an end-of-stream mark, so the subscription does not complete the way
/// a finished file's does; that part of replay is verified against real files.
#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedFileTestBroker> for FileStream {
    type Subscriber = crate::testing::FileTestSubscriber;
    // The stand-in's answer is the file's, so a registration that starts against one starts
    // against the other.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedFileTestBroker,
    ) -> Result<Self::Subscriber, SeaFileError> {
        self.validate()?;
        let subscriber = connected.open(self.stream())?;
        if self.replay {
            let seeker = Seekable::seeker(&subscriber);
            Seeker::seek(&seeker, FilePosition::Beginning).await?;
        }
        Ok(subscriber)
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.channel_extension()
    }
}

#[cfg(feature = "testing")]
impl RedeliveryAddressed<crate::testing::ConnectedFileTestBroker> for FileStream {
    fn redelivery_address(
        &self,
        _connected: &crate::testing::ConnectedFileTestBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, SeaFileError>> {
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
