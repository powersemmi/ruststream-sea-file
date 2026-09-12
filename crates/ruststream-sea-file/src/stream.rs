//! [`FileStream`]: the subscription descriptor for the file transport.

use std::future::{Future, ready};

use ruststream::runtime::IntoSource;
use ruststream::{RedeliveryAddress, SubscriptionSource};
#[cfg(feature = "testing")]
use ruststream::{Seekable, Seeker};

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
    /// Point it at a file whose writer called
    /// [`end_with_eos`](crate::FileBroker::end_with_eos), so the end is written into the file
    /// rather than inferred from it.
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

    /// Where a deferred copy of a delayed message reaches this subscription again.
    ///
    /// A live subscription tails the file, so the stream key is the answer: the publisher appends
    /// under that key and the subscription reads the append. A replay is not tailing anything -
    /// it reads the region the file already held and completes - so a copy written afterwards
    /// would never be read, and the descriptor says so rather than promising a delivery.
    fn redelivery_address_value(&self) -> Option<RedeliveryAddress> {
        (!self.replay).then(|| RedeliveryAddress::new(self.stream.clone()))
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), SeaFileError> {
        if self.stream.is_empty() {
            return Err(SeaFileError::Invalid("stream key must be non-empty".into()));
        }
        Ok(())
    }
}

impl IntoSource for FileStream {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedFileBroker> for FileStream {
    type Subscriber = FileSubscriber;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &ConnectedFileBroker,
    ) -> Result<FileSubscriber, SeaFileError> {
        connected.subscribe_stream(self).await
    }

    fn redelivery_address(
        &self,
        _connected: &ConnectedFileBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, SeaFileError>> {
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

    fn redelivery_address(
        &self,
        _connected: &crate::testing::ConnectedFileTestBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, SeaFileError>> {
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
