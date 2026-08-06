//! [`FileStream`]: the subscription descriptor for the file transport.

use ruststream::SubscriptionSource;

use crate::error::SeaFileError;
use crate::file::ConnectedFileBroker;
use crate::subscriber::FileSubscriber;

/// A subscription descriptor for one stream key in the file.
///
/// A plain descriptor follows the live tail; where reading begins is the framework's
/// `start_at(..)` clause with a [`FilePosition`](crate::FilePosition) (or a live seek through
/// the `Seek` parameter). [`replay`](Self::replay) is the one reading mode the position API
/// cannot express: it reads the finished file and completes the stream at its end instead of
/// following live writes.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
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

    /// Replays the retained file from the beginning and ends at its tail instead of
    /// following live writes; the subscription completes at the end of the file.
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

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), SeaFileError> {
        if self.stream.is_empty() {
            return Err(SeaFileError::Invalid("stream key must be non-empty".into()));
        }
        Ok(())
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
