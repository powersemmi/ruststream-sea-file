//! [`FileStream`]: the subscription descriptor for the file transport.

use ruststream::SubscriptionSource;

use crate::error::SeaFileError;
use crate::file::ConnectedFileBroker;
use crate::subscriber::FileSubscriber;

/// Where a new subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Start {
    /// Only messages written after the subscription attached. The default.
    #[default]
    Latest,
    /// Everything retained in the file.
    Beginning,
}

/// A subscription descriptor for one stream key in the file.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use ruststream_sea_file::{FileStream, Start};
///
/// let live = FileStream::new("orders").start(Start::Beginning);
/// let batch = FileStream::new("orders").replay();
/// # let _ = (live, batch);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct FileStream {
    stream: String,
    start: Start,
    replay: bool,
}

impl FileStream {
    /// Names the stream key.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            start: Start::default(),
            replay: false,
        }
    }

    /// Where the subscription starts. Defaults to [`Start::Latest`].
    pub fn start(mut self, start: Start) -> Self {
        self.start = start;
        self
    }

    /// Replays the retained file and ends at its tail instead of following live writes
    /// (implies [`Start::Beginning`]); the subscription completes at the end of the file.
    pub fn replay(mut self) -> Self {
        self.start = Start::Beginning;
        self.replay = true;
        self
    }

    /// The stream key this descriptor resolves.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub(crate) fn start_value(&self) -> Start {
        self.start
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
    fn replay_implies_beginning() {
        let descriptor = FileStream::new("orders").replay();
        assert_eq!(descriptor.start_value(), Start::Beginning);
        assert!(descriptor.replay_value());
    }
}
