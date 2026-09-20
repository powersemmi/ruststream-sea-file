//! [`StdioTestBroker`]: the in-process stand for the stdio transport and its connected form.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    BatchSubscriber, Broker, BytesMut, ConnectedBroker, DefaultPublish, NamedCopies,
    OutgoingMessage, Publisher, RawMessage, Subscribe, Subscriber, Take,
};

use crate::error::SeaFileError;
use crate::stdio::StdioPublish;
use crate::testing::broker::TestState;
use crate::testing::subscriber::{FileTestMessage, FileTestSubscriber};

/// An in-process stand-in for [`StdioBroker`](crate::StdioBroker): the same routing over a log in
/// memory, with no pipe attached to the process.
///
/// It exists beside [`FileTestBroker`](crate::testing::FileTestBroker) rather than sharing it,
/// because the two transports answer differently about retry copies. Standard output reaches the
/// next process in the pipeline and never this one's own standard input, so a stdio subscription
/// addresses none of its copies and the mount site names the destination. A stdio service mounted
/// on the file stand would be told otherwise, and a registration that a pipe refuses would start
/// here.
///
/// A subscription on this stand does not seek either, which is the other thing a pipe cannot do.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::testing::StdioTestBroker;
///
/// let broker = StdioTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct StdioTestBroker {
    state: Arc<TestState>,
}

impl StdioTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> StdioTestPublisher {
        StdioTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl Broker for StdioTestBroker {
    type Error = SeaFileError;
    type Connected = ConnectedStdioTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedStdioTestBroker { state: self.state }))
    }
}

/// The connected form of [`StdioTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedStdioTestBroker {
    state: Arc<TestState>,
}

impl ConnectedStdioTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> StdioTestPublisher {
        StdioTestPublisher {
            state: Arc::clone(&self.state),
        }
    }

    /// Opens an in-process subscription on `name`, the way a bare stream key opens one on a pipe.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError::NotConnected`] once the broker has been shut down.
    fn open(&self, name: &str) -> Result<StdioTestSubscriber, SeaFileError> {
        self.state.ensure_open()?;
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(StdioTestSubscriber(FileTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            name.to_owned(),
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
    }
}

impl ConnectedBroker for ConnectedStdioTestBroker {
    type Error = SeaFileError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.close();
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedStdioTestBroker {
    type Subscriber = StdioTestSubscriber;
    /// The answer a pipe gives: nothing on this transport reaches the subscription again, so the
    /// mount site names where a deferred copy goes and a registration that names none refuses to
    /// start - here as well as against a real pipe.
    type Copies = NamedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }
}

impl TestableBroker for ConnectedStdioTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedStdioTestBroker);

/// The default reply policy is the stdio transport's own, so a `publish("dest")` handler included
/// without an explicit policy is wired here exactly as it is wired against a pipe.
impl DefaultPublish for ConnectedStdioTestBroker {
    type Policy = StdioPublish;
}

/// Publisher for the in-process stdio stand: it records what the service wrote to standard
/// output, under the stream key the line would carry.
///
/// A publisher handed out before the shutdown outlives the connection, so publishing through it
/// afterwards reports [`SeaFileError::NotConnected`] rather than routing into a broker that is
/// gone - the same answer the real publishers give.
///
/// What it does not reproduce is the line format [`StdioPublisher`](crate::StdioPublisher)
/// writes: payloads are recorded as bytes, the text-safe envelope is not applied, and the empty
/// payload a pipe rejects goes through. Those belong to the real transport and are covered
/// against a real pipe, so a test here must not conclude that a payload survives a shell
/// pipeline.
#[derive(Debug, Clone)]
pub struct StdioTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for StdioTestPublisher {
    /// Unlike the pipe it stands in for, the stand keeps what it is given: a recorded delivery
    /// owns its payload.
    type Payload = Take;

    type Error = SeaFileError;
    // The unit type the real publisher declares: a line on standard output takes a stream key and
    // a payload and nothing else.
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.state.ensure_open().map(|()| {
            let headers = msg.headers().clone();
            let name = msg.name();
            self.state
                .publish(name, msg.into_payload().freeze(), headers);
        }))
    }
}

/// Subscriber returned by [`ConnectedStdioTestBroker`]: one stream key of standard input.
///
/// It delivers and batches the way the file stand does, and seeks the way a pipe does, which is
/// not at all: the [`Seekable`](ruststream::Seekable) capability is deliberately absent here, as
/// it is on [`StdioSubscriber`](crate::StdioSubscriber).
#[derive(Debug)]
pub struct StdioTestSubscriber(FileTestSubscriber);

impl Subscriber for StdioTestSubscriber {
    type Message = FileTestMessage;
    type Error = SeaFileError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl BatchSubscriber for StdioTestSubscriber {
    type Batch = Vec<FileTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SeaFileError>> + Send + '_ {
        self.0.batches(size)
    }
}
