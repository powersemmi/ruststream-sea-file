//! [`FileTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::SeaFileError;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::FileTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }
}

/// An in-process stand-in for [`FileBroker`](crate::FileBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::testing::FileTestBroker;
///
/// let broker = FileTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct FileTestBroker {
    state: Arc<TestState>,
}

impl FileTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> FileTestPublisher {
        FileTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl Broker for FileTestBroker {
    type Error = SeaFileError;
    type Connected = ConnectedFileTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedFileTestBroker { state: self.state }))
    }
}

/// The connected form of [`FileTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedFileTestBroker {
    state: Arc<TestState>,
}

impl ConnectedFileTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> FileTestPublisher {
        FileTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl ConnectedBroker for ConnectedFileTestBroker {
    type Error = SeaFileError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl ConnectedFileTestBroker {
    /// Opens an in-process subscription on `name`: what both the `Subscribe` capability and the
    /// file transport's own [`FileStream`](crate::FileStream) descriptor resolve to.
    pub(crate) fn open(&self, name: &str) -> FileTestSubscriber {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        FileTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            name.to_owned(),
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )
    }
}

impl Subscribe for ConnectedFileTestBroker {
    type Subscriber = FileTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(Ok(self.open(name)))
    }
}

impl TestableBroker for ConnectedFileTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
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

ruststream::register_testable_broker!(ConnectedFileTestBroker);

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct FileTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for FileTestPublisher {
    type Error = SeaFileError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }
}

/// The publish policy for [`FileTestPublisher`], mirroring
/// [`FilePublish`](crate::FilePublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_sea_file::testing::FileTestPublish;
///
/// let policy = FileTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct FileTestPublish;

impl PublishPolicy<ConnectedFileTestBroker> for FileTestPublish {
    type Live = FileTestPublisher;

    fn pair(
        self,
        connected: &ConnectedFileTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedFileTestBroker {
    type Policy = FileTestPublish;
}
