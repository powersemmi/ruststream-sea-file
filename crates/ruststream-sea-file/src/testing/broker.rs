//! [`FileTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, Publisher, RawMessage,
    RedeliveryAddress, Subscribe,
};

use crate::error::SeaFileError;
use crate::file::FilePublish;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::FileTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// Set once by `shutdown`. The ladder makes owner-side misuse a compile error, but the
    /// connected form is shareable and hands out publishers, so a handle can outlive the
    /// connection here exactly as it can on the real transports; without this flag such a
    /// handle would keep succeeding against a broker that is gone.
    closed: AtomicBool,
}

impl TestState {
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// Rejects use of a handle that outlived the connection, the way the real transports do.
    fn ensure_open(&self) -> Result<(), SeaFileError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SeaFileError::NotConnected);
        }
        Ok(())
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
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl ConnectedFileTestBroker {
    /// Opens an in-process subscription on `name`: what both the `Subscribe` capability and the
    /// file transport's own [`FileStream`](crate::FileStream) descriptor resolve to.
    ///
    /// # Errors
    ///
    /// Returns [`SeaFileError::NotConnected`] once the broker has been shut down.
    pub(crate) fn open(&self, name: &str) -> Result<FileTestSubscriber, SeaFileError> {
        self.state.ensure_open()?;
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(FileTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            name.to_owned(),
            rx,
            requeue,
            self.state.coordinator().cloned(),
        ))
    }
}

impl Subscribe for ConnectedFileTestBroker {
    type Subscriber = FileTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name))
    }

    /// The stream key, the answer a stream file gives: a service whose mount site binds the
    /// deferred-retry position against a file must be able to start under the harness too.
    fn redelivery_address(&self, name: &str) -> Option<RedeliveryAddress> {
        Some(RedeliveryAddress::new(name.to_owned()))
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
///
/// A publisher handed out before the shutdown outlives the connection, so publishing through it
/// afterwards reports [`SeaFileError::NotConnected`] rather than routing into a broker that is
/// gone - the same answer the file and stdio publishers give.
#[derive(Debug, Clone)]
pub struct FileTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for FileTestPublisher {
    type Error = SeaFileError;
    // The same unit type both real publishers declare: a test must not be able to set something
    // in process that a stream file would have nowhere to put.
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.state.ensure_open().map(|()| {
            self.state.publish(
                msg.name(),
                Bytes::copy_from_slice(msg.payload()),
                msg.headers().clone(),
            );
        }))
    }
}

/// The default reply policy is the file transport's own: a `publish("dest")` handler included
/// without an explicit policy is wired here exactly as it is wired against a stream file.
impl DefaultPublish for ConnectedFileTestBroker {
    type Policy = FilePublish;
}
