//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the file and stdio transports.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the client's
/// error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SeaFileError {
    /// Opening (or creating) the stream file, or attaching stdio, failed.
    #[error("stream connect error on '{target}': {source}")]
    Connect {
        /// The file path or `stdio`.
        target: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Opening a subscription failed.
    #[error("subscribe error on '{stream}': {source}")]
    Subscribe {
        /// The stream key the subscription targeted.
        stream: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The transport failed while receiving.
    #[error("receive error on '{stream}': {source}")]
    Receive {
        /// The stream key (or file) of the subscription.
        stream: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Publishing failed.
    #[error("publish error to '{stream}': {source}")]
    Publish {
        /// The stream key the message targeted.
        stream: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Repositioning failed.
    #[error("seek error on '{stream}': {source}")]
    Seek {
        /// The stream key of the subscription.
        stream: String,
        /// The client's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared state, or after `shutdown`.
    #[error("stream transport is not connected")]
    NotConnected,

    /// A descriptor or payload is invalid for this transport.
    #[error("invalid descriptor: {0}")]
    Invalid(String),
}

/// Boxes a client error into the crate's `Box<dyn StdError>` source form.
pub(crate) fn box_err<E>(err: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(err)
}
