//! Test setup for the examples whose service reads what an earlier producer left in its stream
//! file.
//!
//! In process a stream file is a log in memory, new for each broker value and shared by the
//! broker's clones, as they share the file they connect. So the log is recorded on a clone of the
//! service's broker before the service starts, the way an earlier producer would have left it.

use std::error::Error;

use ruststream::testing::InProcess;
use ruststream_sea_file::file::prelude::*;
use serde::Serialize;

/// Appends `messages` to the file `file` names, each under the stream key its type declares,
/// before the service starts.
pub(crate) async fn record<T>(
    file: &FileBroker,
    messages: impl IntoIterator<Item = T>,
) -> Result<(), Box<dyn Error>>
where
    T: Serialize
        + OutgoingDestination<Form = FixedName>
        + MessageHeaders<Contract = NoHeaders>
        + Sync,
{
    let _connected = file.clone().connect_in_process().await?;
    let publisher = file.publisher();
    for message in messages {
        publisher.message(&message).publish().await?;
    }
    Ok(())
}
