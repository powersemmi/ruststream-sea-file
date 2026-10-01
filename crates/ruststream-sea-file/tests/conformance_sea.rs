//! Conformance: every suite this crate's capabilities justify, run against a real stream file in
//! the temp directory and against the same broker in process.
//!
//! Both modes, because either one alone leaves a hole. The file is what ships, so it is what the
//! contract is really about; the in-process mode is what a service's own tests run on, so a
//! contract it quietly fails is one a test author is misled by. Where the two disagree the
//! disagreement is the finding, and the in-process transport is what gets fixed. The suites that
//! compare the two transports directly (`settlement::matches_in_process`,
//! `in_process::backlog_matches_server`, `in_process::refuses_like_the_server`) connect both.
//!
//! The routing suite runs in process only, because of how it is built: it drives the broker
//! through `TestableBroker`, which reads and feeds the in-process transport.
//!
//! The suites that take `request_reply`, `transactions` and `owned_transactions` are left out:
//! neither transport implements those capabilities. `retry::broker_moves` is left out because
//! both transports leave the retry cap to the runtime, and `message_shape::keyed_order` and
//! `message_shape::publish_options` because a publish carries neither a key nor per-message
//! settings. `lifecycle::shared_handle_closes` is left out because the connected forms are not
//! `Clone`, and `message_shape::describes_addresses_without_credentials` because neither broker
//! is configured with network addresses.
//!
//! The stdio form runs its suites in process only: a real stdio shutdown ends every stdio
//! consumer and producer in the process by the client's design, so one suite would take the rest
//! of the binary's tests down with it. Its round trip is covered over a real pipe in
//! `integration_sea.rs`.

#![cfg(feature = "testing")]

mod common;

use std::time::Duration;

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::{capabilities, harness, lifecycle, message_shape, retry, settlement};
use ruststream::testing::Backlog;
use ruststream_sea_file::{
    ConnectedFileBroker, ConnectedStdioBroker, FileBroker, FilePosition, FilePublish, FileStream,
    StdioBroker, StdioPublish,
};

/// The path the in-process file broker is built with; nothing is opened at it.
const IN_PROCESS_PATH: &str = "/var/lib/ruststream/conformance.ss";

/// A stream key the file's key grammar refuses: a space is outside it.
const REFUSED_KEY: &str = "not a stream key";

/// A sequence no stream key of a conformance run reaches, so a seek there asks the file for a
/// message it does not hold.
const UNWRITTEN_SEQUENCE: u64 = 1_000_000;

/// Nothing the broker or the descriptor contributes to a document may carry a password.
///
/// Neither transport authenticates: a stream file is opened by path and a pipe by being the
/// process's own, so there is no credential to plant and the scan looks for a string the
/// configuration never held. It runs anyway, as the guard it is: the document reports the path
/// and the stream key on purpose, and a field added later that serialized the broker's
/// configuration wholesale would be caught here rather than after it shipped. The publish policies
/// are scanned the same way.
#[test]
fn the_document_carries_no_credential() {
    harness::describes_without_credentials(
        &FileBroker::new(common::tmp_path("credentials")),
        &FileStream::new("orders"),
        "hunter2",
    );
    harness::describes_without_credentials(&StdioBroker::new(), &Name::new("lines"), "hunter2");
    message_shape::publishes_without_credentials::<ConnectedFileBroker, _>(&FilePublish, "hunter2");
    message_shape::publishes_without_credentials::<ConnectedStdioBroker, _>(
        &StdioPublish,
        "hunter2",
    );
}

#[test]
fn file_broker_passes_conformance_suite_in_process() {
    common::rt().block_on(async {
        harness::run_suite(|| FileBroker::new(IN_PROCESS_PATH)).await;
    });
}

/// The stdio broker answers the routing contract in process the same way, because a service's own
/// tests run on it: ordering, settlement, headers and the publish log are the transport's.
#[test]
fn stdio_broker_passes_conformance_suite_in_process() {
    common::rt().block_on(async {
        harness::run_suite(StdioBroker::new).await;
    });
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.

/// The ladder on the stdio broker in process, opened through the bare stream key a pipeline stage
/// writes. The suite publishes to the key it subscribed, which on a pipe reaches the service's own
/// standard input only under loopback, so that is the broker it runs. `redelivery_address` is
/// left out: a pipe addresses none of its copies.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn stdio_broker_passes_lifecycle_in_process() {
    common::rt().block_on(async {
        harness::lifecycle(
            || InProcessBroker::new(StdioBroker::new().loopback()),
            |name| Name::new(name.to_owned()),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The batch contract on the stdio transport in process: a pipe batches on the client the way a
/// file does, so a batch handler under the harness sees the size the mount site asked for.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn stdio_broker_passes_batch_suite_in_process() {
    common::rt().block_on(async {
        capabilities::batches(
            || InProcessBroker::new(StdioBroker::new().loopback()),
            |name| Name::new(name.to_owned()),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The ladder against a real stream file. It opens with the message-shape check: every header
/// comes back byte for byte, a non-UTF-8 value included.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_lifecycle() {
    common::on_a_file(async {
        let path = common::tmp_path("lifecycle");
        let file = path.clone();
        harness::lifecycle(
            move || FileBroker::new(file.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same ladder in process, including the publisher that outlives the shutdown: an in-process
/// transport that kept accepting publishes through a dead handle would teach a service's tests
/// that the aliasing case is harmless, which against a real file it is not.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_lifecycle_in_process() {
    common::rt().block_on(async {
        harness::lifecycle(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// A publish made right before the shutdown reaches a subscription another connection holds on
/// the same file. The file keeps no consumer positions and a live subscription starts at the tip,
/// so the observer opens before the publish: `Backlog::Missed`. Two brokers over one path reach
/// one file, the way two connections reach one server; in process every broker holds a file of
/// its own, so the check runs against the real file only.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_flushes_on_shutdown() {
    common::on_a_file(async {
        let path = common::tmp_path("shutdown-flushes");
        lifecycle::shutdown_flushes(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
            Backlog::Missed,
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// What each settlement answers on a real file, and that the in-process transport answers the
/// same: neither acknowledges, so a service's retry test cannot pass on a settlement production
/// never performs. The transport hands nothing back on its own, so the redelivery timeout is zero.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_settles_like_in_process() {
    common::on_a_file(async {
        let path = common::tmp_path("settlement");
        settlement::matches_in_process(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
            Duration::ZERO,
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The address the descriptor reports, held to its promise against real stream files: a publish
/// there must reach the subscription that reported it. This is what a `retry_after` rests on here,
/// because neither transport settles a delivery. The bare stream key runs it too: the connected
/// form answers `AddressedCopies` for a name, which is how `#[subscriber("orders")]` retries.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_redelivery_address() {
    common::on_a_file(async {
        let path = common::tmp_path("redelivery-address");
        retry::redelivery_address(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        retry::redelivery_address(
            || FileBroker::new(path.clone()),
            |name| Name::new(name.to_owned()),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same promise in process, because a service's own retry tests run here: an in-process
/// transport that reported an address nothing arrived at would pass a registration the file
/// refuses.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_redelivery_address_in_process() {
    common::rt().block_on(async {
        retry::redelivery_address(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        retry::redelivery_address(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| Name::new(name.to_owned()),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The in-process file declares what a real file does with a message written before a
/// subscription opens: a live subscription starts at the tip and never sees it.
#[allow(clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_backlog_matches_the_file() {
    common::on_a_file(async {
        let path = common::tmp_path("backlog");
        in_process::backlog_matches_server(
            || FileBroker::new(path.clone()),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The in-process file refuses what a real file refuses: a stream key outside the key grammar, on
/// the publish and on the subscription.
#[allow(clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_refuses_like_the_file() {
    common::on_a_file(async {
        let path = common::tmp_path("refusals");
        in_process::refuses_like_the_server(
            || FileBroker::new(path.clone()),
            |connected| connected.publisher(),
            [
                Refusal::Publish {
                    name: REFUSED_KEY.to_owned(),
                },
                Refusal::Subscription {
                    source: FileStream::new(REFUSED_KEY),
                },
            ],
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The batch contract against real stream files: the suite opens its subscription at a size
/// smaller than the run, so a batch coming back longer than the mount site asked for fails here.
/// Neither client batches on the wire, so what this pins is the client-side assembly.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_batch_suite() {
    common::on_a_file(async {
        let path = common::tmp_path("batches");
        capabilities::batches(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The same contract on the in-process transport, which batches the same way, so a batch handler
/// under the harness sees what it would see against a file.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_batch_suite_in_process() {
    common::rt().block_on(async {
        capabilities::batches(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// A batched subscription on a file is seekable too: a seek back to a position captured from a
/// batch restarts the batches there.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_batch_seeking_suite() {
    common::on_a_file(async {
        let path = common::tmp_path("batch-seeking");
        capabilities::batch_seeking(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_batch_seeking_suite_in_process() {
    common::rt().block_on(async {
        capabilities::batch_seeking(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// The seeking contract against a real stream file: pinned captured positions, a seek forward
/// skipping what is queued, a seek from a runtime that stops right after, and a seek through a
/// seeker that outlived the shutdown, which must be refused.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_seeking_suite() {
    common::on_a_file(async {
        let path = common::tmp_path("seeking");
        capabilities::seeking(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

/// The seeking contract in process, on the retained log in memory. This is the capability a
/// service is most likely to write tests around, so the in-process file owes it the same answers
/// a file gives.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_passes_seeking_suite_in_process() {
    common::rt().block_on(async {
        capabilities::seeking(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
        )
        .await;
    });
}

/// A seek to a sequence the stream key has not written is refused: the file reads forward for the
/// message and runs out, and the subscription does not move on to somewhere else.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_refuses_an_unwritten_position() {
    common::on_a_file(async {
        let path = common::tmp_path("seeking-unknown");
        capabilities::seeking_unknown_position(
            || FileBroker::new(path.clone()),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
            |_subject| FilePosition::sequence(UNWRITTEN_SEQUENCE),
        )
        .await;
        let _ = std::fs::remove_file(&path);
    });
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[test]
fn file_broker_refuses_an_unwritten_position_in_process() {
    common::rt().block_on(async {
        capabilities::seeking_unknown_position(
            || InProcessBroker::new(FileBroker::new(IN_PROCESS_PATH)),
            |name| FileStream::new(name),
            |connected| connected.publisher(),
            |_subject| FilePosition::sequence(UNWRITTEN_SEQUENCE),
        )
        .await;
    });
}
