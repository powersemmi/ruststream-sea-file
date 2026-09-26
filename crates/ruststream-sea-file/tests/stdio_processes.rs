//! The pipeline shape a stdio service ships in: real processes, real pipes.
//!
//! `producer | service | consumer` is what this transport is for, and nothing else here runs it.
//! The loopback check in `integration_sea.rs` attaches this process's own pipes, which proves the
//! client's line format but not that a service composes with the processes on either side of it -
//! that it reads what a producer writes, answers on its own standard output, and leaves alone the
//! stream keys it did not subscribe to. So the stage under test is a child process: the
//! `stdio_pipeline` example, spawned with its standard input and output piped here.
//!
//! The refusal at the end shares the file because it is the same subject from the other side: what
//! the real broker does with a registration whose deferred copies have nowhere to go. It attaches
//! no pipes of its own - a stdio connect touches standard input and output only when a
//! subscription or a publish does - so it sits beside the child-process tests without disturbing
//! them.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ruststream::runtime::{AppInfo, RustStream};
use ruststream_sea_file::stdio::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// How long the stage is given to answer a line.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Deserialize, Serialize)]
struct Job {
    id: u64,
}

/// The example binary the current build made beside this test.
///
/// Examples land next to the test binaries: under their plain name when the artifact and build
/// directories are the same, and under a content hash when they are split. Every spelling is a
/// candidate, and the newest one is the build of this run: a build directory kept between runs
/// (another toolchain, a cached target directory) also holds older builds of the example, which
/// may not read what this test sends. A `cargo test --examples` run also builds each example as a
/// test harness, which answers any argument with the test runner's report; that build is passed
/// over.
fn example_binary(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary has a path");
    let dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("the test binary sits inside the profile directory")
        .join("examples");
    std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("the examples directory {} must exist: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().is_none()
                && path
                    .file_name()
                    .and_then(|file| file.to_str())
                    .is_some_and(|file| file == name || file.starts_with(&format!("{name}-")))
                && !is_test_harness(path)
        })
        .filter_map(|path| {
            let built = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok()?;
            Some((built, path))
        })
        .max_by_key(|(built, _)| *built)
        .map_or_else(
            || {
                panic!(
                    "the `{name}` example must be built beside the tests, in {}",
                    dir.display(),
                )
            },
            |(_, path)| path,
        )
}

/// Whether `binary` is a test-harness build: libtest names its thread setting in every binary it
/// links into, and the example proper never links it.
fn is_test_harness(binary: &Path) -> bool {
    let marker = b"RUST_TEST_THREADS";
    std::fs::read(binary).is_ok_and(|bytes| {
        bytes
            .windows(marker.len())
            .any(|window| window == marker.as_slice())
    })
}

/// One line in the client's format: the meta fields, then the payload.
fn line(stream: &str, sequence: u64, job: u64) -> String {
    format!("[2024-01-01T00:00:00 | {stream} | {sequence}] {{\"id\":{job}}}\n")
}

/// A service that is one stage of a pipeline: it reads the stream key it subscribed to, answers
/// on its own standard output under the key its reply type declares, and passes over a line on a
/// key it never asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stage_answers_the_keys_it_reads_and_ignores_the_rest() {
    let mut stage = Command::new(example_binary("stdio_pipeline"))
        .arg("run")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the pipeline stage starts");

    let mut input = stage.stdin.take().expect("the stage reads standard input");
    // The middle line is a key this stage never subscribed to. Order is what makes its absence
    // provable: if the stage answered it, that answer would arrive before job 9's.
    for written in [line("jobs", 1, 7), line("chores", 1, 8), line("jobs", 2, 9)] {
        input
            .write_all(written.as_bytes())
            .await
            .expect("the stage accepts input");
    }
    input.flush().await.expect("the input reaches the stage");

    let mut output = BufReader::new(stage.stdout.take().expect("the stage writes output")).lines();
    let mut answers = Vec::new();
    while answers.len() < 2 {
        let read = tokio::time::timeout(ANSWER_TIMEOUT, output.next_line())
            .await
            .expect("the stage answers rather than hanging")
            .expect("the stage's output is readable");
        let Some(read) = read else {
            panic!("the stage closed its output after {answers:?}");
        };
        answers.push(read);
    }

    for (answer, id) in answers.iter().zip([7, 9]) {
        assert!(
            answer.contains("results"),
            "a reply travels under the stream key its type declares, got {answer}",
        );
        assert!(
            answer.contains(&format!("{{\"id\":{id}}}")),
            "the stage answers each job it read, in order, got {answer}",
        );
    }

    stage.kill().await.expect("the stage stops");
}

/// A payload a line cannot carry as it is - one that holds newlines and is padded with spaces -
/// crosses the pipe in the envelope the publisher writes for it, and the stage reads it whole:
/// the job it decodes is the one that was sent, not a fragment of the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_a_line_cannot_carry_crosses_the_pipe_whole() {
    let mut stage = Command::new(example_binary("stdio_pipeline"))
        .arg("run")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the pipeline stage starts");

    // The envelope: a four-byte length of the header block (none here), then the payload, in
    // base64 behind the `rs1:` prefix.
    let payload = b"  {\n  \"id\": 11\n}\n  ";
    let mut framed = 0_u32.to_be_bytes().to_vec();
    framed.extend_from_slice(payload);
    let envelope = format!("rs1:{}", BASE64.encode(&framed));
    let mut input = stage.stdin.take().expect("the stage reads standard input");
    input
        .write_all(format!("[2024-01-01T00:00:00 | jobs | 1] {envelope}\n").as_bytes())
        .await
        .expect("the stage accepts input");
    input.flush().await.expect("the input reaches the stage");

    let mut output = BufReader::new(stage.stdout.take().expect("the stage writes output")).lines();
    let answer = tokio::time::timeout(ANSWER_TIMEOUT, output.next_line())
        .await
        .expect("the stage answers rather than hanging")
        .expect("the stage's output is readable")
        .expect("the stage answers the job");
    assert!(
        answer.contains("{\"id\":11}"),
        "the stage read the whole payload, got {answer}",
    );

    stage.kill().await.expect("the stage stops");
}

/// Asks for a pause before another attempt, which on a pipe means a copy sent downstream.
#[subscriber("jobs")]
async fn defer(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::retry_after(Duration::from_secs(30))
}

/// The refusal a pipeline stage gets from the real broker: standard output reaches the next
/// process and never this one's standard input, so a registration whose deferred copies have no
/// named destination does not start, and says which subscription and which step would fix it.
///
/// A service that started anyway would drop every delayed message on the floor, which is the
/// failure this refusal exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registration_with_nowhere_to_put_its_copies_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("pipeline", "0.1.0")).with_broker(StdioBroker::new(), |b| {
            b.include(defer);
        });

    let failed = app
        .start()
        .await
        .expect_err("a registration whose copies go nowhere must not start");
    let message = failed.to_string();
    assert!(message.contains("jobs"), "{message}");
    assert!(message.contains("NamedCopies"), "{message}");
    assert!(message.contains(".to("), "{message}");
}
