//! How a pipeline stage reads the lines on its standard input, on real pipes.
//!
//! The header of a line is read off standard input itself, so the only faithful place to check
//! what a header may hold is a stage running as a child process. The stage is the
//! `stdio_pipeline` example: it answers each `jobs` line with a `results` line, and ignores the
//! rest.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

/// How long the stage is given to answer a line.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(20);

/// The example binary the current build made beside this test, as `stdio_processes` finds it:
/// the newest plain build in the first `examples` directory above the test binary that holds one.
fn example_binary(name: &str) -> PathBuf {
    let exe = std::env::current_exe().expect("the test binary has a path");
    exe.ancestors()
        .skip(1)
        .map(|ancestor| ancestor.join("examples"))
        .find_map(|dir| example_in(&dir, name))
        .unwrap_or_else(|| {
            panic!(
                "the `{name}` example must be built beside the tests, in an `examples` directory \
                 above {}",
                exe.display(),
            )
        })
}

/// The newest build of the `name` example in one `examples` directory, under its plain name or a
/// hashed one, leaving out test-harness builds.
fn example_in(dir: &Path, name: &str) -> Option<PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
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
        .map(|(_, path)| path)
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

/// The pipeline stage, with its standard input and output piped here.
fn stage() -> Child {
    Command::new(example_binary("stdio_pipeline"))
        .arg("run")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the pipeline stage starts")
}

/// Writes `input` to the stage and returns the first line it answers.
async fn first_answer(stage: &mut Child, input: &[u8]) -> String {
    let mut stdin = stage.stdin.take().expect("the stage reads standard input");
    stdin
        .write_all(input)
        .await
        .expect("the stage accepts input");
    stdin.flush().await.expect("the input reaches the stage");
    let mut output = BufReader::new(stage.stdout.take().expect("the stage writes output")).lines();
    let answer = tokio::time::timeout(ANSWER_TIMEOUT, output.next_line())
        .await
        .expect("the stage answers rather than hanging")
        .expect("the stage's output is readable")
        .expect("the stage answers the line");
    // Kept open until the answer is read: a closed input is not what is under test.
    drop(stdin);
    answer
}

/// A header that holds one word is the stream key, with no timestamp and no sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_of_one_word_names_the_stream_key() {
    let mut stage = stage();

    let answer = first_answer(&mut stage, b"[jobs] {\"id\":5}\n").await;

    assert!(answer.contains("results"), "got {answer}");
    assert!(answer.contains("{\"id\":5}"), "got {answer}");
    stage.kill().await.expect("the stage stops");
}

/// A line with no header names no stream key, and the client hands it to the stage's
/// subscription whatever key that subscription reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_line_with_no_key_reaches_the_subscription() {
    let mut stage = stage();

    let answer = first_answer(&mut stage, b"{\"id\":6}\n").await;

    assert!(answer.contains("results"), "got {answer}");
    assert!(answer.contains("{\"id\":6}"), "got {answer}");
    stage.kill().await.expect("the stage stops");
}

/// A line that is not UTF-8 is not one the format defines: the stage passes over it and answers
/// the lines after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_line_that_is_not_utf8_is_passed_over() {
    let mut stage = stage();

    let answer = first_answer(&mut stage, b"[jobs] \xff\xfe\n[jobs] {\"id\":3}\n").await;

    assert!(answer.contains("{\"id\":3}"), "got {answer}");
    stage.kill().await.expect("the stage stops");
}

/// A line that opens with a bracket that is no header, a JSON array among them, is not one the
/// format defines either: the stage passes over it and answers the lines after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leading_bracket_that_is_no_header_is_passed_over() {
    let mut stage = stage();

    let answer = first_answer(&mut stage, b"[1,2]\n[jobs] {\"id\":4}\n").await;

    assert!(answer.contains("{\"id\":4}"), "got {answer}");
    stage.kill().await.expect("the stage stops");
}
