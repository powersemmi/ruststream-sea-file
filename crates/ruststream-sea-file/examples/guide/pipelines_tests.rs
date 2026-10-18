//! The tests of the Shell pipelines guide's stage written without the macros,
//! `examples/manual/guide/pipelines.rs`.
//!
//! The stage is a plain example, `manual_guide_pipelines`: it is the binary a pipeline runs, and
//! its live test spawns that binary as a child process. A test build of it would be a test
//! harness instead, so the tests run from this target, which compiles the same file as a module.
//!
//! ```text
//! cargo build --example manual_guide_pipelines --features testing
//! cargo test --example manual_guide_pipelines_tests --features testing
//! ```

// The stage's `main` belongs to its own binary.
#[allow(dead_code)]
#[path = "pipelines.rs"]
mod stage;

fn main() {}
