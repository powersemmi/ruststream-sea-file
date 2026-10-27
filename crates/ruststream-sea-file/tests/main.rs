//! The integration suite, one test binary: each file under `tests/` is a module of it, but for
//! the two suites that own the process's standard input and output.

mod common;

mod asyncapi;
mod both_modes;
mod conformance_sea;
mod connect_runtime;
mod envelopes;
mod file_positions;
mod file_retry;
mod in_process;
mod integration_sea;
mod preludes;
mod publish_policies;
mod redelivery;
mod resume;
mod seek_context;
mod size_limit;
mod stdio_line_format;
mod stdio_processes;
mod subscription_sources;
