set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows.
    cargo clippy --workspace --exclude ruststream-sea-file-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-sea-file-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-sea-file-bench --all-targets --all-features
    cargo check -p ruststream-sea-file-bench --all-targets
    cargo check --workspace --no-default-features

test:
    cargo test --workspace --all-features

# The suites that run against the real transports: stream files in the temp directory, the
# process's own standard input and output, and the shipped pipeline stage as a child process.
# There is no server and no docker stand to start - on this broker the local machine is the
# transport - so `just test` covers these too; the recipe exists so the fleet's live-suite command
# means the same thing in every broker repository.
#
# --examples: the pipeline suite spawns the `stdio_pipeline` example, which a run naming test
# targets would otherwise not build.
test-brokers:
    cargo test --workspace --all-features --examples \
        --test integration_sea --test conformance_sea --test file_positions \
        --test file_retry --test stdio_processes --test stdio_retry

# What this crate costs over the sea-streamer-file client it wraps, and what the runtime costs on
# top: every scenario runs three times over - the client driven directly, this crate's own consumer
# and publisher hand-driven, and the service a user writes. There is no stand to start - on this
# broker the local machine is the transport - so the recipe only points the runs at a directory for
# their stream files and removes it again afterwards. On demand only: it takes minutes, it writes
# tens of gigabytes through that directory, and it wants the machine to itself. The page it feeds
# is docs/benchmarks.md.
bench *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    streams="$PWD/target/bench-streams"
    trap 'rm -rf "$streams"' EXIT
    mkdir -p "$streams"
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" RUSTSTREAM_BENCH_DIR="$streams" \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-sea-file-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean

ci: check test typo security
