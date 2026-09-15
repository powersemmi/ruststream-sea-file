set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo check --workspace --all-targets --all-features
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
