#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and the storage it measured them on,
because those are the only things it knows. This script reads that summary, adds the machine,
the build and the versions the run was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results. This crate
publishes the throughput table alone, each loop as its best and worst round, so the document
declares schema 3.

There is no broker here: the transport is a file on the machine's own filesystem. The `broker`
field says so, and the filesystem, the size of a run's stream file and the state of the page
cache take its place - a replay served from cache and one served from the device are different
measurements, and the difference between them dwarfs the number being published.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
"""

import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "Cargo.toml"
LOCK = REPO / "Cargo.lock"

# What `just bench` builds and runs the benchmark with. These are recipe decisions rather than
# machine facts, so they are stated here next to the recipe rather than sniffed.
PROFILE = "bench, inheriting release (opt-level = 3, lto = false, codegen-units = 16)"
FEATURES = "ruststream-sea-file default (none), ruststream macros,json"
RUSTFLAGS = "none (the recipe clears RUSTFLAGS, so the numbers are not tied to this CPU)"
BROKER = "none: the transport is a stream file on this machine's filesystem"
PAGE_CACHE = (
    "warm: every run writes its stream file immediately before reading it back, "
    "so the figures are the filesystem's rather than the device's"
)


def run(*args: str) -> str:
    try:
        return subprocess.run(args, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def proc_field(path: str, key: str) -> str:
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        name, _, value = line.partition(":")
        if name.strip() == key:
            return value.strip()
    return ""


def lscpu() -> dict[str, str]:
    fields = {}
    for line in run("lscpu").splitlines():
        name, _, value = line.partition(":")
        fields[name.strip()] = value.strip()
    return fields


def cores(cpu: dict[str, str]) -> str:
    physical = cpu.get("Core(s) per socket", "")
    sockets = cpu.get("Socket(s)", "1")
    logical = cpu.get("CPU(s)", "")
    if not physical or not logical:
        return "unknown"
    return f"{int(physical) * int(sockets)} physical, {logical} logical"


def frequency(cpu: dict[str, str]) -> str:
    low, high = cpu.get("CPU min MHz", ""), cpu.get("CPU max MHz", "")
    if not low or not high:
        return "unknown"
    return f"{float(low.replace(',', '.')):.0f}-{float(high.replace(',', '.')):.0f} MHz"


def memory() -> str:
    total = proc_field("/proc/meminfo", "MemTotal")
    if not total.endswith(" kB"):
        return "unknown"
    return f"{int(total[:-3]) / (1024 * 1024):.1f} GiB"


def filesystem(directory: str) -> str:
    """The filesystem the stream files were written to, named the way `findmnt` names it.

    The directory itself is transient - the recipe removes it once the run is over - so the
    lookup climbs to the nearest ancestor that is still there. A mount point is what is being
    asked about, and that does not move when a subdirectory goes.
    """
    path = Path(directory).resolve()
    while not path.exists() and path != path.parent:
        path = path.parent
    line = run("findmnt", "-no", "FSTYPE,SOURCE", "-T", str(path)).split()
    return f"{line[0]} on {line[1]}" if len(line) >= 2 else "unknown"


def file_size(byte_count: int) -> str:
    if byte_count <= 0:
        return "unknown"
    return f"{byte_count / (1024 * 1024 * 1024):.2f} GiB per run at its largest"


def round_trip(nanos: int) -> str:
    """What the filesystem charges for one durable append, measured outside every loop.

    A transport with no server still makes a delivery wait on something, and this is it. The
    `broker_bound` flag on a row is decided against this figure, so it is published with the
    numbers and the arithmetic can be checked.
    """
    if nanos <= 0:
        return "unknown"
    return f"{nanos / 1000:.1f} us per durable append"


def crate_version() -> str:
    match = re.search(r'^version = "([^"]+)"', MANIFEST.read_text(encoding="utf-8"), re.M)
    return match.group(1) if match else "unknown"


def core_version() -> str:
    match = re.search(
        r'^name = "ruststream"\nversion = "([^"]+)"', LOCK.read_text(encoding="utf-8"), re.M
    )
    return match.group(1) if match else "unknown"


def environment(storage: dict) -> dict[str, str]:
    cpu = lscpu()
    return {
        "cpu": proc_field("/proc/cpuinfo", "model name") or cpu.get("Model name", "unknown"),
        "architecture": cpu.get("Architecture", "unknown"),
        "cpu_frequency": frequency(cpu),
        "cores": cores(cpu),
        "memory": memory(),
        "memory_speed": "unknown",
        "os": f"Linux {run('uname', '-r').strip()}",
        "broker": BROKER,
        "filesystem": filesystem(storage.get("directory", str(REPO))),
        "file_size": file_size(storage.get("largest_file_bytes", 0)),
        "page_cache": PAGE_CACHE,
        "round_trip": round_trip(storage.get("round_trip_nanos", 0)),
        "rustc": run("rustc", "--version").replace("rustc", "").strip().split()[0],
        "profile": PROFILE,
        "features": FEATURES,
        "rustflags": RUSTFLAGS,
    }


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    document = {
        "schema": 3,
        "crate": "ruststream-sea-file",
        "crate_version": crate_version(),
        "core_version": core_version(),
        "measured_at": date.today().isoformat(),
        "environment": environment(summary.get("storage", {})),
        "scenarios": summary["scenarios"],
    }
    out = Path(sys.argv[2])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
