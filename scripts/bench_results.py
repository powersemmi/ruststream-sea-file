#!/usr/bin/env python3
"""Turn a benchmark run into the published results document.

`benches/paired.rs` reports the scenarios it measured and the storage it measured them on,
because those are the only things it knows. This script reads that summary, adds the machine,
the build and the versions the run was taken against, and writes the document the documentation
site serves at `benchmarks/results.json`.

The schema is the core's, declared at
https://powersemmi.github.io/ruststream/latest/benchmarks/#publishing-results: schema 3, each
loop of the comparison as its best, median and worst round, and the `code` section.

`--code` reads the other run instead: the summary `cargo bench -- --output-format=json` writes for
the code-cost benches under `crates/ruststream-sea-file-bench/benches`, one JSON object per
benchmark. It writes the `code` section, one entry per scenario with instructions and allocations
per message plus what starting the service cost once, by the core's method: every scenario is
measured over one delivery, over MESSAGES and over twice MESSAGES, the slope between the last two
is the steady state, and the one-delivery run is the cold start. Either run keeps the section the
other one wrote.

There is no broker here: the transport is a file on the machine's own filesystem. The `broker`
field says so, and the filesystem, the size of a run's stream file and the state of the page
cache take its place - a replay served from cache and one served from the device are different
measurements, and the difference between them dwarfs the number being published.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json
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


# Deliveries per measured run of the code-cost benches, the default of their `MESSAGES`.
CODE_MESSAGES = 1000

# An instruction count below this on a code run means the measured region stopped matching its
# frame and the run reported the process exit, not that the code got faster. The cold run handles
# one delivery, so it is held to a lower floor.
CODE_FLOOR = 100_000
CODE_COLD_FLOOR = 1_000

# The code table, in reading order: the published name, the benchmark as `file/function`, and
# whether the benchmark's hard limit holds its allocation floor.
CODE_SCENARIOS = [
    ("consume, JSON decode into a small struct", "consume/service", True),
    ("reply, appended to the same file by this crate's publisher", "reply/service", True),
    ("consume in batches of 64, assembled on the client", "batch/service", True),
]


def code_metric(summary: dict, tool: str, name: str) -> int | None:
    """The new value of one metric, out of the nested summary the runner emits."""
    for profile in summary["profiles"]:
        metrics = profile["summaries"]["parts"][0]["metrics_summary"].get(tool)
        if not metrics or name not in metrics:
            continue
        values = metrics[name]["metrics"]
        entry = values["Both"][0] if "Both" in values else next(iter(values.values()))
        return int(entry["Int"])
    return None


def code_runs(path: Path) -> dict[str, dict]:
    """Every benchmark in the run, keyed by `file/function/id`."""
    found = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        summary = json.loads(line)
        key = f"{Path(summary['benchmark_file']).stem}/{summary['function_name']}/{summary['id']}"
        found[key] = {
            "instructions": code_metric(summary, "Callgrind", "Ir"),
            "allocations": code_metric(summary, "Dhat", "TotalBlocks"),
        }
    return found


def code_total(found: dict, key: str, floor: int) -> dict:
    """One run's totals, checked for the two ways this measurement fails silently."""
    if key not in found:
        sys.exit(f"benchmark {key} is not in the run: rename it here or in benches/")
    measured = found[key]
    if measured["instructions"] is None or measured["instructions"] < floor:
        sys.exit(
            f"benchmark {key} reports {measured['instructions']} instructions, below the floor of "
            f"{floor}: collection did not cover the measured region"
        )
    return measured


def per_message(figure: float) -> float:
    """Three places below one, so one allocation for the whole run does not read as zero."""
    return round(figure, 3) if abs(figure) < 1 else round(figure, 1)


def code_section(path: Path) -> list[dict]:
    found = code_runs(path)
    rows = []
    for name, key, gated in CODE_SCENARIOS:
        base = code_total(found, f"{key}/base", CODE_FLOOR)
        twice = code_total(found, f"{key}/twice", CODE_FLOOR)
        if twice["instructions"] <= base["instructions"]:
            sys.exit(f"benchmark {key} does not grow with the message count: no slope to read")
        first = code_total(found, f"{key}/first", CODE_COLD_FLOOR)
        rows.append(
            {
                "name": name,
                "messages": CODE_MESSAGES,
                "framework": {
                    metric: per_message((twice[metric] - base[metric]) / CODE_MESSAGES)
                    for metric in ("instructions", "allocations")
                },
                "cold": {metric: first[metric] for metric in ("instructions", "allocations")},
                "gated": gated,
            }
        )
    return rows


def valgrind() -> str:
    return run("valgrind", "--version").strip().removeprefix("valgrind-") or "unknown"


def main() -> int:
    args = sys.argv[1:]
    code = bool(args) and args[0] == "--code"
    if code:
        args = args[1:]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    source, out = Path(args[0]), Path(args[1])
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    if code:
        document = previous
        document["schema"] = 3
        document["code"] = code_section(source)
        # The code costs carry their own provenance: the paired numbers beside them may come
        # from another run, on another version, on another day.
        document["code_measured"] = {
            "crate_version": crate_version(),
            "core_version": core_version(),
            "measured_at": date.today().isoformat(),
        }
        document.setdefault("environment", {})["valgrind"] = valgrind()
    else:
        summary = json.loads(source.read_text(encoding="utf-8"))
        document = {
            "schema": 3,
            "crate": "ruststream-sea-file",
            "crate_version": crate_version(),
            "core_version": core_version(),
            "measured_at": date.today().isoformat(),
            "environment": environment(summary.get("storage", {})),
            "scenarios": summary["scenarios"],
        }
        if "code" in previous:
            document["code"] = previous["code"]
            if "code_measured" in previous:
                document["code_measured"] = previous["code_measured"]
            if "valgrind" in previous.get("environment", {}):
                document["environment"]["valgrind"] = previous["environment"]["valgrind"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out}")
    if code:
        for row in document["code"]:
            print(
                f"  {row['name']}: {row['framework']['instructions']} instructions, "
                f"{row['framework']['allocations']} allocations per message; cold "
                f"{row['cold']['instructions']} instructions, {row['cold']['allocations']} "
                "allocations"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
