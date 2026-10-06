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
benchmark, in the summary layout gungraun 0.20 writes (its version 7). It writes the `code`
section, one entry per scenario with instructions and allocations per message plus what starting
the service cost once, by the core's method: every scenario is measured over one delivery, over
MESSAGES and over twice MESSAGES, the slope between the last two is the steady state, and the
one-delivery run is the cold start. `--messages` names MESSAGES when the benches were built with
a count other than the default. Either run keeps the section the other one wrote.

A code-cost benchmark that breaches one of its limits fails the run, and in this output format the
runner says nothing more about it: what went over is recorded in the summary alone. So every
breach is printed under the table, the value the run was compared against next to the new one,
and a summary that cannot be converted still prints its breaches before it stops.

There is no broker here: the transport is a file on the machine's own filesystem. The `broker`
field says so, and the filesystem, the size of a run's stream file and the state of the page
cache take its place - a replay served from cache and one served from the device are different
measurements, and the difference between them dwarfs the number being published.

A field the machine does not publish is written as `unknown` rather than guessed: memory speed
comes from the DMI tables, which most systems only let root read.

    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json
    python3 scripts/bench_results.py --code --messages 5000 target/bench-code.json \\
        docs/benchmarks/results.json
"""

import argparse
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


# The summary layout the code-cost run is read in. Every summary states its layout in `version`,
# and a gungraun release that changes the layout changes the number, so a summary of another
# version stops the conversion with a message naming both rather than with a missing field.
SUMMARY_VERSION = "7"

# Deliveries per measured run of the code-cost benches when the recipe names no other count: the
# default of their `MESSAGES`, and the count the published document is measured at.
CODE_MESSAGES = 1000

# An instruction count below this on a code run of the default count means the measured region
# stopped matching its frame and the run reported the process exit, not that the code got faster.
# The floor scales with the count. The cold run handles one delivery, so it is held to a lower
# floor.
CODE_FLOOR = 100_000
CODE_COLD_FLOOR = 1_000

# The three runs of every scenario, by the benchmark id that carries each: the cold start on its
# own, and the two counts whose difference is the steady state.
COLD = "first"
COUNTS = ("base", "twice")

# The code table, in reading order: the published name, the benchmark as `file/function`, and
# whether the benchmark's hard limit holds its allocation floor.
CODE_SCENARIOS = [
    ("consume, JSON decode into a small struct", "consume/service", True),
    ("reply, appended to the same file by this crate's publisher", "reply/service", True),
    ("consume in batches of 64, assembled on the client", "batch/service", True),
]

# The two metrics the table reads, by the names it gives them. A limit on any other metric is
# reported under the runner's own name for it.
METRIC_NAMES = {("Callgrind", "Ir"): "instructions", ("Dhat", "TotalBlocks"): "allocations"}


def code_metric(summary: dict, tool: str, name: str) -> int | None:
    """The new value of one metric: the total of one tool's run, as the runner reports it."""
    for profile in summary["profiles"]:
        if profile["tool"] != tool:
            continue
        values = profile["data"]["total"]["metrics"].get(name, {}).get("values", {})
        # A run compared against a baseline carries the old value next to the new one.
        new = values.get("new")
        return None if new is None else int(new)
    return None


def code_summaries(path: Path) -> list[dict]:
    """Every benchmark summary the code run wrote, one per line, in the layout this script reads."""
    found = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        summary = json.loads(line)
        version = summary.get("version")
        if version != SUMMARY_VERSION:
            sys.exit(
                f"the benchmark summary has layout version {version}, and this script reads "
                f"version {SUMMARY_VERSION}: read the new layout in `code_metric` and `breaches` "
                "and raise SUMMARY_VERSION"
            )
        found.append(summary)
    return found


def benchmark(summary: dict) -> str:
    """The `file/function` a summary belongs to, which is how a scenario names its benchmark."""
    return f"{Path(summary['benchmark_file']).stem}/{summary['function_name']}"


def code_runs(summaries: list[dict]) -> dict[str, dict]:
    """Every benchmark in the run, keyed by `file/function/id`."""
    return {
        f"{benchmark(summary)}/{summary['id']}": {
            "instructions": code_metric(summary, "Callgrind", "Ir"),
            "allocations": code_metric(summary, "DHAT", "TotalBlocks"),
        }
        for summary in summaries
    }


def run_name(run: str, messages: int) -> str:
    """A benchmark id as the number of deliveries its run handled."""
    counts = {COLD: 1, COUNTS[0]: messages, COUNTS[1]: 2 * messages}
    if run not in counts:
        return run
    return "one delivery" if counts[run] == 1 else f"{counts[run]} deliveries"


def as_text(value: int | float) -> str:
    """A metric value as a breach line writes it: a count as it is, a fraction in short form."""
    return str(value) if isinstance(value, int) else f"{value:g}"


def breach(regression: dict, metrics: dict) -> str:
    """One limit a run went over: the metric, the value it was compared against, the new one.

    A limit in percent holds the run to the one it is compared against, and the regression
    carries both values. A plain number is a ceiling the run is held to on its own, and the value
    it was compared against is the one the metric records next to the new one, where there is one.
    """
    [(kind, detail)] = regression.items()
    [(tool, name)] = detail["metric"].items()
    label = METRIC_NAMES.get((tool, name), f"{tool} {name}")
    if kind == "Soft":
        return (
            f"{label} {as_text(detail['old'])} -> {as_text(detail['new'])}, "
            f"{float(detail['diff_pct']):+.2f}% against a limit of +{float(detail['limit']):g}%"
        )
    old = metrics.get(name, {}).get("values", {}).get("old")
    change = "" if old is None else f"{as_text(old)} -> "
    return f"{label} {change}{as_text(detail['new'])} against a limit of {as_text(detail['limit'])}"


def breaches(summaries: list[dict], messages: int) -> list[str]:
    """Every limit the run breached, one line each, named by its scenario and its run."""
    names = {key: name for name, key, _ in CODE_SCENARIOS}
    found = []
    for summary in summaries:
        where = benchmark(summary)
        where = f"{names.get(where, where)}, {run_name(summary['id'], messages)}"
        for profile in summary["profiles"]:
            total = profile["data"]["total"]
            for regression in total["regressions"]:
                found.append(f"{where}: {breach(regression, total['metrics'])}")
    return found


def code_total(found: dict, key: str, floor: int) -> dict:
    """One run's totals, checked for the two ways this measurement fails silently."""
    if key not in found:
        sys.exit(
            f"benchmark {key} is not in the run: it failed before it wrote a summary, or it was "
            "renamed (then rename it here or in benches/)"
        )
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


def code_section(found: dict, messages: int) -> list[dict]:
    """The code table: the steady state and the cold start of every scenario.

    The slope between the two counts is what a message costs once the service is running; the
    one-delivery run is what starting it and taking that delivery cost.
    """
    floor = CODE_FLOOR * messages // CODE_MESSAGES
    rows = []
    for name, key, gated in CODE_SCENARIOS:
        base = code_total(found, f"{key}/{COUNTS[0]}", floor)
        twice = code_total(found, f"{key}/{COUNTS[1]}", floor)
        if twice["instructions"] <= base["instructions"]:
            sys.exit(f"benchmark {key} does not grow with the message count: no slope to read")
        first = code_total(found, f"{key}/{COLD}", CODE_COLD_FLOOR)
        rows.append(
            {
                "name": name,
                "messages": messages,
                "framework": {
                    metric: per_message((twice[metric] - base[metric]) / messages)
                    for metric in ("instructions", "allocations")
                },
                "cold": {metric: first[metric] for metric in ("instructions", "allocations")},
                "gated": gated,
            }
        )
    return rows


def report_breaches(lines: list[str]) -> None:
    """The limits the run breached, which is why it fails, each with both values it compared."""
    if not lines:
        return
    print()
    print("limits breached (totals of one run, old -> new):")
    for line in lines:
        print(f"  {line}")


def valgrind() -> str:
    return run("valgrind", "--version").strip().removeprefix("valgrind-") or "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--code", action="store_true", help="read the code-cost run rather than the paired one"
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=CODE_MESSAGES,
        help="deliveries per measured run of the code-cost benches, the count they were built with",
    )
    parser.add_argument("summary", type=Path, help="the JSON the benchmark run wrote")
    parser.add_argument("output", type=Path, help="the results document to write")
    args = parser.parse_args()
    if args.messages <= 0:
        parser.error("--messages must be a positive number of deliveries")
    out = args.output
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    breached = []
    if args.code:
        runs = code_summaries(args.summary)
        breached = breaches(runs, args.messages)
        try:
            code = code_section(code_runs(runs), args.messages)
        except SystemExit:
            # One failure does not hide another: a summary that cannot be converted still shows
            # what the run breached.
            report_breaches(breached)
            sys.stdout.flush()
            raise
        document = previous
        document["schema"] = 3
        document["code"] = code
        # The code costs carry their own provenance: the paired numbers beside them may come
        # from another run, on another version, on another day.
        document["code_measured"] = {
            "crate_version": crate_version(),
            "core_version": core_version(),
            "measured_at": date.today().isoformat(),
        }
        document.setdefault("environment", {})["valgrind"] = valgrind()
    else:
        summary = json.loads(args.summary.read_text(encoding="utf-8"))
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
    if args.code:
        for row in document["code"]:
            print(
                f"  {row['name']}: {row['framework']['instructions']} instructions, "
                f"{row['framework']['allocations']} allocations per message; cold "
                f"{row['cold']['instructions']} instructions, {row['cold']['allocations']} "
                "allocations"
            )
        report_breaches(breached)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
