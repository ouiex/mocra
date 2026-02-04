#!/usr/bin/env python3
from __future__ import annotations

import math
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import DefaultDict, List, Optional, Tuple

TIMESTAMP_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T[^ ]+)")
DURATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ns|us|\u00b5s|ms|s)\b")
PROCESSOR_RE = re.compile(r"processor_type=([A-Za-z0-9_]+)")
BRACKET_RE = re.compile(r"\[([^\]]+)\]")
EVENT_RE = re.compile(r"Event:\s*([A-Za-z0-9_]+)")
LOGGER_RE = re.compile(r"\s(?:INFO|DEBUG|WARN|ERROR)\s+([^\s:]+):")


def parse_timestamp(line: str) -> Optional[float]:
    match = TIMESTAMP_RE.search(line)
    if not match:
        return None
    ts_str = match.group("ts")
    try:
        return datetime.fromisoformat(ts_str).timestamp()
    except ValueError:
        return None


def extract_node(line: str) -> str:
    match = PROCESSOR_RE.search(line)
    if match:
        return f"processor_type:{match.group(1)}"
    match = BRACKET_RE.search(line)
    if match:
        return f"node:{match.group(1)}"
    match = EVENT_RE.search(line)
    if match:
        return f"event:{match.group(1)}"
    match = LOGGER_RE.search(line)
    if match:
        return f"module:{match.group(1)}"
    return "unknown"


def to_milliseconds(value: float, unit: str) -> float:
    if unit == "s":
        return value * 1000.0
    if unit == "ms":
        return value
    if unit in {"us", "\u00b5s"}:
        return value / 1000.0
    if unit == "ns":
        return value / 1_000_000.0
    return value


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    index = max(0, math.ceil(pct / 100.0 * len(values_sorted)) - 1)
    return values_sorted[index]


def compute_rate(count: int, total_ms: float) -> float:
    if count <= 0 or total_ms <= 0:
        return 0.0
    return count / (total_ms / 1000.0)


def latest_log_file(log_dir: Path) -> Path:
    if not log_dir.exists() or not log_dir.is_dir():
        raise FileNotFoundError(f"Log directory not found: {log_dir}")
    log_files = [p for p in log_dir.iterdir() if p.is_file() and p.suffix == ".log"]
    if not log_files:
        raise FileNotFoundError(f"No .log files found in {log_dir}")
    return max(log_files, key=lambda p: p.stat().st_mtime)


def analyze_log(log_file: Path) -> Tuple[DefaultDict[str, List[float]], int]:
    durations_by_node: DefaultDict[str, List[float]] = defaultdict(list)
    total_count = 0

    with log_file.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            duration_matches = list(DURATION_RE.finditer(line))
            if not duration_matches:
                continue
            node = extract_node(line)
            for duration_match in duration_matches:
                value = float(duration_match.group("value"))
                unit = duration_match.group("unit")
                durations_by_node[node].append(to_milliseconds(value, unit))
                total_count += 1

    return durations_by_node, total_count


def print_report(log_file: Path, durations_by_node: DefaultDict[str, List[float]], total_count: int) -> None:
    if total_count == 0:
        print(f"No durations found in {log_file}")
        return

    print(f"Latest log: {log_file}")
    print(f"Samples: {total_count}")
    header = (
        f"{'node':<40} {'count':>8} {'total_ms':>12} {'avg_ms':>10} {'p95_ms':>10} "
        f"{'min_ms':>10} {'max_ms':>10} {'rate/s':>10}"
    )
    print(header)
    print("-" * len(header))

    rows = []
    all_durations: List[float] = []
    for node, durations in durations_by_node.items():
        if not durations:
            continue
        count = len(durations)
        total_ms = sum(durations)
        avg_ms = total_ms / count
        p95_ms = percentile(durations, 95.0)
        min_ms = min(durations)
        max_ms = max(durations)
        rate = compute_rate(count, total_ms)
        all_durations.extend(durations)
        rows.append((total_ms, node, count, total_ms, avg_ms, p95_ms, min_ms, max_ms, rate))

    for _, node, count, total_ms, avg_ms, p95_ms, min_ms, max_ms, rate in sorted(rows, reverse=True):
        node_label = node[:40]
        print(
            f"{node_label:<40} {count:>8d} {total_ms:>12.3f} {avg_ms:>10.3f} {p95_ms:>10.3f} "
            f"{min_ms:>10.3f} {max_ms:>10.3f} {rate:>10.3f}"
        )

    total_ms = sum(all_durations)
    avg_ms = total_ms / len(all_durations)
    p95_ms = percentile(all_durations, 95.0)
    min_ms = min(all_durations)
    max_ms = max(all_durations)
    rate = compute_rate(len(all_durations), total_ms)
    print("-" * len(header))
    print(
        f"{'TOTAL':<40} {len(all_durations):>8d} {total_ms:>12.3f} {avg_ms:>10.3f} {p95_ms:>10.3f} "
        f"{min_ms:>10.3f} {max_ms:>10.3f} {rate:>10.3f}"
    )


def main() -> None:
    target_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd() / "logs"
    if target_path.is_file():
        log_file = target_path
    else:
        log_file = latest_log_file(target_path)
    durations_by_node, total_count = analyze_log(log_file)
    print_report(log_file, durations_by_node, total_count)
    if total_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
