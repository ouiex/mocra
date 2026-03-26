#!/usr/bin/env python3
"""Collect DAG runtime alert baselines from Prometheus and render a markdown report."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
import statistics
import sys
import urllib.parse
import urllib.request
from typing import Dict, List, Tuple

PROFILE_THRESHOLDS = {
    "dev": {
        "warning_rate_per_sec": 1.0,
        "non_retryable_rate_per_sec": 0.5,
        "retryable_rate_per_sec": 2.0,
    },
    "staging": {
        "warning_rate_per_sec": 0.8,
        "non_retryable_rate_per_sec": 0.3,
        "retryable_rate_per_sec": 1.5,
    },
    "prod": {
        "warning_rate_per_sec": 0.5,
        "non_retryable_rate_per_sec": 0.2,
        "retryable_rate_per_sec": 1.0,
    },
}

RANGE_QUERIES = {
    "critical_events": 'sum(rate(dag_alert_event_total{severity="critical"}[5m]))',
    "warning_events": 'sum(rate(dag_alert_event_total{severity="warning"}[5m]))',
    "non_retryable_errors": 'sum(rate(dag_execute_error_total{error_class="NonRetryable"}[5m]))',
    "retryable_errors": 'sum(rate(dag_execute_error_total{error_class="Retryable"}[5m]))',
    "snapshot_save_errors": 'sum(rate(dag_run_state_store_total{result="save_error"}[5m]))',
}

INSTANT_QUERIES = {
    "critical_by_source_event": 'sum by (source,event) (rate(dag_alert_event_total{severity="critical"}[5m]))',
    "warning_by_source_event": 'sum by (source,event) (rate(dag_alert_event_total{severity="warning"}[5m]))',
    "execute_error_by_code_class": 'sum by (error_code,error_class) (rate(dag_execute_error_total[5m]))',
    "run_state_save_error_by_stage": 'sum by (stage) (rate(dag_run_state_store_total{result="save_error"}[5m]))',
}


def _http_get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8")
    data = json.loads(payload)
    if data.get("status") != "success":
        raise RuntimeError(f"prometheus query failed: {data}")
    return data


def query_range(prom_url: str, expr: str, start_ts: int, end_ts: int, step_sec: int) -> List[float]:
    params = urllib.parse.urlencode(
        {
            "query": expr,
            "start": str(start_ts),
            "end": str(end_ts),
            "step": str(step_sec),
        }
    )
    url = f"{prom_url.rstrip('/')}/api/v1/query_range?{params}"
    data = _http_get_json(url)
    result = data.get("data", {}).get("result", [])

    series_values: List[float] = []
    for item in result:
        values = item.get("values", [])
        for _, raw in values:
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isnan(v) and not math.isinf(v):
                series_values.append(v)
    return series_values


def query_instant(prom_url: str, expr: str) -> List[Tuple[Dict[str, str], float]]:
    params = urllib.parse.urlencode({"query": expr})
    url = f"{prom_url.rstrip('/')}/api/v1/query?{params}"
    data = _http_get_json(url)
    result = data.get("data", {}).get("result", [])

    rows: List[Tuple[Dict[str, str], float]] = []
    for item in result:
        metric = item.get("metric", {})
        value = item.get("value", [None, "0"])
        try:
            v = float(value[1])
        except (TypeError, ValueError, IndexError):
            v = 0.0
        rows.append((metric, v))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows


def summarize(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"avg": 0.0, "p95": 0.0, "max": 0.0}

    sorted_vals = sorted(values)
    idx = int(math.ceil(0.95 * len(sorted_vals))) - 1
    idx = max(0, min(idx, len(sorted_vals) - 1))

    return {
        "avg": statistics.fmean(values),
        "p95": sorted_vals[idx],
        "max": sorted_vals[-1],
    }


def verdict(metric_name: str, stats: Dict[str, float], profile: str) -> str:
    t = PROFILE_THRESHOLDS[profile]
    if metric_name == "critical_events":
        return "PASS" if stats["max"] <= 0.0 else "FAIL"
    if metric_name == "warning_events":
        return "PASS" if stats["avg"] <= t["warning_rate_per_sec"] else "CONDITIONAL"
    if metric_name == "non_retryable_errors":
        return "PASS" if stats["avg"] <= t["non_retryable_rate_per_sec"] else "CONDITIONAL"
    if metric_name == "retryable_errors":
        return "PASS" if stats["avg"] <= t["retryable_rate_per_sec"] else "CONDITIONAL"
    if metric_name == "snapshot_save_errors":
        return "PASS" if stats["max"] <= 0.0 else "FAIL"
    return "CONDITIONAL"


def build_verdicts(range_stats: Dict[str, Dict[str, float]], profile: str) -> Dict[str, str]:
    return {name: verdict(name, stats, profile) for name, stats in range_stats.items()}


def evaluate_rollout_gates(range_stats: Dict[str, Dict[str, float]], profile: str) -> List[str]:
    thresholds = PROFILE_THRESHOLDS[profile]
    failures: List[str] = []

    if range_stats.get("critical_events", {}).get("max", 0.0) > 0.0:
        failures.append("GateA failed: critical_events max > 0")

    if range_stats.get("warning_events", {}).get("avg", 0.0) > thresholds["warning_rate_per_sec"]:
        failures.append("GateB failed: warning_events avg above profile threshold")

    if (
        range_stats.get("non_retryable_errors", {}).get("avg", 0.0)
        > thresholds["non_retryable_rate_per_sec"]
    ):
        failures.append("GateC failed: non_retryable_errors avg above profile threshold")

    if range_stats.get("snapshot_save_errors", {}).get("max", 0.0) > 0.0:
        failures.append("GateD failed: snapshot_save_errors max > 0")

    return failures


def build_archive_path(output_path: Path, archive_dir: Path, archive_prefix: str, now: dt.datetime) -> Path:
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    suffix = output_path.suffix if output_path.suffix else ".md"
    return archive_dir / f"{archive_prefix}_{timestamp}{suffix}"


def render_table_rows(rows: List[Tuple[Dict[str, str], float]], label_order: List[str]) -> List[str]:
    if not rows:
        return ["| n/a | 0.0000 |", "|---|---:|"]

    lines = ["| Labels | Value |", "|---|---:|"]
    for metric, v in rows:
        labels = []
        for key in label_order:
            if key in metric:
                labels.append(f"{key}={metric[key]}")
        if not labels:
            labels.append("(none)")
        lines.append(f"| {', '.join(labels)} | {v:.4f} |")
    return lines


def build_report(
    prom_url: str,
    profile: str,
    lookback_hours: int,
    step_sec: int,
    range_stats: Dict[str, Dict[str, float]],
    instant_rows: Dict[str, List[Tuple[Dict[str, str], float]]],
) -> str:
    now = dt.datetime.utcnow()
    start = now - dt.timedelta(hours=lookback_hours)

    lines: List[str] = []
    lines.append("# DAG Alerts Baseline Report")
    lines.append("")
    lines.append(f"- Generated at (UTC): {now.isoformat(timespec='seconds')}Z")
    lines.append(f"- Prometheus URL: {prom_url}")
    lines.append(f"- Profile: {profile}")
    lines.append(f"- Range: {start.isoformat(timespec='seconds')}Z to {now.isoformat(timespec='seconds')}Z")
    lines.append(f"- Step: {step_sec}s")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Avg | P95 | Max | Verdict |")
    lines.append("|---|---:|---:|---:|---|")
    for name in RANGE_QUERIES.keys():
        st = range_stats[name]
        lines.append(
            f"| {name} | {st['avg']:.4f} | {st['p95']:.4f} | {st['max']:.4f} | {verdict(name, st, profile)} |"
        )
    lines.append("")

    lines.append("## Critical Events By Source/Event")
    lines.append("")
    lines.extend(
        render_table_rows(
            instant_rows["critical_by_source_event"],
            ["source", "event"],
        )
    )
    lines.append("")

    lines.append("## Warning Events By Source/Event")
    lines.append("")
    lines.extend(
        render_table_rows(
            instant_rows["warning_by_source_event"],
            ["source", "event"],
        )
    )
    lines.append("")

    lines.append("## Execute Errors By Code/Class")
    lines.append("")
    lines.extend(
        render_table_rows(
            instant_rows["execute_error_by_code_class"],
            ["error_code", "error_class"],
        )
    )
    lines.append("")

    lines.append("## Run-State Save Errors By Stage")
    lines.append("")
    lines.extend(
        render_table_rows(
            instant_rows["run_state_save_error_by_stage"],
            ["stage"],
        )
    )
    lines.append("")

    lines.append("## Calibration Notes")
    lines.append("")
    lines.append("- Compare Avg/P95 with docs/alerts/dag_alerts_calibration_runbook.md environment profile.")
    lines.append("- If critical_events max > 0, treat as immediate rollback gate in gray rollout.")
    lines.append("- If warning/non-retryable stays above profile for 24h, keep preview mode and postpone cutover.")
    lines.append("")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect DAG alerts baseline from Prometheus")
    parser.add_argument("--prom-url", required=True, help="Prometheus base URL, e.g. http://127.0.0.1:9090")
    parser.add_argument("--profile", choices=["dev", "staging", "prod"], default="prod")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--step-seconds", type=int, default=60)
    parser.add_argument(
        "--output",
        default="docs/dashboards/dag_alerts_baseline_latest.md",
        help="Output markdown report path",
    )
    parser.add_argument(
        "--archive-dir",
        default="docs/dashboards/archive",
        help="Directory for timestamped baseline report archives",
    )
    parser.add_argument(
        "--archive-prefix",
        default="dag_alerts_baseline",
        help="Filename prefix for timestamped archive report",
    )
    parser.add_argument(
        "--summary-json",
        default="",
        help="Optional JSON summary output path",
    )
    parser.add_argument(
        "--fail-on-gate",
        action="store_true",
        help="Exit with non-zero code if rollout gates are not satisfied",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print traceback on failure for troubleshooting",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    now = dt.datetime.utcnow()
    end_ts = int(dt.datetime.utcnow().timestamp())
    start_ts = end_ts - int(args.lookback_hours * 3600)

    range_stats: Dict[str, Dict[str, float]] = {}
    for key, expr in RANGE_QUERIES.items():
        values = query_range(args.prom_url, expr, start_ts, end_ts, args.step_seconds)
        range_stats[key] = summarize(values)

    instant_rows: Dict[str, List[Tuple[Dict[str, str], float]]] = {}
    for key, expr in INSTANT_QUERIES.items():
        instant_rows[key] = query_instant(args.prom_url, expr)

    report = build_report(
        args.prom_url,
        args.profile,
        args.lookback_hours,
        args.step_seconds,
        range_stats,
        instant_rows,
    )

    verdicts = build_verdicts(range_stats, args.profile)
    gate_failures = evaluate_rollout_gates(range_stats, args.profile)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")

    archive_dir = Path(args.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = build_archive_path(output_path, archive_dir, args.archive_prefix, now)
    archive_path.write_text(report, encoding="utf-8")

    if args.summary_json:
        summary_path = Path(args.summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary = {
            "generated_at_utc": now.isoformat(timespec="seconds") + "Z",
            "profile": args.profile,
            "prom_url": args.prom_url,
            "lookback_hours": args.lookback_hours,
            "step_seconds": args.step_seconds,
            "range_stats": range_stats,
            "verdicts": verdicts,
            "gate_failures": gate_failures,
            "output": str(output_path),
            "archive": str(archive_path),
        }
        summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    print(f"baseline report generated: {output_path}")
    print(f"baseline report archived: {archive_path}")
    if gate_failures:
        print("rollout gate result: FAILED")
        for msg in gate_failures:
            print(f"- {msg}")
    else:
        print("rollout gate result: PASSED")

    if args.fail_on_gate and gate_failures:
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        debug_enabled = "--debug" in sys.argv
        print(f"baseline collection failed: {exc}", file=sys.stderr)
        if debug_enabled:
            raise
        raise SystemExit(1)
