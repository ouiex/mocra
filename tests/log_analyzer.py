#!/usr/bin/env python3
"""
Mocra Log Analyzer
==================

此脚本用于分析 Mocra 系统生成的日志文件，计算关键性能指标。
功能：
1. 自动定位最新的日志文件。
2. 解析每行日志的时间戳、节点/处理器名称、耗时信息。
3. 计算每个节点的延迟统计（Avg, P50, P95, P99, Max）。
4. 基于日志的起止时间计算系统的全局平均 RPS (Requests Per Second)。

Usage:
    python3 tests/log_analyzer.py [path/to/logfile]
"""

import re
import sys
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional, Tuple

# --- Regex Patterns ---
# ISO8601 Timestamp at the start of the line or in a field
TIMESTAMP_PATTERNS = [
    re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)"),
    re.compile(r"ts=(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
]

# Duration extraction: "took 123ms", "duration=1.5s"
DURATION_RE = re.compile(r"(?:took|duration=|cost)\s*(?P<value>\d+(?:\.\d+)?)(?P<unit>ns|us|µs|ms|s)\b")

# Node/Context extraction
# Priority: processor_type=X -> node:X -> event:X -> module prefix
PROCESSOR_RE = re.compile(r"processor_type=(?P<name>[A-Za-z0-9_]+)")
NODE_BRACKET_RE = re.compile(r"\[(?P<name>[^\]]+)\]")
EVENT_RE = re.compile(r"Event:\s*(?P<name>[A-Za-z0-9_]+)")
MODULE_LEVEL_RE = re.compile(r"\s+(?:INFO|WARN|ERROR)\s+(?P<name>[^\s:]+):")

def get_latest_log_file(log_dir: Path) -> Path:
    """Find the most recently modified .log file in the directory."""
    if not log_dir.exists():
        print(f"Error: Log directory '{log_dir}' not found.")
        sys.exit(1)
    
    logs = list(log_dir.glob("*.log"))
    if not logs:
        print(f"Error: No .log files found in '{log_dir}'.")
        sys.exit(1)
    
    # Sort by modification time, descending
    return max(logs, key=lambda p: p.stat().st_mtime)

def parse_duration(value: str, unit: str) -> float:
    """Convert duration to milliseconds."""
    val = float(value)
    if unit == 's':
        return val * 1000.0
    elif unit == 'ms':
        return val
    elif unit in ['us', 'µs']:
        return val / 1000.0
    elif unit == 'ns':
        return val / 1_000_000.0
    return val

def extract_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from log line."""
    for pattern in TIMESTAMP_PATTERNS:
        match = pattern.search(line)
        if match:
            try:
                # Handle ISO format potentially with Z or offset, though simple split usually works for local logs
                ts_str = match.group("ts")
                # Truncate nanoseconds/microseconds to recognized format if needed, 
                # but fromisoformat usually handles 6 digits.
                return datetime.fromisoformat(ts_str)
            except ValueError:
                pass
    return None

def extract_node_name(line: str) -> str:
    """Determine the component responsible for the log line."""
    # 1. Direct processor type
    m = PROCESSOR_RE.search(line)
    if m:
        return f"Processor:{m.group('name')}"
    
    # 2. Event
    m = EVENT_RE.search(line)
    if m:
        return f"Event:{m.group('name')}"
    
    # 3. [NodeName] style
    m = NODE_BRACKET_RE.search(line)
    if m:
        return f"Node:{m.group('name')}"
    
    # 4. Module name from logger
    m = MODULE_LEVEL_RE.search(line)
    if m:
        return f"Module:{m.group('name')}"
        
    return "Unknown"

def calculate_percentile(data: List[float], percentile: float) -> float:
    """Calculate the p-th percentile of data."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    d0 = data[int(f)]
    d1 = data[int(c)]
    return d0 + (d1 - d0) * (k - f)

def analyze_logs(file_path: Path):
    print(f"Analyze target: {file_path.absolute()}")
    print("-" * 80)

    node_durations: Dict[str, List[float]] = defaultdict(list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_duration_logs = 0
    total_lines = 0

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                total_lines += 1
                
                # 1. Update global time range
                ts = extract_timestamp(line)
                if ts:
                    if start_time is None or ts < start_time:
                        start_time = ts
                    if end_time is None or ts > end_time:
                        end_time = ts
                
                # 2. Extract Duration
                dur_match = DURATION_RE.search(line)
                if dur_match:
                    ms = parse_duration(dur_match.group("value"), dur_match.group("unit"))
                    node = extract_node_name(line)
                    node_durations[node].append(ms)
                    total_duration_logs += 1

    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

    if total_lines == 0:
        print("Log file is empty.")
        return

    # --- Report Generation ---

    # 1. Global Throughput
    wall_duration_seconds = 0.0
    rps = 0.0
    if start_time and end_time:
        diff = end_time - start_time
        wall_duration_seconds = diff.total_seconds()
        
        # We estimate "Requests" based on how many operations logged a duration.
        # Ideally, we should count specific "Task Completed" events for strict RPS,
        # but taking the max count of a specific processor is a good proxy for pipeline throughput.
        # Or simply use the total duration logs as "operations".
        # Let's use the count of the most frequent processor as "Throughput" proxy (Pipeline bottleneck).
        max_ops = 0
        if node_durations:
            max_ops = max(len(v) for v in node_durations.values())
        
        if wall_duration_seconds > 0:
            rps = max_ops / wall_duration_seconds

    print(f"Time Range: {start_time} to {end_time}")
    print(f"Wall Time : {wall_duration_seconds:.4f}s")
    print(f"Throughput: ~{rps:.2f} ops/sec (based on busiest node)")
    print("-" * 80)
    
    # 2. Per Node Stats
    # Header
    print(f"{'Node / Context':<35} | {'Count':>8} | {'Avg(ms)':>9} | {'P95(ms)':>9} | {'P99(ms)':>9} | {'Max(ms)':>9} | {'Total(ms)':>10}")
    print("-" * 110)

    # Sort by Total Time desc
    sorted_nodes = sorted(node_durations.items(), key=lambda x: sum(x[1]), reverse=True)

    for node, values in sorted_nodes:
        if not values:
            continue
        
        values.sort()
        count = len(values)
        total_ms = sum(values)
        avg_ms = mean(values)
        p95 = calculate_percentile(values, 95)
        p99 = calculate_percentile(values, 99)
        max_ms = values[-1]

        print(f"{node:<35} | {count:>8} | {avg_ms:>9.2f} | {p95:>9.2f} | {p99:>9.2f} | {max_ms:>9.2f} | {total_ms:>10.0f}")

    print("-" * 110)

def main():
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
        if target.is_dir():
            log_file = get_latest_log_file(target)
        else:
            log_file = target
    else:
        # Default to ./logs directory
        log_file = get_latest_log_file(Path.cwd() / "logs")

    analyze_logs(log_file)

if __name__ == "__main__":
    main()
