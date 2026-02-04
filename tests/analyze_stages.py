#!/usr/bin/env python3
"""
Mocra Log Analyzer - Event Stage Analysis
=========================================

This script analyzes Mocra engine logs to calculate specific stage durations based on
event pairs and processor logs.

It supports two modes of analysis:
1. **Precise ID Matching** (for DownloadProcessor): Uses request_id to pair Start/End logs.
2. **Sequential Event Matching*: Assumes 'Event: Type' logs appear in Start -> End pairs.
   (Useful for single-threaded tests or low-concurrency analysis where IDs are missing in console logs).

Usage:
    python3 tests/analyze_stages.py [path/to/logfile]
"""

import re
import sys
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional, Tuple, Any

# --- Regex Patterns ---
TS_PATTERN = r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)"
TIMESTAMP_RE = re.compile(TS_PATTERN)

# 1. Generic Console Log Event (Ambiguous Start/End)
# "Event: download | Time: 1738228708087"
CONSOLE_EVENT_RE = re.compile(r"Event:\s*(?P<type>[A-Za-z0-9_]+)\s*\|\s*Time:")

# 2. Detailed DownloadProcessor Logs (Precise ID)
# Start: "[DownloadProcessor] begin process: request_id=..."
# End:   "[DownloadProcessor] download finished: request_id=..."
DL_START_RE = re.compile(r"\[DownloadProcessor\] begin process:.*request_id=(?P<id>[a-f0-9\-]+)")
DL_END_RE = re.compile(r"\[DownloadProcessor\] download finished:.*request_id=(?P<id>[a-f0-9\-]+)")

# 3. Task Processor Logs (Generic)
# "Starting processing with processor: TaskProcessor"
PROCESSOR_START_RE = re.compile(r"Starting processing with processor:\s*(?P<name>[A-Za-z0-9_]+)")

# Pre-defined Stages for Sequential Analysis
# We assume the generic events come in pairs: 1st=Start, 2nd=End
SEQUENTIAL_STAGES = [
    "task_model",
    "parser_task_model",
    "task",
    "request",
    "download",
    "response_middleware",
    "parser",
    "data_store",
    "module"
]

def get_latest_log_file(log_dir: Path) -> Path:
    if not log_dir.exists():
        # Try current directory first
        if (Path.cwd() / "logs").exists():
            log_dir = Path.cwd() / "logs"
        else:
            print(f"Error: Log directory '{log_dir}' not found.")
            sys.exit(1)
    
    logs = list(log_dir.glob("*.log"))
    if not logs:
        print(f"Error: No .log files found in '{log_dir}'.")
        sys.exit(1)
    return max(logs, key=lambda p: p.stat().st_mtime)

def extract_timestamp(line: str) -> Optional[datetime]:
    match = TIMESTAMP_RE.search(line)
    if match:
        try:
            return datetime.fromisoformat(match.group("ts"))
        except ValueError:
            pass
    return None

def analyze_stages(file_path: Path):
    print(f"Analyzing Stages in: {file_path.absolute()}")
    print("-" * 100)
    
    # Storage for Analysis
    # 1. ID-based (Most Accurate)
    dl_starts: Dict[str, datetime] = {}
    stage_durations: Dict[str, List[float]] = defaultdict(list)
    
    # 2. Sequential (Heuristic)
    # Stack per event type to handle nested or sequential calls
    # For simple sequential: if stack empty -> Start, else -> End (pop)
    seq_stacks: Dict[str, List[datetime]] = defaultdict(list)
    
    parsed_lines = 0
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            ts = extract_timestamp(line)
            if not ts:
                continue
            parsed_lines += 1
            
            # --- Type A: Download Processor (ID Based) ---
            start_match = DL_START_RE.search(line)
            if start_match:
                req_id = start_match.group("id")
                dl_starts[req_id] = ts
                continue
                
            end_match = DL_END_RE.search(line)
            if end_match:
                req_id = end_match.group("id")
                if req_id in dl_starts:
                    duration = (ts - dl_starts[req_id]).total_seconds() * 1000
                    stage_durations["Download (Exact)"].append(duration)
                    del dl_starts[req_id]
                continue

            # --- Type B: Generic Event (Sequential Heuristic) ---
            event_match = CONSOLE_EVENT_RE.search(line)
            if event_match:
                etype = event_match.group("type")
                if etype in SEQUENTIAL_STAGES:
                    # Heuristic: Start -> End -> Start -> End
                    # If we have a pending start, close it.
                    if seq_stacks[etype]:
                        start_ts = seq_stacks[etype].pop()
                        duration = (ts - start_ts).total_seconds() * 1000
                        stage_durations[f"Event: {etype}"].append(duration)
                    else:
                        seq_stacks[etype].append(ts)
                continue
                
            # --- Type C: Processor Start (Generic) ---
            proc_match = PROCESSOR_START_RE.search(line)
            if proc_match:
                # We can't easily find the End for this without ID or specific End log.
                # But we can count them.
                pass

    if parsed_lines == 0:
        print("No valid log lines found.")
        return

    print(f"{'Stage Name':<30} | {'Count':>8} | {'Avg(ms)':>10} | {'Min(ms)':>10} | {'Max(ms)':>10} | {'Total(ms)':>10}")
    print("-" * 100)
    
    found_any = False
    for stage, durs in sorted(stage_durations.items()):
        if not durs:
            continue
        found_any = True
        count = len(durs)
        avg = mean(durs)
        total = sum(durs)
        print(f"{stage:<30} | {count:>8} | {avg:>10.2f} | {min(durs):>10.2f} | {max(durs):>10.2f} | {total:>10.0f}")
        
    if not found_any:
        print("No paired stages found. Ensure logs contain 'Event: ...' pairs or '[DownloadProcessor] ...' pairs.")
        print("Note: Sequential analysis requires the Start event followed by an End event of the same type.")

    print("-" * 100)
    if "Download (Exact)" in stage_durations:
        print("* 'Download (Exact)' uses request_id matching and is highly accurate.")
    print("* 'Event: ...' stages assume sequential execution (Start -> End) in the log.")

def main():
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd() / "logs"
    if target.is_dir():
        log_file = get_latest_log_file(target)
    else:
        log_file = target
    analyze_stages(log_file)

if __name__ == "__main__":
    main()
