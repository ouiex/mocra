import re
import statistics
from datetime import datetime
from collections import defaultdict
import sys

# Log file path
LOG_FILE = "logs/app.log.2026-01-09"

# Regex patterns (Updated based on actual log content)
PATTERNS = {
    # 1. Request Generated
    # Log: INFO engine::task::module_processor_with_chain: [chain] ... generated request id=...
    "REQ_GEN": re.compile(
        r"INFO engine::task::module_processor_with_chain: \[chain\] .*? generated request id=(?P<id>[a-f0-9\-]+)"
    ),
    # 1.1 Request Prepared
    # Log: INFO engine::task::module: [Module] request prepared: request_id=...
    "REQ_PREP": re.compile(
        r"INFO engine::task::module: \[Module\] request prepared: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 1.2 Task Processor Returned
    # Log: INFO engine::chain::task_model_chain: [TaskProcessor] generate returned: request_id=...
    "TASK_PROC_RET": re.compile(
        r"INFO engine::chain::task_model_chain: \[TaskProcessor\] generate returned: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 1.3 Flatten Yield
    # Log: INFO engine::chain::task_model_chain: [FlattenStreamProcessor] yielding request: request_id=...
    "FLATTEN_YIELD": re.compile(
        r"INFO engine::chain::task_model_chain: \[FlattenStreamProcessor\] yielding request: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 2. Request Send (Publish)
    "REQ_SEND": re.compile(
        r"INFO engine::chain::task_model_chain: \[RequestPublish\] publish request: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 2.5 Queue Forward Received (Queue Manager)
    # Log: INFO queue::manager: [QueueManager] forward_channel received: topic=request id=...
    "Q_FWD_RECV": re.compile(
        r"INFO queue::manager: \[QueueManager\] forward_channel received: topic=request id=(?P<id>[a-f0-9\-]+)"
    ),
    # 2.6 Queue Forward Published
    "Q_FWD_PUB": re.compile(
        r"INFO queue::manager: \[QueueManager\] forward_channel published: topic=request id=(?P<id>[a-f0-9\-]+)"
    ),
    # 3. Request Receive (Download Chain)
    "REQ_RECV": re.compile(
        r"INFO engine::chain::download_chain: \[DownloadProcessor\] begin process: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 4. Download Start
    "DL_START": re.compile(
        r"INFO engine::chain::download_chain: \[DownloadProcessor\] acquired downloader, start download: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 5. Download End
    "DL_END": re.compile(
        r"INFO engine::chain::download_chain: \[DownloadProcessor\] download finished: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 6. Response Send
    "RESP_SEND": re.compile(
        r"INFO engine::chain::download_chain: \[ResponsePublish\] publishing response: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 7. Response Receive (Parser Chain)
    "RESP_RECV": re.compile(
        r"INFO engine::chain::parser_chain: \[ResponseModuleProcessor\] start: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 8. Parse Start
    "PARSE_START": re.compile(
        r"INFO engine::chain::parser_chain: \[ResponseParserProcessor\] start parse: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 9. Parse End
    "PARSE_END": re.compile(
        r"INFO engine::chain::parser_chain: \[ResponseParserProcessor\] parser returned: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 10. Store Start
    "STORE_START": re.compile(
        r"INFO engine::chain::parser_chain: \[DataStoreProcessor\] start store: request_id=(?P<id>[a-f0-9\-]+)"
    ),
    # 11. Store End
    "STORE_END": re.compile(
        r"INFO engine::chain::parser_chain: \[DataStoreProcessor\] store success, request_id=(?P<id>[a-f0-9\-]+)"
    ),
}


def parse_timestamp(line):
    # Example: 2026-01-08T23:20:55.563857+08:00
    # Matches ISO 8601 format at the start of the line
    match = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2})", line)
    if match:
        try:
            return datetime.fromisoformat(match.group(1))
        except ValueError:
            pass
    return None


def calculate_stats(durations):
    if not durations:
        return None
    return {
        "count": len(durations),
        "avg": statistics.mean(durations),
        "min": min(durations),
        "max": max(durations),
        "p50": statistics.median(durations),
        "p95": sorted(durations)[int(len(durations) * 0.95)],
    }


def main():
    print(f"Analyzing log file: {LOG_FILE}")
    events_by_id = defaultdict(dict)

    try:
        with open(LOG_FILE, "r") as f:
            for line in f:
                timestamp = parse_timestamp(line)
                if not timestamp:
                    continue

                for state, pattern in PATTERNS.items():
                    match = pattern.search(line)
                    if match:
                        request_id = match.group("id")
                        # Only keep the first occurrence of a state for a request to handle retries/duplicates simply
                        if state not in events_by_id[request_id]:
                            events_by_id[request_id][state] = timestamp
                        break
    except FileNotFoundError:
        print(f"Error: Log file {LOG_FILE} not found.")
        return

    print(f"Found {len(events_by_id)} unique requests.")

    # Define metrics to calculate
    # (Name, Start State, End State)
    metric_definitions = [
        ("1. Generation (Prep)", "REQ_GEN", "REQ_PREP"),
        ("2. Generation (Chain)", "REQ_PREP", "TASK_PROC_RET"),
        ("3. Pipeline Overhead", "TASK_PROC_RET", "FLATTEN_YIELD"),
        ("4. Stream to Publish", "FLATTEN_YIELD", "REQ_SEND"),
        ("5. Queue Wait (Total)", "REQ_GEN", "DL_START"),
        ("   - Queue Transport", "REQ_SEND", "REQ_RECV"),
        ("   - Semaphore Wait", "REQ_RECV", "DL_START"),
        ("6. Download", "DL_START", "DL_END"),
        ("7. Parser Wait", "DL_END", "PARSE_START"),
        ("   - Resp Transport", "RESP_SEND", "RESP_RECV"),
        ("8. Parsing", "PARSE_START", "PARSE_END"),
        ("9. Storage", "STORE_START", "STORE_END"),
        ("Total Duration", "REQ_GEN", "STORE_END"),
    ]

    metrics = defaultdict(list)

    for req_id, events in events_by_id.items():
        for name, start_state, end_state in metric_definitions:
            if start_state in events and end_state in events:
                duration = (events[end_state] - events[start_state]).total_seconds()
                metrics[name].append(duration)

    # Print Report
    print("\n" + "=" * 100)
    print(
        f"{'Metric':<30} | {'Count':<6} | {'Avg (s)':<8} | {'Min (s)':<8} | {'Max (s)':<8} | {'P95 (s)':<8}"
    )
    print("-" * 100)

    for name, _, _ in metric_definitions:
        stats = calculate_stats(metrics[name])
        if stats:
            print(
                f"{name:<30} | {stats['count']:<6} | {stats['avg']:<8.4f} | {stats['min']:<8.4f} | {stats['max']:<8.4f} | {stats['p95']:<8.4f}"
            )
        else:
            print(
                f"{name:<30} | {'0':<6} | {'N/A':<8} | {'N/A':<8} | {'N/A':<8} | {'N/A':<8}"
            )
    print("=" * 100)

    # Throughput Analysis
    if events_by_id:
        all_times = []
        for events in events_by_id.values():
            all_times.extend(events.values())

        if all_times:
            start_time = min(all_times)
            end_time = max(all_times)
            duration = (end_time - start_time).total_seconds()

            print(f"\nTime Range: {start_time} to {end_time} ({duration:.2f}s)")
            if duration > 0:
                print(f"Estimated Throughput: {len(events_by_id) / duration:.2f} req/s")


if __name__ == "__main__":
    main()
