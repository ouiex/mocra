from datetime import datetime, timezone, timedelta

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    return dt.isoformat()

def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

def timestamp_ms() -> int:
    return int(utc_now().timestamp() * 1000)
