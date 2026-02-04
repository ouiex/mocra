"""Type conversion helpers for JSON/Excel values to Polars Series."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, List, Optional

try:
    import polars as pl
except Exception:  # pragma: no cover - optional dependency
    pl = None


def _is_null_like(value: str) -> bool:
    trimmed = value.strip()
    return trimmed == "" or trimmed.lower() == "null"


def _as_bool_like(value: str) -> Optional[bool]:
    trimmed = value.strip().lower()
    if trimmed == "true":
        return True
    if trimmed == "false":
        return False
    return None


class JsonTarget(str, Enum):
    UTF8 = "utf8"
    BOOLEAN = "boolean"
    FLOAT64 = "float64"
    INT64 = "int64"
    UINT64 = "uint64"


def _detect_target(values: Iterable[Any]) -> JsonTarget:
    has_non_bool_string = False
    has_bool = False
    has_number = False
    has_float = False
    has_i64 = False
    has_u64 = False

    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            if has_number:
                has_non_bool_string = True
                break
            has_bool = True
            continue
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            if has_bool:
                has_non_bool_string = True
                break
            has_number = True
            if isinstance(v, float):
                has_float = True
            elif v < 0:
                has_i64 = True
            else:
                has_u64 = True
            continue
        if isinstance(v, str):
            bool_like = _as_bool_like(v)
            null_like = _is_null_like(v)
            if bool_like is not None or null_like:
                if bool_like is not None:
                    has_bool = True
            else:
                has_non_bool_string = True
                break
            continue
        # arrays/objects/others
        has_non_bool_string = True
        break

    if has_non_bool_string or (has_number and has_bool):
        return JsonTarget.UTF8
    if has_number:
        if has_float or (has_i64 and has_u64):
            return JsonTarget.FLOAT64
        if has_i64:
            return JsonTarget.INT64
        return JsonTarget.UINT64
    if has_bool:
        return JsonTarget.BOOLEAN
    return JsonTarget.UTF8


def json_value_to_any_value(values: List[Any]) -> List[Any]:
    target = _detect_target(values)
    out: List[Any] = []

    for v in values:
        if target == JsonTarget.UTF8:
            if v is None:
                out.append(None)
            elif isinstance(v, bool):
                out.append(str(v))
            elif isinstance(v, (int, float)):
                out.append(str(v))
            elif isinstance(v, str):
                out.append(None if _is_null_like(v) else v)
            else:
                out.append(str(v))
        elif target == JsonTarget.BOOLEAN:
            if isinstance(v, bool):
                out.append(v)
            elif isinstance(v, str):
                out.append(_as_bool_like(v))
            else:
                out.append(None)
        elif target == JsonTarget.FLOAT64:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                out.append(float(v))
            else:
                out.append(None)
        elif target == JsonTarget.INT64:
            if isinstance(v, int) and not isinstance(v, bool):
                out.append(int(v))
            else:
                out.append(None)
        elif target == JsonTarget.UINT64:
            if isinstance(v, int) and not isinstance(v, bool) and v >= 0:
                out.append(int(v))
            else:
                out.append(None)
    return out


def serde_values_to_series(name: str, values: List[Any]):
    if pl is None:
        raise ImportError("polars is not installed")

    target = _detect_target(values)
    return json_values_to_series_with_target(name, values, target)


def json_values_to_series_with_target(name: str, values: List[Any], target: JsonTarget):
    if pl is None:
        raise ImportError("polars is not installed")

    if target == JsonTarget.UTF8:
        buf: List[Optional[str]] = []
        for v in values:
            if v is None:
                buf.append(None)
            elif isinstance(v, bool):
                buf.append(str(v))
            elif isinstance(v, (int, float)):
                buf.append(str(v))
            elif isinstance(v, str):
                buf.append(None if _is_null_like(v) else v)
            else:
                buf.append(str(v))
        return pl.Series(name, buf)

    if target == JsonTarget.BOOLEAN:
        buf: List[Optional[bool]] = []
        for v in values:
            if isinstance(v, bool):
                buf.append(v)
            elif isinstance(v, str):
                buf.append(_as_bool_like(v))
            else:
                buf.append(None)
        return pl.Series(name, buf)

    if target == JsonTarget.FLOAT64:
        buf: List[Optional[float]] = []
        for v in values:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                buf.append(float(v))
            else:
                buf.append(None)
        return pl.Series(name, buf)

    if target == JsonTarget.INT64:
        buf: List[Optional[int]] = []
        for v in values:
            if isinstance(v, int) and not isinstance(v, bool):
                buf.append(int(v))
            else:
                buf.append(None)
        return pl.Series(name, buf)

    if target == JsonTarget.UINT64:
        buf = []
        for v in values:
            if isinstance(v, int) and not isinstance(v, bool) and v >= 0:
                buf.append(int(v))
            else:
                buf.append(None)
        return pl.Series(name, buf)

    return pl.Series(name, values)


def excel_values_to_series(name: str, values: List[Any]):
    """Fallback converter for Excel cell values to Polars Series."""
    return serde_values_to_series(name, values)
