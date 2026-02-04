"""Textual numeric parsing with Chinese numeral support (port of Rust utils::to_numeric)."""

from __future__ import annotations

from typing import Optional


_CHINESE_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "两": 2,
}

_BIG_UNITS = {"亿": 100_000_000.0, "万": 10_000.0}
_SMALL_UNITS = {"千": 1000.0, "百": 100.0, "十": 10.0}


def _has_chinese_digits(value: str) -> bool:
    for ch in value:
        if ch in _CHINESE_DIGITS or ch in _BIG_UNITS or ch in _SMALL_UNITS:
            return True
    return False


def to_numeric(input_value: str) -> Optional[float]:
    if input_value is None:
        return None

    s = input_value.strip()
    if s == "" or s == "-":
        return 0.0

    for pat in ("nan", "NaN", "Nan", "NAN"):
        s = s.replace(pat, "")

    s = s.replace(" ", "")
    for sym in ("¥", "￥", "$", "€", "£"):
        s = s.replace(sym, "")

    if s == "":
        return 0.0

    # Fast path
    if not _has_chinese_digits(s) and "%" not in s:
        try:
            return float(s)
        except Exception:
            pass

    negative = False
    if s.startswith("-"):
        negative = True
        s = s[1:]
    if s.startswith("+"):
        s = s[1:]

    percent = s.endswith("%")
    if percent:
        s = s[:-1]

    # Trailing unit after pure arabic number, e.g. 1.2万
    if s and s[-1] in _BIG_UNITS | _SMALL_UNITS:
        unit = _BIG_UNITS.get(s[-1]) or _SMALL_UNITS.get(s[-1])
        core = s[:-1]
        if core and all(ch.isdigit() or ch == "." for ch in core):
            try:
                base = float(core)
                value = base * unit
                if percent:
                    value /= 100.0
                return -value if negative else value
            except Exception:
                pass

    # Split decimal part
    if "点" in s:
        int_part, dec_part = s.split("点", 1)
    elif "." in s:
        int_part, dec_part = s.split(".", 1)
    else:
        int_part, dec_part = s, ""

    # Parse Chinese integer part
    section_total = 0.0
    current = 0.0
    total = 0.0
    last_was_digit = False

    for ch in int_part:
        if ch in {"零", "〇"}:
            last_was_digit = False
            continue
        if ch in _SMALL_UNITS:
            unit = _SMALL_UNITS[ch]
            if last_was_digit:
                section_total += current * unit
            else:
                section_total += unit
            current = 0.0
            last_was_digit = False
            continue
        if ch in _BIG_UNITS:
            unit = _BIG_UNITS[ch]
            section_total += current
            total += section_total * unit
            section_total = 0.0
            current = 0.0
            last_was_digit = False
            continue
        if ch in _CHINESE_DIGITS:
            digit = _CHINESE_DIGITS[ch]
            if last_was_digit:
                section_total = section_total * 10.0 + digit
                current = 0.0
            else:
                current = float(digit)
            last_was_digit = True
            continue
        if ch.isdigit():
            digit = int(ch)
            if last_was_digit:
                section_total = section_total * 10.0 + digit
                current = 0.0
            else:
                current = float(digit)
            last_was_digit = True
            continue
        if ch in {",", "+", "-"}:
            continue
        return None

    section_total += current
    total += section_total

    # Decimal part
    decimal_value = 0.0
    if dec_part:
        scale = 0
        for ch in dec_part:
            if ch in _CHINESE_DIGITS:
                digit = _CHINESE_DIGITS[ch]
            elif ch.isdigit():
                digit = int(ch)
            else:
                continue
            scale += 1
            decimal_value += digit / (10 ** scale)

    value = total + decimal_value
    if percent:
        value /= 100.0
    if negative:
        value = -value
    return value
