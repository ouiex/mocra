"""String case conversion utilities (port of Rust utils::string_case)."""

from __future__ import annotations


def to_camel_case(value: str) -> str:
    result = []
    capitalize_next = False
    for i, ch in enumerate(value):
        if ch == "_":
            capitalize_next = True
            continue
        if capitalize_next:
            result.append(ch.upper())
            capitalize_next = False
        elif i == 0:
            result.append(ch.lower())
        else:
            result.append(ch)
    return "".join(result)


def to_snake_case(value: str) -> str:
    result = []
    for i, ch in enumerate(value):
        if ch.isupper():
            if i != 0:
                result.append("_")
            result.append(ch.lower())
        else:
            result.append(ch)
    return "".join(result)


def to_kebab_case(value: str) -> str:
    result = []
    for i, ch in enumerate(value):
        if ch.isupper():
            if i != 0:
                result.append("-")
            result.append(ch.lower())
        else:
            result.append(ch)
    return "".join(result)


def to_pascal_case(value: str) -> str:
    result = []
    capitalize_next = True
    for ch in value:
        if ch == "_":
            capitalize_next = True
            continue
        if capitalize_next:
            result.append(ch.upper())
            capitalize_next = False
        else:
            result.append(ch)
    return "".join(result)


def to_title_case(value: str) -> str:
    result = []
    capitalize_next = True
    for ch in value:
        if ch.isspace():
            capitalize_next = True
            result.append(ch)
        elif capitalize_next:
            result.append(ch.upper())
            capitalize_next = False
        else:
            result.append(ch)
    return "".join(result)


def to_sentence_case(value: str) -> str:
    result = []
    capitalize_next = True
    for ch in value:
        if ch in {".", "!", "?"}:
            capitalize_next = True
            result.append(ch)
        elif capitalize_next and not ch.isspace():
            result.append(ch.upper())
            capitalize_next = False
        else:
            result.append(ch)
    return "".join(result)


def to_constant_case(value: str) -> str:
    result = []
    for i, ch in enumerate(value):
        if ch.isupper():
            if i != 0:
                result.append("_")
            result.append(ch)
        elif ch == "_":
            result.append("_")
        else:
            result.append(ch.upper())
    return "".join(result)


def to_dot_case(value: str) -> str:
    result = []
    for i, ch in enumerate(value):
        if ch.isupper():
            if i != 0:
                result.append(".")
            result.append(ch.lower())
        else:
            result.append(ch)
    return "".join(result)
