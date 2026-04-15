"""Java literal decoding helpers shared by extraction and rendering."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def decode_java_literal_text(
    raw_text: str,
    *,
    prefix_operators: Sequence[str] = (),
) -> bool | int | float | str:
    text = _apply_java_prefix_operators(raw_text.strip(), prefix_operators)
    if text == "true":
        return True
    if text == "false":
        return False
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    numeric_value = parse_java_numeric_literal(text)
    if numeric_value is not None:
        return numeric_value
    return text


def infer_java_literal_type(
    raw_text: str,
    *,
    prefix_operators: Sequence[str] = (),
) -> str:
    text = _apply_java_prefix_operators(raw_text.strip(), prefix_operators)
    if text == "null":
        return "Void"
    if text in {"true", "false"}:
        return "Boolean"
    if (text.startswith('"') and text.endswith('"')) or (
        text.startswith("'") and text.endswith("'")
    ):
        return "String"

    numeric_value = parse_java_numeric_literal(text)
    if numeric_value is None:
        return "Any"

    body, suffix = _split_java_numeric_suffix(text)
    if suffix == "l":
        return "Long"
    if suffix == "f":
        return "Float"
    if suffix == "d":
        return "Double"
    if any(marker in body for marker in (".", "e", "E", "p", "P")):
        return "Double"
    if isinstance(numeric_value, int) and not -(2**31) <= numeric_value <= (2**31 - 1):
        return "Long"
    return "Integer"


def parse_java_numeric_literal(raw_text: str) -> int | float | None:
    text = raw_text.strip()
    if not text:
        return None

    sign = 1
    if text[0] in {"+", "-"}:
        sign = -1 if text[0] == "-" else 1
        text = text[1:]
    if not text:
        return None

    body, suffix = _split_java_numeric_suffix(text)
    normalized_body = body.replace("_", "")
    if not normalized_body:
        return None

    if suffix == "l":
        integer_value = _parse_java_integer_literal(normalized_body)
        return None if integer_value is None else sign * integer_value
    if suffix in {"d", "f"}:
        float_value = _parse_java_float_literal(normalized_body)
        return None if float_value is None else sign * float_value
    if any(marker in normalized_body for marker in (".", "e", "E", "p", "P")):
        float_value = _parse_java_float_literal(normalized_body)
        return None if float_value is None else sign * float_value

    integer_value = _parse_java_integer_literal(normalized_body)
    return None if integer_value is None else sign * integer_value


def _apply_java_prefix_operators(
    raw_text: str,
    prefix_operators: Sequence[str],
) -> str:
    sign = 1
    for operator in prefix_operators:
        if operator == "+":
            continue
        if operator == "-":
            sign *= -1
            continue
        return raw_text
    if sign < 0 and not raw_text.startswith("-"):
        return f"-{raw_text}"
    return raw_text


def _split_java_numeric_suffix(text: str) -> tuple[str, str | None]:
    suffix = text[-1].lower()
    if suffix in {"d", "f", "l"}:
        return text[:-1], suffix
    return text, None


def _parse_java_integer_literal(text: str) -> int | None:
    try:
        if text.startswith(("0x", "0X")):
            return int(text, 0)
        if text.startswith(("0b", "0B")):
            return int(text, 0)
        if len(text) > 1 and text.startswith("0"):
            return int(text, 8)
        return int(text, 10)
    except ValueError:
        return None


def _parse_java_float_literal(text: str) -> float | None:
    try:
        if text.startswith(("0x", "0X")):
            return float.fromhex(text)
        return float(text)
    except ValueError:
        return None
