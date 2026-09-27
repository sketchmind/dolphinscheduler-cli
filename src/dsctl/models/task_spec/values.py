from __future__ import annotations

import json
import re
from typing import cast

from dsctl.models.common import (
    GlobalParamSpec,
    YamlObject,
    YamlValue,
    is_yaml_object,
)

UNICODE_EDGE_WHITESPACE_PATTERN = (
    r"\x09\x0a\x0b\x0c\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)

SHELL_ARGUMENT_JSON_SCHEMA_PATTERN = r"^[A-Za-z0-9_./:@%+=,\-]+$"
SHELL_ARGUMENT_PATTERN = re.compile(SHELL_ARGUMENT_JSON_SCHEMA_PATTERN)

_DS_PARAMETER_PLACEHOLDER_PATTERN = re.compile(
    r"\$\{(?P<braced>[^{}]+)\}|\$\[(?P<bracketed>[^\[\]]+)\]"
)


def emr_placeholder_names(value: str) -> frozenset[str]:
    """Return DS placeholder names without claiming they are substituted."""
    return frozenset(
        name.strip()
        for match in _DS_PARAMETER_PLACEHOLDER_PATTERN.finditer(value)
        if (name := (match.group("braced") or match.group("bracketed"))).strip()
    )


def contains_ds_parameter_placeholder(value: str) -> bool:
    """Return whether text contains either DS placeholder syntax."""
    return _DS_PARAMETER_PLACEHOLDER_PATTERN.search(value) is not None


def emr_local_param_placeholder_names(value: str) -> frozenset[str]:
    """Return only ${name} placeholders that DS binds from localParams."""
    return frozenset(
        name
        for match in _DS_PARAMETER_PLACEHOLDER_PATTERN.finditer(value)
        if (name := match.group("braced")) is not None and name.strip()
    )


def _reject_nonstandard_json_constant(value: str) -> YamlValue:
    message = f"non-standard JSON constant {value} is unsupported"
    raise ValueError(message)


def validate_json_object_text(
    value: str,
    *,
    field: str,
) -> YamlObject:
    """Parse one strict JSON object while retaining a field-specific error."""
    try:
        parsed = cast(
            "YamlValue",
            json.loads(
                value,
                parse_constant=_reject_nonstandard_json_constant,
            ),
        )
    except (json.JSONDecodeError, ValueError) as exc:
        message = f"{field} must contain a JSON object"
        raise ValueError(message) from exc
    if not is_yaml_object(parsed):
        message = f"{field} must contain a JSON object"
        raise ValueError(message)
    return parsed


def _validate_unique_input_varchar_params(
    parameters: list[GlobalParamSpec],
    *,
    task_type: str,
) -> None:
    """Validate one portable task-local substitution subset."""
    seen_names: set[str] = set()
    duplicate_names: set[str] = set()
    for parameter in parameters:
        if parameter.prop in seen_names:
            duplicate_names.add(parameter.prop)
        seen_names.add(parameter.prop)
    if duplicate_names:
        names = ", ".join(sorted(duplicate_names))
        message = (
            f"{task_type} localParams prop names must be unique; duplicates: {names}"
        )
        raise ValueError(message)
    if any(parameter.direct.value != "IN" for parameter in parameters):
        message = f"{task_type} localParams direct must be IN; OUT is unsupported"
        raise ValueError(message)
    if any(parameter.type.value != "VARCHAR" for parameter in parameters):
        message = f"{task_type} localParams type must be VARCHAR"
        raise ValueError(message)


def _normalize_task_name_ref(value: str) -> str:
    """Normalize one task-name reference used inside logical task params."""
    normalized = value.strip()
    if not normalized:
        message = "Task node references must not be empty"
        raise ValueError(message)
    return normalized
