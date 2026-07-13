from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject, JsonValue


def command_contract_rows(
    command_data: Mapping[str, JsonValue],
    *,
    action: str,
) -> list[JsonObject]:
    """Derive output rows from one canonical action-local schema contract."""
    command_row: JsonObject = {
        "kind": "command",
        "name": action,
        "description": str(command_data.get("summary", "")),
    }
    invocation = command_data.get("invocation")
    if isinstance(invocation, str):
        command_row["invocation"] = invocation
    rows: list[JsonObject] = [command_row]
    rows.extend(
        _parameter_row("argument", item)
        for item in _command_items(command_data, "arguments")
    )
    rows.extend(
        _parameter_row("option", item)
        for item in _command_items(command_data, "options")
    )
    constraints = command_data.get("constraints")
    if isinstance(constraints, list):
        rows.extend(
            _constraint_row(item) for item in constraints if isinstance(item, Mapping)
        )
    payload = command_data.get("payload")
    if isinstance(payload, Mapping):
        rows.extend(_payload_rows(payload))
    data_shape = command_data.get("data_shape")
    if isinstance(data_shape, Mapping):
        rows.extend(_mapping_rows("data_shape", data_shape))
    return rows


def _command_items(
    command_data: Mapping[str, JsonValue],
    key: str,
) -> list[Mapping[str, JsonValue]]:
    value = command_data.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _parameter_row(kind: str, item: Mapping[str, JsonValue]) -> JsonObject:
    row: JsonObject = {
        "kind": kind,
        "name": str(item.get("name", "")),
        "type": str(item.get("type", "")),
        "required": bool(item.get("required", False)),
        "description": str(item.get("description", "")),
    }
    flag = item.get("flag")
    if isinstance(flag, str):
        row["flag"] = flag
    value = _parameter_value(item)
    if value:
        row["value"] = value
    discovery_command = item.get("discovery_command")
    if isinstance(discovery_command, str):
        row["discovery_command"] = discovery_command
    return row


def _constraint_row(item: Mapping[str, JsonValue]) -> JsonObject:
    return {
        "kind": "constraint",
        "name": str(item.get("kind", "")),
        "value": _constraint_summary(item),
    }


def _constraint_summary(item: Mapping[str, JsonValue]) -> str:
    alternatives = item.get("alternatives")
    if isinstance(alternatives, list):
        rendered = [
            "+".join(str(field) for field in alternative)
            for alternative in alternatives
            if isinstance(alternative, list)
        ]
        body = " | ".join(rendered)
    else:
        fields = item.get("fields")
        rendered_fields = (
            [str(field) for field in fields] if isinstance(fields, list) else []
        )
        body = ", ".join(rendered_fields)
        if len(body) > 80:
            body = f"{len(rendered_fields)} fields; inspect JSON command.constraints"
    if_present = item.get("if_present")
    if isinstance(if_present, str):
        return f"if {if_present}: {body}"
    if_absent = item.get("if_absent")
    if isinstance(if_absent, str):
        return f"if absent {if_absent}: {body}"
    return body


def _parameter_value(item: Mapping[str, JsonValue]) -> str:
    parts: list[str] = []
    selector = item.get("selector")
    if isinstance(selector, str):
        parts.append(f"selector={selector}")
    minimum = item.get("minimum")
    if isinstance(minimum, int | float) and not isinstance(minimum, bool):
        parts.append(f"minimum={minimum}")
    resolution = item.get("resolution")
    if "default" in item and not isinstance(resolution, Mapping):
        parts.append(f"default={_compact_value(item['default'])}")
    if isinstance(resolution, Mapping):
        precedence = resolution.get("precedence")
        fallback = resolution.get("fallback")
        if isinstance(precedence, list) and "fallback" in resolution:
            sources = [
                "pref" if source == "project_preference" else str(source)
                for source in precedence
                if source != "default"
            ]
            sources.append("null" if fallback is None else _compact_value(fallback))
            parts.append(f"resolve={'>'.join(sources)}")
    choices = item.get("choices")
    if isinstance(choices, list):
        parts.append(f"choices={_choices_summary(choices, item=item)}")
    if item.get("multiple") is True:
        parts.append("multiple=true")
    value_name = item.get("value_name")
    if isinstance(value_name, str):
        parts.append(f"value_name={value_name}")
    return "; ".join(parts)


def _choices_summary(
    choices: list[JsonValue],
    *,
    item: Mapping[str, JsonValue],
) -> str:
    if len(choices) <= 8:
        return _compact_value(choices)
    discovery_command = item.get("discovery_command")
    if isinstance(discovery_command, str):
        return f"{len(choices)} values; use discovery_command"
    return f"{len(choices)} values"


def _payload_rows(payload: Mapping[str, JsonValue]) -> list[JsonObject]:
    interesting_keys = (
        "format",
        "source_option",
        "source_options",
        "raw_option",
        "template_discovery_command",
        "template_command",
        "template_command_pattern",
        "patch_template_command",
        "file_source_command",
        "file_schedule",
        "file_template_command",
        "schedule_on_create",
        "schedule_on_edit",
        "schema_command_pattern",
        "template_json_path",
        "template_payload_path",
        "paste_into",
        "target_command",
        "target_commands",
        "type_discovery_command",
        "type_enum",
        "upstream_request_shape",
    )
    return [
        {
            "kind": "payload",
            "name": key,
            "value": _compact_value(payload[key]),
        }
        for key in interesting_keys
        if key in payload
    ]


def _mapping_rows(
    kind: str,
    value: Mapping[str, JsonValue],
) -> list[JsonObject]:
    return [
        {
            "kind": kind,
            "name": str(key),
            "value": _compact_value(item),
        }
        for key, item in value.items()
    ]


def _compact_value(value: JsonValue) -> str:
    if isinstance(value, list):
        scalar_values: list[str] = []
        for item in value:
            if isinstance(item, dict | list):
                return ", ".join(str(nested) for nested in value)
            scalar_values.append(_compact_scalar(item))
        return ", ".join(scalar_values)
    if isinstance(value, str | int | float | bool) or value is None:
        return _compact_scalar(value)
    return str(value)


def _compact_scalar(value: JsonValue) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


__all__ = ["command_contract_rows"]
