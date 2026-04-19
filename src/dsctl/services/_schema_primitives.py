from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.yaml_io import JsonObject, JsonValue


def use_target_options(*, clear_help: str) -> list[dict[str, object]]:
    """Build the shared `use` command options."""
    return [
        option(
            "clear",
            value_type="boolean",
            description=clear_help,
            default=False,
        ),
        option(
            "scope",
            value_type="string",
            description="Select which persisted context layer to update.",
            default="project",
            choices=["project", "user"],
        ),
    ]


def project_option() -> dict[str, object]:
    """Build the shared project selector option."""
    return option(
        "project",
        value_type="string",
        description="Project name or code. Falls back to stored project context.",
        selector="name_or_code",
    )


def workflow_option(*, description: str) -> dict[str, object]:
    """Build the shared workflow selector option."""
    return option(
        "workflow",
        value_type="string",
        description=description,
        selector="name_or_code",
    )


def confirm_risk_option() -> dict[str, object]:
    """Build the shared high-risk confirmation option."""
    return option(
        "confirm-risk",
        value_type="string",
        description=(
            "Explicit confirmation token returned by a previous high-risk "
            "validation failure."
        ),
    )


def group(
    name: str,
    *,
    summary: str,
    commands: list[dict[str, object]],
    group_action: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build one schema command group payload."""
    data: dict[str, object] = {
        "kind": "group",
        "name": name,
        "summary": summary,
        "commands": commands,
    }
    if group_action is not None:
        data["group_action"] = group_action
    return data


def command(
    name: str,
    *,
    action: str,
    summary: str,
    arguments: list[dict[str, object]] | None = None,
    options: list[dict[str, object]] | None = None,
    payload: JsonObject | None = None,
    payload_schema: JsonObject | None = None,
) -> dict[str, object]:
    """Build one schema command payload."""
    data: JsonObject = {
        "kind": "command",
        "name": name,
        "action": action,
        "summary": summary,
        "arguments": cast("JsonValue", arguments or []),
        "options": cast("JsonValue", options or []),
    }
    if payload is not None:
        data["payload"] = payload
    if payload_schema is not None:
        data["payload_schema"] = payload_schema
    return cast("dict[str, object]", data)


def argument(
    name: str,
    *,
    value_type: str,
    description: str,
    required: bool = True,
    selector: str | None = None,
    choices: Sequence[object] | None = None,
    discovery_command: str | None = None,
) -> dict[str, object]:
    """Build one schema positional-argument payload."""
    data: dict[str, object] = {
        "kind": "argument",
        "name": name,
        "type": value_type,
        "required": required,
        "description": description,
    }
    if selector is not None:
        data["selector"] = selector
    if choices is not None:
        data["choices"] = list(choices)
    if discovery_command is not None:
        data["discovery_command"] = discovery_command
    return data


def option(
    name: str,
    *,
    value_type: str,
    description: str,
    required: bool = False,
    default: object | None = None,
    value_name: str | None = None,
    selector: str | None = None,
    choices: Sequence[object] | None = None,
    multiple: bool = False,
    examples: Sequence[str] | None = None,
    supported_keys: Sequence[str] | None = None,
    discovery_command: str | None = None,
) -> dict[str, object]:
    """Build one schema option payload."""
    data: dict[str, object] = {
        "kind": "option",
        "name": name,
        "flag": f"--{name}",
        "type": value_type,
        "required": required,
        "description": description,
    }
    if default is not None:
        data["default"] = default
    if value_name is not None:
        data["value_name"] = value_name
    if selector is not None:
        data["selector"] = selector
    if choices is not None:
        data["choices"] = list(choices)
    if multiple:
        data["multiple"] = True
    if examples is not None:
        data["examples"] = list(examples)
    if supported_keys is not None:
        data["supported_keys"] = list(supported_keys)
    if discovery_command is not None:
        data["discovery_command"] = discovery_command
    return data
