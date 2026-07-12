from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.command_contract import (
    CommandContract,
    GlobalOptionContract,
    InputContract,
    MissingDefault,
    ValueResolution,
)

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
        description=(
            "Project name or code. Run `dsctl project list` to discover values; "
            "falls back to stored project context."
        ),
        selector="name_or_code",
        discovery_command="dsctl project list",
    )


def workflow_option(*, description: str) -> dict[str, object]:
    """Build the shared workflow selector option."""
    return option(
        "workflow",
        value_type="string",
        description=description,
        selector="name_or_code",
        discovery_command="dsctl workflow list",
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


def command_from_contract(contract: CommandContract) -> JsonObject:
    """Project one canonical command contract into the stable schema shape."""
    return cast(
        "JsonObject",
        command(
            contract.name,
            action=contract.action,
            summary=contract.summary,
            arguments=cast(
                "list[dict[str, object]]",
                [_argument_from_contract(item) for item in contract.arguments],
            ),
            options=cast(
                "list[dict[str, object]]",
                [option_from_contract(item) for item in contract.options],
            ),
        ),
    )


def bounded_global_option_from_contract(
    contract: GlobalOptionContract,
) -> JsonObject:
    """Project one root option into the bounded invocation schema."""
    data: JsonObject = {
        "flag": contract.flag,
        "placement": "before_command",
    }
    input_contract = contract.input
    if input_contract.value_name is not None:
        data["value_name"] = input_contract.value_name
    if input_contract.choices:
        data["choices"] = list(input_contract.choices)
    if input_contract.normalization != "identity":
        data["normalization"] = input_contract.normalization
    if not isinstance(input_contract.fixed_default, MissingDefault):
        data["default"] = cast("JsonValue", input_contract.fixed_default)
    if input_contract.value_type == "boolean":
        data["type"] = "boolean"
    if contract.requirement is not None:
        required_name, required_value = contract.requirement
        data["requires"] = {f"--{required_name}": required_value}
    return data


def full_global_option_from_contract(
    contract: GlobalOptionContract,
) -> JsonObject:
    """Project one root option into the expanded schema representation."""
    input_contract = contract.input
    data = option_from_contract(input_contract)
    data["placement"] = "before_command"
    if contract.requirement is not None:
        required_name, required_value = contract.requirement
        data["requires"] = {f"--{required_name}": required_value}
    return data


def _argument_from_contract(contract: InputContract) -> JsonObject:
    return cast(
        "JsonObject",
        argument(
            contract.name,
            value_type=contract.value_type,
            description=contract.description,
            required=contract.required,
            selector=contract.selector,
            choices=contract.choices or None,
            discovery_command=contract.discovery_command,
        ),
    )


def option_from_contract(contract: InputContract) -> JsonObject:
    """Project one canonical option, preserving explicit null defaults."""
    data = cast(
        "JsonObject",
        option(
            contract.name,
            value_type=contract.value_type,
            description=contract.description,
            required=contract.required,
            value_name=contract.value_name,
            selector=contract.selector,
            choices=contract.choices or None,
            multiple=contract.multiple,
            discovery_command=contract.discovery_command,
            resolution=contract.resolution,
            normalization=(
                None if contract.normalization == "identity" else contract.normalization
            ),
        ),
    )
    if not isinstance(contract.fixed_default, MissingDefault):
        data["default"] = cast("JsonValue", contract.fixed_default)
    if contract.path_rules is not None:
        rules = contract.path_rules
        data["input_policy"] = {
            "exists": rules.exists,
            "file_okay": rules.file_okay,
            "dir_okay": rules.dir_okay,
            "readable": rules.readable,
            "resolve_path": rules.resolve_path,
        }
    return data


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
    placement: str | None = None,
    minimum: int | None = None,
    resolution: ValueResolution | None = None,
    normalization: str | None = None,
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
    data.update(
        {
            key: value
            for key, value in (
                ("default", default),
                ("value_name", value_name),
                ("selector", selector),
                ("discovery_command", discovery_command),
                ("placement", placement),
                ("minimum", minimum),
                ("normalization", normalization),
            )
            if value is not None
        }
    )
    if choices is not None:
        data["choices"] = list(choices)
    if multiple:
        data["multiple"] = True
    if examples is not None:
        data["examples"] = list(examples)
    if supported_keys is not None:
        data["supported_keys"] = list(supported_keys)
    if resolution is not None:
        data["resolution"] = {
            "precedence": list(resolution.precedence),
            "fallback": resolution.fallback,
        }
    return data
