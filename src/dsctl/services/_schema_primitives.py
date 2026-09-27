from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from dsctl.command_contract import (
    COMMAND_CATALOG,
    CommandContract,
    CommandEffects,
    GlobalOptionContract,
    InputContract,
    MissingDefault,
    ValueResolution,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.support.yaml_io import JsonObject, JsonValue


@dataclass(frozen=True, slots=True)
class CommandSchemaOverride:
    """Schema facts that cannot be derived from one command contract."""

    payload: JsonObject | None = None
    payload_schema: JsonObject | None = None
    domain_choices: Mapping[str, Sequence[str]] | None = None


def group(
    name: str,
    *,
    summary: str,
    commands: list[JsonObject],
    group_action: JsonObject | None = None,
) -> JsonObject:
    """Build one schema command group payload."""
    data: JsonObject = {
        "kind": "group",
        "name": name,
        "summary": summary,
        "commands": commands,
    }
    if group_action is not None:
        data["group_action"] = group_action
    return data


def catalog_group(
    name: str,
    *,
    summary: str,
    command_overrides: Mapping[str, CommandSchemaOverride],
    nested_group_summaries: Mapping[tuple[str, ...], str],
) -> JsonObject:
    """Project one catalog route prefix into its nested schema command tree."""
    contracts = tuple(
        contract for contract in COMMAND_CATALOG.commands if contract.route[0] == name
    )
    if not contracts:
        message = f"schema group {name!r} has no canonical commands"
        raise ValueError(message)

    actions = {contract.action for contract in contracts}
    unexpected_overrides = sorted(set(command_overrides) - actions)
    if unexpected_overrides:
        message = (
            f"schema group {name!r} has overrides for unrelated actions: "
            f"{unexpected_overrides}"
        )
        raise ValueError(message)

    expected_nested_groups = {
        contract.route[:depth]
        for contract in contracts
        for depth in range(2, len(contract.route))
    }
    if set(nested_group_summaries) != expected_nested_groups:
        message = (
            f"schema group {name!r} nested summaries differ from catalog routes: "
            f"expected {sorted(expected_nested_groups)!r}, "
            f"got {sorted(nested_group_summaries)!r}"
        )
        raise ValueError(message)

    group_actions = [contract for contract in contracts if len(contract.route) == 1]
    if len(group_actions) > 1:
        message = f"schema group {name!r} has multiple group-level actions"
        raise ValueError(message)
    group_action = (
        _command_with_override(group_actions[0], command_overrides)
        if group_actions
        else None
    )
    return group(
        name,
        summary=summary,
        commands=_catalog_command_nodes(
            prefix=(name,),
            contracts=contracts,
            command_overrides=command_overrides,
            nested_group_summaries=nested_group_summaries,
        ),
        group_action=group_action,
    )


def _catalog_command_nodes(
    *,
    prefix: tuple[str, ...],
    contracts: tuple[CommandContract, ...],
    command_overrides: Mapping[str, CommandSchemaOverride],
    nested_group_summaries: Mapping[tuple[str, ...], str],
) -> list[JsonObject]:
    nodes: list[JsonObject] = []
    emitted_groups: set[tuple[str, ...]] = set()
    for contract in contracts:
        if len(contract.route) <= len(prefix):
            continue
        child_path = (*prefix, contract.route[len(prefix)])
        if len(contract.route) == len(child_path):
            nodes.append(_command_with_override(contract, command_overrides))
            continue
        if child_path in emitted_groups:
            continue
        emitted_groups.add(child_path)
        child_contracts = tuple(
            candidate
            for candidate in contracts
            if candidate.route[: len(child_path)] == child_path
        )
        nodes.append(
            group(
                child_path[-1],
                summary=nested_group_summaries[child_path],
                commands=_catalog_command_nodes(
                    prefix=child_path,
                    contracts=child_contracts,
                    command_overrides=command_overrides,
                    nested_group_summaries=nested_group_summaries,
                ),
            )
        )
    return nodes


def _command_with_override(
    contract: CommandContract,
    overrides: Mapping[str, CommandSchemaOverride],
) -> JsonObject:
    override = overrides.get(contract.action, CommandSchemaOverride())
    return command(
        contract.action,
        payload=override.payload,
        payload_schema=override.payload_schema,
        domain_choices=override.domain_choices,
    )


def command(
    action: str,
    *,
    payload: JsonObject | None = None,
    payload_schema: JsonObject | None = None,
    domain_choices: Mapping[str, Sequence[str]] | None = None,
) -> JsonObject:
    """Enrich canonical invocation facts with domain payload and effect metadata."""
    contract = COMMAND_CATALOG.command(action)
    data = command_from_contract(contract)
    if contract.raw_output_format is not None:
        payload = {
            **(payload or {}),
            "format": contract.raw_output_format,
            "output": "raw_document",
        }
    if any(option.name == "dry-run" for option in contract.options):
        data["dry_run_output"] = {
            "default_view": "changes, constraints and prepared mutation order",
            "request_details_columns": "requests",
            "all_details_columns": "*",
            "request_plan_path": "data.requests",
            "apply_reprepares": True,
        }
    for key, value in (
        ("payload", payload),
        ("payload_schema", payload_schema),
    ):
        if value is not None:
            data[key] = value
    if domain_choices:
        pending = dict(domain_choices)
        for kind in ("arguments", "options"):
            for row in cast("list[JsonObject]", data[kind]):
                input_name = cast("str", row["name"])
                if input_name in pending:
                    row["choices"] = list(pending.pop(input_name))
        if pending:
            message = f"{action} has undeclared domain-choice inputs: {sorted(pending)}"
            raise ValueError(message)
    return data


def command_from_contract(contract: CommandContract) -> JsonObject:
    """Project canonical command invocation facts into the schema representation."""
    return {
        "kind": "command",
        "name": contract.name,
        "action": contract.action,
        "summary": contract.summary,
        "effects": effects_from_contract(contract.effects),
        "arguments": [_argument_from_contract(item) for item in contract.arguments],
        "options": [
            option_from_contract(item) for item in contract.options if not item.hidden
        ],
    }


def effects_from_contract(contract: CommandEffects) -> JsonObject:
    """Project one reviewed effects contract without deriving from command names."""
    data: JsonObject = {
        "remote": contract.remote,
        "local": contract.local,
    }
    if contract.dry_run is not None:
        data["dry_run"] = {
            "remote": contract.dry_run.remote,
            "local": contract.dry_run.local,
        }
    return data


def bounded_global_option_from_contract(
    contract: GlobalOptionContract,
) -> JsonObject:
    """Project one root option into the bounded invocation schema."""
    data: JsonObject = {
        "flag": contract.flag,
        "placement": "anywhere",
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
    data["placement"] = "anywhere"
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
            discovery_command_pattern=contract.discovery_command_pattern,
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
            value_name=contract.schema_value_name or contract.value_name,
            selector=contract.selector,
            choices=contract.choices or None,
            multiple=contract.multiple,
            minimum=contract.minimum,
            examples=contract.examples or None,
            supported_keys=contract.supported_keys or None,
            discovery_command=contract.discovery_command,
            discovery_command_pattern=contract.discovery_command_pattern,
            resolution=contract.resolution,
            normalization=(
                None if contract.normalization == "identity" else contract.normalization
            ),
        ),
    )
    if not isinstance(contract.fixed_default, MissingDefault):
        data["default"] = cast("JsonValue", contract.fixed_default)
    if contract.legacy_default:
        if contract.resolution is None:
            message = "legacy default projection requires value resolution"
            raise ValueError(message)
        data["default"] = contract.resolution.fallback
    if contract.maximum is not None:
        data["maximum"] = contract.maximum
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
    discovery_command_pattern: str | None = None,
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
    _project_discovery_reference(
        data,
        discovery_command=discovery_command,
        discovery_command_pattern=discovery_command_pattern,
    )
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
    discovery_command_pattern: str | None = None,
    placement: str | None = None,
    minimum: int | float | None = None,
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
                ("placement", placement),
                ("minimum", minimum),
                ("normalization", normalization),
            )
            if value is not None
        }
    )
    _project_discovery_reference(
        data,
        discovery_command=discovery_command,
        discovery_command_pattern=discovery_command_pattern,
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


def _project_discovery_reference(
    data: dict[str, object],
    *,
    discovery_command: str | None,
    discovery_command_pattern: str | None,
) -> None:
    """Project an exact command or a v2-compatible command pattern."""
    if discovery_command is not None and discovery_command_pattern is not None:
        message = "discovery command and pattern are mutually exclusive"
        raise ValueError(message)
    if discovery_command is not None:
        data["discovery_command"] = discovery_command
    if discovery_command_pattern is not None:
        if "<" in discovery_command_pattern or ">" in discovery_command_pattern:
            message = "command patterns must use uppercase metavars, not angle brackets"
            raise ValueError(message)
        data["discovery_command_pattern"] = discovery_command_pattern
        # Schema v2 compatibility alias. Remove only with the next schema major.
        data["discovery_command"] = discovery_command_pattern
