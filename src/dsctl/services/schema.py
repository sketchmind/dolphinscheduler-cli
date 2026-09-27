from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from difflib import get_close_matches
from typing import TYPE_CHECKING, NoReturn, TypeAlias, cast

from dsctl import __version__
from dsctl.cli_surface import (
    COMMAND_GROUPS,
    TEMPLATE_RESOURCE,
    TOP_LEVEL_COMMANDS,
    stable_leaf_actions,
)
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.command_references import project_command_references
from dsctl.data_shapes import (
    data_shape_schema_for_action,
    data_shapes_by_view_schema_for_action,
)
from dsctl.errors import ConfigError, UserInputError
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.schema_contract_rows import command_contract_rows
from dsctl.services._discovery_commands import (
    render_discovery_command,
    render_discovery_reference,
)
from dsctl.services._schema_constraints import constraints_for_action
from dsctl.services._schema_groups import (
    GROUP_SUMMARIES,
    NESTED_GROUP_SUMMARIES,
    build_schema_group,
)
from dsctl.services._schema_primitives import (
    bounded_global_option_from_contract,
    catalog_group,
    full_global_option_from_contract,
)
from dsctl.services._schema_primitives import command as _command
from dsctl.services._schema_version_projection import (
    project_command_for_version,
    project_data_shape_for_version,
)
from dsctl.services._surface_metadata import (
    confirmation_schema_data,
    error_schema_data,
    output_schema_data,
    selection_schema_data,
)
from dsctl.services.capabilities import (
    action_capability_metadata,
    compatible_read_actions,
    schema_capabilities_data,
    unresolved_action_capability,
    unresolved_ds_data,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import supported_task_template_types
from dsctl.services.version_resolution import CompatibilityResolution, resolve_target
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_version_support,
    supported_version_metadata,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject

SCOPED_SCHEMA_HEADER_KEYS = (
    "schema_version",
    "view",
    "cli",
    "ds",
    "supported_ds_versions",
    "ds_versions",
    "global_options",
    "selection",
    "output",
    "errors",
    "confirmation",
)
SCHEMA_VERSION = 3


@dataclass(frozen=True, slots=True)
class SchemaIndexScope:
    """Discover the bounded root schema index."""


@dataclass(frozen=True, slots=True)
class SchemaGroupScope:
    """Discover one command group's action index."""

    name: str


@dataclass(frozen=True, slots=True)
class SchemaActionScope:
    """Discover one complete action-local command contract."""

    action: str


@dataclass(frozen=True, slots=True)
class SchemaGroupsScope:
    """List valid group selectors."""


@dataclass(frozen=True, slots=True)
class SchemaActionsScope:
    """List valid action selectors."""


SchemaExpandableScope: TypeAlias = (
    SchemaIndexScope | SchemaGroupScope | SchemaActionScope
)


@dataclass(frozen=True, slots=True)
class SchemaFullScope:
    """Expand only a root, group, or action scope."""

    scope: SchemaExpandableScope


SchemaScope: TypeAlias = (
    SchemaIndexScope
    | SchemaGroupScope
    | SchemaActionScope
    | SchemaGroupsScope
    | SchemaActionsScope
    | SchemaFullScope
)


@dataclass(frozen=True, slots=True)
class SchemaQuery:
    """One valid progressive-discovery query."""

    scope: SchemaScope


def get_schema_result(
    *,
    env_file: str | None = None,
    group: str | None = None,
    command_action: str | None = None,
    list_groups: bool = False,
    list_commands: bool = False,
    full: bool = False,
) -> CommandResult:
    """Return one progressive machine-readable CLI schema view."""
    query = _schema_query(
        group=group,
        command_action=command_action,
        list_groups=list_groups,
        list_commands=list_commands,
        full=full,
    )
    scope = (
        query.scope.scope if isinstance(query.scope, SchemaFullScope) else query.scope
    )
    if isinstance(scope, SchemaGroupScope) and scope.name not in COMMAND_GROUPS:
        _raise_unknown_schema_group(scope.name, env_file=env_file)
    if (
        isinstance(scope, SchemaActionScope)
        and scope.action not in stable_leaf_actions()
    ):
        _raise_unknown_schema_action(scope.action, env_file=env_file)
    try:
        resolution = resolve_target(env_file, mode="local")
    except ConfigError as error:
        if error.details.get("reason") != "version_not_resolved":
            raise
        resolution = None
    if resolution is None or isinstance(resolution, CompatibilityResolution):
        return _discover_unresolved_schema(query, resolution, env_file=env_file)
    return _discover_schema(query, ds_version=resolution.version, env_file=env_file)


def _schema_query(
    *,
    group: str | None,
    command_action: str | None,
    list_groups: bool,
    list_commands: bool,
    full: bool,
) -> SchemaQuery:
    """Translate CLI flags into a query that cannot hold conflicting scopes."""
    scope_count = sum(
        (
            group is not None,
            command_action is not None,
            list_groups,
            list_commands,
        )
    )
    if scope_count > 1:
        message = (
            "--group, --command, --list-groups, and --list-commands are "
            "mutually exclusive"
        )
        raise UserInputError(
            message,
            suggestion=(
                "Pass only one schema scope option, or omit them for the schema index."
            ),
        )
    if full and (list_groups or list_commands):
        message = "--full cannot be combined with schema list views"
        raise UserInputError(
            message,
            suggestion=(
                "Use --full alone, combine it with --group/--command, or remove it."
            ),
        )
    if group is not None:
        group_scope = SchemaGroupScope(group.strip())
        return SchemaQuery(SchemaFullScope(group_scope) if full else group_scope)
    if list_groups:
        return SchemaQuery(SchemaGroupsScope())
    if list_commands:
        return SchemaQuery(SchemaActionsScope())
    if command_action is not None:
        action_scope = SchemaActionScope(command_action.strip())
        return SchemaQuery(SchemaFullScope(action_scope) if full else action_scope)
    index_scope = SchemaIndexScope()
    return SchemaQuery(SchemaFullScope(index_scope) if full else index_scope)


def _discover_schema(
    query: SchemaQuery, *, ds_version: str, env_file: str | None = None
) -> CommandResult:
    """Build only the representation required by one validated query."""
    scope = query.scope
    if isinstance(scope, SchemaFullScope):
        return _full_schema_result(
            scope.scope, ds_version=ds_version, env_file=env_file
        )
    if isinstance(scope, SchemaIndexScope):
        return CommandResult(
            data=_schema_index_data(ds_version=ds_version, env_file=env_file),
            resolved={"schema": {"view": "index"}},
        )
    if isinstance(scope, SchemaGroupScope):
        data = _schema_group_index_data(
            scope.name, ds_version=ds_version, env_file=env_file
        )
        return CommandResult(
            data=data,
            resolved={"schema": {"view": "group", "group": scope.name}},
        )
    if isinstance(scope, SchemaActionScope):
        data = _schema_action_data(
            scope.action, ds_version=ds_version, env_file=env_file
        )
        return CommandResult(
            data=data,
            resolved={"schema": {"view": "command", "command": scope.action}},
        )

    discovery_data = _schema_discovery_source(ds_version=ds_version)
    if isinstance(scope, SchemaGroupsScope):
        return CommandResult(
            data=_schema_group_discovery_rows(discovery_data, env_file=env_file),
            resolved={"schema": {"view": "groups"}},
        )
    return CommandResult(
        data=_schema_action_discovery_rows(ds_version=ds_version, env_file=env_file),
        resolved={"schema": {"view": "commands"}},
    )


def _full_schema_result(
    scope: SchemaExpandableScope,
    *,
    ds_version: str,
    env_file: str | None = None,
) -> CommandResult:
    """Return the expanded representation retained behind explicit --full."""
    data = require_json_object(
        _schema_data(ds_version=ds_version, env_file=env_file),
        label="schema data",
    )
    resolved: JsonObject = {"view": "full"}
    if isinstance(scope, SchemaGroupScope):
        data = _schema_group_data(data, scope.name, ds_version=ds_version)
        resolved["scope"] = "group"
        resolved["group"] = scope.name
    elif isinstance(scope, SchemaActionScope):
        data = _schema_command_data(data, scope.action, ds_version=ds_version)
        resolved["scope"] = "command"
        resolved["command"] = scope.action
    return CommandResult(data=data, resolved={"schema": resolved})


def _schema_index_data(*, ds_version: str, env_file: str | None = None) -> JsonObject:
    """Return a bounded index containing names, not expanded contracts."""
    groups: list[JsonObject] = []
    grouped_action_count = 0
    for group_name in COMMAND_GROUPS:
        group = _build_schema_group(group_name, ds_version=ds_version)
        actions = _schema_group_action_index(
            group,
            group_name=group_name,
            ds_version=ds_version,
            env_file=env_file,
        )
        grouped_action_count += len(actions)
        available_action_count = sum(
            item["availability"] != "unsupported" for item in actions
        )
        groups.append(
            {
                "name": group_name,
                "summary": str(group.get("summary", "")),
                "action_count": len(actions),
                "available_action_count": available_action_count,
                "actions": [str(item["action"]) for item in actions],
                "schema_command": render_discovery_command(
                    "schema", values={"group": group_name}, env_file=env_file
                ),
                "help_command": f"dsctl {group_name} --help",
            }
        )

    root_actions: list[JsonObject] = []
    support = get_version_support(ds_version)
    for action in TOP_LEVEL_COMMANDS:
        capability = support.catalog.entries[action]
        contract = COMMAND_CATALOG.command(action)
        root_actions.append(
            {
                "action": action,
                "summary": contract.summary,
                "effects": {
                    "remote": contract.effects.remote,
                    "local": contract.effects.local,
                },
                "availability": capability.availability.value,
                "verification": capability.verification.value,
                "schema_command": render_discovery_command(
                    "schema", values={"command": action}, env_file=env_file
                ),
                "help_command": f"dsctl {action} --help",
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "view": "index",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
        "action_inventory_scope": "installed_cli_surface",
        "global_options": _bounded_global_options(),
        "action_count": len(root_actions) + grouped_action_count,
        "groups": groups,
        "root_actions": root_actions,
        "links": [
            {
                "rel": "group_schema",
                "command_pattern": "dsctl schema --group GROUP",
            },
            {
                "rel": "action_schema",
                "command_pattern": "dsctl schema --command ACTION",
            },
            {
                "rel": "capabilities",
                "command": render_discovery_command("capabilities", env_file=env_file),
            },
        ],
    }


def _schema_group_index_data(
    group_name: str, *, ds_version: str, env_file: str | None = None
) -> JsonObject:
    """Return one group's bounded action index."""
    group = _build_schema_group_or_error(group_name, ds_version=ds_version)
    actions = _schema_group_action_index(
        group,
        group_name=group_name,
        ds_version=ds_version,
        env_file=env_file,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "view": "group",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
        "group": {
            "name": group_name,
            "summary": str(group.get("summary", "")),
            "action_count": len(actions),
            "available_action_count": sum(
                item["availability"] != "unsupported" for item in actions
            ),
        },
        "actions": actions,
        "links": [
            {
                "rel": "action_schema",
                "command_pattern": "dsctl schema --command ACTION",
            },
            {
                "rel": "help",
                "command": f"dsctl {group_name} --help",
            },
        ],
    }


def _schema_action_data(
    command_action: str, *, ds_version: str, env_file: str | None = None
) -> JsonObject:
    """Return one complete command contract without shared global repetition."""
    command, group = _build_schema_action_or_error(
        command_action, ds_version=ds_version
    )
    command = _annotate_command_node_data_shape(
        command, ds_version=ds_version, env_file=env_file
    )
    support = get_version_support(ds_version)
    data: JsonObject = {
        "schema_version": SCHEMA_VERSION,
        "view": "command",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
        "capability": action_capability_metadata(support, command_action),
        "global_options": _bounded_global_options(),
        "command": command,
        "links": [
            {
                "rel": "help",
                "command": _help_command_for_action(command_action),
            },
        ],
    }
    if group is not None:
        group_name = str(group["name"])
        data["group"] = {
            "name": group_name,
            "summary": str(group.get("summary", "")),
            "schema_command": render_discovery_command(
                "schema", values={"group": group_name}, env_file=env_file
            ),
            "help_command": f"dsctl {group_name} --help",
        }
        links = data["links"]
        if isinstance(links, list):
            links.append(
                {
                    "rel": "group_schema",
                    "command": render_discovery_command(
                        "schema", values={"group": group_name}, env_file=env_file
                    ),
                }
            )
    return require_json_object(
        project_command_references(data),
        label="command schema data",
    )


def _schema_cli_data() -> JsonObject:
    return {"name": "dsctl", "version": __version__}


def _schema_ds_data(ds_version: str) -> JsonObject:
    support = get_version_support(ds_version)
    return {
        "selected_version": support.server_version,
        "contract_version": support.contract_version,
        "support_level": support.support_level,
        "tested": support.tested,
    }


def _bounded_global_options() -> list[JsonObject]:
    """Return the minimum global contract needed to construct an invocation."""
    return [
        require_json_object(
            bounded_global_option_from_contract(option),
            label=f"{option.flag} bounded global option",
        )
        for option in sorted(
            COMMAND_CATALOG.global_options,
            key=lambda item: item.schema_order,
        )
    ]


def _build_schema_group(group_name: str, *, ds_version: str) -> JsonObject:
    task_types = (
        list(
            supported_task_template_types(
                catalog=get_task_authoring_catalog(ds_version)
            )
        )
        if group_name == TEMPLATE_RESOURCE
        else []
    )
    return require_json_object(
        build_schema_group(group_name, task_types=task_types),
        label="schema command group",
    )


def _build_schema_group_or_error(group_name: str, *, ds_version: str) -> JsonObject:
    if group_name in COMMAND_GROUPS:
        return _build_schema_group(group_name, ds_version=ds_version)
    return _raise_unknown_schema_group(group_name)


def _build_schema_action_or_error(
    command_action: str,
    *,
    ds_version: str,
) -> tuple[JsonObject, JsonObject | None]:
    if command_action in TOP_LEVEL_COMMANDS:
        return _top_level_command_schema(command_action), None

    group_name = command_action.partition(".")[0]
    if group_name in COMMAND_GROUPS:
        group = _build_schema_group(group_name, ds_version=ds_version)
        command = _find_action_node(group, command_action)
        if command is not None:
            normalized = dict(command)
            normalized.setdefault("kind", "command")
            normalized.setdefault("name", group_name)
            normalized.setdefault("arguments", [])
            normalized.setdefault("options", [])
            return normalized, group
    return _raise_unknown_schema_action(command_action)


def _schema_group_action_index(
    group: JsonObject,
    *,
    group_name: str,
    ds_version: str,
    env_file: str | None = None,
) -> list[JsonObject]:
    rows = _schema_command_discovery_rows_from_node(
        group, group_name=group_name, env_file=env_file
    )
    support = get_version_support(ds_version)
    actions: list[JsonObject] = []
    for row in rows:
        action = str(row["action"])
        capability = support.catalog.entries[action]
        actions.append(
            {
                "action": action,
                "name": str(row["name"]),
                "summary": str(row["summary"]),
                "availability": capability.availability.value,
                "verification": capability.verification.value,
                "schema_command": str(row["schema_command"]),
                "help_command": _help_command_for_action(action),
                "effects": row["effects"],
            }
        )
    return actions


def _schema_discovery_source(*, ds_version: str) -> JsonObject:
    return {
        "commands": [_top_level_command_schema(name) for name in TOP_LEVEL_COMMANDS]
        + [_build_schema_group(name, ds_version=ds_version) for name in COMMAND_GROUPS]
    }


def _schema_action_discovery_rows(
    *, ds_version: str, env_file: str | None = None
) -> list[JsonObject]:
    source = _schema_discovery_source(ds_version=ds_version)
    rows: list[JsonObject] = []
    for item in _schema_command_nodes(source):
        rows.extend(
            _schema_command_discovery_rows_from_node(
                item, group_name=None, env_file=env_file
            )
        )
    return rows


def _help_command_for_action(action: str) -> str:
    return f"{_action_command(action)} --help"


def _schema_invocation(action: str, command: JsonObject) -> str:
    """Return one exact CLI path plus argument/option placeholders."""
    base = COMMAND_CATALOG.command(action).invocation_prefix
    arguments = command.get("arguments")
    if isinstance(arguments, list):
        for item in arguments:
            if not isinstance(item, Mapping):
                continue
            name = item.get("name")
            if not isinstance(name, str):
                continue
            placeholder = name.replace("-", "_").upper()
            base += (
                f" {placeholder}"
                if item.get("required") is True
                else f" [{placeholder}]"
            )
    options = command.get("options")
    if isinstance(options, list) and options:
        base += " [OPTIONS]"
    return base


def _action_command(action: str) -> str:
    return COMMAND_CATALOG.command(action).command_path


def _schema_data(*, ds_version: str, env_file: str | None = None) -> dict[str, object]:
    task_types = list(
        supported_task_template_types(catalog=get_task_authoring_catalog(ds_version))
    )
    command_groups = _command_groups(task_types)
    commands = [
        require_json_object(command_data, label="schema command data")
        for command_data in (
            *(_top_level_command_schema(name) for name in TOP_LEVEL_COMMANDS),
            *(command_groups[name] for name in COMMAND_GROUPS),
        )
    ]
    data: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "view": "full",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
        "supported_ds_versions": list(SUPPORTED_VERSIONS),
        "ds_versions": list(supported_version_metadata()),
        "global_options": [
            full_global_option_from_contract(option)
            for option in sorted(
                COMMAND_CATALOG.global_options,
                key=lambda item: item.schema_order,
            )
        ],
        "selection": selection_schema_data(),
        "output": output_schema_data(),
        "errors": error_schema_data(),
        "confirmation": confirmation_schema_data(),
        "capabilities": schema_capabilities_data(
            ds_version=ds_version, env_file=env_file
        ),
        "commands": _annotate_command_data_shapes(
            commands, ds_version=ds_version, env_file=env_file
        ),
    }
    return cast(
        "dict[str, object]",
        require_json_object(
            project_command_references(data),
            label="full schema data",
        ),
    )


def _command_groups(task_types: list[str]) -> dict[str, JsonObject]:
    return {
        name: build_schema_group(
            name,
            task_types=task_types if name == TEMPLATE_RESOURCE else (),
        )
        for name in COMMAND_GROUPS
    }


def _schema_group_data(
    schema_data: JsonObject,
    group_name: str,
    *,
    ds_version: str,
) -> JsonObject:
    group = _find_schema_group(schema_data, group_name)
    scoped = _schema_header(schema_data)
    scoped["commands"] = [group]
    rows = _schema_group_summary_rows(group)
    support = get_version_support(ds_version)
    for row in rows:
        action = row.get("action")
        if not isinstance(action, str):
            continue
        capability = support.catalog.entries[action]
        row["availability"] = capability.availability.value
        row["verification"] = capability.verification.value
    scoped["rows"] = rows
    return scoped


def _schema_command_data(
    schema_data: JsonObject,
    command_action: str,
    *,
    ds_version: str,
) -> JsonObject:
    command = _find_schema_command(schema_data, command_action)
    scoped = _schema_header(schema_data)
    scoped["capability"] = action_capability_metadata(
        get_version_support(ds_version), command_action
    )
    scoped["commands"] = [command]
    scoped["rows"] = _schema_command_detail_rows(command, action=command_action)
    return scoped


def _schema_header(schema_data: JsonObject) -> JsonObject:
    return {key: schema_data[key] for key in SCOPED_SCHEMA_HEADER_KEYS}


def _schema_group_discovery_rows(
    schema_data: JsonObject, *, env_file: str | None = None
) -> list[JsonObject]:
    rows: list[JsonObject] = []
    for item in _schema_command_nodes(schema_data):
        if item.get("kind") != "group":
            continue
        name = item.get("name")
        if not isinstance(name, str):
            continue
        rows.append(
            {
                "name": name,
                "summary": str(item.get("summary", "")),
                "action_count": len(_schema_command_actions(item)),
                "schema_command": render_discovery_command(
                    "schema", values={"group": name}, env_file=env_file
                ),
            }
        )
    return rows


def _schema_command_discovery_rows_from_node(
    node: JsonObject,
    *,
    group_name: str | None,
    env_file: str | None = None,
) -> list[JsonObject]:
    if node.get("kind") == "command":
        action = node.get("action")
        if not isinstance(action, str):
            return []
        return [
            {
                "action": action,
                "group": group_name,
                "name": str(node.get("name", "")),
                "summary": str(node.get("summary", "")),
                "effects": _schema_base_effects(node),
                "schema_command": render_discovery_command(
                    "schema", values={"command": action}, env_file=env_file
                ),
            }
        ]
    if node.get("kind") != "group":
        return []

    current_group_name = group_name
    node_name = node.get("name")
    if current_group_name is None and isinstance(node_name, str):
        current_group_name = node_name

    rows: list[JsonObject] = []
    group_action = node.get("group_action")
    if isinstance(group_action, dict):
        action_data = require_json_object(group_action, label="schema group action")
        action = action_data.get("action")
        if isinstance(action, str):
            rows.append(
                {
                    "action": action,
                    "group": current_group_name,
                    "name": str(node.get("name", "")),
                    "summary": str(action_data.get("summary", "")),
                    "effects": _schema_base_effects(action_data),
                    "schema_command": render_discovery_command(
                        "schema", values={"command": action}, env_file=env_file
                    ),
                }
            )
    for child in _schema_group_commands(node):
        rows.extend(
            _schema_command_discovery_rows_from_node(
                child,
                group_name=current_group_name,
                env_file=env_file,
            )
        )
    return rows


def _schema_group_summary_rows(group_data: JsonObject) -> list[JsonObject]:
    group_action_name: str | None = None
    group_action = group_data.get("group_action")
    if isinstance(group_action, Mapping):
        action_data = require_json_object(group_action, label="schema group action")
        action = action_data.get("action")
        if isinstance(action, str):
            group_action_name = action
    group_name = group_data.get("name")
    discovered = _schema_command_discovery_rows_from_node(
        group_data,
        group_name=group_name if isinstance(group_name, str) else None,
    )
    return [
        {
            "kind": (
                "group_action" if row.get("action") == group_action_name else "command"
            ),
            "action": row["action"],
            "name": row["name"],
            "summary": row["summary"],
            "effects": row["effects"],
            "schema_command": row["schema_command"],
        }
        for row in discovered
    ]


def _schema_command_detail_rows(
    command_node: JsonObject,
    *,
    action: str,
) -> list[JsonObject]:
    command_data = _find_action_node(command_node, action)
    if command_data is None:
        return []
    return command_contract_rows(command_data, action=action)


def _schema_base_effects(command_data: JsonObject) -> JsonObject:
    """Return the bounded normal-invocation effects used by discovery rows."""
    effects = command_data.get("effects")
    if not isinstance(effects, Mapping):
        message = "schema command is missing its reviewed effects contract"
        raise TypeError(message)
    remote = effects.get("remote")
    local = effects.get("local")
    if not isinstance(remote, str) or not isinstance(local, str):
        message = "schema command effects require remote and local values"
        raise TypeError(message)
    return {"remote": remote, "local": local}


def _find_action_node(node: JsonObject, action: str) -> JsonObject | None:
    if node.get("kind") == "command" and node.get("action") == action:
        return node
    if node.get("kind") != "group":
        return None
    group_action = node.get("group_action")
    if isinstance(group_action, Mapping) and group_action.get("action") == action:
        return require_json_object(group_action, label="schema group action")
    for child in _schema_group_commands(node):
        matched = _find_action_node(child, action)
        if matched is not None:
            return matched
    return None


def _find_schema_group(schema_data: JsonObject, group_name: str) -> JsonObject:
    for item in _schema_command_nodes(schema_data):
        if item.get("kind") == "group" and item.get("name") == group_name:
            return item
    return _raise_unknown_schema_group(group_name)


def _find_schema_command(schema_data: JsonObject, command_action: str) -> JsonObject:
    for item in _schema_command_nodes(schema_data):
        matched = _match_schema_command_node(item, command_action)
        if matched is not None:
            return matched
    return _raise_unknown_schema_action(command_action)


def _raise_unknown_schema_group(
    group_name: str, *, env_file: str | None = None
) -> NoReturn:
    available = list(COMMAND_GROUPS)
    candidates = [
        {
            "name": name,
            "schema_command": render_discovery_command(
                "schema", values={"group": name}, env_file=env_file
            ),
        }
        for name in get_close_matches(group_name, available, n=3, cutoff=0.5)
    ]
    details: JsonObject = {
        "requested": group_name,
        "available_count": len(available),
        "candidates": candidates,
        "discovery_command": render_discovery_command(
            "schema", values={"list-groups": True}, env_file=env_file
        ),
    }
    if candidates:
        suggestion = (
            f"Retry with `{candidates[0]['schema_command']}`, or browse "
            f"`{details['discovery_command']}`."
        )
    else:
        suggestion = f"Run `{details['discovery_command']}` to choose a group name."
    message = f"Unknown schema group: {group_name}"
    raise UserInputError(message, details=details, suggestion=suggestion)


def _raise_unknown_schema_action(
    command_action: str, *, env_file: str | None = None
) -> NoReturn:
    available = sorted(stable_leaf_actions())
    candidates: list[JsonObject] = []
    for action in get_close_matches(command_action, available, n=3, cutoff=0.5):
        group = action.partition(".")[0] if "." in action else None
        candidates.append(
            {
                "action": action,
                "group": group,
                "schema_command": render_discovery_command(
                    "schema", values={"command": action}, env_file=env_file
                ),
            }
        )
    details: JsonObject = {
        "requested": command_action,
        "available_count": len(available),
        "candidates": candidates,
        "discovery_command": render_discovery_command("schema", env_file=env_file),
    }
    if candidates:
        suggestion = (
            f"Retry with `{candidates[0]['schema_command']}`, or browse "
            f"`{details['discovery_command']}`."
        )
    else:
        suggestion = (
            f"Run `{details['discovery_command']}` to browse the bounded action index."
        )
    message = f"Unknown schema action: {command_action}"
    raise UserInputError(message, details=details, suggestion=suggestion)


def _match_schema_command_node(
    node: JsonObject,
    command_action: str,
) -> JsonObject | None:
    if node.get("kind") == "command" and node.get("action") == command_action:
        return node
    if node.get("kind") != "group":
        return None
    group_action = node.get("group_action")
    if isinstance(group_action, dict) and group_action.get("action") == command_action:
        return _schema_group_with_single_action(
            node,
            group_action=require_json_object(
                group_action,
                label="schema group action",
            ),
        )
    for child in _schema_group_commands(node):
        matched_child = _match_schema_command_node(child, command_action)
        if matched_child is not None:
            return _schema_group_with_single_action(node, command=matched_child)
    return None


def _schema_group_with_single_action(
    group_data: JsonObject,
    *,
    command: JsonObject | None = None,
    group_action: JsonObject | None = None,
) -> JsonObject:
    scoped = dict(group_data)
    scoped["commands"] = [] if command is None else [command]
    if group_action is None:
        scoped.pop("group_action", None)
    else:
        scoped["group_action"] = group_action
    return scoped


def _schema_command_nodes(schema_data: JsonObject) -> list[JsonObject]:
    commands = schema_data.get("commands")
    if not isinstance(commands, list):
        message = "schema data is missing commands"
        raise TypeError(message)
    return [require_json_object(item, label="schema command") for item in commands]


def _schema_group_commands(group_data: JsonObject) -> list[JsonObject]:
    commands = group_data.get("commands")
    if not isinstance(commands, list):
        return []
    return [
        require_json_object(item, label="schema group command") for item in commands
    ]


def _annotate_command_data_shapes(
    commands: list[JsonObject],
    *,
    ds_version: str,
    env_file: str | None = None,
) -> list[JsonObject]:
    return [
        _annotate_command_node_data_shape(
            command_node, ds_version=ds_version, env_file=env_file
        )
        for command_node in commands
    ]


def _annotate_command_node_data_shape(
    command_node: JsonObject,
    *,
    ds_version: str,
    env_file: str | None = None,
) -> JsonObject:
    annotated = dict(command_node)
    action = annotated.get("action")
    if isinstance(action, str):
        annotated = project_command_for_version(
            annotated,
            action=action,
            ds_version=ds_version,
            env_file=env_file,
        )
        _target_schema_input_references(annotated, env_file=env_file)
        _annotate_action_contract(
            annotated,
            action=action,
            label="schema command",
            ds_version=ds_version,
        )
    group_action = annotated.get("group_action")
    if isinstance(group_action, dict):
        group_action_data = require_json_object(
            group_action,
            label="schema group action",
        )
        group_action_name = group_action_data.get("action")
        if isinstance(group_action_name, str):
            group_action_copy = dict(group_action_data)
            _annotate_action_contract(
                group_action_copy,
                action=group_action_name,
                label="schema group action",
                ds_version=ds_version,
            )
            annotated["group_action"] = group_action_copy
    commands_value = annotated.get("commands")
    if isinstance(commands_value, list):
        annotated["commands"] = [
            _annotate_command_node_data_shape(
                require_json_object(item, label="schema nested command"),
                ds_version=ds_version,
                env_file=env_file,
            )
            for item in commands_value
        ]
    return annotated


def _annotate_action_contract(
    contract: JsonObject,
    *,
    action: str,
    label: str,
    ds_version: str,
) -> None:
    """Attach shared invocation, constraint, and output-shape metadata."""
    contract["invocation"] = _schema_invocation(action, contract)
    constraints = constraints_for_action(action, ds_version=ds_version)
    if constraints:
        contract["constraints"] = require_json_value(
            constraints,
            label=f"{label} constraints",
        )
    shape = data_shape_schema_for_action(action)
    if shape is not None:
        contract["data_shape"] = require_json_object(
            project_data_shape_for_version(
                require_json_object(shape, label=f"{label} data shape"),
                action=action,
                ds_version=ds_version,
            ),
            label=f"{label} data shape",
        )
    view_shapes = data_shapes_by_view_schema_for_action(action)
    if view_shapes:
        contract["data_shapes_by_view"] = require_json_object(
            view_shapes,
            label=f"{label} view data shapes",
        )


def _top_level_command_schema(name: str) -> JsonObject:
    return require_json_object(
        _command(name),
        label="top-level command schema",
    )


def _available_schema_command_actions(schema_data: JsonObject) -> list[str]:
    actions: list[str] = []
    for item in _schema_command_nodes(schema_data):
        actions.extend(_schema_command_actions(item))
    return actions


def _schema_command_actions(node: JsonObject) -> list[str]:
    actions: list[str] = []
    action = node.get("action")
    if isinstance(action, str):
        actions.append(action)
    group_action = node.get("group_action")
    if isinstance(group_action, dict):
        group_action_name = group_action.get("action")
        if isinstance(group_action_name, str):
            actions.append(group_action_name)
    for child in _schema_group_commands(node):
        actions.extend(_schema_command_actions(child))
    return actions


def _unresolved_schema_group(group_name: str) -> JsonObject:
    """Use only canonical parser facts; do not select task or payload models."""
    if group_name not in COMMAND_GROUPS:
        _raise_unknown_schema_group(group_name)
    return catalog_group(
        group_name,
        summary=GROUP_SUMMARIES[group_name],
        command_overrides={},
        nested_group_summaries={
            path: summary
            for path, summary in NESTED_GROUP_SUMMARIES.items()
            if path[0] == group_name
        },
    )


def _target_schema_input_references(
    command: JsonObject, *, env_file: str | None
) -> None:
    for collection in ("arguments", "options"):
        inputs = command.get(collection)
        if not isinstance(inputs, list):
            continue
        for item in inputs:
            if not isinstance(item, dict):
                continue
            reference = item.get("discovery_command")
            if isinstance(reference, str):
                item["discovery_command"] = render_discovery_reference(
                    reference, env_file=env_file
                )


def _unresolved_schema_command(
    action: str,
    reads: frozenset[str],
    resolution: CompatibilityResolution | None,
    *,
    env_file: str | None = None,
) -> JsonObject:
    if action not in stable_leaf_actions():
        _raise_unknown_schema_action(action)
    command = _command(action)
    command["invocation"] = _schema_invocation(action, command)
    command["contract_scope"] = "installed_cli_invocation"
    command["version_specific_constraints"] = "unknown"
    command["capability"] = unresolved_action_capability(action, reads, resolution)
    _target_schema_input_references(command, env_file=env_file)
    return command


def _unresolved_schema_action_rows(
    group_name: str | None,
    reads: frozenset[str],
    resolution: CompatibilityResolution | None,
    *,
    env_file: str | None = None,
) -> list[JsonObject]:
    contracts = (
        contract
        for contract in COMMAND_CATALOG.commands
        if group_name is None or contract.route[0] == group_name
    )
    return [
        {
            "action": contract.action,
            "group": contract.route[0] if len(contract.route) > 1 else None,
            "name": contract.name,
            "summary": contract.summary,
            **unresolved_action_capability(contract.action, reads, resolution),
            "schema_command": render_discovery_command(
                "schema", values={"command": contract.action}, env_file=env_file
            ),
            "help_command": _help_command_for_action(contract.action),
        }
        for contract in contracts
    ]


def _discover_unresolved_schema(
    query: SchemaQuery,
    resolution: CompatibilityResolution | None,
    *,
    env_file: str | None = None,
) -> CommandResult:
    reads = compatible_read_actions(resolution)
    scope = query.scope
    expanded = isinstance(scope, SchemaFullScope)
    if isinstance(scope, SchemaFullScope):
        scope = scope.scope
    data: JsonObject = {
        "schema_version": SCHEMA_VERSION,
        "cli": _schema_cli_data(),
        "ds": unresolved_ds_data(resolution),
        "global_options": _bounded_global_options(),
        "action_inventory_scope": "installed_cli_surface",
    }
    if isinstance(scope, SchemaActionScope):
        command = _unresolved_schema_command(
            scope.action, reads, resolution, env_file=env_file
        )
        data.update(
            {
                "view": "full" if expanded else "command",
                "capability": unresolved_action_capability(
                    scope.action, reads, resolution
                ),
                "links": [
                    {"rel": "help", "command": _help_command_for_action(scope.action)}
                ],
            }
        )
        if expanded:
            data["commands"] = [command]
            data["rows"] = _schema_command_detail_rows(command, action=scope.action)
            resolved: JsonObject = {
                "view": "full",
                "scope": "command",
                "command": scope.action,
            }
        else:
            data["command"] = command
            resolved = {"view": "command", "command": scope.action}
        return CommandResult(data=data, resolved={"schema": resolved})
    if isinstance(scope, SchemaGroupScope):
        group = _unresolved_schema_group(scope.name)
        actions = _unresolved_schema_action_rows(
            scope.name, reads, resolution, env_file=env_file
        )
        data.update(
            {
                "view": "full" if expanded else "group",
                "group": {
                    "name": scope.name,
                    "summary": GROUP_SUMMARIES[scope.name],
                    "action_count": len(actions),
                    "available_action_count": sum(
                        row["availability"]
                        not in ("requires_exact_version", "requires_discovery")
                        for row in actions
                    ),
                },
            }
        )
        if expanded:
            data["commands"] = [
                _annotate_unresolved_group(group, reads, resolution, env_file=env_file)
            ]
            data["rows"] = actions
            resolved = {"view": "full", "scope": "group", "group": scope.name}
        else:
            data["actions"] = actions
            resolved = {"view": "group", "group": scope.name}
        return CommandResult(data=data, resolved={"schema": resolved})
    groups: list[JsonObject] = []
    for name in COMMAND_GROUPS:
        actions = _unresolved_schema_action_rows(
            name, reads, resolution, env_file=env_file
        )
        groups.append(
            {
                "name": name,
                "summary": GROUP_SUMMARIES[name],
                "action_count": len(actions),
                "available_action_count": sum(
                    row["availability"]
                    not in ("requires_exact_version", "requires_discovery")
                    for row in actions
                ),
                "actions": [row["action"] for row in actions],
                "schema_command": render_discovery_command(
                    "schema", values={"group": name}, env_file=env_file
                ),
                "help_command": f"dsctl {name} --help",
            }
        )
    if isinstance(scope, SchemaGroupsScope):
        return CommandResult(
            data=groups,
            resolved={
                "schema": {"view": "groups", "ds": unresolved_ds_data(resolution)}
            },
        )
    if isinstance(scope, SchemaActionsScope):
        return CommandResult(
            data=_unresolved_schema_action_rows(
                None, reads, resolution, env_file=env_file
            ),
            resolved={
                "schema": {"view": "commands", "ds": unresolved_ds_data(resolution)}
            },
        )
    data.update(
        {
            "view": "full" if expanded else "index",
            "action_count": len(stable_leaf_actions()),
        }
    )
    if expanded:
        data["commands"] = [
            *(
                _unresolved_schema_command(action, reads, resolution, env_file=env_file)
                for action in TOP_LEVEL_COMMANDS
            ),
            *(
                _annotate_unresolved_group(
                    _unresolved_schema_group(name),
                    reads,
                    resolution,
                    env_file=env_file,
                )
                for name in COMMAND_GROUPS
            ),
        ]
        data["capabilities"] = {
            "ds": unresolved_ds_data(resolution),
            "read_compatible_actions": sorted(reads),
            "authoring": {"requires_exact_version": True},
        }
    else:
        data["groups"] = groups
        data["root_actions"] = [
            row
            for row in _unresolved_schema_action_rows(
                None, reads, resolution, env_file=env_file
            )
            if row["action"] in TOP_LEVEL_COMMANDS
        ]
    return CommandResult(
        data=data, resolved={"schema": {"view": "full" if expanded else "index"}}
    )


def _annotate_unresolved_group(
    group: JsonObject,
    reads: frozenset[str],
    resolution: CompatibilityResolution | None,
    *,
    env_file: str | None = None,
) -> JsonObject:
    result = dict(group)
    action = group.get("action")
    if isinstance(action, str):
        return _unresolved_schema_command(action, reads, resolution, env_file=env_file)
    for field in ("group_action",):
        child = group.get(field)
        if isinstance(child, dict):
            result[field] = _annotate_unresolved_group(
                require_json_object(child, label="catalog group action"),
                reads,
                resolution,
                env_file=env_file,
            )
    children = group.get("commands")
    if isinstance(children, list):
        result["commands"] = [
            _annotate_unresolved_group(
                require_json_object(child, label="catalog command"),
                reads,
                resolution,
                env_file=env_file,
            )
            for child in children
        ]
    return result
