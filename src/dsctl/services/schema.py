from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from difflib import get_close_matches
from typing import TYPE_CHECKING, NoReturn, TypeAlias

from dsctl import __version__
from dsctl.cli_surface import (
    ACCESS_TOKEN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    AUDIT_RESOURCE,
    CLUSTER_RESOURCE,
    COMMAND_GROUPS,
    DATASOURCE_RESOURCE,
    ENUM_RESOURCE,
    ENV_RESOURCE,
    LINT_RESOURCE,
    MONITOR_RESOURCE,
    NAMESPACE_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_PREFERENCE_RESOURCE,
    PROJECT_RESOURCE,
    PROJECT_WORKER_GROUP_RESOURCE,
    QUEUE_RESOURCE,
    RESOURCE_RESOURCE,
    SCHEDULE_RESOURCE,
    TASK_GROUP_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    TASK_RESOURCE,
    TASK_TYPE_RESOURCE,
    TEMPLATE_RESOURCE,
    TENANT_RESOURCE,
    TOP_LEVEL_COMMANDS,
    USE_RESOURCE,
    USER_RESOURCE,
    WORKER_GROUP_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    WORKFLOW_RESOURCE,
    stable_leaf_actions,
)
from dsctl.config import load_selected_ds_version
from dsctl.data_shapes import (
    data_shape_schema_for_action,
    data_shapes_by_view_schema_for_action,
)
from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.schema_contract_rows import command_contract_rows
from dsctl.services._schema_constraints import constraints_for_action
from dsctl.services._schema_groups_context import (
    project_group as _project_group,
)
from dsctl.services._schema_groups_context import (
    project_parameter_group as _project_parameter_group,
)
from dsctl.services._schema_groups_context import (
    project_preference_group as _project_preference_group,
)
from dsctl.services._schema_groups_context import (
    project_worker_group_group as _project_worker_group_group,
)
from dsctl.services._schema_groups_context import use_group as _use_group
from dsctl.services._schema_groups_design import schedule_group as _schedule_group
from dsctl.services._schema_groups_design import task_group as _task_group
from dsctl.services._schema_groups_design import (
    template_group as _template_group,
)
from dsctl.services._schema_groups_design import (
    workflow_group as _workflow_group,
)
from dsctl.services._schema_groups_governance import (
    access_token_group as _access_token_group,
)
from dsctl.services._schema_groups_governance import (
    alert_group_group as _alert_group_group,
)
from dsctl.services._schema_groups_governance import (
    alert_plugin_group as _alert_plugin_group,
)
from dsctl.services._schema_groups_governance import cluster_group as _cluster_group
from dsctl.services._schema_groups_governance import (
    datasource_group as _datasource_group,
)
from dsctl.services._schema_groups_governance import env_group as _env_group
from dsctl.services._schema_groups_governance import (
    namespace_group as _namespace_group,
)
from dsctl.services._schema_groups_governance import queue_group as _queue_group
from dsctl.services._schema_groups_governance import resource_group as _resource_group
from dsctl.services._schema_groups_governance import (
    task_group_group as _task_group_group,
)
from dsctl.services._schema_groups_governance import tenant_group as _tenant_group
from dsctl.services._schema_groups_governance import user_group as _user_group
from dsctl.services._schema_groups_governance import (
    worker_group_group as _worker_group_group,
)
from dsctl.services._schema_groups_meta import enum_group as _enum_group
from dsctl.services._schema_groups_meta import lint_group as _lint_group
from dsctl.services._schema_groups_meta import task_type_group as _task_type_group
from dsctl.services._schema_groups_runtime import (
    audit_group as _audit_group,
)
from dsctl.services._schema_groups_runtime import (
    monitor_group as _monitor_group,
)
from dsctl.services._schema_groups_runtime import (
    task_instance_group as _task_instance_group,
)
from dsctl.services._schema_groups_runtime import (
    workflow_instance_group as _workflow_instance_group,
)
from dsctl.services._schema_primitives import command as _command
from dsctl.services._schema_primitives import option as _option
from dsctl.services._surface_metadata import (
    TOP_LEVEL_COMMAND_SUMMARIES,
    confirmation_schema_data,
    error_schema_data,
    output_schema_data,
    selection_schema_data,
)
from dsctl.services.capabilities import (
    CAPABILITIES_SECTION_CHOICES,
    schema_capabilities_data,
)
from dsctl.services.template import supported_task_template_types
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_version_support,
    supported_version_metadata,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject

SchemaGroupBuilder = Callable[[list[str]], dict[str, object]]
SCOPED_SCHEMA_HEADER_KEYS = (
    "schema_version",
    "view",
    "cli",
    "supported_ds_versions",
    "ds_versions",
    "global_options",
    "selection",
    "output",
    "errors",
    "confirmation",
)
SCHEMA_VERSION = 2


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
    selected_ds_version = load_selected_ds_version(env_file)
    return _discover_schema(query, ds_version=selected_ds_version)


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


def _discover_schema(query: SchemaQuery, *, ds_version: str) -> CommandResult:
    """Build only the representation required by one validated query."""
    scope = query.scope
    if isinstance(scope, SchemaFullScope):
        return _full_schema_result(scope.scope, ds_version=ds_version)
    if isinstance(scope, SchemaIndexScope):
        return CommandResult(
            data=_schema_index_data(ds_version=ds_version),
            resolved={"schema": {"view": "index"}},
        )
    if isinstance(scope, SchemaGroupScope):
        data = _schema_group_index_data(scope.name, ds_version=ds_version)
        return CommandResult(
            data=data,
            resolved={"schema": {"view": "group", "group": scope.name}},
        )
    if isinstance(scope, SchemaActionScope):
        data = _schema_action_data(scope.action, ds_version=ds_version)
        return CommandResult(
            data=data,
            resolved={"schema": {"view": "command", "command": scope.action}},
        )

    discovery_data = _schema_discovery_source()
    if isinstance(scope, SchemaGroupsScope):
        return CommandResult(
            data=_schema_group_discovery_rows(discovery_data),
            resolved={"schema": {"view": "groups"}},
        )
    return CommandResult(
        data=_schema_action_discovery_rows(),
        resolved={"schema": {"view": "commands"}},
    )


def _full_schema_result(
    scope: SchemaExpandableScope,
    *,
    ds_version: str,
) -> CommandResult:
    """Return the expanded representation retained behind explicit --full."""
    data = require_json_object(
        _schema_data(ds_version=ds_version),
        label="schema data",
    )
    resolved: JsonObject = {"view": "full"}
    if isinstance(scope, SchemaGroupScope):
        data = _schema_group_data(data, scope.name)
        resolved["scope"] = "group"
        resolved["group"] = scope.name
    elif isinstance(scope, SchemaActionScope):
        data = _schema_command_data(data, scope.action)
        resolved["scope"] = "command"
        resolved["command"] = scope.action
    return CommandResult(data=data, resolved={"schema": resolved})


def _schema_index_data(*, ds_version: str) -> JsonObject:
    """Return a bounded index containing names, not expanded contracts."""
    groups: list[JsonObject] = []
    grouped_action_count = 0
    for group_name in COMMAND_GROUPS:
        group = _build_schema_group(group_name)
        actions = _schema_group_action_index(group, group_name=group_name)
        grouped_action_count += len(actions)
        groups.append(
            {
                "name": group_name,
                "summary": str(group.get("summary", "")),
                "action_count": len(actions),
                "actions": [str(item["action"]) for item in actions],
                "schema_command": f"dsctl schema --group {group_name}",
                "help_command": f"dsctl {group_name} --help",
            }
        )

    root_actions: list[JsonObject] = [
        {
            "action": action,
            "summary": TOP_LEVEL_COMMAND_SUMMARIES[action],
            "schema_command": f"dsctl schema --command {action}",
            "help_command": f"dsctl {action} --help",
        }
        for action in TOP_LEVEL_COMMANDS
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "view": "index",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
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
                "command": "dsctl capabilities --summary",
            },
        ],
    }


def _schema_group_index_data(group_name: str, *, ds_version: str) -> JsonObject:
    """Return one group's bounded action index."""
    group = _build_schema_group_or_error(group_name)
    actions = _schema_group_action_index(group, group_name=group_name)
    return {
        "schema_version": SCHEMA_VERSION,
        "view": "group",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
        "group": {
            "name": group_name,
            "summary": str(group.get("summary", "")),
            "action_count": len(actions),
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


def _schema_action_data(command_action: str, *, ds_version: str) -> JsonObject:
    """Return one complete command contract without shared global repetition."""
    command, group = _build_schema_action_or_error(command_action)
    command = _annotate_command_node_data_shape(command)
    data: JsonObject = {
        "schema_version": SCHEMA_VERSION,
        "view": "command",
        "cli": _schema_cli_data(),
        "ds": _schema_ds_data(ds_version),
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
            "schema_command": f"dsctl schema --group {group_name}",
            "help_command": f"dsctl {group_name} --help",
        }
        links = data["links"]
        if isinstance(links, list):
            links.append(
                {
                    "rel": "group_schema",
                    "command": f"dsctl schema --group {group_name}",
                }
            )
    return data


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
        {
            "flag": "--env-file",
            "value_name": "PATH",
            "placement": "before_command",
        },
        {
            "flag": "--output-format",
            "value_name": "FORMAT",
            "choices": ["json", "table", "tsv"],
            "default": "json",
            "placement": "before_command",
        },
        {
            "flag": "--columns",
            "value_name": "CSV",
            "placement": "before_command",
        },
        {
            "flag": "--compact",
            "type": "boolean",
            "default": False,
            "placement": "before_command",
            "requires": {"--output-format": "json"},
        },
    ]


def _build_schema_group(group_name: str) -> JsonObject:
    builder = _schema_group_builders()[group_name]
    task_types = (
        list(supported_task_template_types()) if group_name == TEMPLATE_RESOURCE else []
    )
    return require_json_object(builder(task_types), label="schema command group")


def _build_schema_group_or_error(group_name: str) -> JsonObject:
    if group_name in _schema_group_builders():
        return _build_schema_group(group_name)
    return _raise_unknown_schema_group(group_name)


def _build_schema_action_or_error(
    command_action: str,
) -> tuple[JsonObject, JsonObject | None]:
    if command_action in TOP_LEVEL_COMMANDS:
        return _top_level_command_schema(command_action), None

    group_name, separator, _ = command_action.partition(".")
    if separator and group_name in _schema_group_builders():
        group = _build_schema_group(group_name)
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
) -> list[JsonObject]:
    rows = _schema_command_discovery_rows_from_node(group, group_name=group_name)
    return [
        {
            "action": str(row["action"]),
            "name": str(row["name"]),
            "summary": str(row["summary"]),
            "schema_command": str(row["schema_command"]),
            "help_command": _help_command_for_action(str(row["action"])),
        }
        for row in rows
    ]


def _schema_discovery_source() -> JsonObject:
    return {
        "commands": [_top_level_command_schema(name) for name in TOP_LEVEL_COMMANDS]
        + [_build_schema_group(name) for name in COMMAND_GROUPS]
    }


def _schema_action_discovery_rows() -> list[JsonObject]:
    source = _schema_discovery_source()
    rows: list[JsonObject] = []
    for item in _schema_command_nodes(source):
        rows.extend(_schema_command_discovery_rows_from_node(item, group_name=None))
    return rows


def _help_command_for_action(action: str) -> str:
    if action == "use.clear":
        return "dsctl use --help"
    return f"{_action_command(action)} --help"


def _schema_invocation(action: str, command: JsonObject) -> str:
    """Return one exact CLI path plus argument/option placeholders."""
    base = "dsctl use --clear" if action == "use.clear" else _action_command(action)
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
    return f"dsctl {action.replace('.', ' ')}"


def _schema_data(*, ds_version: str) -> dict[str, object]:
    task_types = list(supported_task_template_types())
    command_groups = _command_groups(task_types)
    commands = [
        require_json_object(command_data, label="schema command data")
        for command_data in (
            *(_top_level_command_schema(name) for name in TOP_LEVEL_COMMANDS),
            *(command_groups[name] for name in COMMAND_GROUPS),
        )
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "view": "full",
        "cli": _schema_cli_data(),
        "supported_ds_versions": list(SUPPORTED_VERSIONS),
        "ds_versions": list(supported_version_metadata()),
        "global_options": [
            _option(
                "env-file",
                value_type="path",
                description=(
                    "Load DS_* settings from an env file before reading the process "
                    "environment."
                ),
                value_name="PATH",
                placement="before_command",
            ),
            _option(
                "output-format",
                value_type="string",
                description=(
                    "Render output as json, table, or tsv. json keeps the full "
                    "standard envelope unless --columns is used for explicit "
                    "data projection."
                ),
                default="json",
                choices=["json", "table", "tsv"],
                value_name="FORMAT",
                placement="before_command",
            ),
            _option(
                "columns",
                value_type="string",
                description=(
                    "Comma-separated row/object fields to render or project. "
                    "In json mode this narrows the standard envelope data payload."
                ),
                value_name="CSV",
                placement="before_command",
            ),
            _option(
                "compact",
                value_type="boolean",
                description=(
                    "Emit the standard JSON envelope without indentation. "
                    "Valid only with --output-format json."
                ),
                default=False,
                placement="before_command",
            ),
        ],
        "selection": selection_schema_data(),
        "output": output_schema_data(),
        "errors": error_schema_data(),
        "confirmation": confirmation_schema_data(),
        "capabilities": schema_capabilities_data(ds_version=ds_version),
        "commands": _annotate_command_data_shapes(commands),
    }


def _command_groups(task_types: list[str]) -> dict[str, dict[str, object]]:
    return {
        name: builder(task_types) for name, builder in _schema_group_builders().items()
    }


def _schema_group_data(schema_data: JsonObject, group_name: str) -> JsonObject:
    group = _find_schema_group(schema_data, group_name)
    scoped = _schema_header(schema_data)
    scoped["commands"] = [group]
    scoped["rows"] = _schema_group_summary_rows(group)
    return scoped


def _schema_command_data(schema_data: JsonObject, command_action: str) -> JsonObject:
    command = _find_schema_command(schema_data, command_action)
    scoped = _schema_header(schema_data)
    scoped["commands"] = [command]
    scoped["rows"] = _schema_command_detail_rows(command, action=command_action)
    return scoped


def _schema_header(schema_data: JsonObject) -> JsonObject:
    return {key: schema_data[key] for key in SCOPED_SCHEMA_HEADER_KEYS}


def _schema_group_discovery_rows(schema_data: JsonObject) -> list[JsonObject]:
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
                "schema_command": f"dsctl schema --group {name}",
            }
        )
    return rows


def _schema_command_discovery_rows_from_node(
    node: JsonObject,
    *,
    group_name: str | None,
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
                "schema_command": f"dsctl schema --command {action}",
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
                    "schema_command": f"dsctl schema --command {action}",
                }
            )
    for child in _schema_group_commands(node):
        rows.extend(
            _schema_command_discovery_rows_from_node(
                child,
                group_name=current_group_name,
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


def _raise_unknown_schema_group(group_name: str) -> NoReturn:
    available = list(COMMAND_GROUPS)
    candidates = [
        {
            "name": name,
            "schema_command": f"dsctl schema --group {name}",
        }
        for name in get_close_matches(group_name, available, n=3, cutoff=0.5)
    ]
    details: JsonObject = {
        "requested": group_name,
        "available_count": len(available),
        "candidates": candidates,
        "discovery_command": "dsctl schema --list-groups",
    }
    if candidates:
        suggestion = (
            f"Retry with `{candidates[0]['schema_command']}`, or browse "
            "`dsctl schema --list-groups`."
        )
    else:
        suggestion = "Run `dsctl schema --list-groups` to choose a group name."
    message = f"Unknown schema group: {group_name}"
    raise UserInputError(message, details=details, suggestion=suggestion)


def _raise_unknown_schema_action(command_action: str) -> NoReturn:
    available = sorted(stable_leaf_actions())
    candidates: list[JsonObject] = []
    for action in get_close_matches(command_action, available, n=3, cutoff=0.5):
        group = action.partition(".")[0] if "." in action else None
        candidates.append(
            {
                "action": action,
                "group": group,
                "schema_command": f"dsctl schema --command {action}",
            }
        )
    details: JsonObject = {
        "requested": command_action,
        "available_count": len(available),
        "candidates": candidates,
        "discovery_command": "dsctl schema",
    }
    if candidates:
        suggestion = (
            f"Retry with `{candidates[0]['schema_command']}`, or browse `dsctl schema`."
        )
    else:
        suggestion = "Run `dsctl schema` to browse the bounded action index."
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
) -> list[JsonObject]:
    return [
        _annotate_command_node_data_shape(command_node) for command_node in commands
    ]


def _annotate_command_node_data_shape(command_node: JsonObject) -> JsonObject:
    annotated = dict(command_node)
    action = annotated.get("action")
    if isinstance(action, str):
        _annotate_action_contract(annotated, action=action, label="schema command")
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
            )
            annotated["group_action"] = group_action_copy
    commands_value = annotated.get("commands")
    if isinstance(commands_value, list):
        annotated["commands"] = [
            _annotate_command_node_data_shape(
                require_json_object(item, label="schema nested command")
            )
            for item in commands_value
        ]
    return annotated


def _annotate_action_contract(
    contract: JsonObject,
    *,
    action: str,
    label: str,
) -> None:
    """Attach shared invocation, constraint, and output-shape metadata."""
    contract["invocation"] = _schema_invocation(action, contract)
    constraints = constraints_for_action(action)
    if constraints:
        contract["constraints"] = require_json_value(
            constraints,
            label=f"{label} constraints",
        )
    shape = data_shape_schema_for_action(action)
    if shape is not None:
        contract["data_shape"] = require_json_object(
            shape,
            label=f"{label} data shape",
        )
    view_shapes = data_shapes_by_view_schema_for_action(action)
    if view_shapes:
        contract["data_shapes_by_view"] = require_json_object(
            view_shapes,
            label=f"{label} view data shapes",
        )


def _top_level_command_schema(name: str) -> JsonObject:
    if name == "schema":
        return require_json_object(
            _command(
                name,
                action=name,
                summary=TOP_LEVEL_COMMAND_SUMMARIES[name],
                options=[
                    _option(
                        "group",
                        value_type="string",
                        description=(
                            "Return one group's action index. Discover groups "
                            "with `dsctl schema` or "
                            "`dsctl schema --list-groups`."
                        ),
                        discovery_command="dsctl schema --list-groups",
                    ),
                    _option(
                        "command",
                        value_type="string",
                        description=(
                            "Return one complete action-local contract. Discover "
                            "actions with `dsctl schema` or "
                            "`dsctl schema --group GROUP`."
                        ),
                        discovery_command="dsctl schema",
                    ),
                    _option(
                        "list-groups",
                        value_type="boolean",
                        description="List valid values for --group.",
                        default=False,
                    ),
                    _option(
                        "list-commands",
                        value_type="boolean",
                        description="List valid action names for --command.",
                        default=False,
                    ),
                    _option(
                        "full",
                        value_type="boolean",
                        description=(
                            "Return the expanded schema representation. May be "
                            "combined with --group or --command."
                        ),
                        default=False,
                    ),
                ],
            ),
            label="top-level command schema",
        )
    if name == "capabilities":
        return require_json_object(
            _command(
                name,
                action=name,
                summary=TOP_LEVEL_COMMAND_SUMMARIES[name],
                options=[
                    _option(
                        "summary",
                        value_type="boolean",
                        description="Return lightweight capability discovery.",
                        default=False,
                    ),
                    _option(
                        "section",
                        value_type="string",
                        description=(
                            "Return one top-level capability section. Supported: "
                            f"{', '.join(CAPABILITIES_SECTION_CHOICES)}. Discover "
                            "values with `dsctl schema --command capabilities`."
                        ),
                        choices=list(CAPABILITIES_SECTION_CHOICES),
                        discovery_command="dsctl schema --command capabilities",
                    ),
                ],
            ),
            label="top-level command schema",
        )
    return require_json_object(
        _command(
            name,
            action=name,
            summary=TOP_LEVEL_COMMAND_SUMMARIES[name],
        ),
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


def _static_group_builder(
    factory: Callable[[], dict[str, object]],
) -> SchemaGroupBuilder:
    def build(_task_types: list[str]) -> dict[str, object]:
        return factory()

    return build


def _schema_group_builders() -> dict[str, SchemaGroupBuilder]:
    return {
        USE_RESOURCE: _static_group_builder(_use_group),
        ENUM_RESOURCE: _static_group_builder(_enum_group),
        LINT_RESOURCE: _static_group_builder(_lint_group),
        TASK_TYPE_RESOURCE: _static_group_builder(_task_type_group),
        ENV_RESOURCE: _static_group_builder(_env_group),
        CLUSTER_RESOURCE: _static_group_builder(_cluster_group),
        DATASOURCE_RESOURCE: _static_group_builder(_datasource_group),
        NAMESPACE_RESOURCE: _static_group_builder(_namespace_group),
        RESOURCE_RESOURCE: _static_group_builder(_resource_group),
        QUEUE_RESOURCE: _static_group_builder(_queue_group),
        WORKER_GROUP_RESOURCE: _static_group_builder(_worker_group_group),
        TASK_GROUP_RESOURCE: _static_group_builder(_task_group_group),
        ALERT_PLUGIN_RESOURCE: _static_group_builder(_alert_plugin_group),
        ALERT_GROUP_RESOURCE: _static_group_builder(_alert_group_group),
        TENANT_RESOURCE: _static_group_builder(_tenant_group),
        USER_RESOURCE: _static_group_builder(_user_group),
        ACCESS_TOKEN_RESOURCE: _static_group_builder(_access_token_group),
        MONITOR_RESOURCE: _static_group_builder(_monitor_group),
        AUDIT_RESOURCE: _static_group_builder(_audit_group),
        PROJECT_RESOURCE: _static_group_builder(_project_group),
        PROJECT_PARAMETER_RESOURCE: _static_group_builder(_project_parameter_group),
        PROJECT_PREFERENCE_RESOURCE: _static_group_builder(_project_preference_group),
        PROJECT_WORKER_GROUP_RESOURCE: _static_group_builder(
            _project_worker_group_group
        ),
        SCHEDULE_RESOURCE: _static_group_builder(_schedule_group),
        TEMPLATE_RESOURCE: _template_group,
        WORKFLOW_RESOURCE: _static_group_builder(_workflow_group),
        WORKFLOW_INSTANCE_RESOURCE: _static_group_builder(_workflow_instance_group),
        TASK_RESOURCE: _static_group_builder(_task_group),
        TASK_INSTANCE_RESOURCE: _static_group_builder(_task_instance_group),
    }
