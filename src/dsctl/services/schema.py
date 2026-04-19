from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

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
)
from dsctl.config import load_selected_ds_version
from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._data_shapes import data_shape_schema_for_action
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
from dsctl.upstream import SUPPORTED_VERSIONS, supported_version_metadata

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject, JsonValue

SchemaGroupBuilder = Callable[[list[str]], dict[str, object]]
SCOPED_SCHEMA_HEADER_KEYS = (
    "schema_version",
    "cli",
    "supported_ds_versions",
    "ds_versions",
    "global_options",
    "selection",
    "output",
    "errors",
    "confirmation",
)


def get_schema_result(
    *,
    env_file: str | None = None,
    group: str | None = None,
    command_action: str | None = None,
    list_groups: bool = False,
    list_commands: bool = False,
) -> CommandResult:
    """Return the stable machine-readable CLI schema for the current surface."""
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
                "Pass only one schema scope option, or omit them for the full schema."
            ),
        )
    selected_ds_version = load_selected_ds_version(env_file)
    data = require_json_object(
        _schema_data(ds_version=selected_ds_version),
        label="schema data",
    )
    if group is not None:
        normalized_group = group.strip()
        scoped_data = _schema_group_data(data, normalized_group)
        return CommandResult(
            data=scoped_data,
            resolved={
                "schema": {
                    "view": "group",
                    "group": normalized_group,
                }
            },
        )
    if list_groups:
        return CommandResult(
            data=_schema_group_discovery_rows(data),
            resolved={
                "schema": {
                    "view": "groups",
                    "next": "dsctl schema --group GROUP",
                }
            },
        )
    if list_commands:
        return CommandResult(
            data=_schema_command_discovery_rows(data),
            resolved={
                "schema": {
                    "view": "commands",
                    "next": "dsctl schema --command ACTION",
                }
            },
        )
    if command_action is not None:
        normalized_action = command_action.strip()
        scoped_data = _schema_command_data(data, normalized_action)
        return CommandResult(
            data=scoped_data,
            resolved={
                "schema": {
                    "view": "command",
                    "command": normalized_action,
                }
            },
        )
    return CommandResult(data=data)


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
        "schema_version": 1,
        "cli": {
            "name": "dsctl",
            "version": __version__,
        },
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
            ),
            _option(
                "columns",
                value_type="string",
                description=(
                    "Comma-separated row/object fields to render or project. "
                    "In json mode this narrows the standard envelope data payload."
                ),
                value_name="CSV",
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
                "command_count": len(_schema_group_commands(item)),
                "schema_command": f"dsctl schema --group {name}",
            }
        )
    return rows


def _schema_command_discovery_rows(schema_data: JsonObject) -> list[JsonObject]:
    rows: list[JsonObject] = []
    for item in _schema_command_nodes(schema_data):
        rows.extend(_schema_command_discovery_rows_from_node(item, group_name=None))
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
    rows: list[JsonObject] = []
    group_action = group_data.get("group_action")
    if isinstance(group_action, Mapping):
        action_data = require_json_object(group_action, label="schema group action")
        action = action_data.get("action")
        if isinstance(action, str):
            rows.append(
                {
                    "kind": "group_action",
                    "action": action,
                    "name": str(group_data.get("name", "")),
                    "summary": str(action_data.get("summary", "")),
                    "schema_command": f"dsctl schema --command {action}",
                }
            )
    for command_node in _schema_group_commands(group_data):
        action = command_node.get("action")
        if not isinstance(action, str):
            continue
        rows.append(
            {
                "kind": "command",
                "action": action,
                "name": str(command_node.get("name", "")),
                "summary": str(command_node.get("summary", "")),
                "schema_command": f"dsctl schema --command {action}",
            }
        )
    return rows


def _schema_command_detail_rows(
    command_node: JsonObject,
    *,
    action: str,
) -> list[JsonObject]:
    command_data = _find_action_node(command_node, action)
    if command_data is None:
        return []
    rows: list[JsonObject] = [
        {
            "kind": "command",
            "name": action,
            "description": str(command_data.get("summary", "")),
        }
    ]
    rows.extend(
        _schema_parameter_row("argument", require_json_object(item, label="argument"))
        for item in _schema_command_items(command_data, "arguments")
    )
    rows.extend(
        _schema_parameter_row("option", require_json_object(item, label="option"))
        for item in _schema_command_items(command_data, "options")
    )
    payload = command_data.get("payload")
    if isinstance(payload, Mapping):
        rows.extend(
            _schema_payload_rows(
                require_json_object(payload, label="schema payload data")
            )
        )
    data_shape = command_data.get("data_shape")
    if isinstance(data_shape, Mapping):
        rows.extend(
            _schema_mapping_rows(
                "data_shape",
                require_json_object(data_shape, label="schema data shape"),
            )
        )
    return rows


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


def _schema_command_items(
    command_data: JsonObject,
    key: str,
) -> list[JsonObject]:
    value = command_data.get(key)
    if not isinstance(value, list):
        return []
    return [require_json_object(item, label=f"schema {key} item") for item in value]


def _schema_parameter_row(kind: str, item: JsonObject) -> JsonObject:
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
    value = _schema_parameter_value(item)
    if value:
        row["value"] = value
    discovery_command = item.get("discovery_command")
    if isinstance(discovery_command, str):
        row["discovery_command"] = discovery_command
    return row


def _schema_parameter_value(item: JsonObject) -> str:
    parts: list[str] = []
    selector = item.get("selector")
    if isinstance(selector, str):
        parts.append(f"selector={selector}")
    if "default" in item:
        parts.append(f"default={_compact_schema_value(item['default'])}")
    choices = item.get("choices")
    if isinstance(choices, list):
        parts.append(f"choices={_compact_schema_value(choices)}")
    if item.get("multiple") is True:
        parts.append("multiple=true")
    value_name = item.get("value_name")
    if isinstance(value_name, str):
        parts.append(f"value_name={value_name}")
    return "; ".join(parts)


def _schema_payload_rows(payload: JsonObject) -> list[JsonObject]:
    interesting_keys = (
        "format",
        "source_option",
        "template_discovery_command",
        "template_command",
        "template_command_pattern",
        "template_json_path",
        "template_payload_path",
        "type_discovery_command",
        "type_enum",
        "upstream_request_shape",
    )
    rows: list[JsonObject] = []
    for key in interesting_keys:
        if key not in payload:
            continue
        rows.append(
            {
                "kind": "payload",
                "name": key,
                "value": _compact_schema_value(payload[key]),
            }
        )
    return rows


def _schema_mapping_rows(
    kind: str,
    value: JsonObject,
) -> list[JsonObject]:
    return [
        {
            "kind": kind,
            "name": str(key),
            "value": _compact_schema_value(item),
        }
        for key, item in value.items()
    ]


def _compact_schema_value(value: JsonValue) -> str:
    if isinstance(value, list):
        scalar_values: list[str] = []
        for item in value:
            if isinstance(item, dict | list):
                return ", ".join(str(nested) for nested in value)
            scalar_values.append(_compact_schema_scalar(item))
        return ", ".join(scalar_values)
    if isinstance(value, str | int | float | bool) or value is None:
        return _compact_schema_scalar(value)
    return str(value)


def _compact_schema_scalar(value: JsonValue) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _find_schema_group(schema_data: JsonObject, group_name: str) -> JsonObject:
    for item in _schema_command_nodes(schema_data):
        if item.get("kind") == "group" and item.get("name") == group_name:
            return item
    available_groups = [
        str(item["name"])
        for item in _schema_command_nodes(schema_data)
        if item.get("kind") == "group" and isinstance(item.get("name"), str)
    ]
    message = f"Unknown schema group: {group_name}"
    raise UserInputError(
        message,
        details={"group": group_name, "available_groups": available_groups},
        suggestion="Run `dsctl schema --list-groups` to choose a group name.",
    )


def _find_schema_command(schema_data: JsonObject, command_action: str) -> JsonObject:
    for item in _schema_command_nodes(schema_data):
        matched = _match_schema_command_node(item, command_action)
        if matched is not None:
            return matched
    message = f"Unknown schema command: {command_action}"
    raise UserInputError(
        message,
        details={
            "command": command_action,
            "available_commands": _available_schema_command_actions(schema_data),
        },
        suggestion="Run `dsctl schema --list-commands` to choose a command action.",
    )


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
        shape = data_shape_schema_for_action(action)
        if shape is not None:
            annotated["data_shape"] = require_json_object(
                shape,
                label="schema data shape",
            )
    group_action = annotated.get("group_action")
    if isinstance(group_action, dict):
        group_action_data = require_json_object(
            group_action,
            label="schema group action",
        )
        group_action_name = group_action_data.get("action")
        if isinstance(group_action_name, str):
            shape = data_shape_schema_for_action(group_action_name)
            if shape is not None:
                group_action_copy = dict(group_action_data)
                group_action_copy["data_shape"] = require_json_object(
                    shape,
                    label="schema data shape",
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
                            "Return schema for one command group. Discover "
                            "values with `dsctl schema --list-groups`."
                        ),
                        discovery_command="dsctl schema --list-groups",
                    ),
                    _option(
                        "command",
                        value_type="string",
                        description=(
                            "Return schema for one stable command action. "
                            "Discover values with `dsctl schema --list-commands`."
                        ),
                        discovery_command="dsctl schema --list-commands",
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
                        description="List valid values for --command.",
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
                            "Return one top-level capability section. Discover "
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
