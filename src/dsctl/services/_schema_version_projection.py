from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.services._workflow.authoring_schema import workflow_authoring_schema_data
from dsctl.services.enums import supported_enum_choices
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import (
    supported_datasource_types,
    supported_task_template_types,
    supported_task_template_variants,
)
from dsctl.upstream.access_tokens import access_token_update_requires_expire_time
from dsctl.upstream.observability import supported_monitor_server_types
from dsctl.upstream.project_parameters import project_parameter_data_type_choices
from dsctl.upstream.runtime_instances import (
    runtime_instance_contract_features,
)
from dsctl.upstream.schedule_environment import (
    schedule_environment_inheritance_supported,
    schedule_environment_limitation,
    workflow_environment_inheritance_supported,
)
from dsctl.upstream.schedules import schedule_contract_features
from dsctl.upstream.task_definition_wire import task_update_contract_features
from dsctl.upstream.workflows import workflow_execution_schedule_time_shape

if TYPE_CHECKING:
    from dsctl.output import JsonObject, JsonValue


_SCHEDULE_OPTION_INTRODUCED_IN = {
    "timezone": "2.0.0",
    "environment-code": "2.0.0",
    "tenant-code": "3.2.0",
    "missed-fire-policy": "3.4.3",
}
_TASK_INSTANCE_OPTION_INTRODUCED_IN = {
    "workflow-instance-name": "2.0.0",
    "execute-type": "3.1.0",
    "task-code": "3.2.0",
}
_WORKFLOW_BACKFILL_DATE_INTRODUCED_IN = "3.1.0"

_LEGACY_AUDIT_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
    }
)
_LEGACY_AUDIT_MODEL_TYPES = ("USER_MODULE", "PROJECT_MODULE")
_LEGACY_AUDIT_OPERATION_TYPES = ("CREATE", "READ", "UPDATE", "DELETE")
_LEGACY_TASK_UPDATE_KEYS = (
    "command",
    "depends_on",
    "description",
    "flag",
    "priority",
    "retry.interval",
    "retry.times",
    "timeout",
    "timeout_notify_strategy",
    "worker_group",
)
_LEGACY_TASK_UPDATE_EXAMPLES = (
    "command=python v2.py",
    "retry.times=5",
    "timeout_notify_strategy=FAILED",
)
_TASK_UPDATE_WIRE_FIELD_BY_KEY = {
    "command": "taskParams",
    "cpu_quota": "cpuQuota",
    "delay": "delayTime",
    "description": "description",
    "environment_code": "environmentCode",
    "flag": "flag",
    "memory_max": "memoryMax",
    "priority": "taskPriority",
    "retry.interval": "failRetryInterval",
    "retry.times": "failRetryTimes",
    "task_group_id": "taskGroupId",
    "task_group_priority": "taskGroupPriority",
    "timeout": "timeout",
    "timeout_notify_strategy": "timeoutNotifyStrategy",
    "worker_group": "workerGroup",
}


def project_command_for_version(
    command: JsonObject,
    *,
    action: str,
    ds_version: str,
    env_file: str | None = None,
) -> JsonObject:
    """Return the usable selected-version command contract.

    Typer deliberately accepts the union of supported-version flags so it can
    return typed compatibility errors. Schema is a planning contract instead:
    it keeps only options that can succeed on the selected DS version and
    records exact upstream absences separately.
    """
    if action == "access-token.update":
        return _project_access_token_update(command, ds_version=ds_version)
    if action in {
        "project-parameter.create",
        "project-parameter.update",
        "project-parameter.list",
    }:
        return _project_project_parameter_command(command, ds_version=ds_version)
    if action == "workflow.create":
        return {
            **command,
            "payload": workflow_authoring_schema_data(ds_version, env_file=env_file),
        }
    local_projection = _project_local_metadata_command(
        command,
        action=action,
        ds_version=ds_version,
    )
    if local_projection is not None:
        return local_projection
    upstream_projection = _project_exact_upstream_command(
        command,
        action=action,
        ds_version=ds_version,
    )
    if upstream_projection is not None:
        return upstream_projection
    if not action.startswith("schedule."):
        return command
    return _project_schedule_command(command, ds_version=ds_version)


def _project_access_token_update(command: JsonObject, *, ds_version: str) -> JsonObject:
    options = command.get("options")
    if not isinstance(options, list) or not access_token_update_requires_expire_time(
        ds_version
    ):
        return command
    projected_options: list[JsonValue] = []
    for value in options:
        if isinstance(value, dict) and value.get("name") == "expire-time":
            option = dict(cast("JsonObject", value))
            option["required"] = True
            projected_options.append(option)
        else:
            projected_options.append(value)
    return {**command, "options": projected_options}


def _project_project_parameter_command(
    command: JsonObject, *, ds_version: str
) -> JsonObject:
    choices = project_parameter_data_type_choices(ds_version)
    if choices is None:
        return command
    return _with_input_choices(
        command, collection="options", name="data-type", choices=choices
    )


def _project_schedule_command(command: JsonObject, *, ds_version: str) -> JsonObject:
    """Advertise only native schedule options and defaults in the selected profile."""
    options = command.get("options")
    if not isinstance(options, list):
        return command

    features = schedule_contract_features(ds_version)
    supported = {
        "timezone": features.timezone,
        "environment-code": features.environment,
        "tenant-code": features.tenant,
        "missed-fire-policy": features.missed_fire_policy,
    }
    projected_options: list[JsonValue] = []
    unavailable: list[JsonObject] = []
    for value in options:
        if not isinstance(value, dict):
            projected_options.append(value)
            continue
        option = cast("JsonObject", value)
        name = option.get("name")
        if not isinstance(name, str) or supported.get(name, True):
            if name == "environment-code":
                inherits = schedule_environment_inheritance_supported(ds_version)
                option = {**option, "runtime_inheritance": inherits}
                if not inherits:
                    option = {
                        **option,
                        "runtime_limitation": schedule_environment_limitation(
                            ds_version
                        ),
                        "description": (
                            "This DS version does not pass a schedule environment to "
                            "default tasks. Positive selections are rejected; set "
                            "environment_code on each task that needs it (also applies "
                            "to manual runs). Use 0 for no schedule environment; omit "
                            "on update to preserve the existing value."
                        ),
                    }
            if name == "missed-fire-policy":
                option = {
                    **option,
                    "choices": list(features.missed_fire_policy_choices),
                    "upstream_default": features.missed_fire_policy_default,
                }
            projected_options.append(option)
            continue
        unavailable.append(
            {
                "flag": f"--{name}",
                "availability": "upstream_absent",
                "introduced_in": _SCHEDULE_OPTION_INTRODUCED_IN[name],
                "instruction": "omit",
            }
        )

    projected = dict(command)
    projected["options"] = projected_options
    if unavailable:
        projected["unavailable_options"] = unavailable
    else:
        projected.pop("unavailable_options", None)
    return projected


def _project_exact_upstream_command(
    command: JsonObject,
    *,
    action: str,
    ds_version: str,
) -> JsonObject | None:
    if action in {"workflow.run", "workflow.run-task", "workflow.backfill"}:
        projected = (
            _project_workflow_backfill(command, ds_version=ds_version)
            if action == "workflow.backfill"
            else command
        )
        return _project_execution_environment(projected, ds_version=ds_version)
    if action in {"alert-group.create", "alert-group.update"}:
        return _project_alert_group_mutation(
            command,
            action=action,
            ds_version=ds_version,
        )
    if action == "audit.list" and ds_version in _LEGACY_AUDIT_VERSIONS:
        return _project_legacy_audit_list(command)
    if action == "monitor.server":
        return _with_input_choices(
            command,
            collection="arguments",
            name="node_type",
            choices=supported_monitor_server_types(ds_version),
        )
    if action == "workflow-instance.edit":
        required = runtime_instance_contract_features(
            ds_version
        ).instance_dag_edit_requires_sync
        constraints: JsonObject = {"dag_changes_require_sync_definition": required}
        if required:
            constraints.update(
                {
                    "sync_definition_requires_online": True,
                    "applies_when": "--sync-definition with persistent changes",
                    "definition_metadata_sync_may_set_online": True,
                    "publication_condition": "native definition metadata differs",
                }
            )
        return {**command, "native_edit_constraints": constraints}
    if action == "task-instance.list":
        return _project_task_instance_list(command, ds_version=ds_version)
    if action == "task.update":
        projected = (
            _project_legacy_task_command(command, action=action)
            if ds_version == "1.3.9"
            else command
        )
        return _project_task_update_fields(projected, ds_version=ds_version)
    if ds_version == "1.3.9" and action in {"task.list", "task.get"}:
        return _project_legacy_task_command(command, action=action)
    return None


def _project_execution_environment(
    command: JsonObject, *, ds_version: str
) -> JsonObject:
    options = command.get("options")
    if not isinstance(options, list):
        return command
    if not schedule_contract_features(ds_version).environment:
        unavailable = list(
            cast("list[JsonObject]", command.get("unavailable_options", []))
        )
        unavailable.append(
            {
                "flag": "--environment-code",
                "availability": "upstream_absent",
                "introduced_in": "2.0.0",
                "instruction": "omit",
            }
        )
        return {
            **command,
            "options": [
                option
                for option in options
                if not isinstance(option, dict)
                or option.get("name") != "environment-code"
            ],
            "unavailable_options": unavailable,
        }
    inherits = workflow_environment_inheritance_supported(ds_version)
    projected: list[JsonValue] = []
    for value in options:
        if isinstance(value, dict) and value.get("name") == "environment-code":
            option: JsonObject = {**value, "runtime_inheritance": inherits}
            if not inherits:
                option["description"] = (
                    "This DS version does not pass a workflow environment to default "
                    "tasks. Positive selections are rejected; set environment_code "
                    "on each task that needs it (affects scheduled and manual runs). "
                    "Use 0 for no workflow environment."
                )
            projected.append(option)
        else:
            projected.append(value)
    return {**command, "options": projected}


def _project_workflow_backfill(
    command: JsonObject,
    *,
    ds_version: str,
) -> JsonObject:
    if workflow_execution_schedule_time_shape(ds_version) == "json":
        return command
    options = command.get("options")
    if not isinstance(options, list):
        return command
    projected = dict(command)
    projected["options"] = [
        option
        for option in options
        if not isinstance(option, dict) or option.get("name") != "date"
    ]
    unavailable = list(cast("list[JsonObject]", command.get("unavailable_options", [])))
    unavailable.append(
        {
            "flag": "--date",
            "availability": "upstream_absent",
            "introduced_in": _WORKFLOW_BACKFILL_DATE_INTRODUCED_IN,
            "instruction": "use --start and --end",
        }
    )
    projected["unavailable_options"] = unavailable
    return projected


def _project_alert_group_mutation(
    command: JsonObject,
    *,
    action: str,
    ds_version: str,
) -> JsonObject:
    options = command.get("options")
    if not isinstance(options, list):
        return command
    legacy = ds_version == "1.3.9"
    hidden = {"instance-id", "clear-instance-ids"} if legacy else {"group-type"}
    projected_options: list[JsonValue] = []
    unavailable: list[JsonObject] = []
    for value in options:
        if not isinstance(value, dict):
            projected_options.append(value)
            continue
        option = dict(cast("JsonObject", value))
        name = option.get("name")
        if isinstance(name, str) and name in hidden:
            unavailable.append(
                {
                    "flag": f"--{name}",
                    "availability": "upstream_absent",
                    "instruction": "omit",
                }
            )
            continue
        if legacy and action == "alert-group.create" and name == "group-type":
            option["required"] = True
        projected_options.append(option)
    projected = dict(command)
    projected["options"] = projected_options
    if unavailable:
        projected["unavailable_options"] = unavailable
    return projected


def _project_task_instance_list(
    command: JsonObject,
    *,
    ds_version: str,
) -> JsonObject:
    features = runtime_instance_contract_features(ds_version)
    supported = {
        "workflow-instance-name": features.task_workflow_instance_name,
        "execute-type": features.task_execute_type,
        "task-code": features.task_code,
    }
    options = command.get("options")
    if not isinstance(options, list):
        return command
    projected_options: list[JsonValue] = []
    unavailable: list[JsonObject] = []
    for value in options:
        if not isinstance(value, dict):
            projected_options.append(value)
            continue
        option = cast("JsonObject", value)
        name = option.get("name")
        if not isinstance(name, str) or supported.get(name, True):
            projected_options.append(option)
            continue
        unavailable.append(
            {
                "flag": f"--{name}",
                "availability": "upstream_absent",
                "introduced_in": _TASK_INSTANCE_OPTION_INTRODUCED_IN[name],
                "instruction": "omit",
            }
        )
    projected = dict(command)
    projected["options"] = projected_options
    if unavailable:
        projected["unavailable_options"] = unavailable
    else:
        projected.pop("unavailable_options", None)
    return projected


def _project_legacy_task_command(
    command: JsonObject,
    *,
    action: str,
) -> JsonObject:
    arguments = command.get("arguments")
    projected = dict(command)
    if action == "task.get":
        projected["summary"] = "Get one task definition by exact name."
    elif action == "task.update":
        projected["summary"] = (
            "Update one task by exact name; use workflow edit for other DAG "
            "changes, workflow-instance edit for repairs."
        )
    if isinstance(arguments, list):
        projected_arguments: list[JsonValue] = []
        for value in arguments:
            if not isinstance(value, dict) or value.get("name") != "task":
                projected_arguments.append(value)
                continue
            task_argument = dict(cast("JsonObject", value))
            task_argument["selector"] = "opaque_name"
            task_argument["description"] = (
                "Exact task name inside the selected workflow. Use `dsctl task "
                "list` to discover values."
            )
            projected_arguments.append(task_argument)
        projected["arguments"] = projected_arguments
    if action != "task.update":
        return projected

    options = projected.get("options")
    if not isinstance(options, list):
        return projected
    projected_options: list[JsonValue] = []
    for value in options:
        if not isinstance(value, dict) or value.get("name") != "set":
            projected_options.append(value)
            continue
        set_option = dict(cast("JsonObject", value))
        set_option["supported_keys"] = list(_LEGACY_TASK_UPDATE_KEYS)
        set_option["examples"] = list(_LEGACY_TASK_UPDATE_EXAMPLES)
        projected_options.append(set_option)
    projected["options"] = projected_options
    return projected


def _project_task_update_fields(
    command: JsonObject,
    *,
    ds_version: str,
) -> JsonObject:
    supported = _task_update_supported_keys(ds_version)
    options = command.get("options")
    if not isinstance(options, list):
        return command
    projected_options: list[JsonValue] = []
    for value in options:
        if not isinstance(value, dict) or value.get("name") != "set":
            projected_options.append(value)
            continue
        set_option = dict(cast("JsonObject", value))
        keys = set_option.get("supported_keys")
        if isinstance(keys, list):
            set_option["supported_keys"] = [key for key in keys if key in supported]
        examples = set_option.get("examples")
        if isinstance(examples, list):
            set_option["examples"] = [
                example
                for example in examples
                if isinstance(example, str) and example.partition("=")[0] in supported
            ]
        projected_options.append(set_option)
    projected = dict(command)
    projected["options"] = projected_options
    return projected


def _task_update_supported_keys(ds_version: str) -> frozenset[str]:
    if ds_version == "1.3.9":
        return frozenset(_LEGACY_TASK_UPDATE_KEYS)
    features = task_update_contract_features(ds_version)
    supported = {
        key
        for key, wire_field in _TASK_UPDATE_WIRE_FIELD_BY_KEY.items()
        if wire_field in features.request_fields
    }
    if features.dependency_update:
        supported.add("depends_on")
    return frozenset(supported)


def _project_legacy_audit_list(command: JsonObject) -> JsonObject:
    options = command.get("options")
    if not isinstance(options, list):
        return command
    projected_options: list[JsonValue] = []
    for value in options:
        if not isinstance(value, dict):
            projected_options.append(value)
            continue
        option = dict(cast("JsonObject", value))
        name = option.get("name")
        if name == "model-name":
            continue
        if name == "model-type":
            option["multiple"] = False
            option["choices"] = list(_LEGACY_AUDIT_MODEL_TYPES)
            option.pop("discovery_command", None)
            option["description"] = (
                "One exact AuditResourceType value supported by this DS version."
            )
        elif name == "operation-type":
            option["multiple"] = False
            option["choices"] = list(_LEGACY_AUDIT_OPERATION_TYPES)
            option.pop("discovery_command", None)
            option["description"] = (
                "One exact AuditOperationType value supported by this DS version."
            )
        projected_options.append(option)

    projected = dict(command)
    projected["options"] = projected_options
    unavailable = projected.get("unavailable_options")
    unavailable_options = list(unavailable) if isinstance(unavailable, list) else []
    unavailable_options.append(
        {
            "flag": "--model-name",
            "availability": "upstream_absent",
            "introduced_in": "3.2.2",
            "instruction": "omit",
        }
    )
    projected["unavailable_options"] = unavailable_options
    return projected


def _project_local_metadata_command(
    command: JsonObject,
    *,
    action: str,
    ds_version: str,
) -> JsonObject | None:
    if action == "enum.list":
        return _with_input_choices(
            command,
            collection="arguments",
            name="enum",
            choices=supported_enum_choices(ds_version=ds_version),
        )
    if action in {"task-type.get", "task-type.schema", "template.task"}:
        catalog = get_task_authoring_catalog(ds_version)
        projected = _with_input_choices(
            command,
            collection="arguments",
            name="task_type",
            choices=supported_task_template_types(catalog=catalog),
        )
        if action == "template.task":
            projected = _with_input_choices(
                projected,
                collection="options",
                name="variant",
                choices=supported_task_template_variants(catalog=catalog),
            )
        return projected
    if action == "template.datasource":
        return _with_input_choices(
            command,
            collection="options",
            name="type",
            choices=supported_datasource_types(ds_version),
        )
    return None


def _with_input_choices(
    command: JsonObject,
    *,
    collection: str,
    name: str,
    choices: tuple[str, ...],
) -> JsonObject:
    values = command.get(collection)
    if not isinstance(values, list):
        return command
    projected_values: list[JsonValue] = []
    for value in values:
        if not isinstance(value, dict) or value.get("name") != name:
            projected_values.append(value)
            continue
        projected_value = dict(cast("JsonObject", value))
        projected_value["choices"] = list(choices)
        projected_values.append(projected_value)
    projected = dict(command)
    projected[collection] = projected_values
    return projected


def project_data_shape_for_version(
    shape: JsonObject,
    *,
    action: str,
    ds_version: str,
) -> JsonObject:
    """Project shared output columns onto one exact identity epoch."""
    if ds_version != "1.3.9" or action not in {"task.list", "task.get"}:
        return shape
    projected = dict(shape)
    projected["default_columns"] = ["id", "name"]
    return projected


__all__ = ["project_command_for_version", "project_data_shape_for_version"]
