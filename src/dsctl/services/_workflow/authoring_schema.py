"""Bounded workflow YAML discovery from models and exact authoring capabilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.command_contract import COMMAND_CATALOG
from dsctl.models.workflow_spec import (
    WorkflowMetadataSpec,
    WorkflowScheduleSpec,
    WorkflowSpec,
)
from dsctl.output import require_json_object
from dsctl.services.enums import supported_enum_member_values
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.version_resolution import selected_target_globals
from dsctl.upstream import get_enum_spec
from dsctl.upstream.schedules import schedule_contract_features
from dsctl.upstream.workflows import supports_workflow_execution_type

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

    from dsctl.output import JsonObject, JsonValue


def workflow_authoring_schema_data(
    ds_version: str, *, env_file: str | None = None
) -> JsonObject:
    """Describe workflow metadata/schedules without expanding task-type models.

    JSON Schema represents model structure; field validators, graph rules and
    task-specific contracts still require the selected profile's workflow lint.
    All projections are fresh, so callers cannot mutate model or catalog state.
    """
    catalog = get_task_authoring_catalog(ds_version)
    metadata = _schema_object(WorkflowMetadataSpec.model_json_schema())
    schedule = _schema_object(WorkflowScheduleSpec.model_json_schema())
    definitions = _schema_object(metadata.pop("$defs"))
    definitions.update(_schema_object(schedule.pop("$defs")))
    _describe_fields(
        metadata,
        {
            "name": "Non-empty workflow name.",
            "project": "Resolution: --project, workflow.project, then saved context.",
            "description": "Optional workflow description.",
            "timeout": "Minutes; 0 disables the workflow timeout.",
            "global_params": (
                "Mapping shorthand creates IN/VARCHAR entries; list form carries "
                "explicit types and directions."
            ),
            "execution_type": "Workflow concurrency policy for this exact profile.",
            "release_state": "Desired final state after initial OFFLINE creation.",
        },
    )
    _describe_fields(
        schedule,
        {
            "cron": "Quartz cron with 6 or 7 fields.",
            "timezone": "Required IANA timezone when a schedule is authored.",
            "start": "Start time: YYYY-MM-DD HH:MM:SS.",
            "end": "End time: YYYY-MM-DD HH:MM:SS; must follow start.",
            "failure_strategy": "Omit to use the selected schedule defaults.",
            "priority": "Omit to use the selected schedule defaults.",
            "release_state": "Defaults to OFFLINE if enabled is also omitted.",
            "enabled": (
                "Alias: true=ONLINE, false=OFFLINE; "
                "must not conflict with release_state."
            ),
        },
    )
    for model_enum, upstream_enum in (
        ("FailureStrategy", "failure-strategy"),
        ("Priority", "priority"),
        ("ReleaseState", "release-state"),
    ):
        _narrow_enum(
            definitions,
            model_enum,
            supported_enum_member_values(upstream_enum, ds_version=ds_version),
        )
    _narrow_enum(definitions, "DataType", catalog.parameter_data_types)
    _narrow_enum(
        definitions,
        "Direct",
        ("IN", "OUT")
        if catalog.parameter_semantics.output.var_pool_transport
        else ("IN",),
    )
    _project_execution_type(definitions, ds_version=ds_version)
    _project_schedule(schedule, ds_version=ds_version)
    schedule["description"] = (
        f"{schedule['description']} "
        "Attached schedules require workflow.release_state=ONLINE."
    )
    definitions[WorkflowMetadataSpec.__name__] = metadata
    definitions[WorkflowScheduleSpec.__name__] = schedule

    document = _schema_object(WorkflowSpec.model_json_schema())
    all_definitions = _schema_object(document["$defs"])
    task_model = _schema_object(all_definitions["WorkflowTaskSpec"])
    task_fields = _schema_object(task_model["properties"])
    fields = _schema_object(document["properties"])
    tasks = _schema_object(fields["tasks"])
    tasks.update(
        {
            "minItems": 1,
            "items": {
                "type": "object",
                "required": task_model["required"],
                "properties": {name: task_fields[name] for name in ("name", "type")},
                "description": (
                    "Identity only; remaining fields use task_authoring links."
                ),
            },
        }
    )
    fields["tasks"] = tasks
    document["properties"] = fields
    document["$defs"] = definitions
    _omit_model_labels(document)
    commands = _authoring_commands(env_file=env_file)
    return {
        "format": "yaml",
        "ds_version": ds_version,
        "source_option": "--file",
        "template_command": commands["template"],
        "lint_command_pattern": commands["lint"],
        "validation_scope": (
            "Metadata/schedule structure; not a complete task or graph validator. "
            "Query task schemas for common fields/task_params, then lint. "
            "Keep the same DS_VERSION/--env-file."
        ),
        "yaml_schema": document,
        "task_authoring": {
            "list_command": commands["tasks"],
            "schema_command_pattern": commands["task_schema"],
            "template_command_pattern": commands["task_template"],
            "shell_params_template_command": commands["shell_params"],
            "parameter_syntax_command": commands["parameters"],
            "rules": (
                "Unique task names; depends_on must exist and form a DAG. "
                "Exactly one of command or task_params; replace command before "
                "adding task_params.localParams."
            ),
        },
        "execution_context": {
            "tenant": (
                "Not a YAML field. Definition tenantCode, where required, comes from "
                "the current user. Attached schedules have no YAML tenant override. "
                "Runtime/schedule options: inspect linked commands; omitted values "
                "use exact project-preference/current-user/default resolution."
            ),
            "runtime_schema_command": commands["runtime"],
            "schedule_schema_command": commands["schedule"],
        },
    }


def _authoring_commands(*, env_file: str | None) -> dict[str, str]:
    requests: dict[str, tuple[str, dict[str, str | bool]]] = {
        "template": ("template.workflow", {"raw": True}),
        "lint": ("lint.workflow", {"file": "FILE"}),
        "tasks": ("template.task", {}),
        "task_schema": ("task-type.schema", {"task_type": "TYPE"}),
        "task_template": ("template.task", {"task_type": "TYPE", "raw": True}),
        "shell_params": (
            "template.task",
            {"task_type": "SHELL", "raw": True},
        ),
        "parameters": ("template.params", {"topic": "context"}),
        "runtime": ("schema", {"command": "workflow.run"}),
        "schedule": ("schema", {"command": "schedule.create"}),
    }
    return {
        name: COMMAND_CATALOG.render(
            action,
            values=values,
            global_values=selected_target_globals(env_file),
        )
        for name, (action, values) in requests.items()
    }


def _describe_fields(schema: JsonObject, descriptions: Mapping[str, str]) -> None:
    fields = _schema_object(schema["properties"])
    for name, description in descriptions.items():
        field = _schema_object(fields[name])
        field["description"] = description
        fields[name] = field
    schema["properties"] = fields


def _omit_model_labels(document: JsonObject) -> None:
    """Drop automatic class/field titles, retaining authored semantic descriptions."""
    definitions = _schema_object(document["$defs"])
    document.pop("title", None)
    document.pop("description", None)
    for name, value in definitions.items():
        schema = _schema_object(value)
        schema.pop("title", None)
        if name not in {"WorkflowScheduleSpec", "WorkflowExecutionType"}:
            schema.pop("description", None)
        properties = schema.get("properties")
        if isinstance(properties, dict):
            for field in properties.values():
                if isinstance(field, dict):
                    field.pop("title", None)
        definitions[name] = schema
    document["$defs"] = definitions


def _narrow_enum(definitions: JsonObject, name: str, allowed: Collection[str]) -> None:
    enum = _schema_object(definitions[name])
    values = enum["enum"]
    if not isinstance(values, list):
        message = f"Workflow authoring model enum {name} has no choices"
        raise TypeError(message)
    enum["enum"] = [
        value for value in values if isinstance(value, str) and value in allowed
    ]
    if not enum["enum"]:
        message = f"Workflow authoring model enum {name} has no exact-profile choices"
        raise ValueError(message)
    definitions[name] = enum


def _project_execution_type(definitions: JsonObject, *, ds_version: str) -> None:
    if not supports_workflow_execution_type(ds_version):
        enum = _schema_object(definitions["WorkflowExecutionType"])
        enum.pop("enum")
        enum["const"] = "PARALLEL"
        enum["description"] = (
            "This profile has fixed PARALLEL behavior; omit or keep PARALLEL."
        )
        definitions["WorkflowExecutionType"] = enum
        return
    spec = get_enum_spec(ds_version, "workflow-execution-type-enum") or get_enum_spec(
        ds_version, "process-execution-type-enum"
    )
    if spec is None:
        message = f"DS {ds_version} workflow execution capability has no exact enum"
        raise ValueError(message)
    _narrow_enum(
        definitions,
        "WorkflowExecutionType",
        tuple(str(member.value) for member in spec.members),
    )


def _project_schedule(schedule: JsonObject, *, ds_version: str) -> None:
    fields = _schema_object(schedule["properties"])
    features = schedule_contract_features(ds_version)
    if features.missed_fire_policy:
        fields["missed_fire_policy"] = {
            "type": "string",
            "enum": list(features.missed_fire_policy_choices),
            "description": (
                "Omit on create to use the exact native default; "
                "omitted updates preserve the stored policy."
            ),
        }
    else:
        fields.pop("missed_fire_policy")
    if features.timezone:
        field = _schema_object(fields["timezone"])
        field.pop("anyOf")
        field.pop("default")
        field["type"] = "string"
        fields["timezone"] = field
        required = schedule["required"]
        if not isinstance(required, list):
            message = "Workflow schedule model has no required fields"
            raise ValueError(message)
        schedule["required"] = [*required, "timezone"]
    else:
        fields.pop("timezone")
        schedule["description"] = (
            "Optional schedule; this profile uses the server-local timezone."
        )
    schedule["properties"] = fields


def _schema_object(value: JsonValue) -> JsonObject:
    return require_json_object(value, label="workflow authoring schema")
