from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from difflib import get_close_matches
from shlex import quote
from typing import TYPE_CHECKING, TypedDict, cast

from dsctl.errors import UserInputError
from dsctl.models.common import DataType, Direct, Priority
from dsctl.models.task_spec import (
    DependentFailurePolicy,
    DependentRelation,
    DependentType,
    DependResult,
    HttpCheckCondition,
    HttpParametersType,
    HttpRequestMethod,
    TaskExecutionStatus,
    TaskRunFlag,
    TaskTimeoutNotifyStrategy,
    canonical_task_type,
    task_params_model_for_type,
)
from dsctl.models.workflow_spec import COMMAND_TASK_TYPES
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services import _task_templates

if TYPE_CHECKING:
    from collections.abc import Sequence
    from enum import Enum

    from dsctl.support.yaml_io import JsonObject, JsonValue


class TaskAuthoringFieldData(TypedDict, total=False):
    """One authoring field accepted by workflow task YAML."""

    path: str
    type: str
    required: bool
    default: JsonValue
    choices: list[str]
    active_when: str
    choice_source: str
    choice_value: str
    related_commands: list[str]
    compile_path: str
    description: str


class TaskAuthoringStateRuleData(TypedDict, total=False):
    """One task-type-specific field state rule."""

    when: str
    condition_paths: list[str]
    active_paths: list[str]
    inactive_paths: list[str]
    compile_policy: dict[str, str]
    description: str


class TaskAuthoringChoiceSourceData(TypedDict, total=False):
    """How to discover valid values for an authoring field."""

    path: str
    command: str
    value: str
    description: str
    related_commands: list[str]


class TaskAuthoringCompileMappingData(TypedDict):
    """How an authoring field maps to the DS create/update payload."""

    authoring_path: str
    ds_payload_path: str
    description: str


class TaskTypeSummaryRowData(TypedDict, total=False):
    """Compact row for task-type get output."""

    kind: str
    name: str
    summary: str
    command: str


class TaskTypeSummaryData(TypedDict):
    """Local authoring summary for one DS task type."""

    task_type: str
    category: str
    kind: str
    default_variant: str
    variants: list[str]
    payload_modes: list[str]
    required_paths: list[str]
    required_paths_by_payload_mode: dict[str, list[str]]
    template_command: str
    raw_template_command: str
    schema_command: str
    template_index_command: str
    parameter_command: str
    choice_sources: list[TaskAuthoringChoiceSourceData]
    workflow_usage: dict[str, str]
    rows: list[TaskTypeSummaryRowData]


class TaskTypeAuthoringSchemaData(TypedDict):
    """Legacy full local authoring contract for one DS task type."""

    task_type: str
    category: str
    kind: str
    schema: JsonObject
    fields: list[TaskAuthoringFieldData]
    state_rules: list[TaskAuthoringStateRuleData]
    choice_sources: list[TaskAuthoringChoiceSourceData]
    compile_mappings: list[TaskAuthoringCompileMappingData]
    template_command: str
    raw_template_command: str


@dataclass(frozen=True)
class _TaskTypeAuthoringContract:
    """Canonical facts projected into bounded or legacy task schema views."""

    task_type: str
    category: str
    kind: str
    fields: list[TaskAuthoringFieldData]
    state_rules: list[TaskAuthoringStateRuleData]


class _MissingDefault:
    """Sentinel for field descriptors without a default value."""


_MISSING = _MissingDefault()
_TASK_TYPE_SCHEMA_VERSION = 2
_COMPILE_MAPPING_DESCRIPTION = (
    "Compiled by workflow create/edit before sending DS REST form fields."
)
_SCRIPT_TASK_TYPES = frozenset({*COMMAND_TASK_TYPES, "REMOTESHELL"})
_DEPENDENT_CYCLE_VALUES = ("hour", "day", "week", "month")
_DEPENDENT_DATE_VALUES_BY_CYCLE = {
    "hour": (
        "currentHour",
        "last1Hour",
        "last2Hours",
        "last3Hours",
        "last24Hours",
    ),
    "day": ("today", "last1Days", "last2Days", "last3Days", "last7Days"),
    "week": (
        "thisWeek",
        "lastWeek",
        "lastMonday",
        "lastTuesday",
        "lastWednesday",
        "lastThursday",
        "lastFriday",
        "lastSaturday",
        "lastSunday",
    ),
    "month": (
        "thisMonth",
        "thisMonthBegin",
        "lastMonth",
        "lastMonthBegin",
        "lastMonthEnd",
    ),
}
_DEPENDENT_DATE_VALUE_CHOICES = tuple(
    dict.fromkeys(
        value
        for cycle_values in _DEPENDENT_DATE_VALUES_BY_CYCLE.values()
        for value in cycle_values
    )
)


@dataclass(frozen=True)
class _FieldSpec:
    """Internal field descriptor that renders to stable JSON rows."""

    path: str
    value_type: str
    required: bool = False
    default: JsonValue | _MissingDefault = _MISSING
    choices: tuple[str, ...] = ()
    active_when: str | None = None
    choice_source: str | None = None
    related_commands: tuple[str, ...] = ()
    compile_path: str | None = None
    description: str = ""

    def to_data(self) -> TaskAuthoringFieldData:
        """Return the JSON-safe field row."""
        data: TaskAuthoringFieldData = {
            "path": self.path,
            "type": self.value_type,
            "required": self.required,
            "description": self.description,
        }
        if self.default is not _MISSING:
            data["default"] = require_json_value(
                self.default,
                label=f"task authoring field default {self.path}",
            )
        if self.choices:
            data["choices"] = list(self.choices)
        if self.active_when is not None:
            data["active_when"] = self.active_when
        if self.choice_source is not None:
            data["choice_source"] = self.choice_source
        if self.related_commands:
            data["related_commands"] = list(self.related_commands)
        if self.compile_path is not None:
            data["compile_path"] = self.compile_path
        return data


def task_type_summary_result(task_type: str) -> CommandResult:
    """Return a local authoring summary for one DS task type."""
    normalized = require_supported_authoring_task_type(task_type)
    data = task_type_summary_data(normalized)
    warnings, warning_details = _generic_task_warnings(normalized)
    return CommandResult(
        data=require_json_object(data, label="task type summary data"),
        resolved={"task_type": normalized},
        warnings=warnings,
        warning_details=warning_details,
    )


def task_type_schema_result(
    task_type: str,
    *,
    field: str | None = None,
    json_schema: bool = False,
    compile_mappings: bool = False,
    full: bool = False,
) -> CommandResult:
    """Return one bounded authoring view or the explicit legacy full contract."""
    selected_field = _normalize_schema_field(field)
    view = _task_type_schema_view(
        field=selected_field,
        json_schema=json_schema,
        compile_mappings=compile_mappings,
        full=full,
    )
    normalized = require_supported_authoring_task_type(task_type)
    contract = _task_type_authoring_contract(normalized)
    data = _project_task_type_schema(
        contract,
        view=view,
        field=selected_field,
    )
    warnings, warning_details = _generic_task_warnings(normalized)
    resolved: JsonObject = {"task_type": normalized, "view": view}
    if selected_field is not None:
        resolved["field"] = selected_field
    return CommandResult(
        data=require_json_object(data, label="task type authoring schema data"),
        resolved=resolved,
        warnings=warnings,
        warning_details=warning_details,
    )


def task_type_summary_data(task_type: str) -> TaskTypeSummaryData:
    """Build the compact task authoring summary for one supported task type."""
    normalized = require_supported_authoring_task_type(task_type)
    metadata = _task_templates.task_template_metadata()[normalized]
    fields = _fields_for(normalized)
    required_paths_by_payload_mode = _required_paths_by_payload_mode(
        normalized,
        fields,
    )
    mode_specific_paths = {
        path for paths in required_paths_by_payload_mode.values() for path in paths
    }
    required_paths = [
        field["path"]
        for field in fields
        if field.get("required") is True
        and field["path"] not in mode_specific_paths
        and not (required_paths_by_payload_mode and field["path"] == "task_params")
    ]
    return TaskTypeSummaryData(
        task_type=normalized,
        category=metadata["category"],
        kind=metadata["kind"],
        default_variant=metadata["default_variant"],
        variants=metadata["variants"],
        payload_modes=metadata["payload_modes"],
        required_paths=required_paths,
        required_paths_by_payload_mode=required_paths_by_payload_mode,
        template_command=f"dsctl template task {normalized}",
        raw_template_command=f"dsctl template task {normalized} --raw",
        schema_command=f"dsctl task-type schema {normalized}",
        template_index_command="dsctl template task",
        parameter_command="dsctl template params",
        choice_sources=_choice_sources_for(normalized),
        workflow_usage={
            "paste_into": "workflow YAML tasks[]",
            "validate": "dsctl lint workflow FILE",
            "dry_run": "dsctl workflow create --file FILE --dry-run",
        },
        rows=_summary_rows(normalized),
    )


def _required_paths_by_payload_mode(
    task_type: str,
    fields: Sequence[TaskAuthoringFieldData],
) -> dict[str, list[str]]:
    """Return leaf requirements that depend on a script payload mode."""
    if task_type not in _SCRIPT_TASK_TYPES:
        return {}
    task_params_paths = [
        field["path"]
        for field in fields
        if field.get("required") is True and field["path"].startswith("task_params.")
    ]
    if task_type in COMMAND_TASK_TYPES:
        return {
            "command": ["command"],
            "task_params": task_params_paths,
        }
    return {"task_params": task_params_paths}


def _task_type_authoring_contract(task_type: str) -> _TaskTypeAuthoringContract:
    """Build canonical authoring facts once before selecting a representation."""
    metadata = _task_templates.task_template_metadata()[task_type]
    fields = _fields_for(task_type)
    state_rules = _state_rules_for(task_type)
    return _TaskTypeAuthoringContract(
        task_type=task_type,
        category=metadata["category"],
        kind=metadata["kind"],
        fields=fields,
        state_rules=state_rules,
    )


def _normalize_schema_field(field: str | None) -> str | None:
    if field is None:
        return None
    normalized = field.strip()
    if normalized:
        return normalized
    message = "--field must include an authoring field path."
    raise UserInputError(
        message,
        details={"field": field},
        suggestion="Remove --field or pass a path from `dsctl task-type schema TYPE`.",
    )


def _task_type_schema_view(
    *,
    field: str | None,
    json_schema: bool,
    compile_mappings: bool,
    full: bool,
) -> str:
    selected = [
        flag
        for flag, enabled in (
            ("--field", field is not None),
            ("--json-schema", json_schema),
            ("--compile-mappings", compile_mappings),
            ("--full", full),
        )
        if enabled
    ]
    if len(selected) > 1:
        message = "Task type schema view selectors cannot be combined."
        raise UserInputError(
            message,
            details={
                "constraint": "at_most_one_of",
                "selected": selected,
            },
            suggestion=(
                "Use only one of --field, --json-schema, --compile-mappings, or --full."
            ),
        )
    if field is not None:
        return "field"
    if json_schema:
        return "json_schema"
    if compile_mappings:
        return "compile_mappings"
    if full:
        return "full"
    return "fields"


def _project_task_type_schema(
    contract: _TaskTypeAuthoringContract,
    *,
    view: str,
    field: str | None,
) -> JsonObject:
    if view == "full":
        return require_json_object(
            _legacy_task_type_schema_data(contract),
            label="legacy task type authoring schema data",
        )

    data = _bounded_task_type_schema_data(contract)
    if view == "fields":
        data["fields"] = cast("JsonValue", contract.fields)
        data["state_rules"] = cast("JsonValue", contract.state_rules)
    elif view == "field" and field is not None:
        selected = _require_authoring_field(contract, field)
        data["fields"] = cast("JsonValue", [selected])
        data["state_rules"] = cast(
            "JsonValue",
            _state_rules_for_field(contract.state_rules, field),
        )
    elif view == "json_schema":
        data["schema"] = _json_schema_for(contract.task_type, fields=contract.fields)
    elif view == "compile_mappings":
        mappings = _compile_mappings_from_fields(contract.fields)
        data["compile_mapping_policy"] = _COMPILE_MAPPING_DESCRIPTION
        data["compile_mappings"] = cast(
            "JsonValue",
            [
                {
                    "authoring_path": mapping["authoring_path"],
                    "ds_payload_path": mapping["ds_payload_path"],
                }
                for mapping in mappings
            ],
        )
    else:
        message = f"Unsupported internal task type schema view '{view}'"
        raise RuntimeError(message)
    return data


def _bounded_task_type_schema_data(
    contract: _TaskTypeAuthoringContract,
) -> JsonObject:
    return {
        "schema_version": _TASK_TYPE_SCHEMA_VERSION,
        "task_type": contract.task_type,
        "category": contract.category,
        "kind": contract.kind,
        "links": {
            "fields": f"dsctl task-type schema {contract.task_type}",
            "field": (
                f"dsctl task-type schema {contract.task_type} --field 'FIELD_PATH'"
            ),
            "json_schema": (
                f"dsctl task-type schema {contract.task_type} --json-schema"
            ),
            "compile_mappings": (
                f"dsctl task-type schema {contract.task_type} --compile-mappings"
            ),
            "full": f"dsctl task-type schema {contract.task_type} --full",
            "template": f"dsctl template task {contract.task_type}",
            "raw_template": f"dsctl template task {contract.task_type} --raw",
        },
    }


def _legacy_task_type_schema_data(
    contract: _TaskTypeAuthoringContract,
) -> TaskTypeAuthoringSchemaData:
    legacy_fields: list[TaskAuthoringFieldData] = []
    for field in contract.fields:
        legacy_field = dict(field)
        legacy_field.pop("choice_value", None)
        legacy_fields.append(cast("TaskAuthoringFieldData", legacy_field))
    choice_sources = _choice_sources_from_fields(contract.fields)
    compile_mappings = _compile_mappings_from_fields(contract.fields)
    schema = deepcopy(_json_schema_for(contract.task_type, fields=legacy_fields))
    metadata_value = schema.get("x-dsctl")
    if not isinstance(metadata_value, dict):
        message = "task authoring JSON Schema is missing x-dsctl metadata"
        raise TypeError(message)
    metadata = dict(metadata_value)
    metadata["state_rules"] = cast("JsonValue", contract.state_rules)
    metadata["choice_sources"] = cast("JsonValue", choice_sources)
    metadata["compile_mappings"] = cast("JsonValue", compile_mappings)
    schema["x-dsctl"] = metadata
    return TaskTypeAuthoringSchemaData(
        task_type=contract.task_type,
        category=contract.category,
        kind=contract.kind,
        schema=schema,
        fields=legacy_fields,
        state_rules=contract.state_rules,
        choice_sources=choice_sources,
        compile_mappings=compile_mappings,
        template_command=f"dsctl template task {contract.task_type}",
        raw_template_command=f"dsctl template task {contract.task_type} --raw",
    )


def _require_authoring_field(
    contract: _TaskTypeAuthoringContract,
    field_path: str,
) -> TaskAuthoringFieldData:
    fields_by_path = {field["path"]: field for field in contract.fields}
    selected = fields_by_path.get(field_path)
    if selected is not None:
        return selected

    open_plugin_field = contract.kind == "generic" and field_path.startswith(
        "task_params."
    )
    candidates: list[dict[str, str]] = []
    if not open_plugin_field:
        candidates.extend(
            {
                "path": candidate_path,
                "command": (
                    f"dsctl task-type schema {contract.task_type} --field "
                    f"{quote(candidate_path)}"
                ),
            }
            for candidate_path in get_close_matches(
                field_path,
                list(fields_by_path),
                n=3,
                cutoff=0.45,
            )
        )
    details: JsonObject = {
        "task_type": contract.task_type,
        "field": field_path,
        "available_count": len(fields_by_path),
        "candidates": cast("JsonValue", candidates),
        "discovery_command": f"dsctl task-type schema {contract.task_type}",
    }
    if open_plugin_field:
        details["open_task_params"] = True
        suggestion = (
            "This generic task type accepts plugin-defined task_params; inspect an "
            "exported workflow or the upstream plugin contract."
        )
    elif candidates:
        suggestion = (
            f"Retry with `{candidates[0]['command']}`, or inspect the bounded "
            "field catalog."
        )
    else:
        suggestion = (
            f"Run `dsctl task-type schema {contract.task_type}` to inspect the "
            "bounded field catalog."
        )
    message = f"Unknown {contract.task_type} authoring field '{field_path}'."
    raise UserInputError(
        message,
        details=details,
        suggestion=suggestion,
    )


def _state_rules_for_field(
    rules: Sequence[TaskAuthoringStateRuleData],
    field_path: str,
) -> list[TaskAuthoringStateRuleData]:
    return [rule for rule in rules if _state_rule_mentions_field(rule, field_path)]


def _state_rule_mentions_field(
    rule: TaskAuthoringStateRuleData,
    field_path: str,
) -> bool:
    related_paths = [
        *rule.get("condition_paths", []),
        *rule.get("active_paths", []),
        *rule.get("inactive_paths", []),
        *rule.get("compile_policy", {}),
    ]
    normalized_field = _normalize_authoring_path(field_path)
    return any(
        normalized_field == normalized_path
        or normalized_field.startswith(f"{normalized_path}.")
        or normalized_path.startswith(f"{normalized_field}.")
        for path in related_paths
        if (normalized_path := _normalize_authoring_path(path))
    )


def _normalize_authoring_path(path: str) -> str:
    """Normalize array markers while retaining dotted field boundaries."""
    return path.replace("[]", "")


def require_supported_authoring_task_type(task_type: str) -> str:
    """Normalize and validate one task type for local authoring commands."""
    normalized = canonical_task_type(task_type)
    if normalized in _task_templates.supported_task_template_types():
        return normalized
    message = f"Unsupported task type '{task_type}'."
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "available_task_types_count": len(
                _task_templates.supported_task_template_types()
            ),
            "discovery_command": "dsctl template task",
        },
        suggestion="Run `dsctl template task` to inspect supported task types.",
    )


def _fields_for(task_type: str) -> list[TaskAuthoringFieldData]:
    fields = [
        field.to_data()
        for field in (*_common_fields(task_type), *_task_specific_fields(task_type))
    ]
    for field in fields:
        source = field.get("choice_source")
        if isinstance(source, str):
            choice_value = _choice_source_value(field["path"], source)
            if choice_value is not None:
                field["choice_value"] = choice_value
    return fields


def _common_fields(task_type: str) -> tuple[_FieldSpec, ...]:
    payload_rule = (
        "required when command is absent"
        if task_type in COMMAND_TASK_TYPES
        else "required"
    )
    fields = [
        _FieldSpec(
            "name",
            "string",
            required=True,
            compile_path="taskDefinitionJson[].name",
            description="Task name unique inside one workflow YAML document.",
        ),
        _FieldSpec(
            "type",
            "string",
            required=True,
            default=task_type,
            choices=(task_type,),
            compile_path="taskDefinitionJson[].taskType",
            description="DS-native task type.",
        ),
        _FieldSpec(
            "description",
            "string",
            default="",
            compile_path="taskDefinitionJson[].description",
            description="Optional task description.",
        ),
        _FieldSpec(
            "task_params",
            "object",
            required=task_type not in COMMAND_TASK_TYPES,
            active_when=payload_rule,
            compile_path="taskDefinitionJson[].taskParams",
            description="DS task plugin payload for this task type.",
        ),
    ]
    if task_type in COMMAND_TASK_TYPES:
        fields.append(
            _FieldSpec(
                "command",
                "string",
                required=False,
                active_when="allowed when task_params is absent",
                compile_path="taskDefinitionJson[].taskParams.rawScript",
                description="Shortcut for simple script-like tasks.",
            )
        )
    fields.extend(
        (
            _FieldSpec(
                "flag",
                "enum",
                default=TaskRunFlag.YES.value,
                choices=_enum_values(TaskRunFlag),
                compile_path="taskDefinitionJson[].flag",
                description="Whether DS should run this task.",
            ),
            _FieldSpec(
                "worker_group",
                "string",
                default="default",
                choice_source="dsctl worker-group list",
                related_commands=("dsctl worker-group list",),
                compile_path="taskDefinitionJson[].workerGroup",
                description="Worker group used to dispatch the task.",
            ),
            _FieldSpec(
                "environment_code",
                "integer",
                choice_source="dsctl environment list",
                related_commands=(
                    "dsctl environment list",
                    "dsctl template environment",
                    "dsctl environment create --name NAME --config-file env.sh",
                ),
                compile_path="taskDefinitionJson[].environmentCode",
                description="Optional environment code bound to worker execution.",
            ),
            _FieldSpec(
                "task_group_id",
                "integer",
                choice_source="dsctl task-group list",
                related_commands=(
                    "dsctl task-group list",
                    "dsctl task-group create --name NAME --group-size N",
                ),
                compile_path="taskDefinitionJson[].taskGroupId",
                description="Optional DS task-group id for resource throttling.",
            ),
            _FieldSpec(
                "task_group_priority",
                "integer",
                default=0,
                active_when="valid only when task_group_id is set",
                compile_path="taskDefinitionJson[].taskGroupPriority",
                description="Priority inside the selected task group.",
            ),
            _FieldSpec(
                "priority",
                "enum",
                default=Priority.MEDIUM.value,
                choices=_enum_values(Priority),
                compile_path="taskDefinitionJson[].taskPriority",
                description="DS task priority.",
            ),
            _FieldSpec(
                "retry.times",
                "integer",
                default=0,
                compile_path="taskDefinitionJson[].failRetryTimes",
                description="Retry count after task failure.",
            ),
            _FieldSpec(
                "retry.interval",
                "integer",
                default=0,
                compile_path="taskDefinitionJson[].failRetryInterval",
                description="Retry interval in minutes.",
            ),
            _FieldSpec(
                "timeout",
                "integer",
                default=0,
                compile_path="taskDefinitionJson[].timeout",
                description="Timeout in minutes; 0 disables timeout handling.",
            ),
            _FieldSpec(
                "timeout_notify_strategy",
                "enum",
                choices=_enum_values(TaskTimeoutNotifyStrategy),
                active_when="requires timeout > 0",
                compile_path="taskDefinitionJson[].timeoutNotifyStrategy",
                description="Timeout warning/failure behavior.",
            ),
            _FieldSpec(
                "delay",
                "integer",
                default=0,
                compile_path="taskDefinitionJson[].delayTime",
                description="Delay execution by this many minutes.",
            ),
            _FieldSpec(
                "cpu_quota",
                "integer",
                compile_path="taskDefinitionJson[].cpuQuota",
                description="Optional CPU quota; -1 follows DS default behavior.",
            ),
            _FieldSpec(
                "memory_max",
                "integer",
                compile_path="taskDefinitionJson[].memoryMax",
                description="Optional memory limit; -1 follows DS default behavior.",
            ),
            _FieldSpec(
                "depends_on[]",
                "string",
                default=(),
                choice_source="other tasks in the same workflow YAML",
                compile_path="workflowTaskRelationList",
                description="Upstream task names for ordinary DAG edges.",
            ),
        )
    )
    return tuple(fields)


def _task_specific_fields(task_type: str) -> tuple[_FieldSpec, ...]:
    if task_type in {"SHELL", "PYTHON"}:
        return _script_fields()
    if task_type == "REMOTESHELL":
        return _remote_shell_fields()
    if task_type == "SQL":
        return _sql_fields()
    if task_type == "HTTP":
        return _http_fields()
    if task_type == "SUB_WORKFLOW":
        return _sub_workflow_fields()
    if task_type == "DEPENDENT":
        return _dependent_fields()
    if task_type == "SWITCH":
        return _switch_fields()
    if task_type == "CONDITIONS":
        return _conditions_fields()
    return ()


def _script_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.rawScript",
            "string",
            required=True,
            active_when="required when task_params is used instead of command",
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description="Script body executed by the worker.",
        ),
        *_parameter_fields(),
        *_resource_fields(description="Attached DS resources used by the script."),
    )


def _remote_shell_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.rawScript",
            "string",
            required=True,
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description="Remote shell script body.",
        ),
        _FieldSpec(
            "task_params.type",
            "enum",
            default="SSH",
            choices=("SSH",),
            compile_path="taskDefinitionJson[].taskParams.type",
            description="Remote connection mode used by the DS plugin.",
        ),
        _FieldSpec(
            "task_params.datasource",
            "integer",
            required=True,
            choice_source="dsctl datasource list",
            related_commands=(
                "dsctl datasource list",
                "dsctl datasource get DATASOURCE",
                "dsctl datasource test DATASOURCE",
                "dsctl template datasource",
            ),
            compile_path="taskDefinitionJson[].taskParams.datasource",
            description="Datasource id containing remote shell connection settings.",
        ),
        *_parameter_fields(),
    )


def _sql_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.type",
            "enum",
            required=True,
            default="MYSQL",
            choice_source="dsctl enum list db-type",
            related_commands=(
                "dsctl enum list db-type",
                "dsctl template datasource",
                "dsctl template datasource --type TYPE",
            ),
            compile_path="taskDefinitionJson[].taskParams.type",
            description="Datasource type used by the SQL plugin.",
        ),
        _FieldSpec(
            "task_params.datasource",
            "integer",
            required=True,
            choice_source="dsctl datasource list",
            related_commands=(
                "dsctl datasource list",
                "dsctl datasource get DATASOURCE",
                "dsctl datasource test DATASOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.datasource",
            description="Datasource id.",
        ),
        _FieldSpec(
            "task_params.sql",
            "string",
            required=True,
            compile_path="taskDefinitionJson[].taskParams.sql",
            description="SQL text.",
        ),
        _FieldSpec(
            "task_params.sqlType",
            "integer",
            required=True,
            default=0,
            choices=("0", "1"),
            compile_path="taskDefinitionJson[].taskParams.sqlType",
            description="0=query statements that return rows; 1=non-query statements.",
        ),
        _FieldSpec(
            "task_params.sendEmail",
            "boolean",
            default=False,
            active_when="normally only meaningful when sqlType=0",
            compile_path="taskDefinitionJson[].taskParams.sendEmail",
            description="Ask DS to email query results.",
        ),
        _FieldSpec(
            "task_params.displayRows",
            "integer",
            default=10,
            active_when="normally only meaningful when sqlType=0",
            compile_path="taskDefinitionJson[].taskParams.displayRows",
            description="Maximum displayed result rows for query SQL.",
        ),
        _FieldSpec(
            "task_params.showType",
            "string",
            default="TABLE",
            active_when="normally only meaningful when sqlType=0",
            compile_path="taskDefinitionJson[].taskParams.showType",
            description="Result display type.",
        ),
        _FieldSpec(
            "task_params.connParams",
            "string",
            default="",
            compile_path="taskDefinitionJson[].taskParams.connParams",
            description="Datasource connection parameter override.",
        ),
        _FieldSpec(
            "task_params.preStatements[]",
            "string",
            default=(),
            compile_path="taskDefinitionJson[].taskParams.preStatements",
            description="Statements run before the main SQL.",
        ),
        _FieldSpec(
            "task_params.postStatements[]",
            "string",
            default=(),
            compile_path="taskDefinitionJson[].taskParams.postStatements",
            description="Statements run after the main SQL.",
        ),
        _FieldSpec(
            "task_params.groupId",
            "integer",
            default=0,
            active_when="required by DS email setup when sendEmail=true",
            choice_source="dsctl alert-group list",
            related_commands=(
                "dsctl alert-group list",
                "dsctl alert-group create --name NAME --instance-id ID",
            ),
            compile_path="taskDefinitionJson[].taskParams.groupId",
            description="Alert group id used for SQL result email.",
        ),
        _FieldSpec(
            "task_params.title",
            "string",
            default="",
            active_when="required by DS email setup when sendEmail=true",
            compile_path="taskDefinitionJson[].taskParams.title",
            description="Email title for SQL result notifications.",
        ),
        _FieldSpec(
            "task_params.limit",
            "integer",
            default=0,
            compile_path="taskDefinitionJson[].taskParams.limit",
            description="Optional DS SQL result limit.",
        ),
        *_parameter_fields(),
    )


def _http_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.url",
            "string",
            required=True,
            compile_path="taskDefinitionJson[].taskParams.url",
            description="HTTP URL.",
        ),
        _FieldSpec(
            "task_params.httpMethod",
            "enum",
            required=True,
            choices=_enum_values(HttpRequestMethod),
            compile_path="taskDefinitionJson[].taskParams.httpMethod",
            description="HTTP request method.",
        ),
        _FieldSpec(
            "task_params.httpParams[]",
            "object",
            default=(),
            compile_path="taskDefinitionJson[].taskParams.httpParams",
            description="HTTP query parameters or headers.",
        ),
        _FieldSpec(
            "task_params.httpParams[].prop",
            "string",
            compile_path="taskDefinitionJson[].taskParams.httpParams[].prop",
            description="HTTP parameter or header name.",
        ),
        _FieldSpec(
            "task_params.httpParams[].httpParametersType",
            "enum",
            choices=_enum_values(HttpParametersType),
            compile_path="taskDefinitionJson[].taskParams.httpParams[].httpParametersType",
            description="Whether an item is a query parameter or header.",
        ),
        _FieldSpec(
            "task_params.httpParams[].value",
            "string",
            compile_path="taskDefinitionJson[].taskParams.httpParams[].value",
            description="HTTP parameter or header value.",
        ),
        _FieldSpec(
            "task_params.httpBody",
            "string",
            default="",
            active_when="usually used with POST or PUT",
            compile_path="taskDefinitionJson[].taskParams.httpBody",
            description="HTTP request body.",
        ),
        _FieldSpec(
            "task_params.httpCheckCondition",
            "enum",
            default=HttpCheckCondition.STATUS_CODE_DEFAULT.value,
            choices=_enum_values(HttpCheckCondition),
            compile_path="taskDefinitionJson[].taskParams.httpCheckCondition",
            description="HTTP success check strategy.",
        ),
        _FieldSpec(
            "task_params.condition",
            "string",
            default="",
            active_when="used when httpCheckCondition requires a custom condition",
            compile_path="taskDefinitionJson[].taskParams.condition",
            description="Custom HTTP check expression.",
        ),
        _FieldSpec(
            "task_params.connectTimeout",
            "integer",
            required=True,
            default=10000,
            compile_path="taskDefinitionJson[].taskParams.connectTimeout",
            description="Connection timeout in milliseconds.",
        ),
        *_parameter_fields(),
    )


def _sub_workflow_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.workflowDefinitionCode",
            "integer",
            required=True,
            choice_source="dsctl workflow list",
            related_commands=(
                "dsctl workflow list --project PROJECT",
                "dsctl workflow get WORKFLOW --project PROJECT",
            ),
            compile_path="taskDefinitionJson[].taskParams.workflowDefinitionCode",
            description="Child workflow definition code.",
        ),
        *_parameter_fields(include_resources=True),
    )


def _dependent_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.dependence.relation",
            "enum",
            required=True,
            choices=_enum_values(DependentRelation),
            compile_path="taskDefinitionJson[].taskParams.dependence.relation",
            description="Top-level dependency relation.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[]",
            "object",
            required=True,
            compile_path="taskDefinitionJson[].taskParams.dependence.dependTaskList",
            description="Dependency branch groups.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].relation",
            "enum",
            required=True,
            choices=_enum_values(DependentRelation),
            compile_path=(
                "taskDefinitionJson[].taskParams.dependence.dependTaskList[].relation"
            ),
            description="Relation inside one branch group.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[]",
            "object",
            required=True,
            compile_path=(
                "taskDefinitionJson[].taskParams.dependence."
                "dependTaskList[].dependItemList"
            ),
            description="One upstream workflow or task dependency.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].dependentType",
            "enum",
            required=True,
            choices=_enum_values(DependentType),
            description="Whether this item targets a workflow or a task.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].projectCode",
            "integer",
            required=True,
            choice_source="dsctl project list",
            related_commands=(
                "dsctl project list",
                "dsctl project get PROJECT",
            ),
            description="Upstream project code.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].definitionCode",
            "integer",
            required=True,
            choice_source="dsctl workflow list --project PROJECT",
            related_commands=(
                "dsctl workflow list --project PROJECT",
                "dsctl workflow get WORKFLOW --project PROJECT",
            ),
            description="Upstream workflow definition code.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].depTaskCode",
            "integer",
            required=True,
            choice_source="dsctl task list --project PROJECT --workflow WORKFLOW",
            related_commands=(
                "dsctl task list --project PROJECT --workflow WORKFLOW",
                "dsctl task get TASK --project PROJECT --workflow WORKFLOW",
            ),
            description="Upstream task code, or 0 when targeting the workflow.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].cycle",
            "enum",
            required=True,
            choices=_DEPENDENT_CYCLE_VALUES,
            description="DS dependency cycle such as day.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].dateValue",
            "enum",
            required=True,
            choices=_DEPENDENT_DATE_VALUE_CHOICES,
            active_when="valid values depend on cycle; see state_rules",
            description="DS dependency date window such as today or last1Days.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].dependResult",
            "enum",
            choices=_enum_values(DependResult),
            description="Expected upstream result state.",
        ),
        _FieldSpec(
            "task_params.dependence.checkInterval",
            "integer",
            default=10,
            description="Dependency check interval in seconds.",
        ),
        _FieldSpec(
            "task_params.dependence.failurePolicy",
            "enum",
            choices=_enum_values(DependentFailurePolicy),
            description="Failure behavior while waiting on dependencies.",
        ),
        _FieldSpec(
            "task_params.dependence.failureWaitingTime",
            "integer",
            active_when="used when failurePolicy=DEPENDENT_FAILURE_WAITING",
            description="Maximum waiting time for the waiting failure policy.",
        ),
        *_parameter_fields(include_resources=True),
    )


def _switch_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.switchResult.dependTaskList[]",
            "object",
            default=(),
            compile_path="taskDefinitionJson[].taskParams.switchResult.dependTaskList",
            description="Ordered conditional branches.",
        ),
        _FieldSpec(
            "task_params.switchResult.dependTaskList[].condition",
            "string",
            required=True,
            description="Branch condition expression.",
        ),
        _FieldSpec(
            "task_params.switchResult.dependTaskList[].nextNode",
            "string",
            required=True,
            choice_source="other tasks in the same workflow YAML",
            compile_path=(
                "taskDefinitionJson[].taskParams.switchResult.dependTaskList[].nextNode"
            ),
            description="Downstream task name for this branch; compiled to task code.",
        ),
        _FieldSpec(
            "task_params.switchResult.nextNode",
            "string",
            choice_source="other tasks in the same workflow YAML",
            compile_path="taskDefinitionJson[].taskParams.switchResult.nextNode",
            description="Default downstream task name; compiled to task code.",
        ),
        *_parameter_fields(),
    )


def _conditions_fields() -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.dependence.relation",
            "enum",
            required=True,
            choices=_enum_values(DependentRelation),
            description="Top-level relation for upstream status checks.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[]",
            "object",
            required=True,
            description="Groups of upstream task status checks.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].relation",
            "enum",
            required=True,
            choices=_enum_values(DependentRelation),
            description="Relation inside one upstream status-check group.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[]",
            "object",
            required=True,
            description="One upstream workflow or task status check.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].dependentType",
            "enum",
            required=True,
            choices=_enum_values(DependentType),
            description="Whether this item targets a workflow or a task.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].projectCode",
            "integer",
            required=True,
            choice_source="dsctl project list",
            related_commands=(
                "dsctl project list",
                "dsctl project get PROJECT",
            ),
            description="Upstream project code.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].definitionCode",
            "integer",
            required=True,
            choice_source="dsctl workflow list --project PROJECT",
            related_commands=(
                "dsctl workflow list --project PROJECT",
                "dsctl workflow get WORKFLOW --project PROJECT",
            ),
            description="Upstream workflow definition code.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].depTaskCode",
            "integer",
            required=True,
            choice_source="dsctl task list --project PROJECT --workflow WORKFLOW",
            related_commands=(
                "dsctl task list --project PROJECT --workflow WORKFLOW",
                "dsctl task get TASK --project PROJECT --workflow WORKFLOW",
            ),
            description="Upstream task code, or 0 when targeting the workflow.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].cycle",
            "enum",
            required=True,
            choices=_DEPENDENT_CYCLE_VALUES,
            description="DS dependency cycle such as day.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].dateValue",
            "enum",
            required=True,
            choices=_DEPENDENT_DATE_VALUE_CHOICES,
            active_when="valid values depend on cycle; see state_rules",
            description="DS dependency date window such as today or last1Days.",
        ),
        _FieldSpec(
            "task_params.dependence.dependTaskList[].dependItemList[].status",
            "enum",
            required=True,
            choices=_enum_values(TaskExecutionStatus),
            description="Required upstream task execution state.",
        ),
        _FieldSpec(
            "task_params.conditionResult.successNode[]",
            "string",
            required=True,
            choice_source="other tasks in the same workflow YAML",
            compile_path="taskDefinitionJson[].taskParams.conditionResult.successNode",
            description="Downstream task names when conditions succeed.",
        ),
        _FieldSpec(
            "task_params.conditionResult.failedNode[]",
            "string",
            required=True,
            choice_source="other tasks in the same workflow YAML",
            compile_path="taskDefinitionJson[].taskParams.conditionResult.failedNode",
            description="Downstream task names when conditions fail.",
        ),
        *_parameter_fields(),
    )


def _resource_fields(
    *, description: str = "Attached DS resources."
) -> tuple[_FieldSpec, ...]:
    return (
        _FieldSpec(
            "task_params.resourceList[]",
            "object",
            default=(),
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
            ),
            compile_path="taskDefinitionJson[].taskParams.resourceList",
            description=description,
        ),
        _FieldSpec(
            "task_params.resourceList[].resourceName",
            "string",
            choice_source="dsctl resource list --dir DIR",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.resourceList[].resourceName",
            description=(
                "DS resource fullName path stored as ResourceInfo.resourceName."
            ),
        ),
    )


def _parameter_fields(*, include_resources: bool = False) -> tuple[_FieldSpec, ...]:
    fields = [
        _FieldSpec(
            "task_params.localParams[]",
            "object",
            default=(),
            related_commands=(
                "dsctl template params --topic property",
                "dsctl template params --topic built-in",
                "dsctl template params --topic output",
            ),
            compile_path="taskDefinitionJson[].taskParams.localParams",
            description="Task-local DS Property entries.",
        ),
        _FieldSpec(
            "task_params.localParams[].prop",
            "string",
            compile_path="taskDefinitionJson[].taskParams.localParams[].prop",
            description="Parameter name referenced as ${name} at runtime.",
        ),
        _FieldSpec(
            "task_params.localParams[].direct",
            "enum",
            choices=_enum_values(Direct),
            compile_path="taskDefinitionJson[].taskParams.localParams[].direct",
            description="Parameter direction.",
        ),
        _FieldSpec(
            "task_params.localParams[].type",
            "enum",
            choices=_enum_values(DataType),
            compile_path="taskDefinitionJson[].taskParams.localParams[].type",
            description="Parameter data type.",
        ),
        _FieldSpec(
            "task_params.localParams[].value",
            "string",
            compile_path="taskDefinitionJson[].taskParams.localParams[].value",
            description="Optional parameter value or DS expression.",
        ),
        _FieldSpec(
            "task_params.varPool[]",
            "object",
            default=(),
            related_commands=("dsctl template params --topic output",),
            compile_path="taskDefinitionJson[].taskParams.varPool",
            description=(
                "Runtime output parameter pool; usually empty in authored YAML."
            ),
        ),
    ]
    if include_resources:
        fields.extend(_resource_fields())
    return tuple(fields)


def _state_rules_for(task_type: str) -> list[TaskAuthoringStateRuleData]:
    if task_type == "SQL":
        return [
            {
                "when": "task_params.sqlType == 0",
                "condition_paths": ["task_params.sqlType"],
                "active_paths": [
                    "task_params.sendEmail",
                    "task_params.displayRows",
                    "task_params.showType",
                    "task_params.groupId",
                    "task_params.title",
                ],
                "inactive_paths": [],
                "compile_policy": {},
                "description": "Query SQL may produce displayable rows and OUT params.",
            },
            {
                "when": "task_params.sqlType == 1",
                "condition_paths": ["task_params.sqlType"],
                "active_paths": [
                    "task_params.preStatements",
                    "task_params.postStatements",
                ],
                "inactive_paths": [
                    "task_params.displayRows",
                    "task_params.showType",
                    "task_params.groupId",
                    "task_params.title",
                ],
                "compile_policy": {
                    "task_params.sendEmail": "prefer false",
                    "task_params.localParams": "send [] when absent",
                    "task_params.varPool": "send [] when absent",
                    "task_params.preStatements": "send [] when absent",
                    "task_params.postStatements": "send [] when absent",
                },
                "description": (
                    "Non-query SQL is for DDL/DML statements; do not model it "
                    "as a result-set query."
                ),
            },
        ]
    if task_type in {"DEPENDENT", "CONDITIONS"}:
        return [
            {
                "when": "dependItem.cycle == hour",
                "condition_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].cycle"
                ],
                "active_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
                ],
                "inactive_paths": [],
                "compile_policy": {
                    "dateValue choices": ", ".join(
                        _DEPENDENT_DATE_VALUES_BY_CYCLE["hour"]
                    )
                },
                "description": "Hourly dependency windows.",
            },
            {
                "when": "dependItem.cycle == day",
                "condition_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].cycle"
                ],
                "active_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
                ],
                "inactive_paths": [],
                "compile_policy": {
                    "dateValue choices": ", ".join(
                        _DEPENDENT_DATE_VALUES_BY_CYCLE["day"]
                    )
                },
                "description": "Daily dependency windows.",
            },
            {
                "when": "dependItem.cycle == week",
                "condition_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].cycle"
                ],
                "active_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
                ],
                "inactive_paths": [],
                "compile_policy": {
                    "dateValue choices": ", ".join(
                        _DEPENDENT_DATE_VALUES_BY_CYCLE["week"]
                    )
                },
                "description": "Weekly dependency windows.",
            },
            {
                "when": "dependItem.cycle == month",
                "condition_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].cycle"
                ],
                "active_paths": [
                    "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
                ],
                "inactive_paths": [],
                "compile_policy": {
                    "dateValue choices": ", ".join(
                        _DEPENDENT_DATE_VALUES_BY_CYCLE["month"]
                    )
                },
                "description": "Monthly dependency windows.",
            },
        ]
    if task_type in COMMAND_TASK_TYPES:
        return [
            {
                "when": "command is set",
                "condition_paths": ["command"],
                "active_paths": ["command"],
                "inactive_paths": ["task_params"],
                "compile_policy": {
                    "command": "compile to task_params.rawScript",
                    "task_params.localParams": "send []",
                    "task_params.resourceList": "send []",
                },
                "description": "Command shorthand is for simple script-like tasks.",
            },
            {
                "when": "task_params is set",
                "condition_paths": ["task_params"],
                "active_paths": ["task_params"],
                "inactive_paths": ["command"],
                "compile_policy": {},
                "description": (
                    "Use task_params for resources, localParams, and plugin fields."
                ),
            },
        ]
    return []


def _choice_sources_for(task_type: str) -> list[TaskAuthoringChoiceSourceData]:
    return _choice_sources_from_fields(_fields_for(task_type))


def _choice_sources_from_fields(
    fields: Sequence[TaskAuthoringFieldData],
) -> list[TaskAuthoringChoiceSourceData]:
    rows: list[TaskAuthoringChoiceSourceData] = []
    seen: set[str] = set()
    for field in fields:
        command = field.get("choice_source")
        if not isinstance(command, str):
            continue
        path = field["path"]
        if path in seen:
            continue
        seen.add(path)
        row: TaskAuthoringChoiceSourceData = {
            "path": path,
            "command": command,
            "description": _choice_source_description(path, command),
        }
        choice_value = field.get("choice_value")
        if isinstance(choice_value, str):
            row["value"] = choice_value
        related_commands = field.get("related_commands")
        if isinstance(related_commands, list) and related_commands:
            row["related_commands"] = related_commands
        rows.append(row)
    return rows


def _choice_source_value(path: str, command: str) -> str | None:
    if "same workflow YAML" in command:
        return "task.name"
    suffix_values = {
        "worker_group": "name",
        "resourceName": "fullName",
        "environment_code": "code",
        "task_group_id": "id",
        "groupId": "id",
        "datasource": "id",
        "workflowDefinitionCode": "code",
        "projectCode": "code",
        "definitionCode": "code",
        "depTaskCode": "code",
    }
    for suffix, value in suffix_values.items():
        if path.endswith(suffix):
            return value
    if "enum list" in command:
        return "name"
    return None


def _choice_source_description(path: str, command: str) -> str:
    if "same workflow YAML" in command:
        return "Choose from other task names in the current workflow YAML."
    if path.endswith("resourceName"):
        return (
            f"Run `{command}` and use `fullName` as "
            f"{path}; upload the file first when it is missing."
        )
    if path.endswith("depTaskCode"):
        return (
            f"Run `{command}` and use the task `code`; use 0 when "
            "dependentType targets the whole workflow."
        )
    return f"Run `{command}` and use the indicated value for {path}."


def _compile_mappings_from_fields(
    fields: Sequence[TaskAuthoringFieldData],
) -> list[TaskAuthoringCompileMappingData]:
    mappings: dict[str, str] = {}
    for field in fields:
        compile_path = field.get("compile_path")
        if isinstance(compile_path, str):
            mappings[field["path"]] = compile_path
    return [
        {
            "authoring_path": authoring_path,
            "ds_payload_path": ds_payload_path,
            "description": (_COMPILE_MAPPING_DESCRIPTION),
        }
        for authoring_path, ds_payload_path in mappings.items()
    ]


def _json_schema_for(
    task_type: str,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> JsonObject:
    task_params_schema = _task_params_json_schema(task_type)
    properties = _top_level_json_schema_properties(fields)
    properties["type"] = {"const": task_type, "type": "string"}
    properties["task_params"] = {"$ref": "#/$defs/task_params"}
    required = ["name", "type"]
    if task_type not in COMMAND_TASK_TYPES:
        required.append("task_params")
    schema: JsonObject = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{task_type} task authoring schema",
        "type": "object",
        "properties": cast("JsonValue", properties),
        "required": required,
        "$defs": {
            "task_params": task_params_schema,
        },
        "x-dsctl": {
            "task_type": task_type,
            "template_command": f"dsctl template task {task_type}",
            "raw_template_command": f"dsctl template task {task_type} --raw",
            "lint_command": "dsctl lint workflow FILE",
        },
    }
    if task_type in COMMAND_TASK_TYPES:
        schema["oneOf"] = [
            {"required": ["command"], "not": {"required": ["task_params"]}},
            {"required": ["task_params"], "not": {"required": ["command"]}},
        ]
    return require_json_object(schema, label="task authoring json schema")


def _task_params_json_schema(task_type: str) -> JsonObject:
    model = task_params_model_for_type(task_type)
    if model is None:
        return {
            "type": "object",
            "additionalProperties": True,
            "description": "Generic DS-native task_params object for this plugin.",
        }
    schema = model.model_json_schema(
        by_alias=True,
        ref_template="#/$defs/task_params/$defs/{model}",
    )
    return require_json_object(schema, label=f"{task_type} task params schema")


def _field_json_schema(field: TaskAuthoringFieldData) -> JsonObject:
    schema: JsonObject = {"description": field["description"]}
    field_type = field["type"]
    if field_type in {"string", "integer", "boolean", "object"}:
        schema["type"] = field_type
    elif field_type == "enum":
        schema["type"] = "string"
    elif field_type.startswith("list"):
        schema["type"] = "array"
    if "choices" in field:
        schema["enum"] = list(field["choices"])
    if "default" in field:
        schema["default"] = field["default"]
    metadata: JsonObject = {}
    for key in (
        "active_when",
        "choice_source",
        "choice_value",
        "related_commands",
        "compile_path",
    ):
        value = field.get(key)
        if value is not None:
            metadata[key] = require_json_value(
                value,
                label=f"task authoring field metadata {field['path']}.{key}",
            )
    if metadata:
        schema["x-dsctl"] = metadata
    return schema


def _top_level_json_schema_properties(
    fields: Sequence[TaskAuthoringFieldData],
) -> JsonObject:
    properties: JsonObject = {}
    for field in fields:
        path = field["path"]
        if path == "task_params" or path.startswith("task_params."):
            continue
        _insert_authoring_field_schema(properties, path=path, field=field)
    return properties


def _insert_authoring_field_schema(
    properties: JsonObject,
    *,
    path: str,
    field: TaskAuthoringFieldData,
) -> None:
    current = properties
    segments = path.split(".")
    for index, segment in enumerate(segments):
        is_array = segment.endswith("[]")
        name = segment.removesuffix("[]")
        if index == len(segments) - 1:
            current[name] = (
                _array_field_json_schema(field)
                if is_array
                else _field_json_schema(field)
            )
            return
        current = _nested_authoring_properties(
            current,
            name=name,
            is_array=is_array,
            path=path,
        )


def _nested_authoring_properties(
    properties: JsonObject,
    *,
    name: str,
    is_array: bool,
    path: str,
) -> JsonObject:
    existing = properties.get(name)
    if existing is None:
        nested: JsonObject = {"type": "object", "properties": {}}
        node: JsonObject = {"type": "array", "items": nested} if is_array else nested
        properties[name] = node
    elif isinstance(existing, dict):
        node = existing
    else:
        message = f"task authoring schema path conflicts at {path}"
        raise TypeError(message)

    target = node.get("items") if is_array else node
    if not isinstance(target, dict):
        message = f"task authoring schema path has invalid container at {path}"
        raise TypeError(message)
    nested_properties = target.get("properties")
    if not isinstance(nested_properties, dict):
        message = f"task authoring schema path has no object properties at {path}"
        raise TypeError(message)
    return nested_properties


def _array_field_json_schema(field: TaskAuthoringFieldData) -> JsonObject:
    item_schema = _field_json_schema(field)
    array_schema: JsonObject = {
        "type": "array",
        "items": item_schema,
    }
    for key in ("description", "default", "x-dsctl"):
        value = item_schema.pop(key, None)
        if value is not None:
            if key == "default" and isinstance(value, tuple):
                value = list(value)
            array_schema[key] = value
    return array_schema


def _summary_rows(task_type: str) -> list[TaskTypeSummaryRowData]:
    metadata = _task_templates.task_template_metadata()[task_type]
    rows: list[TaskTypeSummaryRowData] = [
        {
            "kind": "command",
            "name": "schema",
            "summary": "Bounded field contract, state rules, and value discovery.",
            "command": f"dsctl task-type schema {task_type}",
        },
        {
            "kind": "command",
            "name": "json-schema",
            "summary": "Nested validation schema without repeated authoring metadata.",
            "command": f"dsctl task-type schema {task_type} --json-schema",
        },
        {
            "kind": "command",
            "name": "compile-mappings",
            "summary": "Authoring paths mapped to DS REST payload paths.",
            "command": f"dsctl task-type schema {task_type} --compile-mappings",
        },
        {
            "kind": "command",
            "name": "full-schema",
            "summary": "Expanded compatibility contract for audits and generators.",
            "command": f"dsctl task-type schema {task_type} --full",
        },
        {
            "kind": "command",
            "name": "template",
            "summary": "Default task YAML fragment.",
            "command": f"dsctl template task {task_type}",
        },
        {
            "kind": "command",
            "name": "raw-template",
            "summary": "Copyable YAML fragment without the JSON envelope.",
            "command": f"dsctl template task {task_type} --raw",
        },
    ]
    rows.extend(
        {
            "kind": "variant",
            "name": variant,
            "summary": metadata["variant_summaries"][variant],
            "command": f"dsctl template task {task_type} --variant {variant}",
        }
        for variant in metadata["variants"]
    )
    return rows


def _generic_task_warnings(
    task_type: str,
) -> tuple[list[str], list[JsonObject]]:
    if _task_templates.task_template_kind(task_type) != "generic":
        return [], []
    message = (
        f"{task_type} has a generic task_params template; inspect upstream plugin "
        "payloads or an exported workflow before production use."
    )
    return [
        message,
    ], [
        {
            "code": "generic_task_template",
            "task_type": task_type,
            "message": message,
        }
    ]


def _enum_values(enum_type: type[Enum]) -> tuple[str, ...]:
    values: list[str] = []
    for item in enum_type:
        value = getattr(item, "value", item)
        values.append(str(value))
    return tuple(values)


__all__ = [
    "TaskAuthoringChoiceSourceData",
    "TaskAuthoringCompileMappingData",
    "TaskAuthoringFieldData",
    "TaskAuthoringStateRuleData",
    "TaskTypeAuthoringSchemaData",
    "TaskTypeSummaryData",
    "require_supported_authoring_task_type",
    "task_type_schema_result",
    "task_type_summary_data",
    "task_type_summary_result",
]
