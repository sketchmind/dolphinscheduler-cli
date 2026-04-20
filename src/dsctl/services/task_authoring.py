from __future__ import annotations

from dataclasses import dataclass
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
    related_commands: list[str]
    compile_path: str
    description: str


class TaskAuthoringStateRuleData(TypedDict, total=False):
    """One task-type-specific field state rule."""

    when: str
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
    template_command: str
    raw_template_command: str
    schema_command: str
    template_index_command: str
    parameter_command: str
    choice_sources: list[TaskAuthoringChoiceSourceData]
    workflow_usage: dict[str, str]
    rows: list[TaskTypeSummaryRowData]


class TaskTypeAuthoringSchemaData(TypedDict):
    """Full local authoring contract for one DS task type."""

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
    rows: list[TaskAuthoringFieldData]


class _MissingDefault:
    """Sentinel for field descriptors without a default value."""


_MISSING = _MissingDefault()
_COMMAND_TASK_TYPES = frozenset({"PYTHON", "REMOTESHELL", "SHELL"})
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


def task_type_schema_result(task_type: str) -> CommandResult:
    """Return the full local authoring schema for one DS task type."""
    normalized = require_supported_authoring_task_type(task_type)
    data = task_type_schema_data(normalized)
    warnings, warning_details = _generic_task_warnings(normalized)
    return CommandResult(
        data=require_json_object(data, label="task type authoring schema data"),
        resolved={"task_type": normalized},
        warnings=warnings,
        warning_details=warning_details,
    )


def task_type_summary_data(task_type: str) -> TaskTypeSummaryData:
    """Build the compact task authoring summary for one supported task type."""
    normalized = require_supported_authoring_task_type(task_type)
    metadata = _task_templates.task_template_metadata()[normalized]
    required_paths = [
        field["path"]
        for field in _fields_for(normalized)
        if field.get("required") is True
    ]
    return TaskTypeSummaryData(
        task_type=normalized,
        category=metadata["category"],
        kind=metadata["kind"],
        default_variant=metadata["default_variant"],
        variants=metadata["variants"],
        payload_modes=metadata["payload_modes"],
        required_paths=required_paths,
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


def task_type_schema_data(task_type: str) -> TaskTypeAuthoringSchemaData:
    """Build the full task authoring contract for one supported task type."""
    normalized = require_supported_authoring_task_type(task_type)
    metadata = _task_templates.task_template_metadata()[normalized]
    fields = _fields_for(normalized)
    return TaskTypeAuthoringSchemaData(
        task_type=normalized,
        category=metadata["category"],
        kind=metadata["kind"],
        schema=_json_schema_for(normalized, fields=fields),
        fields=fields,
        state_rules=_state_rules_for(normalized),
        choice_sources=_choice_sources_for(normalized),
        compile_mappings=_compile_mappings_for(normalized),
        template_command=f"dsctl template task {normalized}",
        raw_template_command=f"dsctl template task {normalized} --raw",
        rows=fields,
    )


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
    return [
        field.to_data()
        for field in (*_common_fields(task_type), *_task_specific_fields(task_type))
    ]


def _common_fields(task_type: str) -> tuple[_FieldSpec, ...]:
    payload_rule = (
        "required when command is absent"
        if task_type in _COMMAND_TASK_TYPES
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
            required=task_type not in _COMMAND_TASK_TYPES,
            active_when=payload_rule,
            compile_path="taskDefinitionJson[].taskParams",
            description="DS task plugin payload for this task type.",
        ),
    ]
    if task_type in _COMMAND_TASK_TYPES:
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
            "string",
            default="SSH",
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
            choice_source="dsctl template params --topic property",
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
    if task_type in _COMMAND_TASK_TYPES:
        return [
            {
                "when": "command is set",
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
    rows: list[TaskAuthoringChoiceSourceData] = []
    seen: set[str] = set()
    for field in _fields_for(task_type):
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
            "value": _choice_source_value(path, command),
            "description": _choice_source_description(path, command),
        }
        related_commands = field.get("related_commands")
        if isinstance(related_commands, list) and related_commands:
            row["related_commands"] = related_commands
        rows.append(row)
    return rows


def _choice_source_value(path: str, command: str) -> str:
    if "same workflow YAML" in command:
        return "task.name"
    suffix_values = {
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
    return "name"


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


def _compile_mappings_for(task_type: str) -> list[TaskAuthoringCompileMappingData]:
    mappings: dict[str, str] = {}
    for field in _fields_for(task_type):
        compile_path = field.get("compile_path")
        if isinstance(compile_path, str):
            mappings[field["path"]] = compile_path
    return [
        {
            "authoring_path": authoring_path,
            "ds_payload_path": ds_payload_path,
            "description": (
                "Compiled by workflow create/edit before sending DS REST form fields."
            ),
        }
        for authoring_path, ds_payload_path in mappings.items()
    ]


def _json_schema_for(
    task_type: str,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> JsonObject:
    task_params_schema = _task_params_json_schema(task_type)
    properties = {
        _top_level_property_name(field["path"]): _field_json_schema(field)
        for field in fields
        if not field["path"].startswith("task_params.")
    }
    properties["type"] = {"const": task_type, "type": "string"}
    properties["task_params"] = {"$ref": "#/$defs/task_params"}
    required = ["name", "type"]
    if task_type not in _COMMAND_TASK_TYPES:
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
            "state_rules": cast("JsonValue", _state_rules_for(task_type)),
            "choice_sources": cast("JsonValue", _choice_sources_for(task_type)),
            "compile_mappings": cast("JsonValue", _compile_mappings_for(task_type)),
        },
    }
    if task_type in _COMMAND_TASK_TYPES:
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
    schema = model.model_json_schema(by_alias=True)
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
    for key in ("active_when", "choice_source", "related_commands", "compile_path"):
        value = field.get(key)
        if value is not None:
            metadata[key] = require_json_value(
                value,
                label=f"task authoring field metadata {field['path']}.{key}",
            )
    if metadata:
        schema["x-dsctl"] = metadata
    return schema


def _top_level_property_name(path: str) -> str:
    return path.split(".", maxsplit=1)[0].removesuffix("[]")


def _summary_rows(task_type: str) -> list[TaskTypeSummaryRowData]:
    metadata = _task_templates.task_template_metadata()[task_type]
    rows: list[TaskTypeSummaryRowData] = [
        {
            "kind": "command",
            "name": "schema",
            "summary": (
                "Full field contract, state rules, choices, and compile mapping."
            ),
            "command": f"dsctl task-type schema {task_type}",
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
    "task_type_schema_data",
    "task_type_schema_result",
    "task_type_summary_data",
    "task_type_summary_result",
]
