from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import yaml
from pydantic import (
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from dsctl.models.common import (
    FailureStrategy,
    GlobalParamSpec,
    ModelValidationError,
    Priority,
    ReleaseState,
    RetrySpec,
    WorkflowExecutionType,
    YamlObject,
    YamlSpecModel,
    YamlValue,
    is_yaml_object,
    model_validation_issues,
    yaml_value_validation_issue,
)
from dsctl.models.task_spec import (
    TaskRunFlag,
    TaskTimeoutNotifyStrategy,
    canonical_task_type,
    normalize_task_params,
    normalize_task_run_flag,
)
from dsctl.support.quartz import normalize_quartz_cron_text

if TYPE_CHECKING:
    from pathlib import Path

    from pydantic import SerializerFunctionWrapHandler

COMMAND_TASK_TYPES = frozenset({"PYTHON", "SHELL"})
_AUTHORING_CONTEXT_KEY = "dsctl_workflow_authoring"

TaskParamsNormalizer = Callable[[str, YamlObject], YamlObject]
TaskTypeAuthorizer = Callable[[str], None]
TaskIdentityValidator = Callable[[str, str], None]
GlobalParamsValidator = Callable[[list[GlobalParamSpec]], None]


@dataclass(frozen=True, slots=True)
class WorkflowAuthoringContext:
    """Explicit task-parameter authority for one workflow parse operation."""

    authorize_task_type: TaskTypeAuthorizer
    validate_task_identity: TaskIdentityValidator
    normalize_task_params: TaskParamsNormalizer
    validate_global_params: GlobalParamsValidator
    schedule_timezone_supported: bool = True
    schedule_missed_fire_policy_choices: tuple[str, ...] = ()
    defer_graph_validation: bool = False


class WorkflowMetadataSpec(YamlSpecModel):
    """Workflow-level YAML fields used by create and export."""

    name: str
    project: str | None = None
    description: str | None = None
    timeout: int = Field(default=0, ge=0)
    global_params: dict[str, str | None] | list[GlobalParamSpec] | None = None
    execution_type: WorkflowExecutionType = WorkflowExecutionType.PARALLEL
    release_state: ReleaseState = ReleaseState.OFFLINE

    @field_validator("name", "project")
    @classmethod
    def validate_non_empty_text(cls, value: str | None) -> str | None:
        """Reject empty workflow text fields after trimming whitespace."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Workflow text fields must not be empty"
            raise ValueError(message)
        return normalized


class WorkflowTaskSpec(YamlSpecModel):
    """One task entry in workflow YAML."""

    name: str
    type: str
    description: str | None = None
    task_params: YamlObject | None = None
    command: str | None = None
    flag: TaskRunFlag = TaskRunFlag.YES
    worker_group: str | None = None
    environment_code: int | None = Field(default=None, ge=1)
    task_group_id: int | None = Field(default=None, ge=1)
    task_group_priority: int | None = Field(default=None, ge=0)
    priority: Priority = Priority.MEDIUM
    retry: RetrySpec = Field(default_factory=RetrySpec)
    timeout_notify_strategy: TaskTimeoutNotifyStrategy | None = None
    timeout: int = Field(default=0, ge=0)
    delay: int = Field(default=0, ge=0)
    cpu_quota: int | None = Field(default=None, ge=-1)
    memory_max: int | None = Field(default=None, ge=-1)
    depends_on: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def validate_system_managed_fields(cls, value: YamlValue) -> YamlValue:
        """Reject DS-managed task identity fields from authored workflow YAML."""
        if not isinstance(value, Mapping):
            return value
        for field_name in ("code", "version"):
            if field_name in value:
                message = (
                    f"Task field '{field_name}' is system-managed and cannot be set "
                    "in workflow YAML"
                )
                raise ValueError(message)
        return value

    @field_validator("name", "worker_group")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject empty task text fields after trimming whitespace."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized

    @field_validator("flag", mode="before")
    @classmethod
    def validate_run_flag(cls, value: YamlValue) -> YamlValue:
        """Normalize one YAML task flag before enum validation."""
        return normalize_task_run_flag(value)

    @field_validator("type")
    @classmethod
    def validate_task_type(cls, value: str) -> str:
        """Normalize one task type to the DS-native canonical value."""
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return canonical_task_type(normalized)

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, value: list[str]) -> list[str]:
        """Normalize task dependencies and reject duplicates or blanks."""
        normalized: list[str] = []
        seen: set[str] = set()
        for dependency in value:
            candidate = dependency.strip()
            if not candidate:
                message = "Task dependencies must not contain empty names"
                raise ValueError(message)
            if candidate in seen:
                message = f"Task dependency '{candidate}' is duplicated"
                raise ValueError(message)
            seen.add(candidate)
            normalized.append(candidate)
        return normalized

    @model_validator(mode="after")
    def validate_task_payload(self, info: ValidationInfo) -> WorkflowTaskSpec:
        """Require one task payload source and validate command shorthands."""
        if self.task_params is None and self.command is None:
            message = f"Task '{self.name}' must define either task_params or command"
            raise ValueError(message)
        if self.task_params is not None and self.command is not None:
            message = f"Task '{self.name}' cannot define both task_params and command"
            raise ValueError(message)
        if self.command is not None and self.type == "REMOTESHELL":
            message = (
                f"Task '{self.name}' cannot use command shorthand for REMOTESHELL; "
                "define task_params.rawScript and task_params.datasource instead"
            )
            raise ValueError(message)
        if self.command is not None and self.type.upper() not in COMMAND_TASK_TYPES:
            message = (
                f"Task '{self.name}' only supports command shorthand for "
                "SHELL and PYTHON types"
            )
            raise ValueError(message)
        _authorize_task_type(info, self.type)
        _validate_task_identity(info, self.type, self.name)
        if self.task_group_priority is not None and self.task_group_id is None:
            message = (
                f"Task '{self.name}' requires task_group_id when "
                "task_group_priority is set"
            )
            raise ValueError(message)
        if self.timeout == 0 and self.timeout_notify_strategy is not None:
            message = (
                f"Task '{self.name}' requires timeout > 0 when "
                "timeout_notify_strategy is set"
            )
            raise ValueError(message)
        if self.task_params is not None:
            try:
                normalizer = _task_params_normalizer(info)
                self.task_params = normalizer(self.type, self.task_params)
            except ValueError as exc:
                message = f"Task '{self.name}' {exc}"
                raise ValueError(message) from exc
        authoring_context = _workflow_authoring_context_from_info(info)
        if self.name in self.depends_on and not (
            authoring_context is not None and authoring_context.defer_graph_validation
        ):
            message = f"Task '{self.name}' cannot depend on itself"
            raise ValueError(message)
        return self


class WorkflowScheduleSpec(YamlSpecModel):
    """Optional schedule block accepted by the YAML parser."""

    cron: str
    timezone: str | None = None
    start: str
    end: str
    failure_strategy: FailureStrategy | None = None
    priority: Priority | None = None
    release_state: ReleaseState | None = None
    enabled: bool | None = None
    missed_fire_policy: str | None = None

    @field_validator("cron", "start", "end")
    @classmethod
    def validate_schedule_text(cls, value: str) -> str:
        """Reject empty schedule text fields after trimming whitespace."""
        normalized = value.strip()
        if not normalized:
            message = "Schedule text fields must not be empty"
            raise ValueError(message)
        return normalized

    @model_serializer(mode="wrap")
    def serialize_policy_omission(
        self, handler: SerializerFunctionWrapHandler
    ) -> YamlObject:
        """Keep an omitted policy omitted when a valid model is normalized again."""
        data = cast("YamlObject", handler(self))
        if self.missed_fire_policy is None:
            data.pop("missed_fire_policy", None)
        return data

    @field_validator("missed_fire_policy")
    @classmethod
    def validate_missed_fire_policy(cls, value: str | None) -> str:
        """Require a non-null native enum name when this field is authored."""
        if value is None or not value.strip():
            msg = "schedule.missed_fire_policy must be a non-empty enum name"
            raise ValueError(msg)
        return value.strip().upper()

    @field_validator("timezone")
    @classmethod
    def validate_optional_timezone(cls, value: str | None) -> str | None:
        """Normalize an explicitly authored timezone without inventing one."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Schedule text fields must not be empty"
            raise ValueError(message)
        return normalized

    @field_validator("cron")
    @classmethod
    def validate_quartz_cron(cls, value: str) -> str:
        """Require Quartz-style cron field counts for workflow YAML schedules."""
        return normalize_quartz_cron_text(value, label="schedule.cron")

    @model_validator(mode="after")
    def validate_selected_version_contract(
        self,
        info: ValidationInfo,
    ) -> WorkflowScheduleSpec:
        """Keep aliases consistent and enforce the selected timezone dialect."""
        if self.enabled is None or self.release_state is None:
            pass
        else:
            expected = ReleaseState.ONLINE if self.enabled else ReleaseState.OFFLINE
            if self.release_state != expected:
                message = "schedule.enabled conflicts with schedule.release_state"
                raise ValueError(message)
        authoring_context = _workflow_authoring_context_from_info(info)
        if self.missed_fire_policy is not None and authoring_context is not None:
            choices = authoring_context.schedule_missed_fire_policy_choices
            if not choices:
                msg = (
                    "The selected exact DS schedule contract has no "
                    "missed_fire_policy field"
                )
                raise ValueError(msg)
            if self.missed_fire_policy not in choices:
                msg = f"schedule.missed_fire_policy must be one of {choices}"
                raise ValueError(msg)
        timezone_supported = (
            True
            if authoring_context is None
            else authoring_context.schedule_timezone_supported
        )
        if timezone_supported and self.timezone is None:
            message = "schedule.timezone is required by the selected DS version"
            raise ValueError(message)
        if not timezone_supported and self.timezone is not None:
            message = (
                "The selected DS version uses the server-local timezone and cannot "
                "represent schedule.timezone."
            )
            raise ValueError(message)
        return self

    def desired_release_state(self) -> ReleaseState:
        """Return the final schedule lifecycle state requested by the YAML."""
        if self.release_state is not None:
            return self.release_state
        if self.enabled is not None:
            return ReleaseState.ONLINE if self.enabled else ReleaseState.OFFLINE
        return ReleaseState.OFFLINE


class WorkflowSpec(YamlSpecModel):
    """Full workflow YAML document consumed by `workflow create`."""

    workflow: WorkflowMetadataSpec
    tasks: list[WorkflowTaskSpec]
    schedule: WorkflowScheduleSpec | None = None

    @field_validator("tasks")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[WorkflowTaskSpec],
    ) -> list[WorkflowTaskSpec]:
        """Require at least one task in the workflow YAML."""
        if not value:
            message = "Workflow YAML must contain at least one task"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_unique_task_names(self, info: ValidationInfo) -> WorkflowSpec:
        """Reject duplicate task names and apply exact parameter semantics."""
        authoring_context = _workflow_authoring_context_from_info(info)
        if not (
            authoring_context is not None and authoring_context.defer_graph_validation
        ):
            task_names = [task.name for task in self.tasks]
            duplicates = {name for name in task_names if task_names.count(name) > 1}
            if duplicates:
                duplicate = sorted(duplicates)[0]
                message = f"Task '{duplicate}' is duplicated in workflow YAML"
                raise ValueError(message)
        global_params = self.workflow.global_params
        if authoring_context is not None and isinstance(global_params, list):
            authoring_context.validate_global_params(global_params)
        return self


def validate_workflow_document(
    document: YamlValue,
    *,
    authoring_context: WorkflowAuthoringContext | None = None,
) -> WorkflowSpec:
    """Validate one workflow document under an explicit authoring authority."""
    return WorkflowSpec.model_validate(
        document,
        context=_workflow_authoring_validation_context(authoring_context),
    )


def validate_workflow_task_document(
    document: YamlValue,
    *,
    authoring_context: WorkflowAuthoringContext | None = None,
) -> WorkflowTaskSpec:
    """Validate one workflow task under an explicit authoring authority."""
    return WorkflowTaskSpec.model_validate(
        document,
        context=_workflow_authoring_validation_context(authoring_context),
    )


def load_workflow_spec(
    path: Path,
    *,
    authoring_context: WorkflowAuthoringContext | None = None,
) -> WorkflowSpec:
    """Load one workflow YAML file into the validated spec model."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        message = f"Could not read workflow YAML: {exc}"
        raise ValueError(message) from exc
    except yaml.YAMLError as exc:
        message = f"Workflow YAML is invalid: {exc}"
        raise ValueError(message) from exc

    if not isinstance(document, Mapping):
        message = "Workflow YAML root must be a mapping"
        raise TypeError(message)
    if not is_yaml_object(document):
        issue = yaml_value_validation_issue(document)
        if issue is None:
            message = "Workflow YAML boundary failed without a validation issue"
            raise RuntimeError(message)
        raise ModelValidationError((issue,))
    try:
        return validate_workflow_document(
            document,
            authoring_context=authoring_context,
        )
    except ValidationError as exc:
        raise ModelValidationError(model_validation_issues(exc)) from exc


def _workflow_authoring_validation_context(
    authoring_context: WorkflowAuthoringContext | None,
) -> dict[str, WorkflowAuthoringContext] | None:
    """Return the Pydantic context shared by nested workflow authoring models."""
    if authoring_context is None:
        return None
    return {_AUTHORING_CONTEXT_KEY: authoring_context}


def _task_params_normalizer(info: ValidationInfo) -> TaskParamsNormalizer:
    authoring_context = _workflow_authoring_context_from_info(info)
    if authoring_context is not None:
        return authoring_context.normalize_task_params
    return normalize_task_params


def _authorize_task_type(info: ValidationInfo, task_type: str) -> None:
    """Apply exact-profile task-type policy when authoring context is present."""
    authoring_context = _workflow_authoring_context_from_info(info)
    if authoring_context is not None:
        authoring_context.authorize_task_type(task_type)


def _validate_task_identity(
    info: ValidationInfo,
    task_type: str,
    task_name: str,
) -> None:
    """Apply exact-profile task-name constraints when authoring is typed."""
    authoring_context = _workflow_authoring_context_from_info(info)
    if authoring_context is not None:
        authoring_context.validate_task_identity(task_type, task_name)


def _workflow_authoring_context_from_info(
    info: ValidationInfo,
) -> WorkflowAuthoringContext | None:
    """Return the explicit workflow authoring authority, when configured."""
    context = info.context
    if not isinstance(context, Mapping):
        return None
    candidate = context.get(_AUTHORING_CONTEXT_KEY)
    return candidate if isinstance(candidate, WorkflowAuthoringContext) else None
