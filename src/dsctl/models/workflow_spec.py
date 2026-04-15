from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import yaml
from pydantic import Field, ValidationError, field_validator, model_validator

from dsctl.models.common import (
    FailureStrategy,
    GlobalParamSpec,
    Priority,
    ReleaseState,
    RetrySpec,
    WorkflowExecutionType,
    YamlObject,
    YamlSpecModel,
    YamlValue,
    first_validation_error_message,
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

COMMAND_TASK_TYPES = frozenset({"PYTHON", "REMOTESHELL", "SHELL"})


class WorkflowMetadataSpec(YamlSpecModel):
    """Workflow-level YAML fields used by create and export."""

    name: str
    project: str | None = None
    description: str | None = None
    timeout: int = Field(default=0, ge=0)
    global_params: dict[str, str] | list[GlobalParamSpec] | None = None
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
    def validate_task_payload(self) -> WorkflowTaskSpec:
        """Require one task payload source and validate command shorthands."""
        if self.task_params is None and self.command is None:
            message = f"Task '{self.name}' must define either task_params or command"
            raise ValueError(message)
        if self.task_params is not None and self.command is not None:
            message = f"Task '{self.name}' cannot define both task_params and command"
            raise ValueError(message)
        if self.command is not None and self.type.upper() not in COMMAND_TASK_TYPES:
            message = (
                f"Task '{self.name}' only supports command shorthand for "
                "SHELL, PYTHON, and REMOTESHELL types"
            )
            raise ValueError(message)
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
                self.task_params = normalize_task_params(self.type, self.task_params)
            except ValueError as exc:
                message = f"Task '{self.name}' {exc}"
                raise ValueError(message) from exc
        if self.name in self.depends_on:
            message = f"Task '{self.name}' cannot depend on itself"
            raise ValueError(message)
        return self


class WorkflowScheduleSpec(YamlSpecModel):
    """Optional schedule block accepted by the YAML parser."""

    cron: str
    timezone: str
    start: str
    end: str
    failure_strategy: FailureStrategy | None = None
    priority: Priority | None = None
    release_state: ReleaseState | None = None
    enabled: bool | None = None

    @field_validator("cron", "timezone", "start", "end")
    @classmethod
    def validate_schedule_text(cls, value: str) -> str:
        """Reject empty schedule text fields after trimming whitespace."""
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
    def validate_enabled_alias(self) -> WorkflowScheduleSpec:
        """Keep `enabled` and `release_state` aliases consistent."""
        if self.enabled is None or self.release_state is None:
            return self
        expected = ReleaseState.ONLINE if self.enabled else ReleaseState.OFFLINE
        if self.release_state != expected:
            message = "schedule.enabled conflicts with schedule.release_state"
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
    def validate_unique_task_names(self) -> WorkflowSpec:
        """Reject duplicate task names before the service compiles the DAG."""
        task_names = [task.name for task in self.tasks]
        duplicates = {name for name in task_names if task_names.count(name) > 1}
        if duplicates:
            duplicate = sorted(duplicates)[0]
            message = f"Task '{duplicate}' is duplicated in workflow YAML"
            raise ValueError(message)
        return self


def load_workflow_spec(path: Path) -> WorkflowSpec:
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
    try:
        return WorkflowSpec.model_validate(document)
    except ValidationError as exc:
        raise ValueError(first_validation_error_message(exc)) from exc
