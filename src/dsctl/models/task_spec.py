from __future__ import annotations

from enum import StrEnum

from pydantic import (
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from dsctl.models.common import (
    GlobalParamSpec,
    YamlObject,
    YamlSpecModel,
    YamlValue,
    first_validation_error_message,
    is_yaml_object,
)


class HttpRequestMethod(StrEnum):
    """Supported DS HTTP task methods."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class HttpCheckCondition(StrEnum):
    """Supported DS HTTP task check conditions."""

    STATUS_CODE_DEFAULT = "STATUS_CODE_DEFAULT"
    STATUS_CODE_CUSTOM = "STATUS_CODE_CUSTOM"
    BODY_CONTAINS = "BODY_CONTAINS"
    BODY_NOT_CONTAINS = "BODY_NOT_CONTAINS"


class HttpParametersType(StrEnum):
    """Supported DS HTTP parameter kinds."""

    PARAMETER = "PARAMETER"
    HEADERS = "HEADERS"


class DependentRelation(StrEnum):
    """Supported DS dependent relation values."""

    AND = "AND"
    OR = "OR"


class DependentType(StrEnum):
    """Supported DS dependent item target kinds."""

    DEPENDENT_ON_WORKFLOW = "DEPENDENT_ON_WORKFLOW"
    DEPENDENT_ON_TASK = "DEPENDENT_ON_TASK"


class DependResult(StrEnum):
    """Supported DS dependent result states."""

    SUCCESS = "SUCCESS"
    WAITING = "WAITING"
    FAILED = "FAILED"
    NON_EXEC = "NON_EXEC"


class DependentFailurePolicy(StrEnum):
    """Supported DS dependent failure policies."""

    DEPENDENT_FAILURE_FAILURE = "DEPENDENT_FAILURE_FAILURE"
    DEPENDENT_FAILURE_WAITING = "DEPENDENT_FAILURE_WAITING"


class TaskExecutionStatus(StrEnum):
    """Supported DS task execution status values."""

    SUBMITTED_SUCCESS = "SUBMITTED_SUCCESS"
    RUNNING_EXECUTION = "RUNNING_EXECUTION"
    PAUSE = "PAUSE"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    NEED_FAULT_TOLERANCE = "NEED_FAULT_TOLERANCE"
    KILL = "KILL"
    DELAY_EXECUTION = "DELAY_EXECUTION"
    FORCED_SUCCESS = "FORCED_SUCCESS"
    DISPATCH = "DISPATCH"


class TaskRunFlag(StrEnum):
    """Supported DS task run-flag values."""

    YES = "YES"
    NO = "NO"


class TaskTimeoutNotifyStrategy(StrEnum):
    """Supported DS task timeout notification strategies."""

    WARN = "WARN"
    FAILED = "FAILED"
    WARNFAILED = "WARNFAILED"


def normalize_task_run_flag(value: YamlValue) -> YamlValue:
    """Normalize one task run flag from YAML-friendly input."""
    if isinstance(value, bool):
        return TaskRunFlag.YES.value if value else TaskRunFlag.NO.value
    if isinstance(value, str):
        return value.strip().upper()
    return value


class TaskParamsSpec(YamlSpecModel):
    """Base class for typed workflow task_params models."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    def to_payload(self) -> YamlObject:
        """Serialize one validated task params model back to plain YAML data."""
        payload = self.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=True,
            exclude_unset=True,
        )
        if not is_yaml_object(payload):
            message = "Validated task params did not serialize to a YAML object"
            raise TypeError(message)
        return payload


class ScriptTaskParamsSpec(TaskParamsSpec):
    """Shared YAML shape for SHELL and PYTHON task params."""

    raw_script: str = Field(alias="rawScript")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[YamlObject] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("raw_script")
    @classmethod
    def validate_script(cls, value: str) -> str:
        """Reject blank raw script payloads."""
        if not value.strip():
            message = "rawScript must not be empty"
            raise ValueError(message)
        return value


class RemoteShellTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for REMOTESHELL task params."""

    raw_script: str = Field(alias="rawScript")
    remote_type: str | None = Field(default=None, alias="type")
    datasource: int | None = Field(default=None, ge=1)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("raw_script")
    @classmethod
    def validate_raw_script(cls, value: str) -> str:
        """Reject blank REMOTESHELL scripts while preserving exact content."""
        if not value.strip():
            message = "rawScript must not be empty"
            raise ValueError(message)
        return value

    @field_validator("remote_type")
    @classmethod
    def validate_remote_type(cls, value: str | None) -> str | None:
        """Reject blank REMOTESHELL type fields after trimming."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized


class SqlTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SQL task params."""

    datasource_type: str = Field(alias="type")
    datasource: int = Field(ge=1)
    sql: str
    sql_type: int = Field(alias="sqlType", ge=0, le=1)
    send_email: bool | None = Field(default=None, alias="sendEmail")
    display_rows: int | None = Field(default=None, alias="displayRows", ge=0)
    show_type: str | None = Field(default=None, alias="showType")
    conn_params: str | None = Field(default=None, alias="connParams")
    pre_statements: list[str] = Field(default_factory=list, alias="preStatements")
    post_statements: list[str] = Field(default_factory=list, alias="postStatements")
    group_id: int | None = Field(default=None, alias="groupId", ge=0)
    title: str | None = None
    limit: int | None = Field(default=None, ge=0)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("datasource_type", "sql", "show_type")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject empty SQL text fields after trimming."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized


class HttpPropertySpec(YamlSpecModel):
    """Typed YAML shape for one HTTP parameter/header entry."""

    prop: str
    http_parameters_type: HttpParametersType = Field(alias="httpParametersType")
    value: str

    @field_validator("prop", "value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject empty HTTP property fields after trimming."""
        normalized = value.strip()
        if not normalized:
            message = "HTTP property fields must not be empty"
            raise ValueError(message)
        return normalized


class HttpTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for HTTP task params."""

    url: str
    http_method: HttpRequestMethod = Field(alias="httpMethod")
    http_params: list[HttpPropertySpec] = Field(
        default_factory=list,
        alias="httpParams",
    )
    http_body: str | None = Field(default=None, alias="httpBody")
    http_check_condition: HttpCheckCondition = Field(
        default=HttpCheckCondition.STATUS_CODE_DEFAULT,
        alias="httpCheckCondition",
    )
    condition: str | None = None
    connect_timeout: int = Field(alias="connectTimeout", gt=0)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("url")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject empty HTTP text fields after trimming."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized


class SubWorkflowTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SUB_WORKFLOW task params."""

    workflow_definition_code: int = Field(alias="workflowDefinitionCode", ge=1)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[YamlObject] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")


class DependentItemSpec(YamlSpecModel):
    """One DS dependent item inside one dependent task branch."""

    model_config = ConfigDict(populate_by_name=True)

    dependent_type: DependentType = Field(alias="dependentType")
    project_code: int = Field(alias="projectCode", ge=1)
    definition_code: int = Field(alias="definitionCode", ge=1)
    dep_task_code: int = Field(alias="depTaskCode", ge=0)
    cycle: str
    date_value: str = Field(alias="dateValue")
    depend_result: DependResult | None = Field(default=None, alias="dependResult")
    parameter_passing: bool | None = Field(default=None, alias="parameterPassing")

    @field_validator("cycle", "date_value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject blank dependent item text fields after trimming."""
        normalized = value.strip()
        if not normalized:
            message = "Dependent item text fields must not be empty"
            raise ValueError(message)
        return normalized


class DependentTaskModelSpec(YamlSpecModel):
    """One dependent task branch group."""

    model_config = ConfigDict(populate_by_name=True)

    depend_item_list: list[DependentItemSpec] = Field(alias="dependItemList")
    relation: DependentRelation

    @field_validator("depend_item_list")
    @classmethod
    def validate_non_empty_items(
        cls,
        value: list[DependentItemSpec],
    ) -> list[DependentItemSpec]:
        """Require at least one dependent item per branch."""
        if not value:
            message = "dependItemList must not be empty"
            raise ValueError(message)
        return value


class DependenceSpec(YamlSpecModel):
    """Dependent task dependence tree."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[DependentTaskModelSpec] = Field(alias="dependTaskList")
    relation: DependentRelation
    check_interval: int | None = Field(default=None, alias="checkInterval", gt=0)
    failure_policy: DependentFailurePolicy | None = Field(
        default=None,
        alias="failurePolicy",
    )
    failure_waiting_time: int | None = Field(
        default=None,
        alias="failureWaitingTime",
        ge=0,
    )

    @field_validator("depend_task_list")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[DependentTaskModelSpec],
    ) -> list[DependentTaskModelSpec]:
        """Require at least one dependent task branch."""
        if not value:
            message = "dependTaskList must not be empty"
            raise ValueError(message)
        return value


class DependentTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for DEPENDENT task params."""

    dependence: DependenceSpec
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    resource_list: list[YamlObject] = Field(
        default_factory=list,
        alias="resourceList",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")


def _normalize_task_name_ref(value: str) -> str:
    """Normalize one task-name reference used inside logical task params."""
    normalized = value.strip()
    if not normalized:
        message = "Task node references must not be empty"
        raise ValueError(message)
    return normalized


class SwitchBranchSpec(YamlSpecModel):
    """One conditional branch in one SWITCH task."""

    model_config = ConfigDict(populate_by_name=True)

    condition: str
    next_node: str = Field(alias="nextNode")

    @field_validator("condition", "next_node")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject blank branch expressions and branch targets."""
        return _normalize_task_name_ref(value)


class SwitchResultSpec(YamlSpecModel):
    """Branching payload used by SWITCH task params."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[SwitchBranchSpec] = Field(
        default_factory=list,
        alias="dependTaskList",
    )
    next_node: str | None = Field(default=None, alias="nextNode")

    @field_validator("next_node")
    @classmethod
    def validate_optional_next_node(cls, value: str | None) -> str | None:
        """Normalize the default branch target when present."""
        if value is None:
            return None
        return _normalize_task_name_ref(value)

    @model_validator(mode="after")
    def validate_branch_targets(self) -> SwitchResultSpec:
        """Require at least one branch target or one default branch target."""
        if not self.depend_task_list and self.next_node is None:
            message = "switchResult must define dependTaskList or nextNode"
            raise ValueError(message)
        return self


class SwitchTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SWITCH task params."""

    switch_result: SwitchResultSpec = Field(alias="switchResult")
    next_branch: str | None = Field(default=None, alias="nextBranch")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    @field_validator("next_branch")
    @classmethod
    def validate_optional_next_branch(cls, value: str | None) -> str | None:
        """Normalize persisted next-branch values from DS exports when present."""
        if value is None:
            return None
        return _normalize_task_name_ref(value)


class ConditionDependentItemSpec(DependentItemSpec):
    """One CONDITIONS upstream item with the expected task status."""

    status: TaskExecutionStatus


class ConditionDependentTaskModelSpec(YamlSpecModel):
    """One CONDITIONS upstream branch group."""

    model_config = ConfigDict(populate_by_name=True)

    depend_item_list: list[ConditionDependentItemSpec] = Field(alias="dependItemList")
    relation: DependentRelation

    @field_validator("depend_item_list")
    @classmethod
    def validate_non_empty_items(
        cls,
        value: list[ConditionDependentItemSpec],
    ) -> list[ConditionDependentItemSpec]:
        """Require at least one upstream item per CONDITIONS branch."""
        if not value:
            message = "dependItemList must not be empty"
            raise ValueError(message)
        return value


class ConditionDependencySpec(YamlSpecModel):
    """Upstream dependency tree for one CONDITIONS task."""

    model_config = ConfigDict(populate_by_name=True)

    depend_task_list: list[ConditionDependentTaskModelSpec] = Field(
        alias="dependTaskList"
    )
    relation: DependentRelation

    @field_validator("depend_task_list")
    @classmethod
    def validate_non_empty_tasks(
        cls,
        value: list[ConditionDependentTaskModelSpec],
    ) -> list[ConditionDependentTaskModelSpec]:
        """Require at least one CONDITIONS upstream branch."""
        if not value:
            message = "dependTaskList must not be empty"
            raise ValueError(message)
        return value


class ConditionResultSpec(YamlSpecModel):
    """Downstream branch selection for one CONDITIONS task."""

    model_config = ConfigDict(populate_by_name=True)

    condition_success: bool | None = Field(default=None, alias="conditionSuccess")
    success_node: list[str] = Field(alias="successNode")
    failed_node: list[str] = Field(alias="failedNode")

    @field_validator("success_node", "failed_node")
    @classmethod
    def validate_non_empty_nodes(cls, value: list[str]) -> list[str]:
        """Require at least one branch target per CONDITIONS outcome."""
        if not value:
            message = "Branch target lists must not be empty"
            raise ValueError(message)
        return [_normalize_task_name_ref(item) for item in value]


class ConditionsTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for CONDITIONS task params."""

    dependence: ConditionDependencySpec
    condition_result: ConditionResultSpec = Field(alias="conditionResult")
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")


_TASK_PARAMS_MODELS: dict[str, type[TaskParamsSpec]] = {
    "CONDITIONS": ConditionsTaskParamsSpec,
    "DEPENDENT": DependentTaskParamsSpec,
    "HTTP": HttpTaskParamsSpec,
    "PYTHON": ScriptTaskParamsSpec,
    "REMOTESHELL": RemoteShellTaskParamsSpec,
    "SHELL": ScriptTaskParamsSpec,
    "SQL": SqlTaskParamsSpec,
    "SUB_WORKFLOW": SubWorkflowTaskParamsSpec,
    "SWITCH": SwitchTaskParamsSpec,
}

_TASK_TYPE_ALIASES: dict[str, str] = {
    "REMOTE_SHELL": "REMOTESHELL",
}


def canonical_task_type(task_type: str) -> str:
    """Normalize one task type name to the DS-native canonical value."""
    normalized = task_type.strip().upper()
    return _TASK_TYPE_ALIASES.get(normalized, normalized)


def supported_typed_task_types() -> tuple[str, ...]:
    """Return the stable task types backed by typed task_params models."""
    return tuple(sorted(_TASK_PARAMS_MODELS))


def normalize_task_params(task_type: str, task_params: YamlObject) -> YamlObject:
    """Validate one known task_params block and return a normalized plain object."""
    model = _TASK_PARAMS_MODELS.get(canonical_task_type(task_type))
    if model is None:
        return task_params
    try:
        return model.model_validate(task_params).to_payload()
    except ValidationError as exc:
        detail = first_validation_error_message(exc)
        message = f"task_params.{detail}"
        raise ValueError(message) from exc


__all__ = [
    "ConditionDependencySpec",
    "ConditionDependentItemSpec",
    "ConditionDependentTaskModelSpec",
    "ConditionResultSpec",
    "ConditionsTaskParamsSpec",
    "DependResult",
    "DependenceSpec",
    "DependentFailurePolicy",
    "DependentItemSpec",
    "DependentRelation",
    "DependentTaskModelSpec",
    "DependentTaskParamsSpec",
    "DependentType",
    "HttpCheckCondition",
    "HttpParametersType",
    "HttpPropertySpec",
    "HttpRequestMethod",
    "HttpTaskParamsSpec",
    "RemoteShellTaskParamsSpec",
    "ScriptTaskParamsSpec",
    "SqlTaskParamsSpec",
    "SubWorkflowTaskParamsSpec",
    "SwitchBranchSpec",
    "SwitchResultSpec",
    "SwitchTaskParamsSpec",
    "TaskExecutionStatus",
    "TaskParamsSpec",
    "canonical_task_type",
    "normalize_task_params",
    "supported_typed_task_types",
]
