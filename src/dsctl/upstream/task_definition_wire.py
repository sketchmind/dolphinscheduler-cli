# ruff: noqa: N815
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from dsctl.generated.task_definition_profiles import (
    TARGET_TASK_DEFINITION_VERSIONS,
    TASK_DEFINITION_PROFILE_SCHEMA_VERSION,
    TASK_DEFINITION_PROFILES,
)
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.read_models import CanonicalWorkflow, WorkflowScalarMetadata
from dsctl.upstream.wire import (
    PreparedWireCallToken,
    WireContractError,
    WireExecution,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._compiled_workflow_runtime import WorkflowPrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        StringEnumValue,
        TaskPayloadRecord,
        WorkflowDagRecord,
        WorkflowTaskRelationRecord,
    )
    from dsctl.upstream.protocols.session import TaskDefinitionUpstreamSession
    from dsctl.upstream.wire import PreparedCompiledWireCall


@dataclass(frozen=True, slots=True)
class TaskTopLevelFieldPolicy:
    """Explicit semantic classification of one profile's task response fields."""

    request_payload: frozenset[str]
    server_managed: frozenset[str]
    response_derived: frozenset[str]
    relation_projection: frozenset[str]
    opaque_preservation: frozenset[str]

    def __post_init__(self) -> None:
        """Reject overlapping categories or invalid preservation claims."""
        categories = (
            self.request_payload,
            self.server_managed,
            self.response_derived,
            self.relation_projection,
        )
        classified: set[str] = set()
        for category in categories:
            overlap = classified.intersection(category)
            if overlap:
                message = (
                    "Task top-level field policy contains overlapping categories: "
                    f"{sorted(overlap)!r}"
                )
                raise ValueError(message)
            classified.update(category)
        invalid_opaque = self.opaque_preservation.difference(self.request_payload)
        if invalid_opaque:
            message = (
                "Opaque-preservation fields must belong to the request payload: "
                f"{sorted(invalid_opaque)!r}"
            )
            raise ValueError(message)

    @property
    def classified_fields(self) -> frozenset[str]:
        """Return every generated response field assigned an explicit role."""
        return frozenset().union(
            self.request_payload,
            self.server_managed,
            self.response_derived,
            self.relation_projection,
        )

    def validate_generated_fields(self, generated_fields: Iterable[str]) -> None:
        """Fail closed when generation and the reviewed profile policy drift."""
        generated = frozenset(generated_fields)
        missing = sorted(self.classified_fields.difference(generated))
        unclassified = sorted(generated.difference(self.classified_fields))
        if not missing and not unclassified:
            return
        message = (
            "Generated task response fields do not match the explicit profile "
            f"classification; missing={missing!r}, unclassified={unclassified!r}"
        )
        raise WireContractError(message)

    def fingerprint_parts(self) -> tuple[str, ...]:
        """Return category-aware recipe identity parts."""
        parts: list[str] = []
        for label, fields in (
            ("request", self.request_payload),
            ("server", self.server_managed),
            ("derived", self.response_derived),
            ("relation", self.relation_projection),
            ("opaque", self.opaque_preservation),
        ):
            parts.extend(f"{label}:{field}" for field in sorted(fields))
        return tuple(parts)


@dataclass(frozen=True, slots=True)
class TaskUpdateWirePolicy:
    """Profile constraints that the deep task module enforces before transport."""

    update_available: bool
    dependency_update: bool
    unsupported_patch_fields: frozenset[str]
    requires_unique_workflow_binding: bool = False

    def fingerprint_parts(self) -> tuple[str, ...]:
        """Return stable recipe identity parts for update semantics."""
        return (
            f"update-available:{str(self.update_available).lower()}",
            f"dependency-update:{str(self.dependency_update).lower()}",
            (
                "unique-workflow-binding:"
                f"{str(self.requires_unique_workflow_binding).lower()}"
            ),
            *(
                f"unsupported:{field}"
                for field in sorted(self.unsupported_patch_fields)
            ),
        )


@dataclass(frozen=True, slots=True)
class TaskUpdateContractFeatures:
    """Exact task-update payload and dependency facts for CLI discovery."""

    request_fields: frozenset[str]
    dependency_update: bool
    whole_workflow_update: bool = False
    dag_workflow_field: str = "processDefinition"
    dag_relation_field: str = "processTaskRelationList"


class WorkflowDagProjectionError(WireContractError):
    """Carry one bounded DAG component failure across the generated seam."""

    def __init__(self, *, ds_version: str, field: str, reason: str) -> None:
        """Store only safe component metadata for the caller error boundary."""
        message = (
            f"DS {ds_version} generated workflow DAG component {field!r} "
            "violated its exact contract"
        )
        super().__init__(message)
        self.field = field
        self.reason = reason


@dataclass(frozen=True, slots=True)
class _TaskVersionSpec:
    dag_module: str
    dag_operations: str
    dag_method: str
    dag_source_operation: str
    dag_workflow_field: str
    dag_relation_field: str
    detail_model_module: str
    detail_model_name: str
    update_method: str | None
    update_params: str | None
    update_source_operation: str | None
    update_response_optional: bool
    whole_workflow_update: bool
    task_group_fields: bool
    resource_limit_fields: bool
    computed_response_fields: frozenset[str]
    field_policy: TaskTopLevelFieldPolicy
    update_policy: TaskUpdateWirePolicy


@dataclass(frozen=True, slots=True)
class _DagBinding:
    module: str
    operations: str
    method: str
    source_operation: str
    workflow_field: str
    relation_field: str


@dataclass(frozen=True, slots=True)
class _UpdateBinding:
    method: str
    params: str


_DETAIL_MODEL_BINDINGS = {
    "org.apache.dolphinscheduler.dao.entity.TaskDefinition": (
        "dao.entities.task_definition",
        "TaskDefinition",
    ),
    "org.apache.dolphinscheduler.api.vo.TaskDefinitionVo": (
        "api.views.task_definition_vo",
        "TaskDefinitionVo",
    ),
    "org.apache.dolphinscheduler.api.vo.TaskDefinitionVO": (
        "api.views.task_definition",
        "TaskDefinitionVO",
    ),
}
_DAG_BINDINGS = {
    "process": _DagBinding(
        module="process_definition",
        operations="ProcessDefinitionOperations",
        method="query_process_definition_by_code",
        source_operation="ProcessDefinitionController.queryProcessDefinitionByCode",
        workflow_field="processDefinition",
        relation_field="processTaskRelationList",
    ),
    "workflow": _DagBinding(
        module="workflow_definition",
        operations="WorkflowDefinitionOperations",
        method="query_workflow_definition_by_code",
        source_operation="WorkflowDefinitionController.queryWorkflowDefinitionByCode",
        workflow_field="workflowDefinition",
        relation_field="workflowTaskRelationList",
    ),
}
_UPDATE_BINDINGS = {
    "TaskDefinitionController.updateTaskDefinition": _UpdateBinding(
        method="update_task_definition",
        params="UpdateTaskDefinitionParams",
    ),
    "TaskDefinitionController.updateTaskWithUpstream": _UpdateBinding(
        method="update_task_with_upstream",
        params="UpdateTaskWithUpstreamParams",
    ),
}
_TASK_GROUP_FIELDS = frozenset({"taskGroupId", "taskGroupPriority"})
_RESOURCE_LIMIT_FIELDS = frozenset({"cpuQuota", "memoryMax", "taskExecuteType"})
_PATCH_FIELD_BY_WIRE_FIELD = {
    "taskGroupId": "task_group_id",
    "taskGroupPriority": "task_group_priority",
    "cpuQuota": "cpu_quota",
    "memoryMax": "memory_max",
}


def _task_spec(version: str, raw_profile: JsonValue) -> _TaskVersionSpec | None:
    profile = _mapping(raw_profile, label=f"DS {version} task profile")
    if not _boolean(profile, "executable"):
        return None
    _validate_profile_header(version, profile)
    dag = _dag_binding(version, profile)
    detail_model_module, detail_model_name = _detail_model_binding(
        version,
        profile,
    )
    update_source_operation, update = _update_binding(version, profile)
    request_fields = _text_set(profile, "request_fields")
    computed_response_fields = _text_set(profile, "computed_response_fields")
    field_policy = _field_policy_from_profile(
        version,
        profile,
        request_fields,
        computed_response_fields=computed_response_fields,
    )
    update_policy = _update_policy_from_profile(
        version,
        profile,
        request_fields,
        source_operation=update_source_operation,
    )
    whole_workflow_update = _boolean(profile, "whole_workflow_update")
    if whole_workflow_update and update_policy.update_available:
        message = (
            f"DS {version} task profile selects standalone and whole-workflow "
            "updates together"
        )
        raise WireContractError(message)
    detail_relation_field = _optional_text(profile, "relation_field")
    _validate_relation_projection(
        version,
        detail_relation_field=detail_relation_field,
        policy=field_policy,
    )
    if (
        update_policy.requires_unique_workflow_binding
        and dag.module != "process_definition"
    ):
        message = f"DS {version} unique task binding guard requires a process DAG"
        raise WireContractError(message)
    return _TaskVersionSpec(
        dag_module=dag.module,
        dag_operations=dag.operations,
        dag_method=dag.method,
        dag_source_operation=dag.source_operation,
        dag_workflow_field=dag.workflow_field,
        dag_relation_field=dag.relation_field,
        detail_model_module=detail_model_module,
        detail_model_name=detail_model_name,
        update_method=None if update is None else update.method,
        update_params=None if update is None else update.params,
        update_source_operation=update_source_operation,
        update_response_optional=_boolean(profile, "update_response_optional"),
        whole_workflow_update=whole_workflow_update,
        task_group_fields=_all_or_none(
            version,
            request_fields,
            _TASK_GROUP_FIELDS,
            label="task-group",
        ),
        resource_limit_fields=_all_or_none(
            version,
            request_fields,
            _RESOURCE_LIMIT_FIELDS,
            label="resource-limit",
        ),
        computed_response_fields=computed_response_fields,
        field_policy=field_policy,
        update_policy=update_policy,
    )


def _validate_profile_header(
    version: str,
    profile: Mapping[str, JsonValue],
) -> None:
    if _text(profile, "identity_wire") != "code-version":
        message = f"DS {version} executable task profile has no code/version identity"
        raise WireContractError(message)
    if _text(profile, "dag_model") != (
        "org.apache.dolphinscheduler.dao.entity.DagData"
    ):
        message = f"DS {version} task profile has an unsupported DAG model"
        raise WireContractError(message)
    if _text(profile, "detail_operation") != (
        "TaskDefinitionController.queryTaskDefinitionDetail"
    ):
        message = f"DS {version} task profile has an unsupported detail operation"
        raise WireContractError(message)


def _dag_binding(
    version: str,
    profile: Mapping[str, JsonValue],
) -> _DagBinding:
    dag_family = _text(profile, "dag_family")
    try:
        dag = _DAG_BINDINGS[dag_family]
    except KeyError as exc:
        message = f"DS {version} task profile has unknown DAG family {dag_family!r}"
        raise WireContractError(message) from exc
    if _text(profile, "dag_operation") != dag.source_operation:
        message = f"DS {version} task profile DAG operation does not match its family"
        raise WireContractError(message)
    if _text(profile, "workflow_field") != dag.workflow_field:
        message = f"DS {version} task profile workflow field does not match its family"
        raise WireContractError(message)
    return dag


def _detail_model_binding(
    version: str,
    profile: Mapping[str, JsonValue],
) -> tuple[str, str]:
    detail_model_key = _text(profile, "detail_model")
    try:
        return _DETAIL_MODEL_BINDINGS[detail_model_key]
    except KeyError as exc:
        message = (
            f"DS {version} task profile has unknown detail model {detail_model_key!r}"
        )
        raise WireContractError(message) from exc


def _update_binding(
    version: str,
    profile: Mapping[str, JsonValue],
) -> tuple[str | None, _UpdateBinding | None]:
    update_source_operation = _optional_text(profile, "update_operation")
    if update_source_operation is None:
        if _boolean(profile, "whole_workflow_update") and not _boolean(
            profile, "update_executable"
        ):
            return None, None
        message = (
            f"DS {version} missing standalone task update has no "
            "reviewed whole-workflow strategy"
        )
        raise WireContractError(message)
    try:
        return update_source_operation, _UPDATE_BINDINGS[update_source_operation]
    except KeyError as exc:
        message = (
            f"DS {version} task profile has unknown update operation "
            f"{update_source_operation!r}"
        )
        raise WireContractError(message) from exc


def _update_policy_from_profile(
    version: str,
    profile: Mapping[str, JsonValue],
    request_fields: frozenset[str],
    *,
    source_operation: str | None,
) -> TaskUpdateWirePolicy:
    unsupported_patch_fields = frozenset(
        patch_field
        for wire_field, patch_field in _PATCH_FIELD_BY_WIRE_FIELD.items()
        if wire_field not in request_fields
    )
    update_available = _boolean(profile, "update_executable")
    dependency_update = _boolean(profile, "dependency_update")
    requires_unique_workflow_binding = _boolean(
        profile,
        "requires_unique_workflow_binding",
    )
    whole_workflow_update = _boolean(profile, "whole_workflow_update")
    if dependency_update and not (update_available or whole_workflow_update):
        message = f"DS {version} terminal task update cannot own dependency changes"
        raise WireContractError(message)
    if update_available and (
        source_operation is None
        or dependency_update is not source_operation.endswith(".updateTaskWithUpstream")
    ):
        message = f"DS {version} task dependency policy conflicts with its endpoint"
        raise WireContractError(message)
    if requires_unique_workflow_binding and (not update_available or dependency_update):
        message = f"DS {version} unique workflow binding requires a standalone update"
        raise WireContractError(message)
    _preserves_is_cache(version, profile, request_fields)
    return TaskUpdateWirePolicy(
        update_available=update_available,
        dependency_update=dependency_update,
        unsupported_patch_fields=unsupported_patch_fields,
        requires_unique_workflow_binding=requires_unique_workflow_binding,
    )


def _preserves_is_cache(
    version: str,
    profile: Mapping[str, JsonValue],
    request_fields: frozenset[str],
) -> bool:
    preserves_is_cache = _boolean(profile, "preserve_is_cache")
    if preserves_is_cache is not ("isCache" in request_fields):
        message = f"DS {version} task cache preservation policy is inconsistent"
        raise WireContractError(message)
    return preserves_is_cache


def _field_policy_from_profile(
    version: str,
    profile: Mapping[str, JsonValue],
    request_fields: frozenset[str],
    *,
    computed_response_fields: frozenset[str],
) -> TaskTopLevelFieldPolicy:
    raw_policy = _mapping(
        profile.get("field_policy"),
        label=f"DS {version} task field policy",
    )
    field_policy = TaskTopLevelFieldPolicy(
        request_payload=_text_set(raw_policy, "request_payload"),
        server_managed=_text_set(raw_policy, "server_managed"),
        response_derived=_text_set(raw_policy, "response_derived"),
        relation_projection=_text_set(raw_policy, "relation_projection"),
        opaque_preservation=_text_set(raw_policy, "opaque_preservation"),
    )
    if field_policy.request_payload != request_fields:
        message = f"DS {version} task profile request fields diverge from its policy"
        raise WireContractError(message)
    if not computed_response_fields.issubset(field_policy.response_derived):
        message = f"DS {version} computed task response fields are not response-derived"
        raise WireContractError(message)
    return field_policy


def _validate_relation_projection(
    version: str,
    *,
    detail_relation_field: str | None,
    policy: TaskTopLevelFieldPolicy,
) -> None:
    expected_relation_projection = (
        frozenset({detail_relation_field})
        if detail_relation_field is not None
        else frozenset()
    )
    if policy.relation_projection != expected_relation_projection:
        message = f"DS {version} task relation projection is inconsistent"
        raise WireContractError(message)


def _mapping(value: JsonValue, *, label: str) -> Mapping[str, JsonValue]:
    if isinstance(value, Mapping) and all(isinstance(key, str) for key in value):
        return value
    message = f"{label} must be an object"
    raise WireContractError(message)


def _text(profile: Mapping[str, JsonValue], key: str) -> str:
    value = profile.get(key)
    if isinstance(value, str) and value:
        return value
    message = f"task profile field {key!r} must be a non-empty string"
    raise WireContractError(message)


def _optional_text(profile: Mapping[str, JsonValue], key: str) -> str | None:
    value = profile.get(key)
    if value is None or isinstance(value, str):
        return value
    message = f"task profile field {key!r} must be a string or null"
    raise WireContractError(message)


def _boolean(profile: Mapping[str, JsonValue], key: str) -> bool:
    value = profile.get(key)
    if isinstance(value, bool):
        return value
    message = f"task profile field {key!r} must be a boolean"
    raise WireContractError(message)


def _text_set(profile: Mapping[str, JsonValue], key: str) -> frozenset[str]:
    value = profile.get(key)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return frozenset(value)
    message = f"task profile field {key!r} must be a string list"
    raise WireContractError(message)


def _all_or_none(
    version: str,
    request_fields: frozenset[str],
    group: frozenset[str],
    *,
    label: str,
) -> bool:
    selected = request_fields.intersection(group)
    if not selected:
        return False
    if selected == group:
        return True
    message = f"DS {version} task profile has a partial {label} field group"
    raise WireContractError(message)


if TASK_DEFINITION_PROFILE_SCHEMA_VERSION != 5:
    message = "Generated task-definition profile schema is unsupported"
    raise WireContractError(message)
if TARGET_TASK_DEFINITION_VERSIONS != TARGET_DS_VERSIONS:
    message = "Generated task-definition and compatibility profile targets diverge"
    raise WireContractError(message)
if tuple(TASK_DEFINITION_PROFILES) != TARGET_TASK_DEFINITION_VERSIONS:
    message = "Generated task-definition profile keys do not match their targets"
    raise WireContractError(message)


_TASK_SPEC_BY_VERSION = {
    version: spec
    for version, raw_profile in TASK_DEFINITION_PROFILES.items()
    if (spec := _task_spec(version, raw_profile)) is not None
}


def _require_task_spec(ds_version: str) -> _TaskVersionSpec:
    try:
        return _TASK_SPEC_BY_VERSION[ds_version]
    except KeyError as exc:
        message = f"DS {ds_version} has no executable task-definition recipe"
        raise WireContractError(message) from exc


def task_update_contract_features(ds_version: str) -> TaskUpdateContractFeatures:
    """Return exact stable-action facts without binding transport."""
    spec = _require_task_spec(ds_version)
    return TaskUpdateContractFeatures(
        request_fields=spec.field_policy.request_payload,
        dependency_update=spec.update_policy.dependency_update,
        whole_workflow_update=spec.whole_workflow_update,
        dag_workflow_field=spec.dag_workflow_field,
        dag_relation_field=spec.dag_relation_field,
    )


@dataclass(frozen=True)
class TaskDefinitionWire:
    """Exact generated task and DAG programs bound to one shared HTTP client."""

    ds_version: str
    recipe_fingerprint: str
    top_level_field_policy: TaskTopLevelFieldPolicy
    update_policy: TaskUpdateWirePolicy
    _spec: _TaskVersionSpec
    _programs: BoundCompiledPrograms[WorkflowPrimitive]

    def get(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> WireExecution[TaskPayloadRecord]:
        """Execute task detail and project it into the stable task protocol."""
        execution = self._programs.execute(
            "task_get",
            self._programs.prepare(
                "task_get", {"projectCode": project_code, "code": task_code}
            ),
        )
        return WireExecution(
            payload=_task_record(
                execution.payload,
                spec=self._spec,
                ds_version=self.ds_version,
                expected_project_code=project_code,
                expected_task_code=task_code,
            ),
            raw_payload=execution.raw_payload,
            request=execution.request,
        )

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        """Execute the exact workflow DAG read used for membership and relations."""
        execution = self._programs.execute(
            "definition_get",
            self._programs.prepare(
                "definition_get", {"projectCode": project_code, "code": workflow_code}
            ),
        )
        return WireExecution(
            payload=_dag_record(
                execution.payload,
                spec=self._spec,
                ds_version=self.ds_version,
                expected_project_code=project_code,
                expected_workflow_code=workflow_code,
            ),
            raw_payload=execution.raw_payload,
            request=execution.request,
        )

    def prepare_update(
        self,
        *,
        project_code: int,
        task_code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> PreparedCompiledWireCall:
        """Prepare the exact generated form request without performing I/O."""
        self._require_update_program()
        args: JsonObject = {
            "projectCode": project_code,
            "code": task_code,
            "taskDefinitionJsonObj": task_definition_json,
        }
        if self.update_policy.dependency_update:
            args["upstreamCodes"] = (
                ",".join(str(code) for code in upstream_codes)
                if upstream_codes and self.update_policy.dependency_update
                else None
            )
        return self._programs.prepare("task_update", args)

    def apply_update(
        self,
        prepared: PreparedWireCallToken,
    ) -> WireExecution[int | None]:
        """Apply one prepared generated mutation exactly once."""
        self._require_update_program()
        execution = self._programs.execute(
            "task_update", cast("PreparedCompiledWireCall", prepared)
        )
        payload = execution.payload
        if payload is not None and not isinstance(payload, int):
            message = "Generated task update returned a non-integer payload"
            raise WireContractError(message)
        return WireExecution(
            payload=payload,
            raw_payload=execution.raw_payload,
            request=execution.request,
        )

    def _require_update_program(self) -> None:
        if self.update_policy.update_available:
            return
        message = f"DS {self.ds_version} has no coherent standalone task-update program"
        raise WireContractError(message)


@dataclass(frozen=True)
class _TaskDefinitionSession:
    definitions: DefinitionReads
    task_definitions: TaskDefinitionWire


class TaskDefinitionAdapter:
    """Narrow exact-version adapter for stable task list/get/update actions."""

    def __init__(self, ds_version: str) -> None:
        """Select an explicitly reviewed recipe without importing its bundle yet."""
        if ds_version not in _TASK_SPEC_BY_VERSION:
            message = f"DS {ds_version} has no executable task-definition recipe"
            raise WireContractError(message)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> TaskDefinitionAdapter:
        """Return a lazy adapter for one exact reviewed version."""
        return cls(ds_version)

    def bind_task_definitions(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskDefinitionUpstreamSession:
        """Bind definition resolution plus exact task/DAG wire programs."""
        if profile.ds_version != self.ds_version:
            message = "Task-definition adapter does not match the selected profile"
            raise WireContractError(message)
        read_session = CodeNativeReadAdapter.for_version(self.ds_version).bind_read(
            profile,
            http_client=http_client,
        )
        return cast(
            "TaskDefinitionUpstreamSession",
            _TaskDefinitionSession(
                definitions=read_session.definitions,
                task_definitions=bind_task_definition_wire(http_client),
            ),
        )


def bind_task_definition_wire(
    client: DolphinSchedulerClient,
) -> TaskDefinitionWire:
    """Bind the selected profile's exact generated task-definition programs."""
    ds_version = client.profile.ds_version
    spec = _require_task_spec(ds_version)
    profile = WORKFLOW_PROGRAMS.profile(ds_version)
    policy = spec.field_policy
    get_program = profile.program("task_get")
    response_adapter = get_program.codec.response_adapter
    if response_adapter is None:
        message = "Compiled task detail has no response schema"
        raise WireContractError(message)
    declared_response_fields = frozenset(
        response_adapter.json_schema().get("properties", {})
    )
    overlapping_computed_fields = sorted(
        declared_response_fields.intersection(spec.computed_response_fields)
    )
    if overlapping_computed_fields:
        message = (
            f"DS {ds_version} computed task response fields overlap generated "
            f"declarations: {overlapping_computed_fields!r}"
        )
        raise WireContractError(message)
    policy.validate_generated_fields(
        declared_response_fields | spec.computed_response_fields
    )
    dag_program = profile.program("definition_get")
    update_program = (
        profile.program("task_update") if spec.update_policy.update_available else None
    )
    return TaskDefinitionWire(
        ds_version=ds_version,
        recipe_fingerprint=_fingerprint(
            "task-definition-recipe-v4",
            ds_version,
            get_program.fingerprint,
            dag_program.fingerprint,
            (
                update_program.fingerprint
                if update_program is not None
                else "task-update:terminal"
            ),
            "project-workflow-task-selector-v2",
            "workflow-dag-dependency-truth-v1",
            *policy.fingerprint_parts(),
            *spec.update_policy.fingerprint_parts(),
        ),
        top_level_field_policy=policy,
        update_policy=spec.update_policy,
        _spec=spec,
        _programs=WORKFLOW_PROGRAMS.bind(profile, client.profile, http_client=client),
    )


@dataclass(frozen=True)
class _CanonicalTaskDefinition:
    id: int | None
    code: int
    name: str | None
    version: int | None
    projectCode: int
    description: str | None
    taskType: str | None
    taskParams: JsonValue | None
    userName: str | None
    projectName: str | None
    workerGroup: str | None
    failRetryTimes: int
    failRetryInterval: int
    timeout: int
    delayTime: int
    resourceIds: str | None
    createTime: str | None
    updateTime: str | None
    modifyBy: str | None
    taskGroupId: int
    taskGroupPriority: int
    environmentCode: int
    taskPriority: StringEnumValue | None
    timeoutFlag: StringEnumValue | None
    timeoutNotifyStrategy: StringEnumValue | None
    taskExecuteType: StringEnumValue | None
    flag: StringEnumValue | None
    isCache: StringEnumValue | None
    cpuQuota: int | None
    memoryMax: int | None


@dataclass(frozen=True)
class _CanonicalTaskRelation:
    preTaskCode: int
    postTaskCode: int
    preTaskVersion: int
    postTaskVersion: int
    conditionParams: JsonValue | None


@dataclass(frozen=True)
class _CanonicalDag:
    workflowDefinition: CanonicalWorkflow
    workflowTaskRelationList: tuple[WorkflowTaskRelationRecord, ...] | None
    taskDefinitionList: tuple[TaskPayloadRecord, ...] | None


def _task_record(
    item: OpaqueGeneratedValue,
    *,
    spec: _TaskVersionSpec,
    ds_version: str,
    expected_project_code: int,
    expected_task_code: int | None,
) -> TaskPayloadRecord:
    code = _positive_int(
        _required_attr(item, "code", ds_version=ds_version),
        field="task.code",
        ds_version=ds_version,
    )
    project_code = _positive_int(
        _required_attr(item, "projectCode", ds_version=ds_version),
        field="task.projectCode",
        ds_version=ds_version,
    )
    if expected_task_code is not None and code != expected_task_code:
        raise _identity_error(
            ds_version=ds_version,
            field="task.code",
            expected=expected_task_code,
            actual=code,
        )
    if project_code != expected_project_code:
        raise _identity_error(
            ds_version=ds_version,
            field="task.projectCode",
            expected=expected_project_code,
            actual=project_code,
        )
    version = _positive_int(
        _required_attr(item, "version", ds_version=ds_version),
        field="task.version",
        ds_version=ds_version,
    )
    record = _CanonicalTaskDefinition(
        id=cast("int | None", _required_attr(item, "id", ds_version=ds_version)),
        code=code,
        name=cast("str | None", _required_attr(item, "name", ds_version=ds_version)),
        version=version,
        projectCode=project_code,
        description=cast(
            "str | None",
            _required_attr(item, "description", ds_version=ds_version),
        ),
        taskType=cast(
            "str | None",
            _required_attr(item, "taskType", ds_version=ds_version),
        ),
        taskParams=cast(
            "JsonValue | None",
            _required_attr(item, "taskParams", ds_version=ds_version),
        ),
        userName=cast(
            "str | None",
            _required_attr(item, "userName", ds_version=ds_version),
        ),
        projectName=cast(
            "str | None",
            _required_attr(item, "projectName", ds_version=ds_version),
        ),
        workerGroup=cast(
            "str | None",
            _required_attr(item, "workerGroup", ds_version=ds_version),
        ),
        failRetryTimes=cast(
            "int",
            _required_attr(item, "failRetryTimes", ds_version=ds_version),
        ),
        failRetryInterval=cast(
            "int",
            _required_attr(item, "failRetryInterval", ds_version=ds_version),
        ),
        timeout=cast("int", _required_attr(item, "timeout", ds_version=ds_version)),
        delayTime=cast("int", _required_attr(item, "delayTime", ds_version=ds_version)),
        resourceIds=cast(
            "str | None",
            _required_attr(item, "resourceIds", ds_version=ds_version),
        ),
        createTime=cast(
            "str | None",
            _required_attr(item, "createTime", ds_version=ds_version),
        ),
        updateTime=cast(
            "str | None",
            _required_attr(item, "updateTime", ds_version=ds_version),
        ),
        modifyBy=cast(
            "str | None",
            _required_attr(item, "modifyBy", ds_version=ds_version),
        ),
        taskGroupId=(
            cast("int", _required_attr(item, "taskGroupId", ds_version=ds_version))
            if spec.task_group_fields
            else 0
        ),
        taskGroupPriority=(
            cast(
                "int",
                _required_attr(item, "taskGroupPriority", ds_version=ds_version),
            )
            if spec.task_group_fields
            else 0
        ),
        environmentCode=cast(
            "int",
            _required_attr(item, "environmentCode", ds_version=ds_version),
        ),
        taskPriority=cast(
            "StringEnumValue | None",
            _required_attr(item, "taskPriority", ds_version=ds_version),
        ),
        timeoutFlag=cast(
            "StringEnumValue | None",
            _required_attr(item, "timeoutFlag", ds_version=ds_version),
        ),
        timeoutNotifyStrategy=cast(
            "StringEnumValue | None",
            _required_attr(item, "timeoutNotifyStrategy", ds_version=ds_version),
        ),
        taskExecuteType=(
            cast(
                "StringEnumValue | None",
                _required_attr(item, "taskExecuteType", ds_version=ds_version),
            )
            if spec.resource_limit_fields
            else None
        ),
        flag=cast(
            "StringEnumValue | None",
            _required_attr(item, "flag", ds_version=ds_version),
        ),
        isCache=(
            cast(
                "StringEnumValue | None",
                _required_attr(item, "isCache", ds_version=ds_version),
            )
            if "isCache" in spec.field_policy.opaque_preservation
            else None
        ),
        cpuQuota=(
            cast(
                "int | None",
                _required_attr(item, "cpuQuota", ds_version=ds_version),
            )
            if spec.resource_limit_fields
            else None
        ),
        memoryMax=(
            cast(
                "int | None",
                _required_attr(item, "memoryMax", ds_version=ds_version),
            )
            if spec.resource_limit_fields
            else None
        ),
    )
    return cast("TaskPayloadRecord", record)


def project_exact_task_payload(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_project_code: int,
    expected_task_code: int | None,
) -> TaskPayloadRecord:
    """Project one exact generated task model into the stable task protocol."""
    return _task_record(
        item,
        spec=_require_task_spec(ds_version),
        ds_version=ds_version,
        expected_project_code=expected_project_code,
        expected_task_code=expected_task_code,
    )


def _dag_record(
    item: OpaqueGeneratedValue,
    *,
    spec: _TaskVersionSpec,
    ds_version: str,
    expected_project_code: int,
    expected_workflow_code: int,
) -> _CanonicalDag:
    try:
        workflow_raw = _required_attr(
            item,
            spec.dag_workflow_field,
            ds_version=ds_version,
        )
    except WireContractError as exc:
        raise WorkflowDagProjectionError(
            ds_version=ds_version,
            field=spec.dag_workflow_field,
            reason="workflow DAG omitted its definition",
        ) from exc
    if workflow_raw is None:
        raise WorkflowDagProjectionError(
            ds_version=ds_version,
            field=spec.dag_workflow_field,
            reason="workflow DAG omitted its definition",
        )
    try:
        workflow = _workflow_record(
            workflow_raw,
            ds_version=ds_version,
            expected_project_code=expected_project_code,
            expected_workflow_code=expected_workflow_code,
        )
    except WireContractError as exc:
        raise WorkflowDagProjectionError(
            ds_version=ds_version,
            field=spec.dag_workflow_field,
            reason="generated workflow definition violated its exact contract",
        ) from exc
    try:
        relations_raw = _required_attr(
            item,
            spec.dag_relation_field,
            ds_version=ds_version,
        )
        relations = (
            None
            if relations_raw is None
            else tuple(
                cast(
                    "WorkflowTaskRelationRecord",
                    _relation_record(relation, ds_version=ds_version),
                )
                for relation in _sequence(relations_raw, ds_version=ds_version)
            )
        )
    except WireContractError as exc:
        raise WorkflowDagProjectionError(
            ds_version=ds_version,
            field=spec.dag_relation_field,
            reason="generated workflow relations violated their exact contract",
        ) from exc
    try:
        tasks_raw = _required_attr(
            item,
            "taskDefinitionList",
            ds_version=ds_version,
        )
        tasks = (
            None
            if tasks_raw is None
            else tuple(
                _task_record(
                    task,
                    spec=spec,
                    ds_version=ds_version,
                    expected_project_code=expected_project_code,
                    expected_task_code=None,
                )
                for task in _sequence(tasks_raw, ds_version=ds_version)
            )
        )
    except WireContractError as exc:
        raise WorkflowDagProjectionError(
            ds_version=ds_version,
            field="taskDefinitionList",
            reason="generated workflow DAG violated its exact task contract",
        ) from exc
    return _CanonicalDag(
        workflowDefinition=workflow,
        workflowTaskRelationList=relations,
        taskDefinitionList=tasks,
    )


def project_exact_workflow_dag(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_project_code: int,
    expected_workflow_code: int,
    scalar_metadata: WorkflowScalarMetadata | None = None,
) -> WorkflowDagRecord:
    """Project one exact generated DAG into stable workflow/task protocols."""
    dag = _dag_record(
        item,
        spec=_require_task_spec(ds_version),
        ds_version=ds_version,
        expected_project_code=expected_project_code,
        expected_workflow_code=expected_workflow_code,
    )

    if scalar_metadata is not None:
        dag = replace(
            dag,
            workflowDefinition=replace(
                dag.workflowDefinition,
                globalParams=scalar_metadata.global_params,
                globalParamMap=scalar_metadata.global_param_map,
                timeout=scalar_metadata.timeout,
            ),
        )
    return cast("WorkflowDagRecord", dag)


def requires_task_cache_preservation(ds_version: str) -> bool:
    """Return whether workflow authoring must preserve exact task cache state."""
    try:
        raw_profile = TASK_DEFINITION_PROFILES[ds_version]
    except KeyError as exc:
        message = f"DS {ds_version} has no task-definition profile"
        raise WireContractError(message) from exc
    profile = _mapping(raw_profile, label=f"DS {ds_version} task profile")
    request_fields = _text_set(profile, "request_fields")
    return _preserves_is_cache(ds_version, profile, request_fields)


def _workflow_record(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_project_code: int,
    expected_workflow_code: int,
) -> CanonicalWorkflow:
    code = _positive_int(
        _required_attr(item, "code", ds_version=ds_version),
        field="workflow.code",
        ds_version=ds_version,
    )
    project_code = _positive_int(
        _required_attr(item, "projectCode", ds_version=ds_version),
        field="workflow.projectCode",
        ds_version=ds_version,
    )
    if code != expected_workflow_code:
        raise _identity_error(
            ds_version=ds_version,
            field="workflow.code",
            expected=expected_workflow_code,
            actual=code,
        )
    if project_code != expected_project_code:
        raise _identity_error(
            ds_version=ds_version,
            field="workflow.projectCode",
            expected=expected_project_code,
            actual=project_code,
        )
    return CanonicalWorkflow(
        id=cast("int | None", _required_attr(item, "id", ds_version=ds_version)),
        code=code,
        name=cast("str | None", _required_attr(item, "name", ds_version=ds_version)),
        version=cast(
            "int | None", _required_attr(item, "version", ds_version=ds_version)
        ),
        projectCode=project_code,
        description=cast(
            "str | None",
            _required_attr(item, "description", ds_version=ds_version),
        ),
        globalParams=cast(
            "str | None",
            _required_attr(item, "globalParams", ds_version=ds_version),
        ),
        globalParamMap=cast(
            "dict[str, str] | None",
            _required_attr(item, "globalParamMap", ds_version=ds_version),
        ),
        createTime=cast(
            "str | None",
            _required_attr(item, "createTime", ds_version=ds_version),
        ),
        updateTime=cast(
            "str | None",
            _required_attr(item, "updateTime", ds_version=ds_version),
        ),
        userId=cast("int", _required_attr(item, "userId", ds_version=ds_version)),
        userName=cast(
            "str | None",
            _required_attr(item, "userName", ds_version=ds_version),
        ),
        projectName=cast(
            "str | None",
            _required_attr(item, "projectName", ds_version=ds_version),
        ),
        timeout=cast("int", _required_attr(item, "timeout", ds_version=ds_version)),
        releaseState=cast(
            "StringEnumValue | None",
            _required_attr(item, "releaseState", ds_version=ds_version),
        ),
        scheduleReleaseState=cast(
            "StringEnumValue | None",
            _required_attr(item, "scheduleReleaseState", ds_version=ds_version),
        ),
        schedule=None,
        executionType=cast(
            "StringEnumValue | None",
            _optional_attr(item, "executionType"),
        ),
    )


def _relation_record(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> _CanonicalTaskRelation:
    return _CanonicalTaskRelation(
        preTaskCode=cast(
            "int", _required_attr(item, "preTaskCode", ds_version=ds_version)
        ),
        postTaskCode=cast(
            "int", _required_attr(item, "postTaskCode", ds_version=ds_version)
        ),
        preTaskVersion=cast(
            "int", _required_attr(item, "preTaskVersion", ds_version=ds_version)
        ),
        postTaskVersion=cast(
            "int", _required_attr(item, "postTaskVersion", ds_version=ds_version)
        ),
        conditionParams=cast(
            "JsonValue | None",
            _required_attr(item, "conditionParams", ds_version=ds_version),
        ),
    )


def _required_attr(
    item: OpaqueGeneratedValue, name: str, *, ds_version: str
) -> OpaqueGeneratedValue:
    try:
        return getattr(item, name)
    except AttributeError as exc:
        message = f"DS {ds_version} generated task payload omitted field {name!r}"
        raise WireContractError(message) from exc


def _optional_attr(
    item: OpaqueGeneratedValue, name: str
) -> OpaqueGeneratedValue | None:
    return getattr(item, name, None)


def _sequence(
    value: OpaqueGeneratedValue, *, ds_version: str
) -> Sequence[OpaqueGeneratedValue]:
    if isinstance(value, (list, tuple)):
        return value
    message = f"DS {ds_version} generated DAG collection was not a sequence"
    raise WireContractError(message)


def _positive_int(value: OpaqueGeneratedValue, *, field: str, ds_version: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    message = f"DS {ds_version} generated {field} was not a positive integer"
    raise WireContractError(message)


def _identity_error(
    *,
    ds_version: str,
    field: str,
    expected: int,
    actual: int,
) -> WireContractError:
    return WireContractError(
        f"DS {ds_version} generated {field} identity mismatch: "
        f"expected {expected}, got {actual}"
    )


def _fingerprint(*parts: str) -> str:
    identity = "\0".join(parts).encode()
    return f"sha256:{hashlib.sha256(identity).hexdigest()}"


__all__ = [
    "TaskDefinitionAdapter",
    "TaskDefinitionWire",
    "TaskTopLevelFieldPolicy",
    "TaskUpdateContractFeatures",
    "TaskUpdateWirePolicy",
    "WorkflowDagProjectionError",
    "bind_task_definition_wire",
    "requires_task_cache_preservation",
    "task_update_contract_features",
]
