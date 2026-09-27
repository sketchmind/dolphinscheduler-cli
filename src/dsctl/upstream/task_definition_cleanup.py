from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiTransportError
from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles
from dsctl.release_gate.task_definition_cleanup import (
    CleanupTaskDetail,
    CleanupTaskHistoryPage,
    CleanupTaskPage,
    CleanupTaskRef,
    TaskDefinitionCleanupPort,
    TaskDeleteAmbiguousError,
    TaskReleaseAmbiguousError,
    canonical_task_params_fingerprint,
)
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._compiled_workflow_runtime import WorkflowPrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms


_SUPPORTED_CLEANUP_PROFILE_SCHEMA_VERSION = 4


@dataclass(frozen=True)
class _CleanupProfile:
    strategy: Literal["direct-delete", "workflow-cascade-proof-only"]
    page_params_epoch: str
    row_fields: tuple[str, str, str]
    execute_types: tuple[str, ...]
    workflow_binding_fields: tuple[str, str, str, str] | None
    history_model: str | None
    pre_delete_release: Literal["none", "offline"]


@dataclass(frozen=True)
class _CleanupBindings:
    ds_version: str
    profile: _CleanupProfile
    programs: BoundCompiledPrograms[WorkflowPrimitive]


class TaskDefinitionCleanupAdapter:
    """Bind the private release gate to the jointly owned exact programs."""

    def __init__(self, ds_version: str) -> None:
        """Select the reviewed cleanup policy and its exact compiled owner."""
        self._profile = _load_cleanup_profile(ds_version)
        if (
            cleanup_profiles.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
            != "release-gate.task-definition.cleanup"
        ):
            message = "Generated task-definition cleanup operations changed"
            raise WireContractError(message)
        self._wire_profile = WORKFLOW_PROGRAMS.profile(ds_version)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> TaskDefinitionCleanupAdapter:
        """Load one explicitly reviewed task-definition cleanup recipe."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskDefinitionCleanupPort:
        """Bind cleanup operations to the shared DS HTTP transport."""
        bindings = _CleanupBindings(
            ds_version=self.ds_version,
            profile=self._profile,
            programs=WORKFLOW_PROGRAMS.bind(
                self._wire_profile, profile, http_client=http_client
            ),
        )
        return cast(
            "TaskDefinitionCleanupPort",
            _TaskDefinitionCleanupPort(
                bindings=bindings,
            ),
        )


@dataclass(frozen=True)
class _TaskDefinitionCleanupPort:
    bindings: _CleanupBindings

    @property
    def ds_version(self) -> str:
        return self.bindings.ds_version

    @property
    def cleanup_strategy(
        self,
    ) -> Literal["direct-delete", "workflow-cascade-proof-only"]:
        return self.bindings.profile.strategy

    @property
    def inventory_execute_types(self) -> tuple[str, ...]:
        return self.bindings.profile.execute_types

    @property
    def pre_delete_release(self) -> Literal["none", "offline"]:
        return self.bindings.profile.pre_delete_release

    def list_page(
        self,
        *,
        project_code: int,
        execute_type: str | None,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskPage:
        params = _page_params(
            self.bindings,
            execute_type=execute_type,
            page_no=page_no,
            page_size=page_size,
        )
        payload = self.bindings.programs.call(
            "task_cleanup_page", {"projectCode": project_code, **params}
        )
        return _project_page(
            payload,
            profile=self.bindings.profile,
            execute_type=execute_type,
            requested_page=page_no,
        )

    def get_detail(
        self,
        *,
        project_code: int,
        code: int,
    ) -> CleanupTaskDetail:
        payload = self.bindings.programs.call(
            "task_get", {"projectCode": project_code, "code": code}
        )
        return _project_detail(payload)

    def list_history_page(
        self,
        *,
        project_code: int,
        code: int,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskHistoryPage:
        if self.bindings.profile.history_model is None:
            message = "Task-definition history is unavailable for this cleanup strategy"
            raise WireContractError(message)
        payload = self.bindings.programs.call(
            "task_cleanup_history",
            {
                "projectCode": project_code,
                "code": code,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        return _project_history_page(payload, requested_page=page_no)

    def delete(self, *, project_code: int, code: int) -> None:
        if self.cleanup_strategy != "direct-delete":
            message = "Workflow-cascade proof-only cleanup cannot delete tasks"
            raise WireContractError(message)
        try:
            self.bindings.programs.call(
                "task_cleanup_delete", {"projectCode": project_code, "code": code}
            )
        except ApiTransportError as exc:
            message = "task delete result is ambiguous and requires reconciliation"
            raise TaskDeleteAmbiguousError(message) from exc

    def release_offline(self, *, project_code: int, code: int) -> None:
        if self.pre_delete_release != "offline":
            message = "Task-definition offline release is unavailable for this profile"
            raise WireContractError(message)
        try:
            self.bindings.programs.call(
                "task_cleanup_release",
                {
                    "projectCode": project_code,
                    "code": code,
                    "releaseState": "OFFLINE",
                },
            )
        except ApiTransportError as exc:
            message = "task offline result is ambiguous and requires reconciliation"
            raise TaskReleaseAmbiguousError(message) from exc

    def prove_no_workflows(self, *, project_code: int) -> None:
        payload = self.bindings.programs.call(
            "definition_page",
            {
                "projectCode": project_code,
                "searchVal": None,
                "pageNo": 1,
                "pageSize": 100,
            },
        )
        rows = _required_attr(payload, "totalList")
        if not (
            (rows is None or (type(rows) is list and not rows))
            and _required_int(payload, "currentPage") == 1
            and _required_int(payload, "pageSize") == 100
            and _required_int(payload, "total") == 0
            and _required_int(payload, "totalPage") in {0, 1}
        ):
            message = "project workflow inventory is not freshly proven empty"
            raise WireContractError(message)


def _load_cleanup_profile(ds_version: str) -> _CleanupProfile:
    profile_data = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA
    if not isinstance(profile_data, dict) or set(profile_data) != {
        "schema_version",
        "semantic_operation",
        "target_versions",
        "full_core_reconciliation_versions",
        "full_core_versions",
        "cross_process_recovery_versions",
        "profiles",
    }:
        message = "Generated task-definition cleanup profile header drifted"
        raise WireContractError(message)
    schema_version = profile_data.get("schema_version")
    semantic_operation = profile_data.get("semantic_operation")
    target_versions = tuple(cleanup_profiles.TARGET_TASK_DEFINITION_CLEANUP_VERSIONS)
    reconciliation_versions = tuple(
        cleanup_profiles.FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
    )
    full_core_versions = tuple(
        cleanup_profiles.FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS
    )
    cross_process_versions = tuple(
        cleanup_profiles.CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS
    )
    if (
        type(schema_version) is not int
        or schema_version != _SUPPORTED_CLEANUP_PROFILE_SCHEMA_VERSION
        or not isinstance(semantic_operation, str)
        or not semantic_operation
        or len(target_versions) != len(set(target_versions))
        or len(reconciliation_versions) != len(set(reconciliation_versions))
        or not set(full_core_versions).issubset(target_versions)
        or not set(reconciliation_versions).issubset(target_versions)
        or not set(cross_process_versions).issubset(reconciliation_versions)
    ):
        message = "Generated task-definition cleanup profile header drifted"
        raise WireContractError(message)
    profiles = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES
    if not isinstance(profiles, dict) or tuple(profiles) != target_versions:
        message = "Generated task-definition cleanup profile identity drifted"
        raise WireContractError(message)
    if ds_version not in target_versions:
        message = f"DS {ds_version} has no reviewed task-definition cleanup recipe"
        raise WireContractError(message)
    raw_profile = profiles.get(ds_version)
    if not isinstance(raw_profile, dict):
        message = "Generated task-definition cleanup profile is missing"
        raise WireContractError(message)
    return _parse_cleanup_profile(raw_profile)


def _parse_cleanup_profile(
    raw: Mapping[str, JsonValue],
) -> _CleanupProfile:
    expected_keys = {
        "strategy",
        "page_params_epoch",
        "page_model",
        "row_fields",
        "execute_types",
        "workflow_binding_fields",
        "history_model",
        "delete_response",
        "pre_delete_release",
    }
    if set(raw) != expected_keys:
        message = "Generated task-definition cleanup profile shape drifted"
        raise WireContractError(message)
    strategy = raw.get("strategy")
    epoch = raw.get("page_params_epoch")
    page_model = raw.get("page_model")
    row_fields = raw.get("row_fields")
    execute_types = raw.get("execute_types")
    workflow_binding_fields = raw.get("workflow_binding_fields")
    history_model = raw.get("history_model")
    delete_response = raw.get("delete_response")
    pre_delete_release = raw.get("pre_delete_release")
    if (
        strategy not in {"direct-delete", "workflow-cascade-proof-only"}
        or not isinstance(epoch, str)
        or epoch not in {"task-search", "workflow-task-search", "execute-type"}
        or not isinstance(page_model, str)
        or not page_model.strip()
        or not isinstance(row_fields, list)
        or len(row_fields) != 3
        or not all(isinstance(field, str) and field for field in row_fields)
        or len(row_fields) != len(set(row_fields))
        or not isinstance(execute_types, list)
        or not all(isinstance(value, str) and value for value in execute_types)
        or len(execute_types) != len(set(execute_types))
        or bool(execute_types) != (epoch == "execute-type")
        or (
            workflow_binding_fields is not None
            and (
                not isinstance(workflow_binding_fields, list)
                or len(workflow_binding_fields) != 4
                or not all(
                    isinstance(value, str) and value
                    for value in workflow_binding_fields
                )
                or len(workflow_binding_fields) != len(set(workflow_binding_fields))
            )
        )
        or (
            history_model is not None
            and (not isinstance(history_model, str) or not history_model.strip())
        )
        or delete_response not in {"void", "optional-process-definition", "unavailable"}
        or pre_delete_release not in {"none", "offline"}
    ):
        message = "Generated task-definition cleanup profile recipe drifted"
        raise WireContractError(message)
    rows = cast("tuple[str, str, str]", tuple(row_fields))
    scopes = tuple(cast("list[str]", execute_types))
    bindings = (
        cast("tuple[str, str, str, str]", tuple(workflow_binding_fields))
        if isinstance(workflow_binding_fields, list)
        else None
    )
    direct_shape = (
        strategy == "direct-delete"
        and bindings is None
        and history_model is None
        and delete_response in {"void", "optional-process-definition"}
        and (
            pre_delete_release in {"none", "offline"}
            and (pre_delete_release == "none" or delete_response == "void")
        )
    )
    proof_shape = (
        strategy == "workflow-cascade-proof-only"
        and epoch == "execute-type"
        and bindings is not None
        and isinstance(history_model, str)
        and bool(history_model.strip())
        and delete_response == "unavailable"
        and pre_delete_release == "none"
    )
    if not direct_shape and not proof_shape:
        message = "Generated task-definition cleanup strategy recipe drifted"
        raise WireContractError(message)
    return _CleanupProfile(
        strategy=cast(
            'Literal["direct-delete", "workflow-cascade-proof-only"]',
            strategy,
        ),
        page_params_epoch=epoch,
        row_fields=rows,
        execute_types=scopes,
        workflow_binding_fields=bindings,
        history_model=history_model,
        pre_delete_release=cast('Literal["none", "offline"]', pre_delete_release),
    )


def _page_params(
    bindings: _CleanupBindings,
    *,
    execute_type: str | None,
    page_no: int,
    page_size: int,
) -> JsonObject:
    epoch = bindings.profile.page_params_epoch
    if epoch == "task-search":
        if execute_type is not None:
            message = "Legacy task-definition inventory does not accept execute type"
            raise WireContractError(message)
        return {
            "taskType": None,
            "searchVal": None,
            "userId": 0,
            "pageNo": page_no,
            "pageSize": page_size,
        }
    if epoch == "workflow-task-search":
        if execute_type is not None:
            message = "Task-definition execute type is unsupported for this profile"
            raise WireContractError(message)
        return {
            "searchWorkflowName": None,
            "searchTaskName": None,
            "taskType": "",
            "pageNo": page_no,
            "pageSize": page_size,
        }
    if execute_type not in bindings.profile.execute_types:
        message = "Task-definition execute type is not a reviewed inventory scope"
        raise WireContractError(message)
    return {
        "searchWorkflowName": None,
        "searchTaskName": None,
        "taskType": "",
        "taskExecuteType": execute_type,
        "pageNo": page_no,
        "pageSize": page_size,
    }


def _project_page(
    payload: OpaqueGeneratedValue,
    *,
    profile: _CleanupProfile,
    execute_type: str | None,
    requested_page: int,
) -> CleanupTaskPage:
    rows_value = _required_attr(payload, "totalList")
    if rows_value is None:
        rows: list[OpaqueGeneratedValue] = []
    elif isinstance(rows_value, list):
        rows = rows_value
    else:
        message = "Generated task-definition cleanup page rows drifted"
        raise WireContractError(message)
    current_page = _required_int(payload, "currentPage")
    page_size = _required_int(payload, "pageSize")
    return CleanupTaskPage(
        execute_type=execute_type,
        requested_page=requested_page,
        current_page=current_page,
        offset=(current_page - 1) * page_size,
        page_size=page_size,
        total=_required_int(payload, "total"),
        total_pages=_required_int(payload, "totalPage"),
        rows=tuple(_project_ref(row, profile=profile) for row in rows),
    )


def _project_ref(
    row: OpaqueGeneratedValue,
    *,
    profile: _CleanupProfile,
) -> CleanupTaskRef:
    code_field, name_field, version_field = profile.row_fields
    workflow_fields = profile.workflow_binding_fields
    if workflow_fields is None:
        workflow_code = None
        workflow_version = None
        workflow_name = None
        workflow_release_state = None
    else:
        (
            workflow_code_field,
            workflow_version_field,
            workflow_name_field,
            workflow_release_state_field,
        ) = workflow_fields
        workflow_code = _required_int(row, workflow_code_field)
        workflow_version = _required_int(row, workflow_version_field)
        workflow_name = _required_text(row, workflow_name_field)
        workflow_release_state = _required_enum_text(
            row,
            workflow_release_state_field,
        )
    return CleanupTaskRef(
        code=_required_int(row, code_field),
        name=_required_text(row, name_field),
        version=_required_int(row, version_field),
        workflow_code=workflow_code,
        workflow_version=workflow_version,
        workflow_name=workflow_name,
        workflow_release_state=workflow_release_state,
    )


def _project_history_page(
    payload: OpaqueGeneratedValue,
    *,
    requested_page: int,
) -> CleanupTaskHistoryPage:
    rows_value = _required_attr(payload, "totalList")
    if rows_value is None:
        rows: list[OpaqueGeneratedValue] = []
    elif isinstance(rows_value, list):
        rows = rows_value
    else:
        message = "Generated task-definition cleanup history rows drifted"
        raise WireContractError(message)
    current_page = _required_int(payload, "currentPage")
    page_size = _required_int(payload, "pageSize")
    return CleanupTaskHistoryPage(
        requested_page=requested_page,
        current_page=current_page,
        offset=(current_page - 1) * page_size,
        page_size=page_size,
        total=_required_int(payload, "total"),
        total_pages=_required_int(payload, "totalPage"),
        rows=tuple(_project_detail(row) for row in rows),
    )


def _project_detail(payload: OpaqueGeneratedValue) -> CleanupTaskDetail:
    task_params = _required_attr(payload, "taskParams")
    if type(task_params) is not dict or set(task_params) != {
        "rawScript",
        "localParams",
        "resourceList",
    }:
        message = "Generated task-definition taskParams keys drifted"
        raise WireContractError(message)
    raw_script = task_params["rawScript"]
    local_params = task_params["localParams"]
    resource_list = task_params["resourceList"]
    if (
        type(raw_script) is not str
        or type(local_params) is not list
        or local_params
        or type(resource_list) is not list
        or resource_list
    ):
        message = "Generated task-definition taskParams shape drifted"
        raise WireContractError(message)
    flag = _required_enum_text(payload, "flag")
    if flag not in {"YES", "NO"}:
        message = "Generated task-definition cleanup flag is not YES or NO"
        raise WireContractError(message)
    return CleanupTaskDetail(
        code=_required_int(payload, "code"),
        name=_required_text(payload, "name"),
        version=_required_int(payload, "version"),
        project_code=_required_int(payload, "projectCode"),
        task_type=_required_text(payload, "taskType"),
        flag=cast("Literal['YES', 'NO']", flag),
        description=_required_text(payload, "description"),
        raw_script=raw_script,
        task_params_fingerprint=canonical_task_params_fingerprint(raw_script),
    )


def _required_attr(value: OpaqueGeneratedValue, name: str) -> OpaqueGeneratedValue:
    try:
        return getattr(value, name)
    except AttributeError as exc:
        message = f"Generated task-definition cleanup response lacks {name!r}"
        raise WireContractError(message) from exc


def _required_int(value: OpaqueGeneratedValue, name: str) -> int:
    field = _required_attr(value, name)
    if type(field) is int:
        return field
    message = f"Generated task-definition cleanup field {name!r} is not an integer"
    raise WireContractError(message)


def _required_text(value: OpaqueGeneratedValue, name: str) -> str:
    field = _required_attr(value, name)
    if type(field) is str:
        return field
    message = f"Generated task-definition cleanup field {name!r} is not text"
    raise WireContractError(message)


def _required_enum_text(value: OpaqueGeneratedValue, name: str) -> str:
    field = _required_attr(value, name)
    if isinstance(field, Enum) and type(field.value) is str:
        return field.value
    message = f"Generated task-definition cleanup field {name!r} is not a text enum"
    raise WireContractError(message)


__all__ = ["TaskDefinitionCleanupAdapter"]
