from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, NoReturn, TypeAlias, TypedDict, TypeGuard, cast

from dsctl.cli_surface import DATASOURCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    DsctlError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
    UserInputError,
)
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.upstream.resolver import datasource, datasource_name
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream._resolver_models import ResolvedDataSource
    from dsctl.upstream.legacy_workflow_graph import DecodedLegacyWorkflowGraph
    from dsctl.upstream.protocol import DataSourceOperations, WorkflowDagRecord


_RESOURCE_NOT_EXIST = 20004
_RESOURCE_NOT_EXIST_OR_NO_PERMISSION = 20016
_USER_NO_OPERATION_PERMISSION = 30001
_NO_CURRENT_OPERATING_PERMISSION = 1400001
_INTERNAL_SERVER_ERROR_ARGS = 10000
_RESOURCE_PERMISSION_MESSAGE = "have no permission to access the resource"
_DATASOURCE_TASK_TYPES = frozenset(
    {
        "ALIYUN_SERVERLESS_SPARK",
        "DATA_QUALITY",
        "K8S",
        "PROCEDURE",
        "REMOTESHELL",
        "SAGEMAKER",
        "SQL",
        "ZEPPELIN",
    }
)

TaskDatasourceBaseline: TypeAlias = Mapping[
    str,
    tuple[str, int | None, str | None],
]


class TaskDatasourceResolutionData(TypedDict):
    """One verified task datasource selection exposed by mutation dry-runs."""

    task: str
    task_type: str
    requested: int | str
    id: int
    name: str
    type: str


def task_datasource_baseline_from_dag(
    dag: WorkflowDagRecord,
) -> dict[str, tuple[str, int | None, str | None]]:
    """Read only native datasource ids needed to avoid rechecking baselines."""
    baseline: dict[str, tuple[str, int | None, str | None]] = {}
    for task in dag.taskDefinitionList or ():
        name = task.name
        task_type = enum_value(task.taskType)
        if not isinstance(name, str) or not isinstance(task_type, str):
            continue
        params = _task_params_object(task.taskParams)
        baseline[name] = (
            task_type,
            _positive_datasource_id(params),
            _datasource_semantic_mode(task_type, params),
        )
    return baseline


def task_datasource_baseline_from_legacy_graph(
    graph: DecodedLegacyWorkflowGraph,
) -> dict[str, tuple[str, int | None, str | None]]:
    """Read canonical ids already decoded from one native 1.3.9 graph."""
    baseline: dict[str, tuple[str, int | None, str | None]] = {}
    for task in graph.tasks:
        params = task.document.get("task_params")
        params_mapping = params if isinstance(params, Mapping) else None
        baseline[task.name] = (
            task.type,
            _positive_datasource_id(params_mapping),
            _datasource_semantic_mode(task.type, params_mapping),
        )
    return baseline


def resolve_workflow_spec_task_datasources(
    datasources: DataSourceOperations | None,
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
    action: str,
    baseline: TaskDatasourceBaseline | None = None,
) -> tuple[WorkflowSpec, list[TaskDatasourceResolutionData]]:
    """Resolve canonical task datasource names and verify changed numeric ids."""
    session = _TaskDatasourceResolutionSession(datasources, action=action)
    baseline_by_name = {} if baseline is None else baseline
    tasks = [
        _resolve_task(
            session,
            task,
            catalog=catalog,
            baseline=baseline_by_name.get(task.name),
        )
        for task in spec.tasks
    ]
    return spec.model_copy(update={"tasks": tasks}), session.resolutions


def resolve_workflow_patch_task_datasources(
    datasources: DataSourceOperations | None,
    patch: WorkflowPatchSpec,
    *,
    catalog: TaskAuthoringCatalog,
    action: str,
    baseline: TaskDatasourceBaseline,
) -> tuple[WorkflowPatchSpec, list[TaskDatasourceResolutionData]]:
    """Resolve datasource references explicitly carried by task patch entries."""
    if patch.tasks is None:
        return patch, []
    session = _TaskDatasourceResolutionSession(datasources, action=action)
    created = [
        _resolve_task(session, task, catalog=catalog, baseline=None)
        for task in patch.tasks.create
    ]
    updated = []
    for update in patch.tasks.update:
        task_set = update.set
        params = task_set.task_params
        baseline_task = baseline.get(update.match.name)
        task_type = task_set.type or (
            None if baseline_task is None else baseline_task[0]
        )
        if not isinstance(task_type, str) or not isinstance(params, Mapping):
            updated.append(update)
            continue
        resolved_params = _resolve_task_params(
            session,
            task_name=update.match.name,
            task_type=task_type,
            params=dict(params),
            catalog=catalog,
            baseline=baseline_task,
        )
        updated.append(
            update.model_copy(
                update={
                    "set": task_set.model_copy(update={"task_params": resolved_params})
                }
            )
        )
    tasks = patch.tasks.model_copy(update={"create": created, "update": updated})
    return patch.model_copy(update={"tasks": tasks}), session.resolutions


def resolve_workflow_edit_task_datasources(
    datasources: DataSourceOperations | None,
    *,
    patch: WorkflowPatchSpec | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    action: str,
    baseline: TaskDatasourceBaseline,
) -> tuple[
    WorkflowPatchSpec | None,
    WorkflowSpec | None,
    list[TaskDatasourceResolutionData],
]:
    """Resolve whichever mutually exclusive edit input carries task params."""
    if patch is not None:
        resolved_patch, resolutions = resolve_workflow_patch_task_datasources(
            datasources,
            patch,
            catalog=catalog,
            action=action,
            baseline=baseline,
        )
        return resolved_patch, spec, resolutions
    if spec is not None:
        resolved_spec, resolutions = resolve_workflow_spec_task_datasources(
            datasources,
            spec,
            catalog=catalog,
            action=action,
            baseline=baseline,
        )
        return patch, resolved_spec, resolutions
    return patch, spec, []


def _resolve_task(
    session: _TaskDatasourceResolutionSession,
    task: WorkflowTaskSpec,
    *,
    catalog: TaskAuthoringCatalog,
    baseline: tuple[str, int | None, str | None] | None,
) -> WorkflowTaskSpec:
    params = task.task_params
    if not isinstance(params, Mapping):
        return task
    resolved = _resolve_task_params(
        session,
        task_name=task.name,
        task_type=task.type,
        params=dict(params),
        catalog=catalog,
        baseline=baseline,
    )
    return task.model_copy(update={"task_params": resolved})


def _resolve_task_params(
    session: _TaskDatasourceResolutionSession,
    *,
    task_name: str,
    task_type: str,
    params: YamlObject,
    catalog: TaskAuthoringCatalog,
    baseline: tuple[str, int | None, str | None] | None,
) -> YamlObject:
    requested = params.get("datasource")
    if not _is_datasource_reference(requested):
        return params
    if isinstance(requested, int) and baseline == (
        task_type,
        requested,
        _datasource_semantic_mode(task_type, params),
    ):
        return params
    expected_type = _canonical_datasource_type(
        task_type,
        params,
        catalog=catalog,
    )
    if expected_type is None:
        if isinstance(requested, str):
            message = (
                f"Task {task_name!r} cannot use a datasource name in this "
                "task parameter mode"
            )
            raise UserInputError(
                message,
                details={
                    "resource": DATASOURCE_RESOURCE,
                    "action": session.action,
                    "task": task_name,
                    "task_type": task_type,
                    "field": "tasks[].task_params.datasource",
                    "requested_reference": requested,
                    "reason": "datasource_name_outside_typed_authoring",
                },
                suggestion=(
                    "Use a reviewed typed datasource-backed task template for the "
                    "selected DolphinScheduler version, or preserve the native id."
                ),
            )
        return params
    resolved = session.resolve(
        requested,
        task_name=task_name,
        task_type=task_type,
        expected_type=expected_type,
    )
    normalized = dict(params)
    normalized["datasource"] = resolved.id
    return normalized


def _canonical_datasource_type(
    task_type: str,
    params: YamlObject,
    *,
    catalog: TaskAuthoringCatalog,
) -> str | None:
    normalized_type = task_type.strip().upper()
    if normalized_type not in _DATASOURCE_TASK_TYPES:
        return None
    if not _is_canonical_typed_params(normalized_type, params, catalog=catalog):
        return None
    return _expected_datasource_type(normalized_type, params, catalog=catalog)


def _is_canonical_typed_params(
    task_type: str,
    params: YamlObject,
    *,
    catalog: TaskAuthoringCatalog,
) -> bool:
    try:
        intent = catalog.effective_authoring_intent(
            task_type,
            requested=TaskAuthoringIntent.TYPED_EDIT,
            task_params=params,
        )
        if intent is not TaskAuthoringIntent.TYPED_EDIT:
            return False
        catalog.normalize_task_params(task_type, params, intent=intent)
    except (DsctlError, TypeError, ValueError):
        return False
    return True


def _expected_datasource_type(
    task_type: str,
    params: YamlObject,
    *,
    catalog: TaskAuthoringCatalog,
) -> str | None:
    surface = catalog.authoring_surface
    if task_type in {"SQL", "PROCEDURE"}:
        declared_type = params.get("type")
        return declared_type.strip().upper() if isinstance(declared_type, str) else None
    if task_type == "DATA_QUALITY":
        return "MYSQL"
    if task_type == "K8S":
        if (
            surface.k8s.connection_mode == "DATASOURCE"
            and params.get("connectionMode") == "DATASOURCE"
        ):
            return "K8S"
        return None
    if task_type == "ALIYUN_SERVERLESS_SPARK":
        return (
            "ALIYUN_SERVERLESS_SPARK"
            if surface.aliyun_serverless_spark.available
            else None
        )
    if task_type == "REMOTESHELL":
        return "SSH" if params.get("type") == "SSH" else None
    if task_type == "SAGEMAKER":
        return "SAGEMAKER" if surface.sagemaker.datasource_required else None
    if task_type == "ZEPPELIN" and (
        surface.zeppelin.connection_mode == "DATASOURCE"
        and params.get("connectionMode") == "DATASOURCE"
    ):
        return "ZEPPELIN"
    return None


class _TaskDatasourceResolutionSession:
    """Cache remote identity lookups for one workflow mutation preparation."""

    def __init__(
        self,
        datasources: DataSourceOperations | None,
        *,
        action: str,
    ) -> None:
        self._datasources = datasources
        self.action = action
        self._cache: dict[tuple[str, int | str], ResolvedDataSource] = {}
        self.resolutions: list[TaskDatasourceResolutionData] = []

    def resolve(
        self,
        requested: int | str,
        *,
        task_name: str,
        task_type: str,
        expected_type: str,
    ) -> ResolvedDataSource:
        if self._datasources is None:
            message = "The selected domain has no exact datasource resolver"
            raise ApiTransportError(
                message,
                details={
                    "resource": DATASOURCE_RESOURCE,
                    "action": self.action,
                    "task": task_name,
                    "task_type": task_type,
                },
                suggestion=(
                    "Verify the selected DolphinScheduler profile and retry the "
                    "workflow mutation."
                ),
            )
        key = ("id" if isinstance(requested, int) else "name", requested)
        try:
            resolved = self._cache.get(key)
            if resolved is None:
                resolved = (
                    datasource(str(requested), adapter=self._datasources)
                    if isinstance(requested, int)
                    else datasource_name(requested, adapter=self._datasources)
                )
                self._cache[key] = resolved
        except NotFoundError as error:
            raise NotFoundError(
                error.message,
                details=_resolution_error_details(
                    error,
                    action=self.action,
                    task_name=task_name,
                    task_type=task_type,
                    requested=requested,
                    expected_type=expected_type,
                ),
                source=error.source,
                suggestion=error.suggestion,
            ) from error
        except ResolutionError as error:
            raise ResolutionError(
                error.message,
                details=_resolution_error_details(
                    error,
                    action=self.action,
                    task_name=task_name,
                    task_type=task_type,
                    requested=requested,
                    expected_type=expected_type,
                ),
                source=error.source,
                suggestion=error.suggestion,
            ) from error
        except ApiResultError as error:
            _raise_task_datasource_api_error(
                error,
                action=self.action,
                task_name=task_name,
                task_type=task_type,
                requested=requested,
                expected_type=expected_type,
            )
        except ApiTransportError as error:
            raise ApiTransportError(
                error.message,
                details=_resolution_error_details(
                    error,
                    action=self.action,
                    task_name=task_name,
                    task_type=task_type,
                    requested=requested,
                    expected_type=expected_type,
                ),
                source=error.source,
                suggestion=error.suggestion,
            ) from error
        except WireContractError as error:
            message = "The selected profile cannot resolve task datasources"
            raise ApiTransportError(
                message,
                details={
                    "resource": DATASOURCE_RESOURCE,
                    "action": self.action,
                    "task": task_name,
                    "task_type": task_type,
                    "requested_reference": requested,
                    "expected_type": expected_type,
                    "reason": str(error),
                },
                suggestion=(
                    "Verify the selected DolphinScheduler version profile and "
                    "retry the workflow mutation."
                ),
            ) from error
        actual_type = resolved.type
        if not isinstance(actual_type, str) or not actual_type.strip():
            message = "Resolved datasource payload was missing its type"
            raise ApiTransportError(
                message,
                details={
                    "resource": DATASOURCE_RESOURCE,
                    "action": self.action,
                    "task": task_name,
                    "task_type": task_type,
                    "requested_reference": requested,
                    "datasource_id": resolved.id,
                    "datasource_name": resolved.name,
                    "expected_type": expected_type,
                    "reason": "missing_datasource_type",
                },
            )
        if actual_type.upper() != expected_type:
            message = (
                f"Datasource {resolved.name!r} has type {actual_type}; task "
                f"{task_name!r} requires {expected_type}"
            )
            raise UserInputError(
                message,
                details={
                    "resource": DATASOURCE_RESOURCE,
                    "action": self.action,
                    "task": task_name,
                    "task_type": task_type,
                    "field": "tasks[].task_params.datasource",
                    "requested_reference": requested,
                    "datasource_id": resolved.id,
                    "datasource_name": resolved.name,
                    "expected_type": expected_type,
                    "actual_type": actual_type,
                    "reason": "datasource_type_mismatch",
                },
                suggestion=(
                    "Run `dsctl datasource list` and choose a visible datasource "
                    f"whose type is {expected_type}; use its exact name or positive id."
                ),
            )
        self.resolutions.append(
            {
                "task": task_name,
                "task_type": task_type,
                "requested": requested,
                "id": resolved.id,
                "name": resolved.name,
                "type": actual_type,
            }
        )
        return resolved


def _task_params_object(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str):
        return None
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, Mapping):
        return None
    return cast("Mapping[str, object]", decoded)


def _positive_datasource_id(
    params: Mapping[str, object] | None,
) -> int | None:
    if params is None:
        return None
    value = params.get("datasource")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _datasource_semantic_mode(
    task_type: str,
    params: Mapping[str, object] | None,
) -> str | None:
    """Return the field that changes one datasource id's required DS type."""
    if params is None:
        return None
    normalized_type = task_type.strip().upper()
    if normalized_type in {"K8S", "ZEPPELIN"}:
        field = "connectionMode"
    elif normalized_type in {"PROCEDURE", "REMOTESHELL", "SQL"}:
        field = "type"
    else:
        return None
    value = params.get(field)
    return value.strip().upper() if isinstance(value, str) else None


def _is_datasource_reference(value: YamlValue | None) -> TypeGuard[int | str]:
    return isinstance(value, str) or (
        isinstance(value, int) and not isinstance(value, bool) and value > 0
    )


def _resolution_error_details(
    error: DsctlError,
    *,
    action: str,
    task_name: str,
    task_type: str,
    requested: int | str,
    expected_type: str,
) -> dict[str, object]:
    return {
        **error.details,
        "resource": DATASOURCE_RESOURCE,
        "action": action,
        "task": task_name,
        "task_type": task_type,
        "field": "tasks[].task_params.datasource",
        "requested_reference": requested,
        "expected_type": expected_type,
    }


def _raise_task_datasource_api_error(
    error: ApiResultError,
    *,
    action: str,
    task_name: str,
    task_type: str,
    requested: int | str,
    expected_type: str,
) -> NoReturn:
    details = {
        "resource": DATASOURCE_RESOURCE,
        "action": action,
        "task": task_name,
        "task_type": task_type,
        "field": "tasks[].task_params.datasource",
        "requested_reference": requested,
        "expected_type": expected_type,
        "result_code": error.result_code,
        "result_message": error.result_message,
    }
    string_only_permission_error = (
        error.result_code == _INTERNAL_SERVER_ERROR_ARGS
        and _RESOURCE_PERMISSION_MESSAGE in error.result_message.lower()
    )
    if (
        error.result_code
        in {_USER_NO_OPERATION_PERMISSION, _NO_CURRENT_OPERATING_PERMISSION}
        or string_only_permission_error
    ):
        message = "Current user cannot access a task datasource"
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl datasource list` as the same user; request datasource "
                "access if it is not visible, then retry."
            ),
        ) from error
    if error.result_code in {
        _RESOURCE_NOT_EXIST,
        _RESOURCE_NOT_EXIST_OR_NO_PERMISSION,
    }:
        message = f"Task datasource {requested!r} was not found or visible"
        raise NotFoundError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl datasource list` as the same user and use one exact "
                "visible datasource name or positive id."
            ),
        ) from error
    message = "DolphinScheduler could not resolve a task datasource"
    raise ApiTransportError(
        message,
        details=details,
        source=error.source,
    ) from error


__all__ = [
    "TaskDatasourceBaseline",
    "TaskDatasourceResolutionData",
    "resolve_workflow_edit_task_datasources",
    "resolve_workflow_patch_task_datasources",
    "resolve_workflow_spec_task_datasources",
    "task_datasource_baseline_from_dag",
    "task_datasource_baseline_from_legacy_graph",
]
