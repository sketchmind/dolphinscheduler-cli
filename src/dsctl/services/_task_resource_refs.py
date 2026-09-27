from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.models.common import is_yaml_object
from dsctl.services._resource_errors import resource_storage_unavailable_error
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    TaskResourceRefIndex,
    pytorch_typed_resource_file_candidate,
    pytorch_typed_resource_id_candidate,
    waterdrop_typed_resource_id_candidate,
)
from dsctl.upstream.task_parameter_projection.resource_info import (
    resource_name_candidate,
    task_file_uses_id,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.upstream.legacy_workflow_graph import DecodedLegacyWorkflowGraph
    from dsctl.upstream.protocol import TaskResourceResolver, WorkflowDagRecord


_RESOURCE_NOT_EXIST = 20004
_RESOURCE_NOT_EXIST_OR_NO_PERMISSION = 20016
_USER_NO_OPERATION_PERMISSION = 30001
_NO_CURRENT_OPERATING_PERMISSION = 1400001
_INTERNAL_SERVER_ERROR_ARGS = 10000
_RESOURCE_PERMISSION_MESSAGE = "have no permission to access the resource"


def resolve_task_resource_refs(
    resources: TaskResourceResolver | None,
    full_names: Sequence[str],
    *,
    boundary_resource: str,
    action: str,
) -> TaskResourceRefIndex:
    """Resolve typed task FILE identities before any persistent mutation phase."""
    if not full_names:
        return TaskResourceRefIndex.from_id_by_full_name({})
    if resources is None:
        message = "The selected domain has no exact task resource resolver"
        raise ApiTransportError(
            message,
            details={"resource": boundary_resource, "action": action},
            suggestion=(
                "Verify the selected DolphinScheduler profile and retry the "
                "workflow mutation."
            ),
        )
    resolved: dict[str, int] = {}
    verified: list[str] = []
    wire_names: dict[str, str] = {}
    for full_name in full_names:
        try:
            resolution = resources.resolve_task_file(full_name)
            verified.append(full_name)
            wire_names[full_name] = resolution.wire_full_name
            if resolution.resource_id is not None:
                resolved[full_name] = resolution.resource_id
        except ApiResultError as error:
            _raise_task_resource_api_error(
                error,
                full_name=full_name,
                boundary_resource=boundary_resource,
                action=action,
            )
        except ApiTransportError as error:
            raise ApiTransportError(
                error.message,
                details={
                    **error.details,
                    "resource": boundary_resource,
                    "action": action,
                    "resource_full_name": full_name,
                },
                source=error.source,
                suggestion=error.suggestion,
            ) from error
        except WireContractError as error:
            message = "The selected profile cannot resolve typed task resources"
            raise ApiTransportError(
                message,
                details={
                    "resource": boundary_resource,
                    "action": action,
                    "resource_full_name": full_name,
                    "reason": str(error),
                },
                suggestion=(
                    "Verify the selected DolphinScheduler version profile and "
                    "retry the workflow mutation."
                ),
            ) from error
    try:
        return TaskResourceRefIndex.from_resolved_files(
            verified,
            id_by_full_name=resolved,
            wire_full_name_by_full_name=wire_names,
        )
    except ValueError as error:
        message = "DolphinScheduler returned conflicting task resource identities"
        raise ApiTransportError(
            message,
            details={
                "resource": boundary_resource,
                "action": action,
                "resource_full_names": list(resolved),
                "resource_ids": list(resolved.values()),
            },
            suggestion=(
                "Verify that each visible task resource FILE has one distinct "
                "positive resource id, then retry."
            ),
        ) from error


def _task_resource_read_candidates_from_dag(
    dag: WorkflowDagRecord,
    *,
    profile_version: str,
) -> tuple[tuple[int, ...], tuple[tuple[str, str], ...]]:
    """Collect separate ordered id and name candidates from one DAG scan."""
    surface = get_task_authoring_surface(profile_version)
    file_uses_ids = task_file_uses_id(profile_version)
    pytorch_uses_ids = surface.pytorch.wire_epoch == "positive-resource-id-python-home"
    pytorch_uses_names = surface.pytorch.wire_epoch == "resource-name-python-launcher"
    resource_ids: list[int] = []
    seen_ids: set[int] = set()
    file_candidates: list[tuple[str, str] | None] = []
    for task in dag.taskDefinitionList or ():
        task_type = enum_value(task.taskType)
        raw_params = (
            task.taskParams
            if task_type in {"JAVA", "MR", "PYTHON", "PYTORCH", "SHELL", "WATERDROP"}
            else None
        )
        if not isinstance(raw_params, str):
            continue
        try:
            params = json.loads(raw_params)
        except (TypeError, ValueError):
            continue
        if not is_yaml_object(params):
            continue
        for resource_info in _resource_info_read_candidates(params, task_type):
            if file_uses_ids:
                _append_resource_id(
                    resource_ids, seen_ids, _resource_info_id_candidate(resource_info)
                )
            else:
                file_candidates.append(resource_name_candidate(resource_info))
        resource_id = None
        if task_type == "WATERDROP":
            resource_id = waterdrop_typed_resource_id_candidate(
                params,
                version=profile_version,
            )
        elif task_type == "PYTORCH" and pytorch_uses_ids:
            resource_id = pytorch_typed_resource_id_candidate(
                params,
                version=profile_version,
            )
        elif task_type == "PYTORCH" and pytorch_uses_names:
            file_candidates.append(
                pytorch_typed_resource_file_candidate(params, version=profile_version)
            )
        _append_resource_id(resource_ids, seen_ids, resource_id)
    return tuple(resource_ids), tuple(
        dict.fromkeys(
            candidate for candidate in file_candidates if candidate is not None
        )
    )


def _resource_info_read_candidates(
    params: YamlObject, task_type: str | None
) -> tuple[YamlValue, ...]:
    if task_type in {"JAVA", "MR"}:
        return (params.get("mainJar"),)
    if task_type in {"SHELL", "PYTHON"}:
        resources = params.get("resourceList")
        return tuple(resources) if isinstance(resources, list) else ()
    return ()


def _resource_info_id_candidate(value: YamlValue) -> YamlValue:
    if isinstance(value, Mapping) and set(value) == {"id"}:
        return value.get("id")
    return None


def _append_resource_id(ids: list[int], seen: set[int], candidate: YamlValue) -> None:
    if (
        isinstance(candidate, int)
        and not isinstance(candidate, bool)
        and candidate > 0
        and candidate not in seen
    ):
        seen.add(candidate)
        ids.append(candidate)


def resolve_dag_read_task_resource_refs(
    resources: TaskResourceResolver | None,
    dag: WorkflowDagRecord,
    *,
    profile_version: str,
) -> TaskResourceRefIndex:
    """Best-effort bind every exact id- or name-backed task FILE read wire."""
    resource_ids, file_candidates = _task_resource_read_candidates_from_dag(
        dag,
        profile_version=profile_version,
    )
    return resolve_read_task_resource_refs(
        resources,
        resource_ids,
        file_candidates=file_candidates,
    )


def mr_resource_ids_from_legacy_graph(
    graph: DecodedLegacyWorkflowGraph,
) -> tuple[int, ...]:
    """Collect positive id-only MR references from one decoded 1.3.9 graph."""
    resource_ids: list[int] = []
    seen: set[int] = set()
    for task in graph.tasks:
        if task.type != "MR":
            continue
        raw_params = task.native_fields.get("params")
        if isinstance(raw_params, str):
            try:
                raw_params = json.loads(raw_params)
            except (TypeError, ValueError):
                continue
        main_jar = (
            raw_params.get("mainJar") if isinstance(raw_params, Mapping) else None
        )
        resource_id = main_jar.get("id") if isinstance(main_jar, Mapping) else None
        if (
            not isinstance(resource_id, int)
            or isinstance(resource_id, bool)
            or resource_id <= 0
            or resource_id in seen
        ):
            continue
        seen.add(resource_id)
        resource_ids.append(resource_id)
    return tuple(resource_ids)


def mr_resource_full_names_from_spec(
    spec: WorkflowSpec,
    *,
    profile_version: str,
) -> tuple[str, ...]:
    """Collect typed old-wire MR JAR fullNames without interpreting opaque state."""
    if get_task_authoring_surface(profile_version).mr.wire_epoch == "resource-name":
        return ()
    full_names: list[str] = []
    seen: set[str] = set()
    for task in spec.tasks:
        if task.type != "MR" or not isinstance(task.task_params, Mapping):
            continue
        if task.task_params.get("programType") == "SCALA":
            continue
        main_jar = task.task_params.get("mainJar")
        if not isinstance(main_jar, str) or main_jar in seen:
            continue
        seen.add(main_jar)
        full_names.append(main_jar)
    return tuple(full_names)


def resolve_read_task_resource_refs(
    resources: TaskResourceResolver | None,
    resource_ids: Sequence[int],
    *,
    file_candidates: Sequence[tuple[str, str]] = (),
) -> TaskResourceRefIndex:
    """Best-effort verify read identities; unresolved native state stays opaque."""
    if resources is None or (not resource_ids and not file_candidates):
        return TaskResourceRefIndex.from_id_by_full_name({})
    resolved: dict[str, int] = {}
    verified: set[str] = set()
    wire_names: dict[str, str] = {}
    conflicting_names: set[str] = set()
    seen_ids: set[int] = set()
    for resource_id in resource_ids:
        if (
            not isinstance(resource_id, int)
            or isinstance(resource_id, bool)
            or resource_id <= 0
            or resource_id in seen_ids
        ):
            continue
        seen_ids.add(resource_id)
        try:
            full_name = resources.resolve_full_name(resource_id)
        except (ApiHttpError, ApiResultError, ApiTransportError, WireContractError):
            continue
        if (
            not isinstance(full_name, str)
            or not full_name.strip()
            or full_name != full_name.strip()
        ):
            continue
        if full_name in conflicting_names:
            continue
        previous_id = resolved.get(full_name)
        if previous_id is not None and previous_id != resource_id:
            resolved.pop(full_name)
            conflicting_names.add(full_name)
            continue
        resolved[full_name] = resource_id
        verified.add(full_name)
        wire_names[full_name] = full_name
    _merge_read_file_candidates(
        resources,
        file_candidates,
        resolved=resolved,
        verified=verified,
        wire_names=wire_names,
    )
    return TaskResourceRefIndex.from_resolved_files(
        verified,
        id_by_full_name=resolved,
        wire_full_name_by_full_name=wire_names,
    )


def _merge_read_file_candidates(
    resources: TaskResourceResolver,
    file_candidates: Sequence[tuple[str, str]],
    *,
    resolved: dict[str, int],
    verified: set[str],
    wire_names: dict[str, str],
) -> None:
    """Merge only name-backed reads reverified to the observed exact wire."""
    for canonical_full_name, observed_wire_name in file_candidates:
        try:
            resolution = resources.resolve_task_file(canonical_full_name)
        except (ApiHttpError, ApiResultError, ApiTransportError, WireContractError):
            continue
        if resolution.wire_full_name != observed_wire_name:
            continue
        previous_wire_name = wire_names.get(canonical_full_name)
        if previous_wire_name is not None and previous_wire_name != observed_wire_name:
            verified.discard(canonical_full_name)
            resolved.pop(canonical_full_name, None)
            wire_names.pop(canonical_full_name, None)
            continue
        verified.add(canonical_full_name)
        wire_names[canonical_full_name] = observed_wire_name
        candidate_resource_id = resolution.resource_id
        if (
            isinstance(candidate_resource_id, int)
            and not isinstance(candidate_resource_id, bool)
            and candidate_resource_id > 0
        ):
            resolved[canonical_full_name] = candidate_resource_id


def _raise_task_resource_api_error(
    error: ApiResultError,
    *,
    full_name: str,
    boundary_resource: str,
    action: str,
) -> None:
    """Translate resource lookup failures at the owning mutation boundary."""
    details: dict[str, str | int | None] = {
        "resource": boundary_resource,
        "action": action,
        "resource_full_name": full_name,
        "result_code": error.result_code,
        "result_message": error.result_message,
    }
    storage_error = resource_storage_unavailable_error(error, details=details)
    if storage_error is not None:
        raise storage_error from error
    string_only_permission_error = (
        error.result_code == _INTERNAL_SERVER_ERROR_ARGS
        and _RESOURCE_PERMISSION_MESSAGE in error.result_message.lower()
    )
    if (
        error.result_code
        in {
            _USER_NO_OPERATION_PERMISSION,
            _NO_CURRENT_OPERATING_PERMISSION,
        }
        or string_only_permission_error
    ):
        message = "Current user cannot access a typed task resource"
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl resource list` as the same user; request FILE "
                "resource access if the resource is not visible, then retry."
            ),
        ) from error
    if error.result_code in {
        _RESOURCE_NOT_EXIST,
        _RESOURCE_NOT_EXIST_OR_NO_PERMISSION,
    }:
        message = f"Task resource {full_name!r} was not found or visible"
        raise NotFoundError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl resource list` as the same user, select one visible "
                "FILE fullName, and retry."
            ),
        ) from error
    message = "DolphinScheduler could not resolve a typed task resource"
    raise ApiTransportError(
        message,
        details=details,
        source=error.source,
    ) from error
