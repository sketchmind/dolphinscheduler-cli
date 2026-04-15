from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import CLUSTER_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import (
    ClusterData,
    optional_text,
    serialize_cluster,
)
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_positive_int,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedClusterData
from dsctl.services.resolver import cluster as resolve_cluster
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import ClusterPayloadRecord


USER_NO_OPERATION_PERM = 30001
CREATE_CLUSTER_ERROR = 120020
CLUSTER_NAME_EXISTS = 120021
UPDATE_CLUSTER_ERROR = 120024
DELETE_CLUSTER_ERROR = 120025
QUERY_CLUSTER_BY_NAME_ERROR = 1200027
QUERY_CLUSTER_BY_CODE_ERROR = 1200028
QUERY_CLUSTER_ERROR = 1200029
CLUSTER_NOT_EXISTS = 120033
DELETE_CLUSTER_RELATED_NAMESPACE_EXISTS = 120034
DESCRIPTION_TOO_LONG_ERROR = 1400004

ClusterPageData: TypeAlias = PageData[ClusterData]


class DeleteClusterData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    cluster: ResolvedClusterData


class _UnsetValue:
    """Sentinel for update fields that should keep their current value."""


UNSET = _UnsetValue()
DescriptionUpdate = str | None | _UnsetValue


def list_clusters_result(
    *,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List clusters with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    return run_with_service_runtime(
        env_file,
        _list_clusters_result,
        search=normalized_search,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
    )


def get_cluster_result(
    cluster: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one cluster payload."""
    return run_with_service_runtime(
        env_file,
        _get_cluster_result,
        cluster=cluster,
    )


def create_cluster_result(
    *,
    name: str,
    config: str,
    description: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one cluster from validated CLI input."""
    normalized_name = require_non_empty_text(name, label="cluster name")
    normalized_config = require_non_empty_text(config, label="cluster config")
    normalized_description = optional_text(description)
    return run_with_service_runtime(
        env_file,
        _create_cluster_result,
        name=normalized_name,
        config=normalized_config,
        description=normalized_description,
    )


def update_cluster_result(
    cluster: str,
    *,
    name: str | None = None,
    config: str | None = None,
    description: DescriptionUpdate = UNSET,
    env_file: str | None = None,
) -> CommandResult:
    """Update one cluster while preserving omitted fields."""
    if name is None and config is None and description is UNSET:
        message = "Cluster update requires at least one field change"
        raise UserInputError(
            message,
            suggestion="Pass at least one update flag such as --name or --config.",
        )

    normalized_name = (
        require_non_empty_text(name, label="cluster name") if name is not None else None
    )
    normalized_config = (
        require_non_empty_text(config, label="cluster config")
        if config is not None
        else None
    )
    normalized_description = (
        optional_text(description)
        if not isinstance(description, _UnsetValue)
        else UNSET
    )
    return run_with_service_runtime(
        env_file,
        _update_cluster_result,
        cluster=cluster,
        name=normalized_name,
        config=normalized_config,
        description=normalized_description,
    )


def delete_cluster_result(
    cluster: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one cluster after explicit confirmation."""
    require_delete_force(force=force, resource_label="Cluster")
    return run_with_service_runtime(
        env_file,
        _delete_cluster_result,
        cluster=cluster,
    )


def _list_clusters_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.clusters
    data: ClusterPageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_cluster,
        resource=CLUSTER_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_cluster_api_error(
            error,
            operation="list",
            cluster_name=search,
        ),
    )
    return CommandResult(
        data=require_json_object(data, label="cluster list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_cluster_result(
    runtime: ServiceRuntime,
    *,
    cluster: str,
) -> CommandResult:
    adapter = runtime.upstream.clusters
    try:
        resolved_cluster = resolve_cluster(cluster, adapter=adapter)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="get",
            cluster_name=cluster,
        ) from error
    try:
        fetched_cluster = adapter.get(code=resolved_cluster.code)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="get",
            cluster_code=resolved_cluster.code,
            cluster_name=resolved_cluster.name,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_cluster(fetched_cluster),
            label="cluster data",
        ),
        resolved={
            "cluster": require_json_object(
                resolved_cluster.to_data(),
                label="resolved cluster",
            )
        },
    )


def _create_cluster_result(
    runtime: ServiceRuntime,
    *,
    name: str,
    config: str,
    description: str | None,
) -> CommandResult:
    try:
        created_cluster = runtime.upstream.clusters.create(
            name=name,
            config=config,
            description=description,
        )
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="create",
            cluster_name=name,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_cluster(created_cluster),
            label="cluster data",
        ),
        resolved={
            "cluster": require_json_object(
                _resolved_cluster_data_from_record(created_cluster),
                label="resolved cluster",
            )
        },
    )


def _update_cluster_result(
    runtime: ServiceRuntime,
    *,
    cluster: str,
    name: str | None,
    config: str | None,
    description: DescriptionUpdate,
) -> CommandResult:
    adapter = runtime.upstream.clusters
    try:
        resolved_cluster = resolve_cluster(cluster, adapter=adapter)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="update",
            cluster_name=cluster,
        ) from error
    try:
        current_cluster = adapter.get(code=resolved_cluster.code)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="get",
            cluster_code=resolved_cluster.code,
            cluster_name=resolved_cluster.name,
        ) from error

    next_name = resolved_cluster.name if name is None else name
    next_config = current_cluster.config if config is None else config
    next_description = (
        current_cluster.description
        if isinstance(description, _UnsetValue)
        else description
    )
    if next_config is None:
        message = "Cluster payload was missing required config"
        raise ApiTransportError(
            message,
            details={"resource": CLUSTER_RESOURCE, "code": resolved_cluster.code},
        )

    try:
        updated_cluster = adapter.update(
            code=resolved_cluster.code,
            name=next_name,
            config=next_config,
            description=next_description,
        )
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="update",
            cluster_code=resolved_cluster.code,
            cluster_name=next_name,
        ) from error
    return CommandResult(
        data=require_json_object(
            serialize_cluster(updated_cluster),
            label="cluster data",
        ),
        resolved={
            "cluster": require_json_object(
                resolved_cluster.to_data(),
                label="resolved cluster",
            )
        },
    )


def _delete_cluster_result(
    runtime: ServiceRuntime,
    *,
    cluster: str,
) -> CommandResult:
    adapter = runtime.upstream.clusters
    try:
        resolved_cluster = resolve_cluster(cluster, adapter=adapter)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="delete",
            cluster_name=cluster,
        ) from error
    try:
        deleted = adapter.delete(code=resolved_cluster.code)
    except ApiResultError as error:
        raise _translate_cluster_api_error(
            error,
            operation="delete",
            cluster_code=resolved_cluster.code,
            cluster_name=resolved_cluster.name,
        ) from error
    return CommandResult(
        data=require_json_object(
            DeleteClusterData(
                deleted=deleted,
                cluster=resolved_cluster.to_data(),
            ),
            label="cluster delete data",
        ),
        resolved={
            "cluster": require_json_object(
                resolved_cluster.to_data(),
                label="resolved cluster",
            )
        },
    )


def _resolved_cluster_data_from_record(
    cluster_record: ClusterPayloadRecord,
) -> ResolvedClusterData:
    cluster_code = cluster_record.code
    cluster_name = cluster_record.name
    if cluster_code is None or cluster_name is None:
        message = "Cluster payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": CLUSTER_RESOURCE},
        )
    return {
        "code": cluster_code,
        "name": cluster_name,
        "description": cluster_record.description,
    }


def _translate_cluster_api_error(
    error: ApiResultError,
    *,
    operation: str,
    cluster_code: int | None = None,
    cluster_name: str | None = None,
) -> Exception:
    details: dict[str, object] = {
        "resource": CLUSTER_RESOURCE,
        "operation": operation,
    }
    if cluster_code is not None:
        details["code"] = cluster_code
    if cluster_name is not None:
        details["name"] = cluster_name

    if error.result_code in {QUERY_CLUSTER_BY_CODE_ERROR, CLUSTER_NOT_EXISTS}:
        identifier = cluster_code if cluster_code is not None else cluster_name
        return NotFoundError(f"Cluster {identifier!r} was not found", details=details)
    if error.result_code == QUERY_CLUSTER_BY_NAME_ERROR:
        return NotFoundError(
            f"Cluster {cluster_name!r} was not found",
            details=details,
        )
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Cluster {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code == CLUSTER_NAME_EXISTS:
        message = "Cluster create/update conflicted with an existing cluster name"
        return ConflictError(message, details=details)
    if error.result_code == DELETE_CLUSTER_RELATED_NAMESPACE_EXISTS:
        message = "Cluster is still referenced by one or more namespaces"
        return ConflictError(message, details=details)
    if error.result_code == DESCRIPTION_TOO_LONG_ERROR:
        return UserInputError(
            "Cluster description was rejected by the upstream API",
            details=details,
            suggestion="Shorten --description and retry.",
        )
    if error.result_code in {
        CREATE_CLUSTER_ERROR,
        UPDATE_CLUSTER_ERROR,
        DELETE_CLUSTER_ERROR,
        QUERY_CLUSTER_ERROR,
    }:
        message = f"Cluster {operation} was rejected by the upstream API"
        return ConflictError(message, details=details)
    return error
