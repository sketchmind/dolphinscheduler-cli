from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import NAMESPACE_RESOURCE
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
    NamespaceData,
    optional_text,
    serialize_namespace,
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
    collect_all_pages,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedNamespaceData
from dsctl.services.resolver import namespace as resolve_namespace
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime

if TYPE_CHECKING:
    from dsctl.upstream.protocol import NamespaceOperations, NamespaceRecord


REQUEST_PARAMS_NOT_VALID_ERROR = 10001
USER_NO_OPERATION_PERM = 30001
CLUSTER_NOT_EXISTS = 120033
K8S_NAMESPACE_EXIST = 1300002
K8S_NAMESPACE_NOT_EXIST = 1300005
K8S_CLIENT_OPS_ERROR = 1300006

NamespacePageData: TypeAlias = PageData[NamespaceData]


class DeleteNamespaceData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    namespace: ResolvedNamespaceData


def list_namespaces_result(
    *,
    env_file: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
) -> CommandResult:
    """List namespaces with explicit paging or auto-exhaust support."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")

    return run_with_service_runtime(
        env_file,
        _list_namespaces_result,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def list_available_namespaces_result(
    *,
    env_file: str | None = None,
) -> CommandResult:
    """List namespaces available to the configured login user."""
    return run_with_service_runtime(
        env_file,
        _list_available_namespaces_result,
    )


def get_namespace_result(
    namespace: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one namespace payload."""
    return run_with_service_runtime(
        env_file,
        _get_namespace_result,
        namespace=namespace,
    )


def create_namespace_result(
    *,
    namespace: str,
    cluster_code: int,
    env_file: str | None = None,
) -> CommandResult:
    """Create one namespace from validated CLI input."""
    normalized_namespace = require_non_empty_text(namespace, label="namespace")
    require_positive_int(cluster_code, label="cluster_code")

    return run_with_service_runtime(
        env_file,
        _create_namespace_result,
        namespace=normalized_namespace,
        cluster_code=cluster_code,
    )


def delete_namespace_result(
    namespace: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one namespace after explicit confirmation."""
    require_delete_force(force=force, resource_label="Namespace")

    return run_with_service_runtime(
        env_file,
        _delete_namespace_result,
        namespace=namespace,
    )


def _list_namespaces_result(
    runtime: ServiceRuntime,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.upstream.namespaces
    data: NamespacePageData = requested_page_data(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
        ),
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=serialize_namespace,
        resource=NAMESPACE_RESOURCE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
        translate_error=lambda error: _translate_namespace_api_error(
            error,
            operation="list",
            namespace_name=search,
        ),
    )

    return CommandResult(
        data=require_json_object(data, label="namespace list data"),
        resolved={
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_namespace_result(
    runtime: ServiceRuntime,
    *,
    namespace: str,
) -> CommandResult:
    adapter = runtime.upstream.namespaces
    resolved_namespace = resolve_namespace(namespace, adapter=adapter)
    fetched_namespace = _find_namespace_by_id(
        adapter,
        namespace_id=resolved_namespace.id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_namespace(fetched_namespace),
            label="namespace data",
        ),
        resolved={
            "namespace": require_json_object(
                resolved_namespace.to_data(),
                label="resolved namespace",
            )
        },
    )


def _list_available_namespaces_result(runtime: ServiceRuntime) -> CommandResult:
    adapter = runtime.upstream.namespaces
    return CommandResult(
        data=[
            require_json_object(
                serialize_namespace(namespace),
                label="available namespace data item",
            )
            for namespace in adapter.available()
        ],
        resolved={
            "scope": "current_user",
        },
    )


def _create_namespace_result(
    runtime: ServiceRuntime,
    *,
    namespace: str,
    cluster_code: int,
) -> CommandResult:
    adapter = runtime.upstream.namespaces
    try:
        created_namespace = adapter.create(
            namespace=namespace,
            cluster_code=cluster_code,
        )
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="create",
            namespace_name=namespace,
            cluster_code=cluster_code,
        ) from error

    resolved_namespace = _resolved_namespace_data_from_record(created_namespace)
    return CommandResult(
        data=require_json_object(
            serialize_namespace(created_namespace),
            label="namespace data",
        ),
        resolved={
            "namespace": require_json_object(
                resolved_namespace,
                label="resolved namespace",
            )
        },
    )


def _delete_namespace_result(
    runtime: ServiceRuntime,
    *,
    namespace: str,
) -> CommandResult:
    adapter = runtime.upstream.namespaces
    resolved_namespace = resolve_namespace(namespace, adapter=adapter)
    try:
        deleted = adapter.delete(namespace_id=resolved_namespace.id)
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="delete",
            namespace_id=resolved_namespace.id,
            namespace_name=resolved_namespace.namespace_name,
            cluster_code=resolved_namespace.cluster_code,
        ) from error

    data: DeleteNamespaceData = {
        "deleted": deleted,
        "namespace": resolved_namespace.to_data(),
    }
    return CommandResult(
        data=require_json_object(data, label="namespace delete data"),
        resolved={
            "namespace": require_json_object(
                resolved_namespace.to_data(),
                label="resolved namespace",
            )
        },
    )


def _find_namespace_by_id(
    adapter: NamespaceOperations,
    *,
    namespace_id: int,
) -> NamespaceRecord:
    pages = collect_all_pages(
        lambda current_page_no, current_page_size: adapter.list(
            page_no=current_page_no,
            page_size=current_page_size,
            search=None,
        ),
        page_no=1,
        page_size=DEFAULT_PAGE_SIZE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
    )
    for item in pages.items:
        if item.id == namespace_id:
            return item
    message = f"Namespace id {namespace_id} was not found"
    raise NotFoundError(
        message,
        details={"resource": NAMESPACE_RESOURCE, "id": namespace_id},
    )


def _resolved_namespace_data_from_record(
    namespace: NamespaceRecord,
) -> ResolvedNamespaceData:
    if namespace.id is None or namespace.namespace is None:
        message = "Namespace payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": NAMESPACE_RESOURCE},
        )
    return {
        "id": namespace.id,
        "namespace": namespace.namespace,
        "clusterCode": namespace.clusterCode,
        "clusterName": namespace.clusterName,
    }


def _translate_namespace_api_error(
    error: ApiResultError,
    *,
    operation: str,
    namespace_id: int | None = None,
    namespace_name: str | None = None,
    cluster_code: int | None = None,
) -> Exception:
    details: dict[str, int | str] = {"operation": operation}
    if namespace_id is not None:
        details["id"] = namespace_id
    if namespace_name is not None:
        details["namespace"] = namespace_name
    if cluster_code is not None:
        details["clusterCode"] = cluster_code

    if error.result_code == K8S_NAMESPACE_NOT_EXIST:
        identifier = namespace_id if namespace_id is not None else namespace_name
        message = f"Namespace {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == CLUSTER_NOT_EXISTS:
        message = f"Cluster {cluster_code!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == K8S_NAMESPACE_EXIST:
        message = (
            "Namespace create conflicted with an existing namespace in the target "
            "cluster"
        )
        return ConflictError(message, details=details)
    if error.result_code == USER_NO_OPERATION_PERM:
        message = f"Namespace {operation} requires additional permissions"
        return PermissionDeniedError(message, details=details)
    if error.result_code == K8S_CLIENT_OPS_ERROR:
        return UserInputError(
            error.message,
            details=details,
            suggestion=_namespace_input_suggestion(operation),
        )
    if error.result_code == REQUEST_PARAMS_NOT_VALID_ERROR:
        message = "Namespace input was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion=_namespace_input_suggestion(operation),
        )
    return error


def _namespace_input_suggestion(operation: str) -> str:
    if operation == "create":
        return "Verify --namespace and --cluster-code, then retry."
    if operation == "delete":
        return "Verify the namespace identifier, then retry."
    return "Verify the namespace command arguments, then retry."
