from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

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
from dsctl.services._page_result import paged_command_result
from dsctl.services._validation import (
    require_delete_force,
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
)
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.namespaces import NAMESPACE_DOMAIN, NamespaceDomain
from dsctl.upstream.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    collect_all_pages,
)
from dsctl.upstream.resolver import ResolvedNamespaceData
from dsctl.upstream.resolver import namespace as resolve_namespace
from dsctl.upstream.serialization import (
    optional_text,
    serialize_namespace,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import NamespaceOperations, NamespaceRecord


REQUEST_PARAMS_NOT_VALID_ERROR = 10001
USER_NO_OPERATION_PERM = 30001
CLUSTER_NOT_EXISTS = 120033
K8S_NAMESPACE_EXIST = 1300002
K8S_NAMESPACE_NOT_EXIST = 1300005
K8S_CLIENT_OPS_ERROR = 1300006


class DeleteNamespaceData(TypedDict):
    """CLI delete confirmation payload."""

    deleted: bool
    deletesKubernetesNamespace: bool
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

    return run_with_bound_domain_service_runtime(
        env_file,
        NAMESPACE_DOMAIN,
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
    return run_with_bound_domain_service_runtime(
        env_file,
        NAMESPACE_DOMAIN,
        _list_available_namespaces_result,
    )


def get_namespace_result(
    namespace: str,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Resolve and fetch one namespace payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        NAMESPACE_DOMAIN,
        _get_namespace_result,
        namespace=namespace,
    )


def create_namespace_result(
    *,
    namespace: str,
    cluster_code: int | None = None,
    k8s: str | None = None,
    limits_cpu: float | None = None,
    limits_memory: int | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Create one namespace from validated CLI input."""
    normalized_namespace = require_non_empty_text(namespace, label="namespace")
    normalized_k8s = optional_text(k8s)
    if cluster_code is not None:
        require_positive_int(cluster_code, label="cluster_code")
    if limits_cpu is not None and limits_cpu < 0:
        message = "limits_cpu must be greater than or equal to 0"
        raise UserInputError(
            message,
            details={"limits_cpu": limits_cpu},
            suggestion="Pass --limits-cpu as a number greater than or equal to 0.",
        )
    if limits_memory is not None:
        require_non_negative_int(limits_memory, label="limits_memory")

    return run_with_bound_domain_service_runtime(
        env_file,
        NAMESPACE_DOMAIN,
        _create_namespace_result,
        namespace=normalized_namespace,
        cluster_code=cluster_code,
        k8s=normalized_k8s,
        limits_cpu=limits_cpu,
        limits_memory=limits_memory,
    )


def delete_namespace_result(
    namespace: str,
    *,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one namespace after explicit confirmation."""
    require_delete_force(force=force, resource_label="Namespace")

    return run_with_bound_domain_service_runtime(
        env_file,
        NAMESPACE_DOMAIN,
        _delete_namespace_result,
        namespace=namespace,
    )


def _list_namespaces_result(
    runtime: BoundDomainServiceRuntime[NamespaceDomain],
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    adapter = runtime.domain.namespaces
    return paged_command_result(
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
        resolved={"search": search},
        translate_error=lambda error: _translate_namespace_api_error(
            error,
            operation="list",
            namespace_name=search,
        ),
    )


def _get_namespace_result(
    runtime: BoundDomainServiceRuntime[NamespaceDomain],
    *,
    namespace: str,
) -> CommandResult:
    adapter = runtime.domain.namespaces
    try:
        resolved_namespace = resolve_namespace(namespace, adapter=adapter)
        fetched_namespace = _find_namespace_by_id(
            adapter,
            namespace_id=resolved_namespace.id,
        )
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="get",
            namespace_name=namespace,
            ds_version=runtime.profile.ds_version,
        ) from error
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


def _list_available_namespaces_result(
    runtime: BoundDomainServiceRuntime[NamespaceDomain],
) -> CommandResult:
    adapter = runtime.domain.namespaces
    try:
        available_namespaces = adapter.available()
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="available",
            ds_version=runtime.profile.ds_version,
        ) from error
    return CommandResult(
        data=[
            require_json_object(
                serialize_namespace(namespace),
                label="available namespace data item",
            )
            for namespace in available_namespaces
        ],
        resolved={
            "scope": "current_user",
        },
    )


def _create_namespace_result(
    runtime: BoundDomainServiceRuntime[NamespaceDomain],
    *,
    namespace: str,
    cluster_code: int | None,
    k8s: str | None,
    limits_cpu: float | None,
    limits_memory: int | None,
) -> CommandResult:
    adapter = runtime.domain.namespaces
    try:
        created_namespace = adapter.create(
            namespace=namespace,
            cluster_code=cluster_code,
            k8s=k8s,
            limits_cpu=limits_cpu,
            limits_memory=limits_memory,
        )
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="create",
            namespace_name=namespace,
            cluster_code=cluster_code,
            k8s=k8s,
            ds_version=runtime.profile.ds_version,
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
    runtime: BoundDomainServiceRuntime[NamespaceDomain],
    *,
    namespace: str,
) -> CommandResult:
    adapter = runtime.domain.namespaces
    try:
        resolved_namespace = resolve_namespace(namespace, adapter=adapter)
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="delete",
            namespace_name=namespace,
            ds_version=runtime.profile.ds_version,
        ) from error
    try:
        deleted = adapter.delete(namespace_id=resolved_namespace.id)
    except ApiResultError as error:
        raise _translate_namespace_api_error(
            error,
            operation="delete",
            namespace_id=resolved_namespace.id,
            namespace_name=resolved_namespace.namespace_name,
            cluster_code=resolved_namespace.cluster_code,
            ds_version=runtime.profile.ds_version,
        ) from error

    data: DeleteNamespaceData = {
        "deleted": deleted,
        "deletesKubernetesNamespace": adapter.deletes_kubernetes_namespace,
        "namespace": resolved_namespace.to_data(),
    }
    warnings = (
        [
            "DolphinScheduler deleted both its registration and the real "
            "Kubernetes namespace."
        ]
        if adapter.deletes_kubernetes_namespace
        else []
    )
    return CommandResult(
        data=require_json_object(data, label="namespace delete data"),
        resolved={
            "namespace": require_json_object(
                resolved_namespace.to_data(),
                label="resolved namespace",
            ),
            "deletes_kubernetes_namespace": (adapter.deletes_kubernetes_namespace),
        },
        warnings=warnings,
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
    k8s: str | None = None,
    ds_version: str | None = None,
) -> Exception:
    details = _namespace_error_details(
        operation=operation,
        namespace_id=namespace_id,
        namespace_name=namespace_name,
        cluster_code=cluster_code,
        k8s=k8s,
    )

    if error.result_code == K8S_NAMESPACE_NOT_EXIST:
        identifier = namespace_id if namespace_id is not None else namespace_name
        message = f"Namespace {identifier!r} was not found"
        return NotFoundError(message, details=details)
    if error.result_code == CLUSTER_NOT_EXISTS:
        cluster_selector: int | str | None = (
            cluster_code if cluster_code is not None else k8s
        )
        message = f"Cluster {cluster_selector!r} was not found"
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
            suggestion=_namespace_input_suggestion(operation, ds_version=ds_version),
        )
    if error.result_code == REQUEST_PARAMS_NOT_VALID_ERROR:
        message = "Namespace input was rejected by the upstream API"
        return UserInputError(
            message,
            details=details,
            suggestion=_namespace_input_suggestion(operation, ds_version=ds_version),
        )
    return error


def _namespace_error_details(
    *,
    operation: str,
    namespace_id: int | None,
    namespace_name: str | None,
    cluster_code: int | None,
    k8s: str | None,
) -> dict[str, int | str]:
    details: dict[str, int | str] = {"operation": operation}
    if namespace_id is not None:
        details["id"] = namespace_id
    if namespace_name is not None:
        details["namespace"] = namespace_name
    if cluster_code is not None:
        details["clusterCode"] = cluster_code
    if k8s is not None:
        details["k8s"] = k8s
    return details


def _namespace_input_suggestion(
    operation: str,
    *,
    ds_version: str | None,
) -> str:
    if operation == "create":
        if ds_version in {
            "3.0.0",
            "3.0.1",
            "3.0.2",
            "3.0.3",
            "3.0.4",
            "3.0.5",
            "3.0.6",
        }:
            return "Verify --namespace and --k8s, then retry."
        return "Verify --namespace and --cluster-code, then retry."
    if operation == "delete":
        return "Verify the namespace identifier, then retry."
    return "Verify the namespace command arguments, then retry."
