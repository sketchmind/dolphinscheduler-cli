"""Declare worker-group policy for the shared compiled-domain engine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    require_model,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_WORKER_GROUP_SCHEMA_VERSION = 2
COMPILED_WORKER_GROUP_SEMANTIC_OPERATIONS = frozenset(
    {
        "worker-group.create",
        "worker-group.delete",
        "worker-group.get",
        "worker-group.page",
        "worker-group.update",
    }
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_WORKER_GROUP = "org.apache.dolphinscheduler.dao.entity.WorkerGroup"
_PAGE_DETAIL = "org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail"
_BASIC_FIELDS = {
    "id",
    "name",
    "addrList",
    "createTime",
    "updateTime",
    "systemDefault",
}
_DESCRIBED_FIELDS = _BASIC_FIELDS | {"description"}
_DESCRIBED_OTHER_FIELDS = _DESCRIBED_FIELDS | {"otherParamsJson"}
_LEGACY_PAGE_FIELDS = {"totalList", "total", "totalPage", "currentPage"}
_MODERN_PAGE_FIELDS = _LEGACY_PAGE_FIELDS | {"pageSize", "pageNo"}

_PAGE_REQUEST = CompiledRequestEpoch(
    method="GET",
    path="worker-groups",
    channel="query",
    request_schema="page",
    request_model="WorkerGroupPageParams",
    request_fields=("pageNo", "pageSize", "searchVal"),
)
_LEGACY_PAGE_REQUEST = CompiledRequestEpoch(
    method="GET",
    path="worker-group/list-paging",
    channel="query",
    request_schema="page",
    request_model="WorkerGroupPageParams",
    request_fields=("pageNo", "pageSize", "searchVal"),
)
_BASIC_SAVE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="worker-groups",
    channel="form",
    request_schema="save_basic",
    request_model="WorkerGroupBasicSaveParams",
    request_fields=("id", "name", "addrList"),
)
_LEGACY_SAVE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="worker-group/save",
    channel="form",
    request_schema="save_basic",
    request_model="WorkerGroupBasicSaveParams",
    request_fields=("id", "name", "addrList"),
)
_DESCRIBED_OTHER_SAVE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="worker-groups",
    channel="form",
    request_schema="save_described_other",
    request_model="WorkerGroupDescribedOtherSaveParams",
    request_fields=("id", "name", "addrList", "description", "otherParamsJson"),
)
_DESCRIBED_SAVE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="worker-groups",
    channel="form",
    request_schema="save_described",
    request_model="WorkerGroupDescribedSaveParams",
    request_fields=("id", "name", "addrList", "description"),
)
_LEGACY_DELETE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="worker-group/delete-by-id",
    channel="form",
    request_schema="id",
    request_model="WorkerGroupIdParams",
    request_fields=("id",),
)
_PATH_DELETE_REQUEST = CompiledRequestEpoch(
    method="DELETE",
    path="worker-groups/{id}",
    channel="path",
    request_schema="id",
    request_model="WorkerGroupIdParams",
    request_fields=("id",),
    path_fields=("id",),
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(_LEGACY_PAGE_REQUEST, _PAGE_REQUEST),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="save",
        requests=(
            _LEGACY_SAVE_REQUEST,
            _BASIC_SAVE_REQUEST,
            _DESCRIBED_OTHER_SAVE_REQUEST,
            _DESCRIBED_SAVE_REQUEST,
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(_LEGACY_DELETE_REQUEST, _PATH_DELETE_REQUEST),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "queryAllWorkerGroupsPaging": "page",
        "saveWorkerGroup": "save",
        "deleteById": "delete",
        "deleteWorkerGroupById": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    leaf = operation.logical_return_type.rsplit(".", 1)[-1]
    if primitive == "save":
        worker_epoch = _require_worker_group_epoch(snapshot)
        if leaf in {"Void", "void"}:
            if worker_epoch == "basic":
                codec = (
                    "save_legacy_void"
                    if operation.path == "worker-group/save"
                    else "save_basic_void"
                )
            elif worker_epoch == "described_other":
                codec = "save_described_other_void"
            else:
                message = "compiled worker_group void save response epoch changed"
                raise ValueError(message)
            return CompiledResponsePolicy(
                codec=codec,
                schema=None,
                capture=None,
            )
        if leaf == "WorkerGroup":
            if worker_epoch == "described_other":
                codec = "save_described_other_entity"
                schema = "save_entity_other"
            elif worker_epoch == "described":
                codec = "save_described_entity"
                schema = "save_entity"
            else:
                message = "compiled worker_group entity save response epoch changed"
                raise ValueError(message)
            return CompiledResponsePolicy(
                codec=codec,
                schema=schema,
                capture={},
            )
    if primitive == "delete" and leaf in {"Void", "void"}:
        codec = (
            "delete_form_void"
            if operation.http_method == "POST"
            else "delete_path_void"
        )
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if primitive == "page":
        schema = _page_response_epoch(snapshot, operation.logical_return_type)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    message = f"compiled worker_group {primitive} response changed: {leaf}"
    raise ValueError(message)


def _page_response_epoch(snapshot: ContractSnapshot, logical_type: str) -> str:
    page = require_model(snapshot, _PAGE, domain="worker_group")
    fields = {field.wire_name for field in page.fields}
    total_list = next(field for field in page.fields if field.wire_name == "totalList")
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    worker_epoch = _require_worker_group_epoch(snapshot)
    if logical_type == f"{_PAGE}<{_WORKER_GROUP}>":
        if (
            worker_epoch == "basic"
            and fields == _LEGACY_PAGE_FIELDS
            and total_list.nullable
            and total_list.default_factory is None
            and not strict
        ):
            return "page_basic_legacy"
        if (
            worker_epoch == "basic"
            and fields == _MODERN_PAGE_FIELDS
            and total_list.nullable
            and total_list.default_factory is None
            and strict
        ):
            return "page_basic_strict"
        if (
            worker_epoch == "described_other"
            and fields == _MODERN_PAGE_FIELDS
            and not total_list.nullable
            and total_list.default_factory == "list"
        ):
            return "page_described_strict" if strict else "page_described"
    if logical_type == f"{_PAGE}<{_PAGE_DETAIL}>":
        detail = require_model(snapshot, _PAGE_DETAIL, domain="worker_group")
        if (
            worker_epoch == "described"
            and {field.wire_name for field in detail.fields}
            == _DESCRIBED_FIELDS | {"source"}
            and fields == _MODERN_PAGE_FIELDS
            and not total_list.nullable
            and total_list.default_factory == "list"
            and not strict
        ):
            return "page_detail"
    message = "compiled worker_group page response epoch changed"
    raise ValueError(message)


def _require_worker_group_epoch(
    snapshot: ContractSnapshot,
) -> Literal["basic", "described", "described_other"]:
    model = require_model(snapshot, _WORKER_GROUP, domain="worker_group")
    fields = {field.wire_name for field in model.fields}
    identifier = next(field for field in model.fields if field.wire_name == "id")
    if fields == _BASIC_FIELDS and (
        identifier.java_type,
        identifier.nullable,
        identifier.default_value,
    ) == ("int", False, "0"):
        return "basic"
    if (
        identifier.java_type,
        identifier.nullable,
        identifier.default_value,
    ) == ("Integer", True, None):
        if fields == _DESCRIBED_OTHER_FIELDS:
            return "described_other"
        if fields == _DESCRIBED_FIELDS:
            return "described"
    message = "compiled worker_group response fields changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = (
        codecs.get("page"),
        codecs.get("save"),
        codecs.get("delete"),
    )
    if coordinate == (
        "page_basic_legacy",
        "save_legacy_void",
        "delete_form_void",
    ):
        return "basic"
    if coordinate == (
        "page_basic_strict",
        "save_basic_void",
        "delete_path_void",
    ):
        return "basic"
    if coordinate[0] in {"page_described_strict", "page_described"} and coordinate[
        1:
    ] == ("save_described_other_void", "delete_path_void"):
        return "described_legacy_not_found"
    if coordinate == (
        "page_described",
        "save_described_other_entity",
        "delete_path_void",
    ):
        return "described_modern_not_found"
    if coordinate == (
        "page_detail",
        "save_described_entity",
        "delete_path_void",
    ):
        return "described_modern_not_found"
    message = f"compiled worker_group recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


WORKER_GROUP_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="worker_group",
    schema_constant="COMPILED_WORKER_GROUP_SCHEMA_VERSION",
    schema_version=COMPILED_WORKER_GROUP_SCHEMA_VERSION,
    semantic_operations=COMPILED_WORKER_GROUP_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_WORKER_GROUP_SCHEMA_VERSION",
    "COMPILED_WORKER_GROUP_SEMANTIC_OPERATIONS",
    "WORKER_GROUP_COMPILED_DOMAIN",
]
