"""Declare tenant policy for the shared compiled-domain engine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_TENANT_SCHEMA_VERSION = 1
COMPILED_TENANT_SEMANTIC_OPERATIONS = frozenset(
    {"tenant.create", "tenant.delete", "tenant.get", "tenant.page", "tenant.update"}
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_TENANT = "org.apache.dolphinscheduler.dao.entity.Tenant"
_REQUIRED_ID_FIELDS = (
    ("id", "int", False, "0", None),
    ("tenantCode", "String", True, None, None),
    ("description", "String", True, None, None),
    ("queueId", "int", False, "0", None),
    ("queueName", "String", True, None, None),
    ("queue", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_LEGACY_NAMED_FIELDS = (
    *_REQUIRED_ID_FIELDS[:2],
    ("tenantName", "String", True, None, None),
    *_REQUIRED_ID_FIELDS[2:],
)
_NULLABLE_ID_FIELDS = (
    ("id", "Integer", True, None, None),
    *_REQUIRED_ID_FIELDS[1:],
)
_LEGACY_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("currentPage", "Integer", True, "0", None),
)
_NULLABLE_PAGE_FIELDS = (
    *_LEGACY_PAGE_FIELDS[:3],
    ("pageSize", "Integer", False, "20", None),
    _LEGACY_PAGE_FIELDS[3],
    ("pageNo", "Integer", True, None, None),
)
_LIST_PAGE_FIELDS = (
    ("totalList", "List<T>", False, None, "list"),
    *_NULLABLE_PAGE_FIELDS[1:],
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="tenant/list-paging",
                channel="query",
                request_schema="page_legacy",
                request_model="TenantLegacyPageParams",
                request_fields=("pageNo", "searchVal", "pageSize"),
            ),
            CompiledRequestEpoch(
                method="GET",
                path="tenants",
                channel="query",
                request_schema="page",
                request_model="TenantPageParams",
                request_fields=("searchVal", "pageNo", "pageSize"),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="tenant/create",
                channel="form",
                request_schema="create_legacy",
                request_model="TenantLegacyCreateParams",
                request_fields=("tenantCode", "tenantName", "queueId", "description"),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="tenants",
                channel="form",
                request_schema="create",
                request_model="TenantCreateParams",
                request_fields=("tenantCode", "queueId", "description"),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="tenant/update",
                channel="form",
                request_schema="update_legacy",
                request_model="TenantLegacyUpdateParams",
                request_fields=(
                    "id",
                    "tenantCode",
                    "tenantName",
                    "queueId",
                    "description",
                ),
            ),
            CompiledRequestEpoch(
                method="PUT",
                path="tenants/{id}",
                channel="path_form",
                request_schema="update",
                request_model="TenantUpdateParams",
                request_fields=("id", "tenantCode", "queueId", "description"),
                path_fields=("id",),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="tenant/delete",
                channel="form",
                request_schema="id",
                request_model="TenantIdParams",
                request_fields=("id",),
            ),
            CompiledRequestEpoch(
                method="DELETE",
                path="tenants/{id}",
                channel="path",
                request_schema="id",
                request_model="TenantIdParams",
                request_fields=("id",),
                path_fields=("id",),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "TenantController":
        return None
    return {
        "queryTenantlistPaging": "page",
        "queryTenantListPaging": "page",
        "createTenant": "create",
        "updateTenant": "update",
        "deleteTenantById": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    logical_type = operation.logical_return_type
    if primitive == "page":
        schema = _page_response_epoch(snapshot, logical_type)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive in {"create", "update", "delete"} and logical_type in {"Void", "void"}:
        codec = (
            f"{primitive}_legacy_void"
            if snapshot.ds_version == "1.3.9"
            else f"{primitive}_void"
        )
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if primitive == "create" and logical_type == _TENANT:
        schema = _entity_response_epoch(snapshot)
        if schema != "legacy_named":
            return CompiledResponsePolicy(
                codec=f"create_{schema}", schema=schema, capture={}
            )
    if primitive in {"update", "delete"} and logical_type == "Boolean":
        return CompiledResponsePolicy(
            codec=f"{primitive}_bool",
            schema="boolean",
            capture=True,
            scalar_annotation="bool",
        )
    message = f"compiled tenant {primitive} response changed: {logical_type}"
    raise ValueError(message)


def _page_response_epoch(snapshot: ContractSnapshot, logical_type: str) -> str:
    if logical_type != f"{_PAGE}<{_TENANT}>":
        message = "compiled tenant page response type changed"
        raise ValueError(message)
    page = require_model(snapshot, _PAGE, domain="tenant")
    fields = model_field_facts(page)
    entity_epoch = _entity_response_epoch(snapshot)
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if entity_epoch == "legacy_named" and fields == _LEGACY_PAGE_FIELDS and not strict:
        return "page_legacy_named"
    if (
        entity_epoch == "entity_required_id"
        and fields == _NULLABLE_PAGE_FIELDS
        and strict
    ):
        return "page_entity_required_id_strict"
    if entity_epoch == "entity_nullable_id" and fields == _LIST_PAGE_FIELDS:
        return "page_entity_nullable_id_strict" if strict else "page_entity_nullable_id"
    message = "compiled tenant page response epoch changed"
    raise ValueError(message)


def _entity_response_epoch(
    snapshot: ContractSnapshot,
) -> Literal["legacy_named", "entity_required_id", "entity_nullable_id"]:
    fields = model_field_facts(require_model(snapshot, _TENANT, domain="tenant"))
    if fields == _LEGACY_NAMED_FIELDS:
        return "legacy_named"
    if fields == _REQUIRED_ID_FIELDS:
        return "entity_required_id"
    if fields == _NULLABLE_ID_FIELDS:
        return "entity_nullable_id"
    message = "compiled tenant entity response epoch changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(
        codecs.get(name) for name in ("page", "create", "update", "delete")
    )
    if coordinate == (
        "page_legacy_named",
        "create_legacy_void",
        "update_legacy_void",
        "delete_legacy_void",
    ):
        return "legacy_named"
    if coordinate == (
        "page_entity_required_id_strict",
        "create_void",
        "update_void",
        "delete_void",
    ):
        return "void"
    if coordinate in {
        (
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        (
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        (
            "page_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
    }:
        return "create_entity"
    if coordinate == (
        "page_entity_nullable_id",
        "create_entity_nullable_id",
        "update_bool",
        "delete_bool",
    ):
        return "boolean"
    message = f"compiled tenant recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


TENANT_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="tenant",
    schema_constant="COMPILED_TENANT_SCHEMA_VERSION",
    schema_version=COMPILED_TENANT_SCHEMA_VERSION,
    semantic_operations=COMPILED_TENANT_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_TENANT_SCHEMA_VERSION",
    "COMPILED_TENANT_SEMANTIC_OPERATIONS",
    "TENANT_COMPILED_DOMAIN",
]
