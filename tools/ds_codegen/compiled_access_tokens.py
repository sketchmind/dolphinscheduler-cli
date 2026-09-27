"""Declare access-token policy for the shared compiled-domain engine."""

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
from ds_codegen.contract_visibility import (
    is_client_supplied_parameter,
    is_required_parameter,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_ACCESS_TOKEN_SCHEMA_VERSION = 1
COMPILED_ACCESS_TOKEN_SEMANTIC_OPERATIONS = frozenset(
    {
        "access-token.list",
        "access-token.get",
        "access-token.create",
        "access-token.update",
        "access-token.delete",
        "access-token.generate",
    }
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ENTITY = "org.apache.dolphinscheduler.dao.entity.AccessToken"
_REQUIRED_ID_FIELDS = (
    ("id", "int", False, "0", None),
    ("userId", "int", False, "0", None),
    ("token", "String", True, None, None),
    ("expireTime", "Date", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
    ("userName", "String", True, None, None),
)
_NULLABLE_ID_FIELDS = (("id", "Integer", True, None, None), *_REQUIRED_ID_FIELDS[1:])
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
_CREATE_FIELDS = ("userId", "expireTime", "token")
_UPDATE_FIELDS = ("id", *_CREATE_FIELDS)

_PRIMITIVES = (
    CompiledPrimitive(
        name="list",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="access-token/list-paging",
                channel="query",
                request_schema="list",
                request_model="AccessTokenListParams",
                request_fields=("pageNo", "searchVal", "pageSize"),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
            CompiledRequestEpoch(
                method="GET",
                path="access-tokens",
                channel="query",
                request_schema="list",
                request_model="AccessTokenListParams",
                request_fields=("pageNo", "searchVal", "pageSize"),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="access-token/create",
                channel="form",
                request_schema="create_required",
                request_model="AccessTokenRequiredCreateParams",
                request_fields=_CREATE_FIELDS,
                required_fields=frozenset(_CREATE_FIELDS),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="access-tokens",
                channel="form",
                request_schema="create_required",
                request_model="AccessTokenRequiredCreateParams",
                request_fields=_CREATE_FIELDS,
                required_fields=frozenset(_CREATE_FIELDS),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="access-tokens",
                channel="form",
                request_schema="create",
                request_model="AccessTokenCreateParams",
                request_fields=_CREATE_FIELDS,
                required_fields=frozenset({"userId", "expireTime"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="access-token/update",
                channel="form",
                request_schema="update_required",
                request_model="AccessTokenRequiredUpdateParams",
                request_fields=_UPDATE_FIELDS,
                required_fields=frozenset(_UPDATE_FIELDS),
            ),
            CompiledRequestEpoch(
                method="PUT",
                path="access-tokens/{id}",
                channel="path_form",
                request_schema="update_required",
                request_model="AccessTokenRequiredUpdateParams",
                request_fields=_UPDATE_FIELDS,
                path_fields=("id",),
                required_fields=frozenset(_UPDATE_FIELDS),
            ),
            CompiledRequestEpoch(
                method="PUT",
                path="access-tokens/{id}",
                channel="path_form",
                request_schema="update",
                request_model="AccessTokenUpdateParams",
                request_fields=_UPDATE_FIELDS,
                path_fields=("id",),
                required_fields=frozenset({"id", "userId", "expireTime"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="access-token/delete",
                channel="form",
                request_schema="id",
                request_model="AccessTokenIdParams",
                request_fields=("id",),
                required_fields=frozenset({"id"}),
            ),
            CompiledRequestEpoch(
                method="DELETE",
                path="access-tokens/{id}",
                channel="path",
                request_schema="id",
                request_model="AccessTokenIdParams",
                request_fields=("id",),
                path_fields=("id",),
                required_fields=frozenset({"id"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="generate",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="access-token/generate",
                channel="form",
                request_schema="generate",
                request_model="AccessTokenGenerateParams",
                request_fields=("userId", "expireTime"),
                required_fields=frozenset({"userId", "expireTime"}),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="access-tokens/generate",
                channel="form",
                request_schema="generate",
                request_model="AccessTokenGenerateParams",
                request_fields=("userId", "expireTime"),
                required_fields=frozenset({"userId", "expireTime"}),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "AccessTokenController":
        return None
    return {
        "queryAccessTokenList": "list",
        "createToken": "create",
        "updateToken": "update",
        "delAccessTokenById": "delete",
        "generateToken": "generate",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    _require_request_fields(operation)
    logical_type = operation.logical_return_type
    if primitive in {"create", "update"}:
        token = next(
            parameter
            for parameter in operation.parameters
            if parameter.wire_name == "token"
        )
        if is_required_parameter(token) != (logical_type == "Void"):
            message = (
                f"compiled access_token {primitive} token requirement "
                "and response epoch changed"
            )
            raise ValueError(message)
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled access_token {primitive} exchange projection changed"
        raise ValueError(message)
    if primitive == "list":
        schema = _page_epoch(snapshot, logical_type)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive in {"create", "update", "delete"} and logical_type == "Void":
        epoch = "legacy_void" if snapshot.ds_version == "1.3.9" else "void"
        return CompiledResponsePolicy(
            codec=f"{primitive}_{epoch}", schema=None, capture=None
        )
    if primitive in {"create", "update"} and logical_type == _ENTITY:
        schema = _entity_epoch(snapshot)
        return CompiledResponsePolicy(
            codec=f"{primitive}_{schema}", schema=schema, capture={}
        )
    if primitive == "delete" and logical_type == "Boolean":
        return CompiledResponsePolicy(
            codec="delete_bool",
            schema="boolean",
            capture=True,
            scalar_annotation="bool",
        )
    if primitive == "generate" and logical_type == "String":
        codec = "generate_legacy" if snapshot.ds_version == "1.3.9" else "generate"
        return CompiledResponsePolicy(
            codec=codec, schema="string", capture="token", scalar_annotation="str"
        )
    message = f"compiled access_token {primitive} response changed: {logical_type}"
    raise ValueError(message)


def _require_request_fields(operation: OperationSpec) -> None:
    expected_types = {
        "pageNo": "Integer",
        "searchVal": "String",
        "pageSize": "Integer",
        "id": "int",
        "userId": "int",
        "expireTime": "String",
        "token": "String",
    }
    if any(
        parameter.java_type != expected_types.get(parameter.wire_name or "")
        or parameter.default_value is not None
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    ):
        message = "compiled access_token request type or default changed"
        raise ValueError(message)


def _page_epoch(snapshot: ContractSnapshot, logical_type: str) -> str:
    if logical_type != f"{_PAGE}<{_ENTITY}>":
        message = "compiled access_token list response type changed"
        raise ValueError(message)
    fields = model_field_facts(require_model(snapshot, _PAGE, domain="access_token"))
    entity_epoch = _entity_epoch(snapshot)
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if entity_epoch == "entity_required_id":
        if fields == _LEGACY_PAGE_FIELDS and not strict:
            return "list_legacy"
        if fields == _NULLABLE_PAGE_FIELDS and strict:
            return "list_required_id_strict"
    if entity_epoch == "entity_nullable_id" and fields == _LIST_PAGE_FIELDS:
        return "list_nullable_id_strict" if strict else "list_nullable_id"
    message = "compiled access_token list response epoch changed"
    raise ValueError(message)


def _entity_epoch(
    snapshot: ContractSnapshot,
) -> Literal["entity_required_id", "entity_nullable_id"]:
    fields = model_field_facts(require_model(snapshot, _ENTITY, domain="access_token"))
    if fields == _REQUIRED_ID_FIELDS:
        return "entity_required_id"
    if fields == _NULLABLE_ID_FIELDS:
        return "entity_nullable_id"
    message = "compiled access_token entity response epoch changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(codecs.get(primitive.name) for primitive in _PRIMITIVES)
    recipes = {
        (
            "list_legacy",
            "create_legacy_void",
            "update_legacy_void",
            "delete_legacy_void",
            "generate_legacy",
        ): "legacy_139",
        (
            "list_required_id_strict",
            "create_void",
            "update_void",
            "delete_void",
            "generate",
        ): "legacy_200",
        (
            "list_required_id_strict",
            "create_entity_required_id",
            "update_entity_required_id",
            "delete_void",
            "generate",
        ): "entity_void",
        (
            "list_nullable_id_strict",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            "delete_void",
            "generate",
        ): "entity_void",
        (
            "list_nullable_id",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            "delete_void",
            "generate",
        ): "entity_void",
        (
            "list_nullable_id",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            "delete_bool",
            "generate",
        ): "entity_bool",
    }
    if coordinate in recipes:
        return recipes[coordinate]
    message = f"compiled access_token recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


ACCESS_TOKEN_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="access_token",
    schema_constant="COMPILED_ACCESS_TOKEN_SCHEMA_VERSION",
    schema_version=COMPILED_ACCESS_TOKEN_SCHEMA_VERSION,
    semantic_operations=COMPILED_ACCESS_TOKEN_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "ACCESS_TOKEN_COMPILED_DOMAIN",
    "COMPILED_ACCESS_TOKEN_SCHEMA_VERSION",
    "COMPILED_ACCESS_TOKEN_SEMANTIC_OPERATIONS",
]
