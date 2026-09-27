"""Declare the environment policy for the shared compiled-domain engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

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

    from ds_codegen.ir import ContractSnapshot, DtoSpec, ModelSpec, OperationSpec

COMPILED_ENVIRONMENT_SCHEMA_VERSION = 3
COMPILED_ENVIRONMENT_SEMANTIC_OPERATIONS = frozenset(
    {
        "environment.create",
        "environment.delete",
        "environment.get",
        "environment.page",
        "environment.update",
    }
)
_DTO = "org.apache.dolphinscheduler.api.dto.EnvironmentDto"
_ENTITY = "org.apache.dolphinscheduler.dao.entity.Environment"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_FIELDS = {
    "id",
    "code",
    "name",
    "config",
    "description",
    "workerGroups",
    "operator",
    "createTime",
    "updateTime",
}
_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="environment/list-paging",
                channel="query",
                request_schema="page",
                request_model="EnvironmentPageParams",
                request_fields=("searchVal", "pageSize", "pageNo"),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="get",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="environment/query-by-code",
                channel="query",
                request_schema="code",
                request_model="EnvironmentCodeParams",
                request_fields=("environmentCode",),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="environment/create",
                channel="form",
                request_schema="create",
                request_model="EnvironmentCreateParams",
                request_fields=("name", "config", "description", "workerGroups"),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="environment/update",
                channel="form",
                request_schema="update",
                request_model="EnvironmentUpdateParams",
                request_fields=(
                    "code",
                    "name",
                    "config",
                    "description",
                    "workerGroups",
                ),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="environment/delete",
                channel="form",
                request_schema="code",
                request_model="EnvironmentCodeParams",
                request_fields=("environmentCode",),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "queryEnvironmentListPaging": "page",
        "queryEnvironmentByCode": "get",
        "createProject": "create",
        "createEnvironment": "create",
        "updateEnvironment": "update",
        "deleteEnvironment": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    leaf = operation.logical_return_type.rsplit(".", 1)[-1]
    if primitive == "create" and leaf == "Long":
        return CompiledResponsePolicy(
            codec="create_int",
            schema="create_int",
            capture=1,
            scalar_annotation="int",
        )
    if primitive in {"update", "delete"} and leaf in {"Void", "void"}:
        return CompiledResponsePolicy(
            codec=f"{primitive}_void",
            schema=None,
            capture=None,
        )
    if primitive == "update" and leaf == "Environment":
        _require_fields(snapshot, _ENTITY, _FIELDS - {"workerGroups"})
        return CompiledResponsePolicy(
            codec="update_entity",
            schema="update_entity",
            capture={},
        )
    if primitive == "get" and leaf == "EnvironmentDto":
        dto = _require_fields(snapshot, _DTO, _FIELDS)
        identifier = next(field for field in dto.fields if field.wire_name == "id")
        if (identifier.java_type, identifier.nullable, identifier.default_value) == (
            "int",
            False,
            "0",
        ):
            schema = "get_legacy"
        elif (identifier.java_type, identifier.nullable, identifier.default_value) == (
            "Integer",
            True,
            None,
        ):
            schema = "get_modern"
        else:
            message = "compiled environment DTO id epoch changed"
            raise ValueError(message)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive == "page":
        page = require_model(snapshot, _PAGE, domain="environment")
        total_list = next(
            field for field in page.fields if field.wire_name == "totalList"
        )
        strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
        if total_list.nullable and total_list.default_factory is None and strict:
            schema = "page_legacy_strict"
        elif not total_list.nullable and total_list.default_factory == "list":
            schema = "page_list_strict" if strict else "page_list"
        else:
            message = "compiled environment page epoch changed"
            raise ValueError(message)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    message = f"compiled environment {primitive} response changed: {leaf}"
    raise ValueError(message)


def _require_fields(
    snapshot: ContractSnapshot,
    import_path: str,
    expected: set[str],
) -> DtoSpec | ModelSpec:
    model = require_model(snapshot, import_path, domain="environment")
    fields = {field.wire_name for field in model.fields}
    if fields != expected:
        message = f"compiled environment response fields changed: {sorted(fields)!r}"
        raise ValueError(message)
    return model


def _recipe(codecs: Mapping[str, str]) -> str:
    """Keep executable schema epochs out of runtime behavior selection."""
    coordinate = (
        codecs.get("page"),
        codecs.get("get"),
        codecs.get("create"),
        codecs.get("update"),
        codecs.get("delete"),
    )
    if (
        coordinate[0] in {"page_legacy_strict", "page_list_strict", "page_list"}
        and coordinate[1] in {"get_legacy", "get_modern"}
        and coordinate[2] == "create_int"
        and coordinate[4] == "delete_void"
    ):
        if coordinate[3] == "update_void":
            return "void_update"
        if coordinate[3] == "update_entity":
            return "entity_update"
    message = f"compiled environment recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


ENVIRONMENT_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="environment",
    schema_constant="COMPILED_ENVIRONMENT_SCHEMA_VERSION",
    schema_version=COMPILED_ENVIRONMENT_SCHEMA_VERSION,
    semantic_operations=COMPILED_ENVIRONMENT_SEMANTIC_OPERATIONS,
    absent_versions=frozenset({"1.3.9"}),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_ENVIRONMENT_SCHEMA_VERSION",
    "COMPILED_ENVIRONMENT_SEMANTIC_OPERATIONS",
    "ENVIRONMENT_COMPILED_DOMAIN",
]
