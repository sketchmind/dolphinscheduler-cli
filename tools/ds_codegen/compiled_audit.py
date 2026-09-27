"""Compile audit reads against exact observability and filter facts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.observability_contract import (
    AUDIT_SEMANTIC_OPERATIONS,
    TARGET_OBSERVABILITY_VERSIONS,
    observability_contract,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from ds_codegen.observability_contract import AuditRecipe

COMPILED_AUDIT_SCHEMA_VERSION = 1
COMPILED_AUDIT_SEMANTIC_OPERATIONS = frozenset(AUDIT_SEMANTIC_OPERATIONS)
_ABSENT_VERSIONS = frozenset(
    version
    for version in TARGET_OBSERVABILITY_VERSIONS
    if observability_contract(version).audit.support == "absent"
)
_METADATA_ABSENT_VERSIONS = frozenset(
    version
    for version in TARGET_OBSERVABILITY_VERSIONS
    if version not in _ABSENT_VERSIONS
    and observability_contract(version).audit.metadata_support == "absent"
)
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_AUDIT = "org.apache.dolphinscheduler.api.dto.AuditDto"
_MODEL_TYPE = "org.apache.dolphinscheduler.api.dto.auditLog.AuditModelTypeDto"
_OPERATION_TYPE = "org.apache.dolphinscheduler.api.dto.auditLog.AuditOperationTypeDto"
_RESOURCE_ENUM = "org.apache.dolphinscheduler.common.enums.AuditResourceType"
_OPERATION_ENUM = "org.apache.dolphinscheduler.common.enums.AuditOperationType"
_USER_NAME = ("userName", "String", True, None, None)
_OPERATION = ("operation", "String", True, None, None)
_RESOURCE_FIELDS = (
    _USER_NAME,
    ("resource", "String", True, None, None),
    _OPERATION,
    ("time", "Date", True, None, None),
    ("resourceName", "String", True, None, None),
)
_MODEL_FIELDS = (
    _USER_NAME,
    ("modelType", "String", True, None, None),
    ("modelName", "String", True, None, None),
    _OPERATION,
    ("createTime", "Date", True, None, None),
    ("description", "String", True, None, None),
    ("detail", "String", True, None, None),
    ("latency", "String", True, None, None),
)
_NULLABLE_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_LIST_PAGE_FIELDS = (
    ("totalList", "List<T>", False, None, "list"),
    *_NULLABLE_PAGE_FIELDS[1:],
)
_NAME_FIELD = ("name", "String", True, None, None)
_MODEL_TYPE_FIELDS = (
    _NAME_FIELD,
    ("child", f"List<{_MODEL_TYPE}>", True, "null", None),
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="projects/audit/audit-log-list",
                channel="query",
                request_schema="singular",
                request_model="AuditSingularPageParams",
                request_fields=(
                    "pageNo",
                    "pageSize",
                    "resourceType",
                    "operationType",
                    "startDate",
                    "endDate",
                    "userName",
                ),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
            CompiledRequestEpoch(
                method="GET",
                path="projects/audit/audit-log-list",
                channel="query",
                request_schema="csv",
                request_model="AuditCsvPageParams",
                request_fields=(
                    "pageNo",
                    "pageSize",
                    "modelTypes",
                    "operationTypes",
                    "startDate",
                    "endDate",
                    "userName",
                    "modelName",
                ),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
        ),
        result_envelope="optional",
    ),
    *(
        CompiledPrimitive(
            name=primitive,
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=f"projects/audit/audit-log-{path}",
                    channel="query",
                    request_schema="empty",
                    request_model="AuditEmptyParams",
                    request_fields=(),
                    required_fields=frozenset(),
                ),
            ),
            result_envelope="optional",
            absent_versions=_METADATA_ABSENT_VERSIONS,
        )
        for primitive, path in (
            ("model_types", "model-type"),
            ("operation_types", "operation-type"),
        )
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "AuditLogController.queryAuditLogListPaging": "page",
        "AuditLogController.queryAuditModelTypeList": "model_types",
        "AuditLogController.queryAuditOperationTypeList": "operation_types",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    recipe = observability_contract(snapshot.ds_version).audit
    expected_source = {
        "page": recipe.list_operation,
        "model_types": recipe.model_type_operation,
        "operation_types": recipe.operation_type_operation,
    }[primitive]
    if recipe.support != "supported" or operation.operation_id != expected_source:
        message = "compiled audit reviewed source recipe changed"
        raise ValueError(message)
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled audit {primitive} exchange projection changed"
        raise ValueError(message)
    _require_request_fields(operation, primitive, recipe)
    logical = operation.logical_return_type
    if primitive == "page" and logical == f"{_PAGE}<{_AUDIT}>":
        schema = _page_epoch(snapshot, recipe)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    model = _MODEL_TYPE if primitive == "model_types" else _OPERATION_TYPE
    if primitive != "page" and logical == f"List<{model}>":
        expected_fields = (
            _MODEL_TYPE_FIELDS if primitive == "model_types" else (_NAME_FIELD,)
        )
        if (
            recipe.metadata_support != "supported"
            or model_field_facts(require_model(snapshot, model, domain="audit"))
            != expected_fields
        ):
            message = f"compiled audit {primitive} response fields changed"
            raise ValueError(message)
        return CompiledResponsePolicy(codec=primitive, schema=primitive, capture=[])
    message = f"compiled audit {primitive} response changed: {logical}"
    raise ValueError(message)


def _require_request_fields(
    operation: OperationSpec, primitive: str, recipe: AuditRecipe
) -> None:
    expected: dict[str, str] = {}
    if primitive == "page":
        expected = {
            "pageNo": "Integer",
            "pageSize": "Integer",
            "startDate": "String",
            "endDate": "String",
            "userName": "String",
        }
        if recipe.filter_wire == "singular-enums" and not recipe.model_name_filter:
            expected.update(resourceType=_RESOURCE_ENUM, operationType=_OPERATION_ENUM)
        elif recipe.filter_wire == "csv-text" and recipe.model_name_filter:
            expected.update(
                modelTypes="String", operationTypes="String", modelName="String"
            )
        else:
            message = "compiled audit reviewed filter recipe changed"
            raise ValueError(message)
    fields = {
        item.wire_name: (item.java_type, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if fields != {name: (java_type, None) for name, java_type in expected.items()}:
        message = f"compiled audit {primitive} request type or default changed"
        raise ValueError(message)


def _page_epoch(snapshot: ContractSnapshot, recipe: AuditRecipe) -> str:
    fields = model_field_facts(require_model(snapshot, _AUDIT, domain="audit"))
    if (
        recipe.record_wire == "legacy-resource"
        and recipe.filter_wire == "singular-enums"
        and recipe.metadata_support == "absent"
        and fields == _RESOURCE_FIELDS
    ):
        _require_filter_enums(snapshot, recipe)
        record = "resource"
    elif (
        recipe.record_wire == "canonical-model"
        and recipe.filter_wire == "csv-text"
        and recipe.metadata_support == "supported"
        and fields == _MODEL_FIELDS
        and not recipe.model_type_values
        and not recipe.operation_type_values
    ):
        record = "model"
    else:
        message = "compiled audit record fields or reviewed recipe changed"
        raise ValueError(message)
    page = model_field_facts(require_model(snapshot, _PAGE, domain="audit"))
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if record == "resource" and page == _NULLABLE_PAGE_FIELDS and strict:
        return "page_resource_nullable_strict"
    if page == _LIST_PAGE_FIELDS:
        return f"page_{record}_list{'_strict' if strict else ''}"
    message = "compiled audit page response epoch changed"
    raise ValueError(message)


def _require_filter_enums(snapshot: ContractSnapshot, recipe: AuditRecipe) -> None:
    expected_enums = (
        (
            _RESOURCE_ENUM,
            (("USER_MODULE", ("0", "USER")), ("PROJECT_MODULE", ("1", "PROJECT"))),
            recipe.model_type_values,
        ),
        (
            _OPERATION_ENUM,
            tuple(
                (name, (str(index), name))
                for index, name in enumerate(("CREATE", "READ", "UPDATE", "DELETE"))
            ),
            recipe.operation_type_values,
        ),
    )
    for import_path, expected_values, recipe_values in expected_enums:
        matches = [enum for enum in snapshot.enums if enum.import_path == import_path]
        if (
            len(matches) != 1
            or matches[0].json_value_field is not None
            or tuple((field.name, field.java_type) for field in matches[0].fields)
            != (("code", "int"), ("enMsg", "String"))
            or tuple(
                (value.name, tuple(value.arguments)) for value in matches[0].values
            )
            != expected_values
            or tuple(name for name, _values in expected_values) != recipe_values
        ):
            message = f"compiled audit filter enum changed: {import_path}"
            raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(
        codecs.get(name) for name in ("page", "model_types", "operation_types")
    )
    if coordinate in {
        ("page_resource_nullable_strict", None, None),
        ("page_resource_list_strict", None, None),
        ("page_resource_list", None, None),
    }:
        return "singular_enums"
    if coordinate == ("page_model_list", "model_types", "operation_types"):
        return "csv_text"
    message = f"compiled audit recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


AUDIT_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="audit",
    schema_constant="COMPILED_AUDIT_SCHEMA_VERSION",
    schema_version=COMPILED_AUDIT_SCHEMA_VERSION,
    semantic_operations=COMPILED_AUDIT_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
    semantic_absent_versions={
        "audit.model-types": _METADATA_ABSENT_VERSIONS,
        "audit.operation-types": _METADATA_ABSENT_VERSIONS,
    },
)

__all__ = ["AUDIT_COMPILED_DOMAIN", "COMPILED_AUDIT_SCHEMA_VERSION"]
