"""Compile exact project-parameter lifecycle and data-type capability epochs."""

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

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_PROJECT_PARAMETER_SCHEMA_VERSION = 1
COMPILED_PROJECT_PARAMETER_SEMANTIC_OPERATIONS = frozenset(
    {
        "project-parameter.page",
        "project-parameter.get",
        "project-parameter.create",
        "project-parameter.update",
        "project-parameter.delete",
    }
)
_ABSENT_VERSIONS = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }
)
_LEGACY_VERSIONS = frozenset({"3.2.0", "3.2.1", "3.2.2"})
_DATA_TYPE_VERSIONS = frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"})
_ENTITY = "org.apache.dolphinscheduler.dao.entity.ProjectParameter"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_PATH = "projects/{projectCode}/project-parameter"
_ID_FIELDS = (
    ("id", "Integer", True, None, None),
    ("userId", "Integer", True, None, None),
)
_VALUE_FIELDS = (
    ("code", "long", False, "0", None),
    ("projectCode", "long", False, "0", None),
    ("paramName", "String", True, None, None),
    ("paramValue", "String", True, None, None),
)
_TIME_FIELDS = (
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_OPERATOR_FIELDS = (("operator", "Integer", True, None, None),)
_USER_FIELDS = (
    ("createUser", "String", True, None, None),
    ("modifyUser", "String", True, None, None),
)
_ENTITY_FIELDS = {
    "basic": (*_ID_FIELDS, *_VALUE_FIELDS, *_TIME_FIELDS),
    "operator": (
        *_ID_FIELDS,
        *_OPERATOR_FIELDS,
        *_VALUE_FIELDS,
        *_TIME_FIELDS,
        *_USER_FIELDS,
    ),
    "data_type": (
        *_ID_FIELDS,
        *_OPERATOR_FIELDS,
        *_VALUE_FIELDS,
        ("paramDataType", "String", True, None, None),
        *_TIME_FIELDS,
        *_USER_FIELDS,
    ),
}
_PAGE_FIELDS = (
    ("totalList", "List<T>", False, None, "list"),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_MUTATION_FIELDS = ("projectParameterName", "projectParameterValue")


def _classify(operation: OperationSpec) -> str | None:
    return {
        "ProjectParameterController.queryProjectParameterListPaging": "page",
        "ProjectParameterController.queryProjectParameterByCode": "get",
        "ProjectParameterController.createProjectParameter": "create",
        "ProjectParameterController.updateProjectParameter": "update",
        "ProjectParameterController.deleteProjectParametersByCode": "delete",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled project_parameter {primitive} exchange projection changed"
        raise ValueError(message)
    modern = snapshot.ds_version in _DATA_TYPE_VERSIONS
    _require_request_fields(operation, primitive, modern=modern)
    logical = operation.logical_return_type
    if primitive == "delete" and logical == "Void":
        return CompiledResponsePolicy(codec="delete", schema=None, capture=None)
    expected = f"{_PAGE}<{_ENTITY}>" if primitive == "page" else _ENTITY
    if logical != expected:
        message = f"compiled project_parameter {primitive} response changed"
        raise ValueError(message)
    epoch = (
        "data_type"
        if modern
        else "operator"
        if snapshot.ds_version == "3.2.2"
        else "basic"
    )
    entity = require_model(snapshot, _ENTITY, domain="project_parameter")
    if entity.extends is not None or model_field_facts(entity) != _ENTITY_FIELDS[epoch]:
        message = "compiled project_parameter entity fields changed"
        raise ValueError(message)
    if primitive == "page":
        page = require_model(snapshot, _PAGE, domain="project_parameter")
        if page.extends is not None or model_field_facts(page) != _PAGE_FIELDS:
            message = "compiled project_parameter page fields changed"
            raise ValueError(message)
    schema = f"{'page' if primitive == 'page' else 'entity'}_{epoch}"
    return CompiledResponsePolicy(
        codec=f"{primitive}_{epoch}", schema=schema, capture={}
    )


def _require_request_fields(
    operation: OperationSpec, primitive: str, *, modern: bool
) -> None:
    expected: dict[str, tuple[str, str | None]] = {"projectCode": ("long", None)}
    if primitive == "page":
        expected.update(
            searchVal=("String", None),
            pageNo=("Integer", None),
            pageSize=("Integer", None),
        )
    if primitive in {"get", "delete", "update"}:
        expected["code"] = ("Long" if primitive == "update" else "long", None)
    if primitive in {"create", "update"}:
        expected.update(dict.fromkeys(_MUTATION_FIELDS, ("String", None)))
    if modern and primitive in {"page", "create", "update"}:
        expected["projectParameterDataType"] = (
            "String",
            "VARCHAR" if primitive == "create" else None,
        )
    actual = {
        item.wire_name: (item.java_type, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if actual != expected:
        message = (
            f"compiled project_parameter {primitive} request type or default changed"
        )
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    for epoch in ("basic", "operator", "data_type"):
        if codecs == {
            **{name: f"{name}_{epoch}" for name in ("page", "get", "create", "update")},
            "delete": "delete",
        }:
            return "data_type" if epoch == "data_type" else "legacy_varchar"
    message = f"compiled project_parameter recipe is unsupported: {codecs!r}"
    raise ValueError(message)


PROJECT_PARAMETER_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="project_parameter",
    schema_constant="COMPILED_PROJECT_PARAMETER_SCHEMA_VERSION",
    schema_version=COMPILED_PROJECT_PARAMETER_SCHEMA_VERSION,
    semantic_operations=COMPILED_PROJECT_PARAMETER_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=(
        CompiledPrimitive(
            name="page",
            requests=tuple(
                CompiledRequestEpoch(
                    method="GET",
                    path=_PATH,
                    channel="path_query",
                    request_schema=f"page_{epoch}",
                    request_model=f"ProjectParameterPage{model}Params",
                    request_fields=(
                        "projectCode",
                        "searchVal",
                        *extra,
                        "pageNo",
                        "pageSize",
                    ),
                    path_fields=("projectCode",),
                    required_fields=frozenset({"projectCode", "pageNo", "pageSize"}),
                    versions=versions,
                )
                for epoch, model, extra, versions in (
                    ("legacy", "Legacy", (), _LEGACY_VERSIONS),
                    (
                        "data_type",
                        "DataType",
                        ("projectParameterDataType",),
                        _DATA_TYPE_VERSIONS,
                    ),
                )
            ),
            result_envelope="optional",
        ),
        CompiledPrimitive(
            name="get",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=f"{_PATH}/{{code}}",
                    channel="path",
                    request_schema="codes",
                    request_model="ProjectParameterCodesParams",
                    request_fields=("projectCode", "code"),
                    path_fields=("projectCode", "code"),
                    required_fields=frozenset({"projectCode", "code"}),
                ),
            ),
            result_envelope="optional",
        ),
        *(
            CompiledPrimitive(
                name=name,
                requests=tuple(
                    CompiledRequestEpoch(
                        method="POST" if name == "create" else "PUT",
                        path=path,
                        channel="path_form",
                        request_schema=f"{name}_{epoch}",
                        request_model=f"ProjectParameter{model}{suffix}Params",
                        request_fields=(*path_fields, *_MUTATION_FIELDS, *extra),
                        path_fields=path_fields,
                        required_fields=frozenset(
                            (
                                *path_fields,
                                *_MUTATION_FIELDS,
                                *(extra if name == "update" else ()),
                            )
                        ),
                        versions=versions,
                    )
                    for epoch, suffix, extra, versions in (
                        ("legacy", "Legacy", (), _LEGACY_VERSIONS),
                        (
                            "data_type",
                            "DataType",
                            ("projectParameterDataType",),
                            _DATA_TYPE_VERSIONS,
                        ),
                    )
                ),
                result_envelope="required",
            )
            for name, model, path, path_fields in (
                ("create", "Create", _PATH, ("projectCode",)),
                (
                    "update",
                    "Update",
                    f"{_PATH}/{{code}}",
                    ("projectCode", "code"),
                ),
            )
        ),
        CompiledPrimitive(
            name="delete",
            requests=(
                CompiledRequestEpoch(
                    method="POST",
                    path=f"{_PATH}/delete",
                    channel="path_form",
                    request_schema="codes",
                    request_model="ProjectParameterCodesParams",
                    request_fields=("projectCode", "code"),
                    path_fields=("projectCode",),
                    required_fields=frozenset({"projectCode", "code"}),
                ),
            ),
            result_envelope="required",
        ),
    ),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_PROJECT_PARAMETER_SCHEMA_VERSION",
    "PROJECT_PARAMETER_COMPILED_DOMAIN",
]
