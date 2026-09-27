"""Declare alert-group policy for the shared compiled-domain engine."""

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

    from ds_codegen.ir import ContractSnapshot, DtoSpec, ModelSpec, OperationSpec

COMPILED_ALERT_GROUP_SCHEMA_VERSION = 2
COMPILED_ALERT_GROUP_SEMANTIC_OPERATIONS = frozenset(
    {
        "alert-group.create",
        "alert-group.delete",
        "alert-group.get",
        "alert-group.page",
        "alert-group.update",
    }
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ALERT_GROUP = "org.apache.dolphinscheduler.dao.entity.AlertGroup"
_ALERT_GROUP_VO = "org.apache.dolphinscheduler.dao.vo.AlertGroupVo"
_ALERT_TYPE = "org.apache.dolphinscheduler.common.enums.AlertType"
_LEGACY_GROUP_FIELDS = (
    ("id", "int", False, "0", None),
    ("groupName", "String", True, None, None),
    ("groupType", _ALERT_TYPE, True, None, None),
    ("description", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_MODERN_GROUP_REQUIRED_ID_FIELDS = (
    ("id", "int", False, "0", None),
    ("groupName", "String", True, None, None),
    ("alertInstanceIds", "String", True, None, None),
    ("description", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
    ("createUserId", "int", False, "0", None),
)
_MODERN_GROUP_NULLABLE_ID_FIELDS = (
    ("id", "Integer", True, None, None),
    *_MODERN_GROUP_REQUIRED_ID_FIELDS[1:],
)
_ALERT_GROUP_VO_FIELDS = (
    ("id", "int", False, "0", None),
    ("groupName", "String", True, None, None),
    ("description", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_LEGACY_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("currentPage", "Integer", True, "0", None),
)
_MODERN_NULLABLE_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_MODERN_LIST_PAGE_FIELDS = (
    ("totalList", "List<T>", False, None, "list"),
    *_MODERN_NULLABLE_PAGE_FIELDS[1:],
)

_LEGACY_PAGE_REQUEST = CompiledRequestEpoch(
    method="GET",
    path="alert-group/list-paging",
    channel="query",
    request_schema="page_legacy",
    request_model="AlertGroupLegacyPageParams",
    request_fields=("pageNo", "searchVal", "pageSize"),
)
_PAGE_REQUEST = CompiledRequestEpoch(
    method="GET",
    path="alert-groups",
    channel="query",
    request_schema="page",
    request_model="AlertGroupPageParams",
    request_fields=("searchVal", "pageNo", "pageSize"),
)
_DIRECT_GET_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="alert-groups/query",
    channel="form",
    request_schema="id",
    request_model="AlertGroupIdParams",
    request_fields=("id",),
)
_LEGACY_CREATE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="alert-group/create",
    channel="form",
    request_schema="create_legacy",
    request_model="AlertGroupLegacyCreateParams",
    request_fields=("groupName", "groupType", "description"),
)
_CREATE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="alert-groups",
    channel="form",
    request_schema="create",
    request_model="AlertGroupCreateParams",
    request_fields=("groupName", "description", "alertInstanceIds"),
)
_LEGACY_UPDATE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="alert-group/update",
    channel="form",
    request_schema="update_legacy",
    request_model="AlertGroupLegacyUpdateParams",
    request_fields=("id", "groupName", "groupType", "description"),
)
_UPDATE_REQUEST = CompiledRequestEpoch(
    method="PUT",
    path="alert-groups/{id}",
    channel="path_form",
    request_schema="update",
    request_model="AlertGroupUpdateParams",
    request_fields=("id", "groupName", "description", "alertInstanceIds"),
    path_fields=("id",),
)
_LEGACY_DELETE_REQUEST = CompiledRequestEpoch(
    method="POST",
    path="alert-group/delete",
    channel="form",
    request_schema="id",
    request_model="AlertGroupIdParams",
    request_fields=("id",),
)
_DELETE_REQUEST = CompiledRequestEpoch(
    method="DELETE",
    path="alert-groups/{id}",
    channel="path",
    request_schema="id",
    request_model="AlertGroupIdParams",
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
        name="direct_get",
        requests=(_DIRECT_GET_REQUEST,),
        result_envelope="optional",
        absent_versions=frozenset({"1.3.9"}),
    ),
    CompiledPrimitive(
        name="create",
        requests=(_LEGACY_CREATE_REQUEST, _CREATE_REQUEST),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(_LEGACY_UPDATE_REQUEST, _UPDATE_REQUEST),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(_LEGACY_DELETE_REQUEST, _DELETE_REQUEST),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "listPaging": "page",
        "queryAlertGroupById": "direct_get",
        "createAlertgroup": "create",
        "createAlertGroup": "create",
        "updateAlertgroup": "update",
        "updateAlertGroupById": "update",
        "delAlertgroupById": "delete",
        "deleteAlertGroupById": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    leaf = operation.logical_return_type.rsplit(".", 1)[-1]
    if snapshot.ds_version == "1.3.9":
        _require_legacy_alert_type(snapshot)
    if primitive == "page":
        schema = _page_response_epoch(snapshot, operation.logical_return_type)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive == "direct_get" and leaf == "AlertGroup":
        schema = _entity_response_epoch(snapshot)
        return CompiledResponsePolicy(
            codec=f"direct_get_{schema}", schema=schema, capture={}
        )
    if primitive == "create":
        if leaf in {"Void", "void"}:
            codec = (
                "create_legacy_void"
                if snapshot.ds_version == "1.3.9"
                else "create_void"
            )
            return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
        if leaf == "AlertGroup":
            schema = _entity_response_epoch(snapshot)
            return CompiledResponsePolicy(
                codec=f"create_{schema}", schema=schema, capture={}
            )
    if primitive == "update":
        if leaf in {"Void", "void"}:
            codec = (
                "update_legacy_void"
                if snapshot.ds_version == "1.3.9"
                else "update_void"
            )
            return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
        if leaf == "AlertGroup":
            schema = _entity_response_epoch(snapshot)
            return CompiledResponsePolicy(
                codec=f"update_{schema}", schema=schema, capture={}
            )
    if primitive == "delete":
        if leaf in {"Void", "void"}:
            codec = (
                "delete_legacy_void"
                if snapshot.ds_version == "1.3.9"
                else "delete_void"
            )
            return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
        if leaf == "Boolean":
            return CompiledResponsePolicy(
                codec="delete_bool",
                schema="delete_bool",
                capture=True,
                scalar_annotation="bool",
            )
    message = f"compiled alert_group {primitive} response changed: {leaf}"
    raise ValueError(message)


def _page_response_epoch(snapshot: ContractSnapshot, logical_type: str) -> str:
    page = require_model(snapshot, _PAGE, domain="alert_group")
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if snapshot.ds_version == "1.3.9":
        _require_model_fields(page, _LEGACY_PAGE_FIELDS, label="legacy page")
        _require_model_fields(
            require_model(snapshot, _ALERT_GROUP, domain="alert_group"),
            _LEGACY_GROUP_FIELDS,
            label="legacy entity",
        )
        if logical_type == f"{_PAGE}<{_ALERT_GROUP}>" and not strict:
            return "page_legacy_alert_type"
    elif snapshot.ds_version == "2.0.0":
        _require_model_fields(page, _MODERN_NULLABLE_PAGE_FIELDS, label="VO page")
        _require_model_fields(
            require_model(snapshot, _ALERT_GROUP_VO, domain="alert_group"),
            _ALERT_GROUP_VO_FIELDS,
            label="VO",
        )
        if logical_type == f"{_PAGE}<{_ALERT_GROUP_VO}>" and strict:
            return "page_vo_strict"
    elif logical_type == f"{_PAGE}<{_ALERT_GROUP}>":
        entity_epoch = _entity_response_epoch(snapshot)
        if entity_epoch == "entity_required_id":
            _require_model_fields(
                page,
                _MODERN_NULLABLE_PAGE_FIELDS,
                label="required-id page",
            )
            if strict:
                return "page_entity_required_id_strict"
        elif entity_epoch == "entity_nullable_id":
            _require_model_fields(
                page,
                _MODERN_LIST_PAGE_FIELDS,
                label="nullable-id page",
            )
            return (
                "page_entity_nullable_id_strict"
                if strict
                else "page_entity_nullable_id"
            )
    message = "compiled alert_group page response epoch changed"
    raise ValueError(message)


def _entity_response_epoch(
    snapshot: ContractSnapshot,
) -> Literal["entity_required_id", "entity_nullable_id"]:
    model = require_model(snapshot, _ALERT_GROUP, domain="alert_group")
    signature = model_field_facts(model)
    if signature == _MODERN_GROUP_REQUIRED_ID_FIELDS:
        return "entity_required_id"
    if signature == _MODERN_GROUP_NULLABLE_ID_FIELDS:
        return "entity_nullable_id"
    message = "compiled alert_group entity response epoch changed"
    raise ValueError(message)


def _require_model_fields(
    model: DtoSpec | ModelSpec,
    expected: tuple[tuple[str, str, bool, str | None, str | None], ...],
    *,
    label: str,
) -> None:
    if model_field_facts(model) != expected:
        message = f"compiled alert_group {label} fields changed"
        raise ValueError(message)


def _require_legacy_alert_type(snapshot: ContractSnapshot) -> None:
    matches = tuple(item for item in snapshot.enums if item.import_path == _ALERT_TYPE)
    if len(matches) != 1:
        message = "compiled alert_group requires the exact AlertType enum"
        raise ValueError(message)
    enum = matches[0]
    fields = tuple((field.name, field.java_type) for field in enum.fields)
    values = tuple((value.name, tuple(value.arguments)) for value in enum.values)
    if fields != (("code", "int"), ("descp", "String")) or values != (
        ("EMAIL", ("0", "email")),
        ("SMS", ("1", "SMS")),
    ):
        message = "compiled alert_group AlertType enum changed"
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = (
        codecs.get("page"),
        codecs.get("direct_get"),
        codecs.get("create"),
        codecs.get("update"),
        codecs.get("delete"),
    )
    if coordinate == (
        "page_legacy_alert_type",
        None,
        "create_legacy_void",
        "update_legacy_void",
        "delete_legacy_void",
    ):
        return "legacy_alert_type"
    if (
        coordinate[0] in {"page_vo_strict", "page_entity_required_id_strict"}
        and coordinate[1] == "direct_get_entity_required_id"
        and coordinate[2:] == ("create_void", "update_void", "delete_void")
    ):
        return "plugin_instances_void_mutations"
    if coordinate in {
        (
            "page_entity_required_id_strict",
            "direct_get_entity_required_id",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        (
            "page_entity_nullable_id_strict",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        (
            "page_entity_nullable_id",
            "direct_get_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
    }:
        return "plugin_instances_entity_create"
    if coordinate[0] == "page_entity_nullable_id" and coordinate[1:] == (
        "direct_get_entity_nullable_id",
        "create_entity_nullable_id",
        "update_entity_nullable_id",
        "delete_bool",
    ):
        return "plugin_instances_entity_mutations"
    message = f"compiled alert_group recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


ALERT_GROUP_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="alert_group",
    schema_constant="COMPILED_ALERT_GROUP_SCHEMA_VERSION",
    schema_version=COMPILED_ALERT_GROUP_SCHEMA_VERSION,
    semantic_operations=COMPILED_ALERT_GROUP_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "ALERT_GROUP_COMPILED_DOMAIN",
    "COMPILED_ALERT_GROUP_SCHEMA_VERSION",
    "COMPILED_ALERT_GROUP_SEMANTIC_OPERATIONS",
]
