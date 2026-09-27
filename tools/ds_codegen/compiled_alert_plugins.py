"""Declare alert-plugin policy for the shared compiled-domain engine."""

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
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_ALERT_PLUGIN_SCHEMA_VERSION = 2
COMPILED_ALERT_PLUGIN_SEMANTIC_OPERATIONS = frozenset(
    {
        "alert-plugin.page",
        "alert-plugin.get",
        "alert-plugin.definition.list",
        "alert-plugin.schema",
        "alert-plugin.create",
        "alert-plugin.update",
        "alert-plugin.delete",
        "alert-plugin.test",
    }
)
_TEST_ABSENT_VERSIONS = frozenset(
    {
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
        "3.2.0",
    }
)
# This is recipe-stage membership, not upstream endpoint absence: only the
# transient delivery-field recipe reads the existing instance before updating.
_BASELINE_UNUSED_VERSIONS = _TEST_ABSENT_VERSIONS | frozenset(
    {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_INSTANCE = "org.apache.dolphinscheduler.dao.entity.AlertPluginInstance"
_VO = "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO"
_DEFINITION = "org.apache.dolphinscheduler.dao.entity.PluginDefine"
_INSTANCE_TYPE = "org.apache.dolphinscheduler.common.enums.AlertPluginInstanceType"
_WARNING_TYPE = "org.apache.dolphinscheduler.common.enums.WarningType"
_REQUIRED_ID = ("id", "int", False, "0", None)
_NULLABLE_ID = ("id", "Integer", True, None, None)
_INSTANCE_FIELDS = (
    _REQUIRED_ID,
    ("pluginDefineId", "int", False, "0", None),
    ("instanceName", "String", True, None, None),
    ("pluginInstanceParams", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_NULLABLE_INSTANCE_FIELDS = (_NULLABLE_ID, *_INSTANCE_FIELDS[1:])
_TRANSIENT_INSTANCE_FIELDS = (
    *_NULLABLE_INSTANCE_FIELDS[:4],
    ("instanceType", _INSTANCE_TYPE, True, None, None),
    ("warningType", _WARNING_TYPE, True, None, None),
    *_NULLABLE_INSTANCE_FIELDS[4:],
)
_VO_FIELDS = (*_INSTANCE_FIELDS, ("alertPluginName", "String", True, None, None))
_DELIVERY_VO_FIELDS = (
    *_VO_FIELDS[:3],
    ("instanceType", "String", True, None, None),
    ("warningType", "String", True, None, None),
    *_VO_FIELDS[3:],
)
_DEFINITION_FIELDS = (
    _REQUIRED_ID,
    ("pluginName", "String", True, None, None),
    ("pluginType", "String", True, None, None),
    ("pluginParams", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_LIST_PAGE_FIELDS = (("totalList", "List<T>", False, None, "list"), *_PAGE_FIELDS[1:])
_REQUIRED_NULLABLE_PAGE_FIELDS = (
    ("totalList", "Optional<List<T>>", False, None, None),
    *_PAGE_FIELDS[1:],
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="alert-plugin-instances",
                channel="query",
                request_schema="page_local_search",
                request_model="AlertPluginLocalPageParams",
                request_fields=("pageNo", "pageSize"),
            ),
            CompiledRequestEpoch(
                method="GET",
                path="alert-plugin-instances",
                channel="query",
                request_schema="page",
                request_model="AlertPluginPageParams",
                request_fields=("searchVal", "pageNo", "pageSize"),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="list",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="alert-plugin-instances/list",
                channel="query",
                request_schema="empty",
                request_model="AlertPluginEmptyParams",
                request_fields=(),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="update_baseline",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="alert-plugin-instances/{id}",
                channel="path",
                request_schema="id",
                request_model="AlertPluginIdParams",
                request_fields=("id",),
                path_fields=("id",),
            ),
        ),
        result_envelope="optional",
        absent_versions=_BASELINE_UNUSED_VERSIONS,
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="alert-plugin-instances",
                channel="form",
                request_schema="create",
                request_model="AlertPluginCreateParams",
                request_fields=(
                    "pluginDefineId",
                    "instanceName",
                    "pluginInstanceParams",
                ),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="alert-plugin-instances",
                channel="form",
                request_schema="create_transient",
                request_model="AlertPluginTransientCreateParams",
                request_fields=(
                    "pluginDefineId",
                    "instanceName",
                    "instanceType",
                    "warningType",
                    "pluginInstanceParams",
                ),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="PUT",
                path="alert-plugin-instances/{id}",
                channel="path_form",
                request_schema="update",
                request_model="AlertPluginUpdateParams",
                request_fields=("id", "instanceName", "pluginInstanceParams"),
                path_fields=("id",),
            ),
            CompiledRequestEpoch(
                method="PUT",
                path="alert-plugin-instances/{id}",
                channel="path_form",
                request_schema="update_transient",
                request_model="AlertPluginTransientUpdateParams",
                request_fields=(
                    "id",
                    "instanceName",
                    "warningType",
                    "pluginInstanceParams",
                ),
                path_fields=("id",),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="DELETE",
                path="alert-plugin-instances/{id}",
                channel="path",
                request_schema="id",
                request_model="AlertPluginIdParams",
                request_fields=("id",),
                path_fields=("id",),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="test_send",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="alert-plugin-instances/test-send",
                channel="form",
                request_schema="test_send",
                request_model="AlertPluginTestSendParams",
                request_fields=("pluginDefineId", "pluginInstanceParams"),
            ),
        ),
        result_envelope="required",
        absent_versions=_TEST_ABSENT_VERSIONS,
    ),
    CompiledPrimitive(
        name="definition_list",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="ui-plugins/query-by-type",
                channel="query",
                request_schema="definition_type",
                request_model="AlertPluginDefinitionTypeParams",
                request_fields=("pluginType",),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="definition_get",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="ui-plugins/{id}",
                channel="path",
                request_schema="id",
                request_model="AlertPluginIdParams",
                request_fields=("id",),
                path_fields=("id",),
            ),
        ),
        result_envelope="optional",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "AlertPluginInstanceController.listPaging": "page",
        "AlertPluginInstanceController."
        "getAlertPluginInstance__get_alert_plugin_instances_list": "list",
        "AlertPluginInstanceController."
        "getAlertPluginInstance__get_alert_plugin_instances_id": "update_baseline",
        "AlertPluginInstanceController.createAlertPluginInstance": "create",
        "AlertPluginInstanceController.updateAlertPluginInstance": "update",
        "AlertPluginInstanceController.updateAlertPluginInstanceById": "update",
        "AlertPluginInstanceController.deleteAlertPluginInstance": "delete",
        "AlertPluginInstanceController.testSendAlertPluginInstance": "test_send",
        "UiPluginController.queryUiPluginsByType": "definition_list",
        "UiPluginController.queryUiPluginDetailById": "definition_get",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    logical_type = operation.logical_return_type
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled alert_plugin {primitive} exchange projection changed"
        raise ValueError(message)
    page_suffix = f"<{_VO}>"
    if primitive == "page" and logical_type.endswith(page_suffix):
        schema = _page_epoch(snapshot, logical_type[: -len(page_suffix)])
        codec = "page_local_search" if snapshot.ds_version == "2.0.0" else schema
        return CompiledResponsePolicy(codec=codec, schema=schema, capture={})
    if primitive == "list":
        vo_epoch = _vo_epoch(snapshot)
        if logical_type == f"List<{_VO}>" and vo_epoch == "plain":
            return CompiledResponsePolicy(codec="list", schema="list", capture=[])
        if logical_type == f"List<{_VO}>" and vo_epoch == "delivery":
            return CompiledResponsePolicy(
                codec="list_delivery",
                schema="list_delivery",
                capture=[],
            )
    if primitive in {"definition_list", "definition_get"}:
        expected_type = (
            f"List<{_DEFINITION}>" if primitive == "definition_list" else _DEFINITION
        )
        if logical_type == expected_type:
            epoch = _definition_epoch(snapshot)
            schema = f"{primitive}_{epoch}"
            return CompiledResponsePolicy(
                codec=schema,
                schema=schema,
                capture=[] if primitive == "definition_list" else {},
            )
    if primitive in {"create", "update", "delete"} and logical_type == "Void":
        return CompiledResponsePolicy(
            codec=f"{primitive}_void", schema=None, capture=None
        )
    if (
        primitive in {"create", "update", "update_baseline"}
        and logical_type == _INSTANCE
    ):
        schema = f"instance_{_instance_epoch(snapshot)}"
        return CompiledResponsePolicy(
            codec=f"{primitive}_{schema}", schema=schema, capture={}
        )
    if primitive in {"delete", "test_send"} and logical_type == "Boolean":
        return CompiledResponsePolicy(
            codec=f"{primitive}_bool",
            schema="boolean",
            capture=True,
            scalar_annotation="bool",
        )
    message = f"compiled alert_plugin {primitive} response changed: {logical_type}"
    raise ValueError(message)


def _page_epoch(snapshot: ContractSnapshot, page_import: str) -> str:
    if "." in page_import:
        page = require_model(snapshot, page_import, domain="alert_plugin")
    else:
        page_matches = [model for model in snapshot.models if model.name == page_import]
        if len(page_matches) != 1:
            message = (
                "compiled alert_plugin requires one exact page response model "
                f"{page_import}"
            )
            raise ValueError(message)
        page = page_matches[0]
    fields = model_field_facts(page)
    vo_epoch = _vo_epoch(snapshot)
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if fields == _REQUIRED_NULLABLE_PAGE_FIELDS and vo_epoch == "plain" and strict:
        return "page_nullable_required_strict"
    if fields == _PAGE_FIELDS and vo_epoch == "plain" and strict:
        return "page_nullable_strict"
    if fields == _LIST_PAGE_FIELDS:
        if vo_epoch == "plain":
            return "page_list_strict" if strict else "page_list"
        if vo_epoch == "delivery" and not strict:
            return "page_delivery"
    message = "compiled alert_plugin page response epoch changed"
    raise ValueError(message)


def _vo_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(require_model(snapshot, _VO, domain="alert_plugin"))
    if fields == _VO_FIELDS:
        return "plain"
    if fields == _DELIVERY_VO_FIELDS:
        return "delivery"
    message = "compiled alert_plugin VO response epoch changed"
    raise ValueError(message)


def _instance_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(
        require_model(snapshot, _INSTANCE, domain="alert_plugin")
    )
    if fields == _INSTANCE_FIELDS:
        return "required_id"
    if fields == _NULLABLE_INSTANCE_FIELDS:
        return "nullable_id"
    if fields == _TRANSIENT_INSTANCE_FIELDS:
        return "transient"
    message = "compiled alert_plugin instance response epoch changed"
    raise ValueError(message)


def _definition_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(
        require_model(snapshot, _DEFINITION, domain="alert_plugin")
    )
    if fields == _DEFINITION_FIELDS:
        return "required_id"
    if fields == (_NULLABLE_ID, *_DEFINITION_FIELDS[1:]):
        return "nullable_id"
    message = "compiled alert_plugin definition response epoch changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(codecs.get(primitive.name) for primitive in _PRIMITIVES)
    recipes = {
        (
            "page_local_search",
            "list",
            None,
            "create_void",
            "update_void",
            "delete_void",
            None,
            "definition_list_required_id",
            "definition_get_required_id",
        ): "void_local_search",
        (
            "page_nullable_strict",
            "list",
            None,
            "create_void",
            "update_void",
            "delete_void",
            None,
            "definition_list_required_id",
            "definition_get_required_id",
        ): "void",
        (
            "page_nullable_strict",
            "list",
            None,
            "create_instance_required_id",
            "update_void",
            "delete_void",
            None,
            "definition_list_required_id",
            "definition_get_required_id",
        ): "create_entity",
        (
            "page_nullable_required_strict",
            "list",
            None,
            "create_instance_nullable_id",
            "update_void",
            "delete_void",
            None,
            "definition_list_nullable_id",
            "definition_get_nullable_id",
        ): "create_entity",
        (
            "page_list_strict",
            "list",
            None,
            "create_instance_nullable_id",
            "update_void",
            "delete_void",
            None,
            "definition_list_nullable_id",
            "definition_get_nullable_id",
        ): "create_entity",
        (
            "page_list",
            "list",
            None,
            "create_instance_nullable_id",
            "update_void",
            "delete_void",
            None,
            "definition_list_nullable_id",
            "definition_get_nullable_id",
        ): "create_entity",
        (
            "page_delivery",
            "list_delivery",
            "update_baseline_instance_transient",
            "create_instance_transient",
            "update_instance_transient",
            "delete_bool",
            "test_send_bool",
            "definition_list_nullable_id",
            "definition_get_nullable_id",
        ): "transient_entity",
        (
            "page_delivery",
            "list_delivery",
            None,
            "create_instance_nullable_id",
            "update_instance_nullable_id",
            "delete_bool",
            "test_send_bool",
            "definition_list_nullable_id",
            "definition_get_nullable_id",
        ): "entity",
    }
    if coordinate in recipes:
        return recipes[coordinate]
    message = f"compiled alert_plugin recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


ALERT_PLUGIN_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="alert_plugin",
    schema_constant="COMPILED_ALERT_PLUGIN_SCHEMA_VERSION",
    schema_version=COMPILED_ALERT_PLUGIN_SCHEMA_VERSION,
    semantic_operations=COMPILED_ALERT_PLUGIN_SEMANTIC_OPERATIONS,
    semantic_absent_versions={"alert-plugin.test": _TEST_ABSENT_VERSIONS},
    absent_versions=frozenset({"1.3.9"}),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "ALERT_PLUGIN_COMPILED_DOMAIN",
    "COMPILED_ALERT_PLUGIN_SCHEMA_VERSION",
    "COMPILED_ALERT_PLUGIN_SEMANTIC_OPERATIONS",
]
