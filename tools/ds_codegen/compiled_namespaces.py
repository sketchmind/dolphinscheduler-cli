"""Compile namespace wire epochs against the reviewed governance recipes."""

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
from ds_codegen.governance_contract import (
    NAMESPACE_SEMANTIC_OPERATIONS,
    TARGET_GOVERNANCE_VERSIONS,
    governance_contract,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.governance_contract import NamespaceRecipe
    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_NAMESPACE_SCHEMA_VERSION = 1
COMPILED_NAMESPACE_SEMANTIC_OPERATIONS = frozenset(NAMESPACE_SEMANTIC_OPERATIONS)
_ABSENT_VERSIONS = frozenset(
    version
    for version in TARGET_GOVERNANCE_VERSIONS
    if governance_contract(version).namespace.support == "absent"
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ENTITY = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_ID_FIELD = ("id", "Integer", True, None, None)
_CODE_FIELD = ("code", "Long", True, None, None)
_NAME_FIELD = ("namespace", "String", True, None, None)
_QUOTA_FIELDS = (
    ("limitsCpu", "Double", True, None, None),
    ("limitsMemory", "Integer", True, None, None),
)
_USER_FIELDS = (
    ("userId", "int", False, "0", None),
    ("userName", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_POD_FIELDS = (
    ("podRequestCpu", "Double", False, "0.0", None),
    ("podRequestMemory", "Integer", False, "0", None),
    ("podReplicas", "Integer", False, "0", None),
)
_CLUSTER_FIELDS = (
    ("clusterCode", "Long", True, None, None),
    ("clusterName", "String", True, None, None),
)
_K8S_QUOTA_FIELDS = (
    _ID_FIELD,
    _NAME_FIELD,
    *_QUOTA_FIELDS,
    *_USER_FIELDS,
    *_POD_FIELDS,
    ("onlineJobNum", "Integer", False, "0", None),
    ("k8s", "String", True, None, None),
)
_CLUSTER_QUOTA_FIELDS = (
    _ID_FIELD,
    _CODE_FIELD,
    _NAME_FIELD,
    *_QUOTA_FIELDS,
    *_USER_FIELDS,
    *_POD_FIELDS,
    *_CLUSTER_FIELDS,
)
_CLUSTER_NAMESPACE_FIELDS = (
    _ID_FIELD,
    _CODE_FIELD,
    _NAME_FIELD,
    *_USER_FIELDS,
    *_CLUSTER_FIELDS,
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

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="k8s-namespace",
                channel="query",
                request_schema="page",
                request_model="NamespacePageParams",
                request_fields=("searchVal", "pageSize", "pageNo"),
                required_fields=frozenset({"pageSize", "pageNo"}),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="available",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="k8s-namespace/available-list",
                channel="query",
                request_schema="empty",
                request_model="NamespaceEmptyParams",
                request_fields=(),
                required_fields=frozenset(),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="k8s-namespace",
                channel="form",
                request_schema="create_k8s_quota",
                request_model="NamespaceK8sQuotaCreateParams",
                request_fields=("namespace", "k8s", "limitsCpu", "limitsMemory"),
                required_fields=frozenset({"namespace", "k8s"}),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="k8s-namespace",
                channel="form",
                request_schema="create_cluster_quota",
                request_model="NamespaceClusterQuotaCreateParams",
                request_fields=(
                    "namespace",
                    "clusterCode",
                    "limitsCpu",
                    "limitsMemory",
                ),
                required_fields=frozenset({"namespace", "clusterCode"}),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="k8s-namespace",
                channel="form",
                request_schema="create_cluster",
                request_model="NamespaceClusterCreateParams",
                request_fields=("namespace", "clusterCode"),
                required_fields=frozenset({"namespace", "clusterCode"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="k8s-namespace/delete",
                channel="form",
                request_schema="id",
                request_model="NamespaceIdParams",
                request_fields=("id",),
                required_fields=frozenset({"id"}),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "K8sNamespaceController":
        return None
    return {
        "queryProjectListPaging": "page",
        "queryNamespaceListPaging": "page",
        "queryAvailableNamespaceList": "available",
        "createNamespace": "create",
        "delNamespaceById": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    recipe = governance_contract(snapshot.ds_version).namespace
    if (
        recipe.support != "supported"
        or not recipe.creates_kubernetes_namespace_if_absent
    ):
        message = "compiled namespace reviewed governance recipe changed"
        raise ValueError(message)
    _require_request_fields(operation, primitive, recipe)
    logical_type = operation.logical_return_type
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled namespace {primitive} exchange projection changed"
        raise ValueError(message)
    if primitive == "page" and logical_type == f"{_PAGE}<{_ENTITY}>":
        if operation.operation_id != recipe.page_operation:
            message = "compiled namespace reviewed page operation changed"
            raise ValueError(message)
        schema = _page_epoch(snapshot, recipe)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive == "available" and logical_type == f"List<{_ENTITY}>":
        schema = f"available_{_entity_epoch(snapshot, recipe)}"
        return CompiledResponsePolicy(codec=schema, schema=schema, capture=[])
    if primitive == "create":
        entity_epoch = _entity_epoch(snapshot, recipe)
        if recipe.create_result == "none" and logical_type == "Void":
            return CompiledResponsePolicy(
                codec=f"create_{entity_epoch}_void", schema=None, capture=None
            )
        if recipe.create_result == "entity" and logical_type == _ENTITY:
            schema = f"entity_{entity_epoch}"
            return CompiledResponsePolicy(
                codec=f"create_{schema}", schema=schema, capture={}
            )
    if primitive == "delete" and logical_type == "Void":
        # Equal POST/Void exchanges do not imply equal destructive behavior.
        codec = (
            "delete_kubernetes"
            if recipe.deletes_kubernetes_namespace
            else "delete_registration"
        )
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    message = f"compiled namespace {primitive} response changed: {logical_type}"
    raise ValueError(message)


def _require_request_fields(
    operation: OperationSpec, primitive: str, recipe: NamespaceRecipe
) -> None:
    parameters = tuple(
        parameter
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    )
    types = {
        "searchVal": "String",
        "pageSize": "Integer",
        "pageNo": "Integer",
        "namespace": "String",
        "k8s": "String",
        "clusterCode": "Long",
        "limitsCpu": "Double",
        "limitsMemory": "Integer",
        "id": "int",
    }
    if any(
        parameter.java_type != types.get(parameter.wire_name or "")
        or parameter.default_value is not None
        for parameter in parameters
    ):
        message = "compiled namespace request type or default changed"
        raise ValueError(message)
    if primitive == "create":
        selector = "k8s" if recipe.selector == "k8s" else "clusterCode"
        fields: tuple[str, ...] = ("namespace", selector)
        if recipe.quotas_supported:
            fields += ("limitsCpu", "limitsMemory")
        if tuple(parameter.wire_name for parameter in parameters) != fields:
            message = (
                "compiled namespace create request contradicts "
                "reviewed selector or quotas"
            )
            raise ValueError(message)


def _entity_epoch(snapshot: ContractSnapshot, recipe: NamespaceRecipe) -> str:
    fields = model_field_facts(require_model(snapshot, _ENTITY, domain="namespace"))
    epochs = {
        ("k8s", True): ("k8s_quota", _K8S_QUOTA_FIELDS),
        ("cluster-code", True): ("cluster_quota", _CLUSTER_QUOTA_FIELDS),
        ("cluster-code", False): ("cluster", _CLUSTER_NAMESPACE_FIELDS),
    }
    expected = epochs.get((recipe.selector, recipe.quotas_supported))
    if expected is not None and fields == expected[1]:
        return expected[0]
    message = (
        "compiled namespace entity response contradicts reviewed selector or quotas"
    )
    raise ValueError(message)


def _page_epoch(snapshot: ContractSnapshot, recipe: NamespaceRecipe) -> str:
    fields = model_field_facts(require_model(snapshot, _PAGE, domain="namespace"))
    entity_epoch = _entity_epoch(snapshot, recipe)
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if entity_epoch == "k8s_quota" and fields == _NULLABLE_PAGE_FIELDS and strict:
        return "page_k8s_quota_strict"
    if fields == _LIST_PAGE_FIELDS:
        if entity_epoch == "cluster_quota":
            return "page_cluster_quota_strict" if strict else "page_cluster_quota"
        if entity_epoch == "cluster" and not strict:
            return "page_cluster"
    message = "compiled namespace page response epoch changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(codecs.get(primitive.name) for primitive in _PRIMITIVES)
    recipes = {
        (
            "page_k8s_quota_strict",
            "available_k8s_quota",
            "create_k8s_quota_void",
            "delete_kubernetes",
        ): "k8s_quota_namespace",
        (
            "page_cluster_quota_strict",
            "available_cluster_quota",
            "create_cluster_quota_void",
            "delete_kubernetes",
        ): "cluster_quota_destructive",
        (
            "page_cluster_quota",
            "available_cluster_quota",
            "create_cluster_quota_void",
            "delete_registration",
        ): "cluster_quota_registration_only",
        (
            "page_cluster",
            "available_cluster",
            "create_cluster_void",
            "delete_registration",
        ): "cluster_registration_only",
        (
            "page_cluster",
            "available_cluster",
            "create_entity_cluster",
            "delete_registration",
        ): "cluster_entity_registration_only",
    }
    if coordinate in recipes:
        return recipes[coordinate]
    message = f"compiled namespace recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


NAMESPACE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="namespace",
    schema_constant="COMPILED_NAMESPACE_SCHEMA_VERSION",
    schema_version=COMPILED_NAMESPACE_SCHEMA_VERSION,
    semantic_operations=COMPILED_NAMESPACE_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_NAMESPACE_SCHEMA_VERSION",
    "COMPILED_NAMESPACE_SEMANTIC_OPERATIONS",
    "NAMESPACE_COMPILED_DOMAIN",
]
