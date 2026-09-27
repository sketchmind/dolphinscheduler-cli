"""Declare the source-owned cluster policy for the compiled-domain engine."""

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

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_CLUSTER_SCHEMA_VERSION = 4
COMPILED_CLUSTER_SEMANTIC_OPERATIONS = frozenset(
    {
        "cluster.create",
        "cluster.delete",
        "cluster.get",
        "cluster.page",
        "cluster.update",
    }
)
_CLUSTER_DTO = "org.apache.dolphinscheduler.api.dto.ClusterDto"
_CLUSTER_ENTITY = "org.apache.dolphinscheduler.dao.entity.Cluster"
_PAGE_INFO = "org.apache.dolphinscheduler.api.utils.PageInfo"


_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="cluster/list-paging",
                channel="query",
                request_schema="page",
                request_model="ClusterPageParams",
                request_fields=("searchVal", "pageSize", "pageNo"),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="get",
        absent_versions=frozenset({"3.4.3"}),
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="cluster/query-by-code",
                channel="query",
                request_schema="code",
                request_model="ClusterCodeParams",
                request_fields=("clusterCode",),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="cluster/create",
                channel="form",
                request_schema="create",
                request_model="ClusterCreateParams",
                request_fields=("name", "config", "description"),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="cluster/update",
                channel="form",
                request_schema="update",
                request_model="ClusterUpdateParams",
                request_fields=("code", "name", "config", "description"),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="cluster/delete",
                channel="form",
                request_schema="code",
                request_model="ClusterCodeParams",
                request_fields=("clusterCode",),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "queryClusterListPaging": "page",
        "queryClusterByCode": "get",
        "createProject": "create",
        "createCluster": "create",
        "updateCluster": "update",
        "deleteCluster": "delete",
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
    if primitive == "delete" and leaf == "Boolean":
        return CompiledResponsePolicy(
            codec="delete_bool",
            schema="delete_bool",
            capture=True,
            scalar_annotation="bool",
        )
    if primitive == "update" and leaf == "Cluster":
        _require_fields(snapshot, _CLUSTER_ENTITY, entity=True)
        return CompiledResponsePolicy(
            codec="update_entity",
            schema="update_entity",
            capture={},
        )
    definitions = _definitions_field(snapshot)
    epoch = definitions.removesuffix("Definitions")
    if primitive == "get":
        schema = f"get_{epoch}"
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive == "page":
        strict = bool(
            cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE_INFO)
        )
        schema = f"page_{epoch}{'_strict' if strict else ''}"
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    message = f"compiled cluster {primitive} response changed: {leaf}"
    raise ValueError(message)


def _definitions_field(snapshot: ContractSnapshot) -> str:
    fields = _require_fields(snapshot, _CLUSTER_DTO, entity=False)
    choices = fields & {"processDefinitions", "workflowDefinitions"}
    if len(choices) != 1:
        message = "compiled cluster definition field changed"
        raise ValueError(message)
    return next(iter(choices))


def _require_fields(
    snapshot: ContractSnapshot,
    import_path: str,
    *,
    entity: bool,
) -> set[str]:
    model = require_model(snapshot, import_path, domain="cluster")
    fields = {field.wire_name for field in model.fields}
    expected = {
        "id",
        "code",
        "name",
        "config",
        "description",
        "operator",
        "createTime",
        "updateTime",
    }
    if not entity:
        expected.add(
            next(iter(fields & {"processDefinitions", "workflowDefinitions"}), "")
        )
    if fields != expected:
        message = f"compiled cluster response fields changed: {sorted(fields)!r}"
        raise ValueError(message)
    return fields


def _recipe(codecs: Mapping[str, str]) -> str:
    """Select reviewed domain behavior while the exact codec facts are available."""
    coordinate = (codecs.get("get"), codecs.get("update"), codecs.get("delete"))
    if coordinate == ("get_process", "update_void", "delete_void"):
        return "process_definitions_void_mutations"
    if coordinate == ("get_process", "update_entity", "delete_bool"):
        return "process_definitions_entity_mutations"
    if coordinate == ("get_workflow", "update_entity", "delete_bool"):
        return "workflow_definitions_entity_mutations"
    if coordinate == (None, "update_entity", "delete_bool") and codecs.get("page") in {
        "page_workflow",
        "page_workflow_strict",
    }:
        return "workflow_definitions_paged_readback"
    message = f"compiled cluster recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


CLUSTER_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="cluster",
    schema_constant="COMPILED_CLUSTER_SCHEMA_VERSION",
    schema_version=COMPILED_CLUSTER_SCHEMA_VERSION,
    semantic_operations=COMPILED_CLUSTER_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(
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
        }
    ),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "CLUSTER_COMPILED_DOMAIN",
    "COMPILED_CLUSTER_SCHEMA_VERSION",
    "COMPILED_CLUSTER_SEMANTIC_OPERATIONS",
]
