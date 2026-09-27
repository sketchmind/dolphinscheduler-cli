"""Compile exact project worker-group lists and replace-all assignment wire."""

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

COMPILED_PROJECT_WORKER_GROUP_SCHEMA_VERSION = 1
COMPILED_PROJECT_WORKER_GROUP_SEMANTIC_OPERATIONS = frozenset(
    {
        "project-worker-group.page",
        "project-worker-group.set",
        "project-worker-group.clear",
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
        "3.2.0",
        "3.2.1",
    }
)
_ENTITY = "org.apache.dolphinscheduler.dao.entity.ProjectWorkerGroup"
_PATH = "projects/{projectCode}/worker-group"
_FIELDS = (
    ("id", "Integer", True, None, None),
    ("projectCode", "Long", True, None, None),
    ("workerGroup", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "ProjectWorkerGroupController.queryWorkerGroups": "list",
        "ProjectWorkerGroupController.queryAssignedWorkerGroups": "list",
        "ProjectWorkerGroupController.assignWorkerGroups": "assign",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    source = (
        "assignWorkerGroups"
        if primitive == "assign"
        else "queryWorkerGroups"
        if snapshot.ds_version == "3.2.2"
        else "queryAssignedWorkerGroups"
    )
    expected: dict[str, tuple[str, str | None]] = {"projectCode": ("long", None)}
    if primitive == "assign":
        expected["workerGroups"] = ("String[]", None)
    actual = {
        item.wire_name: (item.java_type, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if actual != expected:
        message = (
            f"compiled project_worker_group {primitive} request type or default changed"
        )
        raise ValueError(message)
    if (
        operation.operation_id != f"ProjectWorkerGroupController.{source}"
        or operation.logical_return_type
        != ("Void" if primitive == "assign" else f"List<{_ENTITY}>")
        or operation.response_projection
        != ("direct" if primitive == "assign" else "status_data")
        or operation.consumes
    ):
        message = f"compiled project_worker_group {primitive} response changed"
        raise ValueError(message)
    if primitive == "assign":
        return CompiledResponsePolicy(codec="assign", schema=None, capture=None)
    entity = require_model(snapshot, _ENTITY, domain="project_worker_group")
    if entity.extends is not None or model_field_facts(entity) != _FIELDS:
        message = "compiled project_worker_group entity fields changed"
        raise ValueError(message)
    return CompiledResponsePolicy(codec="list", schema="list", capture=[])


def _recipe(codecs: Mapping[str, str]) -> str:
    if codecs == {"list": "list", "assign": "assign"}:
        return "status_data"
    message = f"compiled project_worker_group recipe is unsupported: {codecs!r}"
    raise ValueError(message)


PROJECT_WORKER_GROUP_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="project_worker_group",
    schema_constant="COMPILED_PROJECT_WORKER_GROUP_SCHEMA_VERSION",
    schema_version=COMPILED_PROJECT_WORKER_GROUP_SCHEMA_VERSION,
    semantic_operations=COMPILED_PROJECT_WORKER_GROUP_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    semantic_absent_versions={
        "project-worker-group.clear": frozenset({"3.2.2"}),
    },
    primitives=(
        CompiledPrimitive(
            name="list",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=_PATH,
                    channel="path",
                    request_schema="project",
                    request_model="ProjectWorkerGroupProjectParams",
                    request_fields=("projectCode",),
                    path_fields=("projectCode",),
                    required_fields=frozenset({"projectCode"}),
                ),
            ),
            result_envelope="optional",
        ),
        CompiledPrimitive(
            name="assign",
            requests=(
                CompiledRequestEpoch(
                    method="POST",
                    path=_PATH,
                    channel="path_form",
                    request_schema="assign",
                    request_model="ProjectWorkerGroupAssignParams",
                    request_fields=("projectCode", "workerGroups"),
                    path_fields=("projectCode",),
                    required_fields=frozenset({"projectCode", "workerGroups"}),
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
    "COMPILED_PROJECT_WORKER_GROUP_SCHEMA_VERSION",
    "PROJECT_WORKER_GROUP_COMPILED_DOMAIN",
]
