"""Compile the exact favourite-task catalog without changing discovery semantics."""

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

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_TASK_TYPE_SCHEMA_VERSION = 1
COMPILED_TASK_TYPE_SEMANTIC_OPERATIONS = frozenset({"task-type.list"})
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
    }
)
_ENTITY = "org.apache.dolphinscheduler.api.dto.FavTaskDto"
_TASK_TYPE = ("taskType", "String", True, None, None)
_COLLECTION = ("collection", "boolean", False, "false", None)
_LEGACY_FIELDS = (("taskName", "String", True, None, None), _COLLECTION, _TASK_TYPE)
_CATEGORY_FIELDS = (
    _TASK_TYPE,
    _COLLECTION,
    ("taskCategory", "String", True, None, None),
)


def _classify(operation: OperationSpec) -> str | None:
    return (
        "list" if operation.operation_id == "FavTaskController.listTaskType" else None
    )


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    if (
        primitive != "list"
        or operation.logical_return_type != f"List<{_ENTITY}>"
        or operation.response_projection != "direct"
        or operation.consumes
    ):
        message = "compiled task_type list response changed"
        raise ValueError(message)
    fields = model_field_facts(require_model(snapshot, _ENTITY, domain="task_type"))
    if fields == _LEGACY_FIELDS:
        schema = "list_legacy"
    elif fields == _CATEGORY_FIELDS:
        schema = "list_category"
    else:
        message = "compiled task_type list response fields changed"
        raise ValueError(message)
    return CompiledResponsePolicy(codec=schema, schema=schema, capture=[])


def _recipe(codecs: Mapping[str, str]) -> str:
    if codecs == {"list": "list_legacy"}:
        return "legacy"
    if codecs == {"list": "list_category"}:
        return "category"
    message = f"compiled task_type recipe is unsupported: {codecs!r}"
    raise ValueError(message)


TASK_TYPE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="task_type",
    schema_constant="COMPILED_TASK_TYPE_SCHEMA_VERSION",
    schema_version=COMPILED_TASK_TYPE_SCHEMA_VERSION,
    semantic_operations=COMPILED_TASK_TYPE_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=(
        CompiledPrimitive(
            name="list",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path="favourite/taskTypes",
                    channel="query",
                    request_schema="empty",
                    request_model="TaskTypeEmptyParams",
                    request_fields=(),
                    required_fields=frozenset(),
                ),
            ),
            result_envelope="optional",
        ),
    ),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = ["COMPILED_TASK_TYPE_SCHEMA_VERSION", "TASK_TYPE_COMPILED_DOMAIN"]
