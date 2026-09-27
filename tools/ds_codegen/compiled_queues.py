"""Declare queue policy for the shared compiled-domain engine."""

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

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_QUEUE_SCHEMA_VERSION = 1
COMPILED_QUEUE_SEMANTIC_OPERATIONS = frozenset(
    {"queue.create", "queue.delete", "queue.get", "queue.page", "queue.update"}
)
_DELETE_ABSENT_VERSIONS = frozenset(
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

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_QUEUE = "org.apache.dolphinscheduler.dao.entity.Queue"
_REQUIRED_ID_FIELDS = (
    ("id", "int", False, "0", None),
    ("queueName", "String", True, None, None),
    ("queue", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_NULLABLE_ID_FIELDS = (
    ("id", "Integer", True, None, None),
    *_REQUIRED_ID_FIELDS[1:],
)
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

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="queue/list-paging",
                channel="query",
                request_schema="page",
                request_model="QueuePageParams",
                request_fields=("pageNo", "searchVal", "pageSize"),
            ),
            CompiledRequestEpoch(
                method="GET",
                path="queues",
                channel="query",
                request_schema="page",
                request_model="QueuePageParams",
                request_fields=("pageNo", "searchVal", "pageSize"),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="queue/create",
                channel="form",
                request_schema="create",
                request_model="QueueCreateParams",
                request_fields=("queue", "queueName"),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="queues",
                channel="form",
                request_schema="create",
                request_model="QueueCreateParams",
                request_fields=("queue", "queueName"),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="queue/update",
                channel="form",
                request_schema="update",
                request_model="QueueUpdateParams",
                request_fields=("id", "queue", "queueName"),
            ),
            CompiledRequestEpoch(
                method="PUT",
                path="queues/{id}",
                channel="path_form",
                request_schema="update",
                request_model="QueueUpdateParams",
                request_fields=("id", "queue", "queueName"),
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
                path="queues/{id}",
                channel="path",
                request_schema="id",
                request_model="QueueIdParams",
                request_fields=("id",),
                path_fields=("id",),
            ),
        ),
        result_envelope="required",
        absent_versions=_DELETE_ABSENT_VERSIONS,
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "QueueController":
        return None
    return {
        "queryQueueListPaging": "page",
        "createQueue": "create",
        "updateQueue": "update",
        "deleteQueueById": "delete",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
) -> CompiledResponsePolicy:
    logical_type = operation.logical_return_type
    if primitive == "page":
        schema = _page_response_epoch(snapshot, logical_type)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture={})
    if primitive in {"create", "update", "delete"} and logical_type in {"Void", "void"}:
        codec = (
            f"{primitive}_legacy_void"
            if snapshot.ds_version == "1.3.9"
            else f"{primitive}_void"
        )
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if primitive in {"create", "update"} and logical_type == _QUEUE:
        schema = _entity_response_epoch(snapshot)
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
    message = f"compiled queue {primitive} response changed: {logical_type}"
    raise ValueError(message)


def _page_response_epoch(snapshot: ContractSnapshot, logical_type: str) -> str:
    if logical_type != f"{_PAGE}<{_QUEUE}>":
        message = "compiled queue page response type changed"
        raise ValueError(message)
    fields = model_field_facts(require_model(snapshot, _PAGE, domain="queue"))
    entity_epoch = _entity_response_epoch(snapshot)
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if entity_epoch == "entity_required_id":
        if fields == _LEGACY_PAGE_FIELDS and not strict:
            return "page_legacy"
        if fields == _NULLABLE_PAGE_FIELDS and strict:
            return "page_required_id_strict"
    if entity_epoch == "entity_nullable_id" and fields == _LIST_PAGE_FIELDS:
        return "page_nullable_id_strict" if strict else "page_nullable_id"
    message = "compiled queue page response epoch changed"
    raise ValueError(message)


def _entity_response_epoch(
    snapshot: ContractSnapshot,
) -> Literal["entity_required_id", "entity_nullable_id"]:
    fields = model_field_facts(require_model(snapshot, _QUEUE, domain="queue"))
    if fields == _REQUIRED_ID_FIELDS:
        return "entity_required_id"
    if fields == _NULLABLE_ID_FIELDS:
        return "entity_nullable_id"
    message = "compiled queue entity response epoch changed"
    raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = tuple(
        codecs.get(name) for name in ("page", "create", "update", "delete")
    )
    recipes = {
        ("page_legacy", "create_legacy_void", "update_legacy_void", None): "legacy",
        ("page_required_id_strict", "create_void", "update_void", None): "void",
        (
            "page_required_id_strict",
            "create_entity_required_id",
            "update_void",
            None,
        ): "create_entity",
        (
            "page_nullable_id_strict",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            None,
        ): "entity",
        (
            "page_nullable_id",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            "delete_void",
        ): "void_delete",
        (
            "page_nullable_id",
            "create_entity_nullable_id",
            "update_entity_nullable_id",
            "delete_bool",
        ): "boolean",
    }
    if coordinate in recipes:
        return recipes[coordinate]
    message = f"compiled queue recipe is unsupported: {coordinate!r}"
    raise ValueError(message)


QUEUE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="queue",
    schema_constant="COMPILED_QUEUE_SCHEMA_VERSION",
    schema_version=COMPILED_QUEUE_SCHEMA_VERSION,
    semantic_operations=COMPILED_QUEUE_SEMANTIC_OPERATIONS,
    semantic_absent_versions={"queue.delete": _DELETE_ABSENT_VERSIONS},
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_QUEUE_SCHEMA_VERSION",
    "COMPILED_QUEUE_SEMANTIC_OPERATIONS",
    "QUEUE_COMPILED_DOMAIN",
]
