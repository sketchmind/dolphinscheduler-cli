"""One atomic source owner for the mutually shared workflow-runtime cluster."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from ds_codegen import (
    compiled_workflow_definitions,
    compiled_workflow_instances,
    compiled_workflow_lineage,
    compiled_workflow_schedules,
    compiled_workflow_tasks,
)
from ds_codegen.compiled_domains import CompiledDomainDefinition

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.compiled_domains import CompiledResponsePolicy
    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_WORKFLOW_RUNTIME_SCHEMA_VERSION = 1
_FRAGMENTS = (
    compiled_workflow_definitions,
    compiled_workflow_instances,
    compiled_workflow_tasks,
    compiled_workflow_schedules,
    compiled_workflow_lineage,
)
_PRIMITIVES = tuple(
    primitive for fragment in _FRAGMENTS for primitive in fragment.PRIMITIVES
)
COMPILED_WORKFLOW_RUNTIME_SEMANTIC_OPERATIONS = frozenset(
    name for fragment in _FRAGMENTS for name in fragment.SEMANTIC_OPERATIONS
)


def _classify(operation: OperationSpec) -> str | None:
    matches = [
        name
        for fragment in _FRAGMENTS
        if (name := fragment.classify(operation)) is not None
    ]
    if len(matches) > 1:
        msg = (
            "Workflow source operation has multiple primitive owners: "
            f"{operation.operation_id}"
        )
        raise ValueError(msg)
    return matches[0] if matches else None


def _response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    for fragment in _FRAGMENTS:
        if any(item.name == primitive for item in fragment.PRIMITIVES):
            return cast(
                "CompiledResponsePolicy",
                fragment.response_policy(snapshot, operation, primitive),
            )
    msg = f"Unknown compiled workflow primitive {primitive}"
    raise ValueError(msg)


def _recipe_policy(codecs: Mapping[str, str]) -> str:
    definition_codecs = {
        item.name: codecs[item.name]
        for item in compiled_workflow_definitions.PRIMITIVES
        if item.name in codecs
    }
    exact = compiled_workflow_definitions.recipe_policy(definition_codecs)
    stamp = exact.removeprefix("exact_")
    if any(codec != f"{primitive}_{stamp}" for primitive, codec in codecs.items()):
        msg = "Workflow fragments cross reviewed exact recipes"
        raise ValueError(msg)
    for fragment in _FRAGMENTS[1:]:
        selected = {
            item.name: codecs[item.name]
            for item in fragment.PRIMITIVES
            if item.name in codecs
        }
        fragment.recipe_policy(selected)
    return compiled_workflow_schedules.recipe_policy(
        {
            item.name: codecs[item.name]
            for item in compiled_workflow_schedules.PRIMITIVES
            if item.name in codecs
        }
    )


WORKFLOW_RUNTIME = CompiledDomainDefinition(
    name="workflow_runtime",
    schema_constant="COMPILED_WORKFLOW_RUNTIME_SCHEMA_VERSION",
    schema_version=COMPILED_WORKFLOW_RUNTIME_SCHEMA_VERSION,
    semantic_operations=COMPILED_WORKFLOW_RUNTIME_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response_policy,
    recipe_policy=_recipe_policy,
    semantic_absent_versions={
        name: versions
        for fragment in _FRAGMENTS
        for name, versions in fragment.SEMANTIC_ABSENT_VERSIONS.items()
    },
)
