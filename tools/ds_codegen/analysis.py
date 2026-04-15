from __future__ import annotations

import json
from collections import Counter
from typing import TYPE_CHECKING

from ds_codegen.render.requests_example import (
    _generic_base_type,
    _generic_inner_types,
    _render_python_type,
    _RenderContext,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot

_KNOWN_SCALAR_TYPES = {
    "Any",
    "Boolean",
    "Byte",
    "Date",
    "Double",
    "Float",
    "Integer",
    "Long",
    "Object",
    "Short",
    "String",
    "Void",
    "boolean",
    "byte",
    "double",
    "float",
    "int",
    "long",
    "short",
    "void",
}


def build_contract_analysis(snapshot: ContractSnapshot) -> dict[str, object]:
    context = _RenderContext(
        dtos_by_name={dto.name: dto for dto in snapshot.dtos},
        models_by_name={model.name: model for model in snapshot.models},
        enums_by_name={enum_spec.name: enum_spec for enum_spec in snapshot.enums},
    )
    known_types = (
        set(context.dtos_by_name)
        | set(context.models_by_name)
        | set(context.enums_by_name)
        | _KNOWN_SCALAR_TYPES
    )

    any_return_examples: list[dict[str, object]] = []
    any_return_reasons: Counter[str] = Counter()
    unresolved_returns: list[dict[str, object]] = []
    unknown_type_references: Counter[str] = Counter()

    for operation in snapshot.operations:
        unwrapped_return_type = operation.logical_return_type
        rendered_python_type = _render_python_type(unwrapped_return_type, context)
        if rendered_python_type == "Any":
            reason = _classify_any_return_reason(
                operation.return_type,
                operation.inferred_return_type,
                context,
            )
            any_return_reasons[reason] += 1
            any_return_examples.append(
                {
                    "operation_id": operation.operation_id,
                    "return_type": operation.return_type,
                    "inferred_return_type": operation.inferred_return_type,
                    "unwrapped_return_type": unwrapped_return_type,
                    "reason": reason,
                }
            )
        missing_return_types = sorted(
            {
                type_name
                for type_name in _walk_java_type_names(unwrapped_return_type)
                if type_name not in known_types and type_name != "T"
            }
        )
        if missing_return_types:
            unresolved_returns.append(
                {
                    "operation_id": operation.operation_id,
                    "return_type": operation.return_type,
                    "unwrapped_return_type": unwrapped_return_type,
                    "missing_types": missing_return_types,
                }
            )

    for dto in snapshot.dtos:
        for field in dto.fields:
            for type_name in _walk_java_type_names(field.java_type):
                if type_name not in known_types and type_name != "T":
                    unknown_type_references[type_name] += 1

    for model in snapshot.models:
        for field in model.fields:
            for type_name in _walk_java_type_names(field.java_type):
                if type_name not in known_types and type_name != "T":
                    unknown_type_references[type_name] += 1

    return {
        "ds_version": snapshot.ds_version,
        "operation_count": snapshot.operation_count,
        "dto_count": snapshot.dto_count,
        "model_count": snapshot.model_count,
        "enum_count": snapshot.enum_count,
        "duplicate_operation_id_count": max(
            Counter(
                operation.operation_id for operation in snapshot.operations
            ).values(),
            default=0,
        ),
        "any_returns": {
            "count": len(any_return_examples),
            "by_reason": dict(any_return_reasons),
            "examples": any_return_examples,
        },
        "unresolved_returns": unresolved_returns,
        "unknown_type_references": dict(unknown_type_references),
    }


def write_contract_analysis(
    snapshot: ContractSnapshot,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            build_contract_analysis(snapshot),
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
        + "\n"
    )


def _classify_any_return_reason(
    return_type: str,
    inferred_return_type: str | None,
    context: _RenderContext,
) -> str:
    if inferred_return_type is not None:
        return "inferred_type_still_any"
    if return_type == "Result":
        return "raw_result_without_payload_inference"
    if return_type == "ResponseEntity":
        return "response_entity_without_body_inference"
    wrapper_model = context.models_by_name.get(return_type)
    if wrapper_model is not None and wrapper_model.extends == "Result":
        return "wrapper_result_without_concrete_data"
    if return_type == "void":
        return "void"
    return "unresolved"


def _walk_java_type_names(java_type: str) -> list[str]:
    if java_type.endswith("[]"):
        return _walk_java_type_names(java_type[:-2])
    if java_type.startswith("List<") and java_type.endswith(">"):
        return _walk_java_type_names(java_type[5:-1])
    if java_type.startswith("Map<") and java_type.endswith(">"):
        inner = java_type[4:-1]
        depth = 0
        split_index = -1
        for index, char in enumerate(inner):
            if char == "<":
                depth += 1
                continue
            if char == ">":
                depth -= 1
                continue
            if char == "," and depth == 0:
                split_index = index
                break
        if split_index != -1:
            return _walk_java_type_names(
                inner[:split_index].strip()
            ) + _walk_java_type_names(inner[split_index + 1 :].strip())
    nested_types: list[str] = []
    if "<" in java_type and java_type.endswith(">"):
        for inner_type in _generic_inner_types(java_type):
            nested_types.extend(_walk_java_type_names(inner_type))
    nested_types.append(_generic_base_type(java_type))
    return nested_types
