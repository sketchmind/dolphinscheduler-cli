"""Compare generated DolphinScheduler contract snapshots across DS versions."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from ds_codegen.extract import build_contract_snapshot
from ds_codegen.ir import (
    ContractSnapshot,
    DtoFieldSpec,
    DtoSpec,
    EnumFieldSpec,
    EnumSpec,
    EnumValueSpec,
    ModelSpec,
    OperationSpec,
    ParameterSpec,
)
from ds_codegen.source import codegen_repo_root_for_ds_source

if TYPE_CHECKING:
    from pathlib import Path

JsonObject = dict[str, Any]

# These field lists intentionally omit documentation strings.  Documentation
# churn is useful, but endpoint compatibility analysis should lead with the
# facts that can break generated clients or handwritten version adapters.
OPERATION_FIELDS = (
    "controller",
    "method_name",
    "api_group",
    "http_method",
    "path",
    "consumes",
    "return_type",
    "inferred_return_type",
    "logical_return_type",
    "response_projection",
)
PARAMETER_FIELDS = (
    "java_type",
    "binding",
    "wire_name",
    "required",
    "default_value",
    "hidden",
    "allowable_values",
    "schema_type",
)
STRUCTURED_TYPE_FIELDS = ("import_path", "extends")
MODEL_FIELDS = ("import_path", "kind", "extends")
STRUCTURED_FIELD_FIELDS = (
    "java_type",
    "wire_name",
    "required",
    "default_value",
    "nullable",
    "default_factory",
    "allowable_values",
)
ENUM_FIELDS = ("import_path", "json_value_field")
ENUM_VALUE_FIELDS = ("arguments",)
ENUM_FIELD_FIELDS = ("java_type",)


def build_snapshot_from_ds_source(ds_source_root: Path) -> ContractSnapshot:
    """Build a contract snapshot from one checked-out DolphinScheduler tree."""
    with codegen_repo_root_for_ds_source(ds_source_root) as repo_root:
        return build_contract_snapshot(repo_root)


def load_snapshot(path: Path) -> ContractSnapshot:
    """Load a JSON contract snapshot emitted by ``generate_ds_contract.py``."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"Snapshot file must contain a JSON object: {path}"
        raise TypeError(message)
    return snapshot_from_json(payload)


def snapshot_from_json(payload: JsonObject) -> ContractSnapshot:
    """Deserialize one JSON snapshot into the codegen IR dataclasses."""
    operations = [
        OperationSpec(
            **{
                **_require_mapping(item, label="operation"),
                "parameters": [
                    ParameterSpec(**_require_mapping(parameter, label="parameter"))
                    for parameter in _require_list(
                        _require_mapping(item, label="operation").get("parameters"),
                        label="operation.parameters",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("operations"), label="operations")
    ]
    enums = [
        EnumSpec(
            **{
                **_require_mapping(item, label="enum"),
                "fields": [
                    EnumFieldSpec(**_require_mapping(field, label="enum field"))
                    for field in _require_list(
                        _require_mapping(item, label="enum").get("fields"),
                        label="enum.fields",
                    )
                ],
                "values": [
                    EnumValueSpec(**_require_mapping(value, label="enum value"))
                    for value in _require_list(
                        _require_mapping(item, label="enum").get("values"),
                        label="enum.values",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("enums"), label="enums")
    ]
    dtos = [
        DtoSpec(
            **{
                **_require_mapping(item, label="dto"),
                "fields": [
                    DtoFieldSpec(**_require_mapping(field, label="dto field"))
                    for field in _require_list(
                        _require_mapping(item, label="dto").get("fields"),
                        label="dto.fields",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("dtos"), label="dtos")
    ]
    models = [
        ModelSpec(
            **{
                **_require_mapping(item, label="model"),
                "fields": [
                    DtoFieldSpec(**_require_mapping(field, label="model field"))
                    for field in _require_list(
                        _require_mapping(item, label="model").get("fields"),
                        label="model.fields",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("models"), label="models")
    ]
    return ContractSnapshot(
        ds_version=str(payload["ds_version"]),
        operation_count=int(payload["operation_count"]),
        enum_count=int(payload["enum_count"]),
        dto_count=int(payload["dto_count"]),
        model_count=int(payload["model_count"]),
        operations=operations,
        enums=enums,
        dtos=dtos,
        models=models,
    )


def compare_contract_snapshots(
    *,
    base_label: str,
    base: ContractSnapshot,
    target_label: str,
    target: ContractSnapshot,
) -> JsonObject:
    """Return a structured compatibility diff between two snapshots."""
    sections = {
        "operations": _compare_operations(base.operations, target.operations),
        "dtos": _compare_structured_types(
            base.dtos,
            target.dtos,
            item_fields=STRUCTURED_TYPE_FIELDS,
        ),
        "models": _compare_structured_types(
            base.models,
            target.models,
            item_fields=MODEL_FIELDS,
        ),
        "enums": _compare_enums(base.enums, target.enums),
    }
    return {
        "base": _snapshot_summary(base_label, base),
        "target": _snapshot_summary(target_label, target),
        "summary": {
            name: _collection_summary(section) for name, section in sections.items()
        },
        **sections,
    }


def render_markdown_report(report: JsonObject, *, max_items: int = 50) -> str:
    """Render one JSON diff report as a human-readable Markdown summary."""
    base = _require_mapping(report.get("base"), label="base")
    target = _require_mapping(report.get("target"), label="target")
    summary = _require_mapping(report.get("summary"), label="summary")
    lines = [
        f"# DolphinScheduler Contract Diff: {base['label']} -> {target['label']}",
        "",
        "| Snapshot | DS version | Operations | DTOs | Models | Enums |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        _snapshot_summary_row(base),
        _snapshot_summary_row(target),
        "",
        "## Summary",
        "",
        "| Surface | Added | Removed | Changed |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in ("operations", "dtos", "models", "enums"):
        item = _require_mapping(summary.get(name), label=f"summary.{name}")
        lines.append(
            f"| {name} | {item['added']} | {item['removed']} | {item['changed']} |"
        )
    for name in ("operations", "dtos", "models", "enums"):
        lines.extend(["", f"## {name.title()}", ""])
        lines.extend(_render_collection(report[name], max_items=max_items))
    return "\n".join(lines).rstrip() + "\n"


def _compare_operations(
    base_items: list[OperationSpec],
    target_items: list[OperationSpec],
) -> JsonObject:
    return _compare_named_collection(
        base_items,
        target_items,
        key_fn=lambda item: item.operation_id,
        summary_fn=_operation_summary,
        change_fn=_operation_change,
    )


def _compare_structured_types(
    base_items: list[DtoSpec] | list[ModelSpec],
    target_items: list[DtoSpec] | list[ModelSpec],
    *,
    item_fields: tuple[str, ...],
) -> JsonObject:
    return _compare_named_collection(
        base_items,
        target_items,
        key_fn=lambda item: item.name,
        summary_fn=_structured_type_summary,
        change_fn=lambda base, target: _structured_type_change(
            base,
            target,
            item_fields=item_fields,
        ),
    )


def _compare_enums(
    base_items: list[EnumSpec],
    target_items: list[EnumSpec],
) -> JsonObject:
    return _compare_named_collection(
        base_items,
        target_items,
        key_fn=lambda item: item.name,
        summary_fn=_enum_summary,
        change_fn=_enum_change,
    )


def _compare_named_collection(
    base_items: list[Any],
    target_items: list[Any],
    *,
    key_fn: Any,
    summary_fn: Any,
    change_fn: Any,
) -> JsonObject:
    base_by_key = {key_fn(item): item for item in base_items}
    target_by_key = {key_fn(item): item for item in target_items}
    added = [
        summary_fn(target_by_key[key])
        for key in sorted(set(target_by_key) - set(base_by_key))
    ]
    removed = [
        summary_fn(base_by_key[key])
        for key in sorted(set(base_by_key) - set(target_by_key))
    ]
    changed: list[JsonObject] = []
    for key in sorted(set(base_by_key) & set(target_by_key)):
        change = change_fn(base_by_key[key], target_by_key[key])
        if _has_change(change):
            changed.append({"key": key, **change})
    return {"added": added, "removed": removed, "changed": changed}


def _operation_change(base: OperationSpec, target: OperationSpec) -> JsonObject:
    parameter_diff = _compare_named_collection(
        base.parameters,
        target.parameters,
        key_fn=_parameter_key,
        summary_fn=_parameter_summary,
        change_fn=lambda old, new: {
            "changes": _attribute_changes(old, new, PARAMETER_FIELDS)
        },
    )
    change: JsonObject = {"changes": _attribute_changes(base, target, OPERATION_FIELDS)}
    if _has_change(parameter_diff):
        change["parameters"] = parameter_diff
    return change


def _structured_type_change(
    base: DtoSpec | ModelSpec,
    target: DtoSpec | ModelSpec,
    *,
    item_fields: tuple[str, ...],
) -> JsonObject:
    field_diff = _compare_named_collection(
        base.fields,
        target.fields,
        key_fn=lambda item: item.wire_name,
        summary_fn=_structured_field_summary,
        change_fn=lambda old, new: {
            "changes": _attribute_changes(old, new, STRUCTURED_FIELD_FIELDS)
        },
    )
    change: JsonObject = {"changes": _attribute_changes(base, target, item_fields)}
    if _has_change(field_diff):
        change["fields"] = field_diff
    return change


def _enum_change(base: EnumSpec, target: EnumSpec) -> JsonObject:
    value_diff = _compare_named_collection(
        base.values,
        target.values,
        key_fn=lambda item: item.name,
        summary_fn=_enum_value_summary,
        change_fn=lambda old, new: {
            "changes": _attribute_changes(old, new, ENUM_VALUE_FIELDS)
        },
    )
    field_diff = _compare_named_collection(
        base.fields,
        target.fields,
        key_fn=lambda item: item.name,
        summary_fn=_enum_field_summary,
        change_fn=lambda old, new: {
            "changes": _attribute_changes(old, new, ENUM_FIELD_FIELDS)
        },
    )
    change: JsonObject = {"changes": _attribute_changes(base, target, ENUM_FIELDS)}
    if _has_change(value_diff):
        change["values"] = value_diff
    if _has_change(field_diff):
        change["fields"] = field_diff
    return change


def _attribute_changes(
    base: Any,
    target: Any,
    fields: tuple[str, ...],
) -> list[JsonObject]:
    changes: list[JsonObject] = []
    for field_name in fields:
        before = getattr(base, field_name)
        after = getattr(target, field_name)
        if before != after:
            changes.append({"field": field_name, "before": before, "after": after})
    return changes


def _snapshot_summary(label: str, snapshot: ContractSnapshot) -> JsonObject:
    return {
        "label": label,
        "ds_version": snapshot.ds_version,
        "operation_count": snapshot.operation_count,
        "dto_count": snapshot.dto_count,
        "model_count": snapshot.model_count,
        "enum_count": snapshot.enum_count,
    }


def _operation_summary(item: OperationSpec) -> JsonObject:
    return {
        "key": item.operation_id,
        "method": item.http_method,
        "path": item.path,
        "logical_return_type": item.logical_return_type,
        "parameter_count": len(item.parameters),
    }


def _parameter_summary(item: ParameterSpec) -> JsonObject:
    return {
        "key": _parameter_key(item),
        "name": item.name,
        "wire_name": item.wire_name,
        "binding": item.binding,
        "java_type": item.java_type,
        "required": item.required,
        "default_value": item.default_value,
    }


def _structured_type_summary(item: DtoSpec | ModelSpec) -> JsonObject:
    details: JsonObject = {
        "key": item.name,
        "import_path": item.import_path,
        "field_count": len(item.fields),
    }
    kind = getattr(item, "kind", None)
    if kind is not None:
        details["kind"] = kind
    return details


def _structured_field_summary(item: DtoFieldSpec) -> JsonObject:
    return {
        "key": item.wire_name,
        "name": item.name,
        "wire_name": item.wire_name,
        "java_type": item.java_type,
        "required": item.required,
        "nullable": item.nullable,
        "default_value": item.default_value,
    }


def _enum_summary(item: EnumSpec) -> JsonObject:
    return {
        "key": item.name,
        "import_path": item.import_path,
        "value_count": len(item.values),
    }


def _enum_value_summary(item: EnumValueSpec) -> JsonObject:
    return {"key": item.name, "name": item.name, "arguments": item.arguments}


def _enum_field_summary(item: EnumFieldSpec) -> JsonObject:
    return {"key": item.name, "name": item.name, "java_type": item.java_type}


def _parameter_key(item: ParameterSpec) -> str:
    wire_name = item.wire_name or item.name
    binding = item.binding or "unknown"
    return f"{binding}:{wire_name}"


def _collection_summary(diff: JsonObject) -> JsonObject:
    return {
        "added": len(diff["added"]),
        "removed": len(diff["removed"]),
        "changed": len(diff["changed"]),
    }


def _has_change(diff: JsonObject) -> bool:
    if diff.get("changes"):
        return True
    for value in diff.values():
        if isinstance(value, dict) and _has_change(value):
            return True
        if (
            isinstance(value, list)
            and value
            and any(key in diff for key in ("added", "removed", "changed"))
        ):
            return True
    return False


def _snapshot_summary_row(summary: JsonObject) -> str:
    return (
        f"| {summary['label']} | {summary['ds_version']} | "
        f"{summary['operation_count']} | {summary['dto_count']} | "
        f"{summary['model_count']} | {summary['enum_count']} |"
    )


def _render_collection(diff: Any, *, max_items: int) -> list[str]:
    mapping = _require_mapping(diff, label="collection diff")
    lines: list[str] = []
    for title, key in (("Added", "added"), ("Removed", "removed")):
        items = _limited_items(_require_list(mapping.get(key), label=key), max_items)
        if items:
            lines.append(f"### {title}")
            lines.append("")
            lines.extend(f"- `{item['key']}`" for item in items)
            lines.append("")
    changed = _limited_items(
        _require_list(mapping.get("changed"), label="changed"),
        max_items,
    )
    if changed:
        lines.append("### Changed")
        lines.append("")
        for item in changed:
            lines.append(f"- `{item['key']}`")
            for change in _require_list(item.get("changes"), label="changes"):
                change_mapping = _require_mapping(change, label="change")
                lines.append(
                    "  - "
                    f"`{change_mapping['field']}`: "
                    f"{_inline_value(change_mapping['before'])} -> "
                    f"{_inline_value(change_mapping['after'])}"
                )
            for nested_name in ("parameters", "fields", "values"):
                nested = item.get(nested_name)
                if isinstance(nested, dict) and _has_change(nested):
                    nested_summary = _collection_summary(nested)
                    lines.append(
                        "  - "
                        f"{nested_name}: +{nested_summary['added']} "
                        f"-{nested_summary['removed']} "
                        f"~{nested_summary['changed']}"
                    )
        lines.append("")
    if not lines:
        return ["No changes.", ""]
    return lines


def _limited_items(items: list[Any], max_items: int) -> list[JsonObject]:
    selected = items if max_items < 1 else items[:max_items]
    return [_require_mapping(item, label="diff item") for item in selected]


def _inline_value(value: Any) -> str:
    return f"`{json.dumps(value, ensure_ascii=True, sort_keys=True)}`"


def _require_mapping(value: Any, *, label: str) -> JsonObject:
    if not isinstance(value, dict):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    return value


def _require_list(value: Any, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        message = f"{label} must be a JSON array"
        raise TypeError(message)
    return value


def report_to_json_text(report: JsonObject) -> str:
    """Serialize a report with stable ordering for review and tests."""
    return json.dumps(report, indent=2, ensure_ascii=True, sort_keys=True) + "\n"


def snapshot_to_json_text(snapshot: ContractSnapshot) -> str:
    """Serialize a snapshot using the same shape as the generator output."""
    return json.dumps(asdict(snapshot), indent=2, ensure_ascii=True, sort_keys=True)
