"""Shared type and import resolution helpers for generated packages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.contract_type_refs import (
    ContractTypeExpression,
    UnsupportedContractTypeExpressionError,
    canonicalize_builtin_type_expression,
    generic_inner_types,
    parse_contract_type_expression,
)
from ds_codegen.render.package.planner import python_class_name
from ds_codegen.snapshot_resolution import ResolutionScope

if TYPE_CHECKING:
    from ds_codegen.ir import ReferenceOwnerKind
    from ds_codegen.render.package.planner import PackageRenderContext

SCALAR_PYTHON_TYPES = {
    "Any": "object",
    "ArrayList": "list[object]",
    "ArrayNode": "list[dict[str, object]]",
    "Boolean": "bool",
    "Byte": "int",
    "Date": "str",
    "Double": "float",
    "Float": "float",
    "Integer": "int",
    "Instant": "str",
    "JsonObject": "JsonObject",
    "JsonValue": "JsonValue",
    "Collection": "list[object]",
    "HashMap": "dict[str, object]",
    "LinkedHashMap": "dict[str, object]",
    "LinkedList": "list[object]",
    "List": "list[object]",
    "LocalDate": "str",
    "LocalDateTime": "str",
    "LocalTime": "str",
    "Long": "int",
    "Map": "dict[str, object]",
    "MultipartFile": "UploadFileLike",
    "Object": "object",
    "ObjectNode": "dict[str, object]",
    "OffsetDateTime": "str",
    "OffsetTime": "str",
    "Set": "list[object]",
    "Short": "int",
    "Stream": "list[object]",
    "String": "str",
    "ZonedDateTime": "str",
    "Void": "None",
    "boolean": "bool",
    "byte": "int",
    "double": "float",
    "float": "float",
    "int": "int",
    "long": "int",
    "short": "int",
    "void": "None",
}

_COLLECTION_GENERIC_BASES = frozenset(
    {"ArrayList", "Collection", "LinkedList", "List", "Set", "Stream"}
)
_MAP_GENERIC_BASES = frozenset({"HashMap", "LinkedHashMap", "Map"})


@dataclass(frozen=True)
class _AnnotationPlan:
    annotation: str
    import_targets: frozenset[tuple[tuple[str, ...], str]] = frozenset()


@dataclass(frozen=True)
class _AnnotationPlanner:
    owner_import_path: str | None
    context: PackageRenderContext
    owner_kind: ReferenceOwnerKind
    specialized_java_type: str | None

    def plan(self, expression: ContractTypeExpression) -> _AnnotationPlan:
        java_type = expression.render()
        if _is_type_variable(java_type):
            return _AnnotationPlan(java_type)
        specialized = self.context.specialized_by_java_type.get(java_type)
        if specialized is not None:
            target = (specialized.module_parts, specialized.class_name)
            return _AnnotationPlan(specialized.class_name, frozenset({target}))
        if java_type in {"Byte[]", "byte[]"}:
            return _AnnotationPlan("bytes")
        scalar_type = SCALAR_PYTHON_TYPES.get(java_type)
        if scalar_type is not None:
            return _AnnotationPlan(scalar_type)
        if expression.is_array:
            element = ContractTypeExpression(expression.name, expression.arguments)
            inner = self.plan(element)
            return _container_annotation_plan(f"list[{inner.annotation}]", inner)
        return self._plan_non_array(expression)

    def _plan_non_array(
        self,
        expression: ContractTypeExpression,
    ) -> _AnnotationPlan:
        arguments = expression.arguments
        if expression.name == "Optional" and arguments:
            if len(arguments) != 1:
                inner = ", ".join(argument.render() for argument in arguments)
                message = f"unsupported contract type expression: {inner!r}"
                raise UnsupportedContractTypeExpressionError(message)
            value = self.plan(arguments[0])
            return _container_annotation_plan(f"{value.annotation} | None", value)
        if expression.name in _COLLECTION_GENERIC_BASES and len(arguments) == 1:
            value = self.plan(arguments[0])
            return _container_annotation_plan(f"list[{value.annotation}]", value)
        if expression.name in _MAP_GENERIC_BASES and len(arguments) == 2:
            key, value = (self.plan(argument) for argument in arguments)
            return _container_annotation_plan(
                f"dict[{key.annotation}, {value.annotation}]",
                key,
                value,
            )
        if self.context.type_resolver.is_opaque_external_reference(expression.name):
            return _AnnotationPlan("JsonValue")
        return self._plan_reference(expression)

    def _plan_reference(
        self,
        expression: ContractTypeExpression,
    ) -> _AnnotationPlan:
        import_path = resolve_owner_reference_import_path(
            expression.name,
            self.owner_import_path,
            self.context,
            owner_kind=self.owner_kind,
            specialized_java_type=self.specialized_java_type,
        )
        argument_plans = tuple(self.plan(argument) for argument in expression.arguments)
        assignment = (
            self.context.assignments_by_import_path.get(import_path)
            if import_path is not None
            else None
        )
        if assignment is None:
            return _container_annotation_plan(
                python_class_name(expression.name),
                *argument_plans,
            )
        targets = {target for plan in argument_plans for target in plan.import_targets}
        targets.add((assignment.module_parts, assignment.class_name))
        return _AnnotationPlan(assignment.class_name, frozenset(targets))


def field_annotation_type(
    java_type: str,
    *,
    allow_none: bool,
    owner_import_path: str | None,
    context: PackageRenderContext,
    owner_kind: ReferenceOwnerKind = "structured_type",
    specialized_java_type: str | None = None,
) -> str:
    rendered_type = render_annotation_type(
        java_type,
        owner_import_path=owner_import_path,
        context=context,
        owner_kind=owner_kind,
        specialized_java_type=specialized_java_type,
    )
    if not allow_none or rendered_type == "None" or rendered_type.endswith(" | None"):
        return rendered_type
    return f"{rendered_type} | None"


def render_annotation_type(
    java_type: str,
    *,
    owner_import_path: str | None,
    context: PackageRenderContext,
    owner_kind: ReferenceOwnerKind = "structured_type",
    specialized_java_type: str | None = None,
) -> str:
    return _annotation_plan(
        java_type,
        owner_import_path=owner_import_path,
        context=context,
        owner_kind=owner_kind,
        specialized_java_type=specialized_java_type,
    ).annotation


def collect_annotation_import_targets(
    java_type: str,
    *,
    owner_import_path: str | None,
    current_module_parts: tuple[str, ...],
    context: PackageRenderContext,
    owner_kind: ReferenceOwnerKind = "structured_type",
    specialized_java_type: str | None = None,
) -> set[tuple[tuple[str, ...], str]]:
    plan = _annotation_plan(
        java_type,
        owner_import_path=owner_import_path,
        context=context,
        owner_kind=owner_kind,
        specialized_java_type=specialized_java_type,
    )
    return {
        target for target in plan.import_targets if target[0] != current_module_parts
    }


def _annotation_plan(
    java_type: str,
    *,
    owner_import_path: str | None,
    context: PackageRenderContext,
    owner_kind: ReferenceOwnerKind,
    specialized_java_type: str | None,
) -> _AnnotationPlan:
    planner = _AnnotationPlanner(
        owner_import_path,
        context,
        owner_kind,
        specialized_java_type,
    )
    return planner.plan(parse_contract_type_expression(java_type))


def _container_annotation_plan(
    annotation: str,
    *children: _AnnotationPlan,
) -> _AnnotationPlan:
    return _AnnotationPlan(
        annotation,
        frozenset(target for child in children for target in child.import_targets),
    )


def resolve_owner_reference_import_path(
    reference_name: str,
    owner_import_path: str | None,
    context: PackageRenderContext,
    *,
    owner_kind: ReferenceOwnerKind = "structured_type",
    specialized_java_type: str | None = None,
) -> str | None:
    if context.type_resolver.is_opaque_external_reference(reference_name):
        return None
    candidates = context.type_resolver.candidates(reference_name)
    if len(candidates) == 1:
        return next(iter(candidates))
    if specialized_java_type is not None:
        specialized = context.specialized_by_java_type[specialized_java_type]
        resolved = specialized.reference_import_paths.get(reference_name)
        if resolved is not None:
            return resolved
    if owner_import_path is None:
        message = f"reference {reference_name!r} has no contract resolution owner"
        raise ValueError(message)
    return context.type_resolver.resolve(
        reference_name,
        scope=ResolutionScope(owner_kind, owner_import_path),
    )


def relative_import_statement(
    from_module_parts: tuple[str, ...],
    to_module_parts: tuple[str, ...],
    class_name: str,
) -> str:
    from_package_parts = from_module_parts[:-1]
    to_package_parts = to_module_parts[:-1]
    common_length = 0
    for left, right in zip(from_package_parts, to_package_parts, strict=False):
        if left != right:
            break
        common_length += 1
    up_levels = len(from_package_parts) - common_length
    relative_prefix = "." * (up_levels + 1)
    target_suffix = ".".join(to_module_parts[common_length:])
    return f"from {relative_prefix}{target_suffix} import {class_name}"


def render_scalar_annotation_type(java_type: str) -> str:
    java_type = canonicalize_builtin_type_expression(java_type)
    return SCALAR_PYTHON_TYPES.get(java_type, python_class_name(java_type))


def generic_substitutions(java_type: str) -> dict[str, str]:
    generic_args = generic_inner_types(java_type)
    if len(generic_args) == 1:
        return {"T": generic_args[0]}
    return {}


def _is_type_variable(java_type: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]", java_type))
