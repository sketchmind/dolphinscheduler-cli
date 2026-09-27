"""Stable planning helpers for generated package layout and type naming."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING

from ds_codegen.contract_type_refs import (
    canonicalize_builtin_type_expression,
    generic_base_type,
    generic_inner_types,
    substitute_type_parameters,
)
from ds_codegen.contract_visibility import (
    has_executable_response_type,
    is_client_supplied_parameter,
)
from ds_codegen.render.package.render_support import snake_case
from ds_codegen.snapshot_resolution import ResolutionScope, SnapshotTypeResolver

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ds_codegen.ir import ContractSnapshot, DtoFieldSpec, DtoSpec, ModelSpec


@dataclass(frozen=True)
class AssignedType:
    import_path: str
    class_name: str
    module_parts: tuple[str, ...]


@dataclass(frozen=True)
class SpecializedModel:
    java_type: str
    class_name: str
    base_import_path: str
    module_parts: tuple[str, ...]
    reference_import_paths: dict[str, str]


@dataclass(frozen=True)
class PackageRenderContext:
    snapshot: ContractSnapshot
    type_resolver: SnapshotTypeResolver
    assignments_by_import_path: dict[str, AssignedType]
    specialized_by_java_type: dict[str, SpecializedModel]


@dataclass(frozen=True)
class _PendingTypeUse:
    java_type: str
    scope: ResolutionScope
    substitutions: tuple[tuple[str, _PendingTypeUse], ...] = ()


@dataclass(frozen=True)
class _ResolvedSpecialization:
    base_import_path: str
    reference_import_paths: tuple[tuple[str, str], ...]


def build_package_context(
    snapshot: ContractSnapshot,
) -> PackageRenderContext:
    """Plan stable module/class assignments before rendering any file content."""
    type_resolver = SnapshotTypeResolver.compile(snapshot)
    assignments_by_import_path: dict[str, AssignedType] = {}

    for enum_spec in snapshot.enums:
        assignment = _assign_spec_module(enum_spec.import_path)
        assignments_by_import_path[enum_spec.import_path] = assignment

    for dto_spec in snapshot.dtos:
        assignment = _assign_spec_module(dto_spec.import_path)
        assignments_by_import_path[dto_spec.import_path] = assignment

    for model_spec in snapshot.models:
        assignment = _assign_spec_module(
            model_spec.import_path,
            fields=model_spec.fields,
        )
        assignments_by_import_path[model_spec.import_path] = assignment

    assignments_by_import_path = _dedupe_assigned_type_names(assignments_by_import_path)
    assignments_by_import_path = _dedupe_module_package_collisions(
        assignments_by_import_path
    )

    specializations = _collect_exact_specializations(
        snapshot,
        resolver=type_resolver,
    )
    specialized_class_names = _specialized_class_names(
        specializations,
        assignments_by_import_path=assignments_by_import_path,
    )
    specialized_by_java_type: dict[str, SpecializedModel] = {}
    for specialized_java_type, specialization in sorted(specializations.items()):
        base_import_path = specialization.base_import_path
        base_assignment = assignments_by_import_path[base_import_path]
        specialized_by_java_type[specialized_java_type] = SpecializedModel(
            java_type=specialized_java_type,
            class_name=specialized_class_names[specialized_java_type],
            base_import_path=base_import_path,
            module_parts=base_assignment.module_parts,
            reference_import_paths=dict(specialization.reference_import_paths),
        )

    return PackageRenderContext(
        snapshot=snapshot,
        type_resolver=type_resolver,
        assignments_by_import_path=assignments_by_import_path,
        specialized_by_java_type=specialized_by_java_type,
    )


def _collect_exact_specializations(
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver,
) -> dict[str, _ResolvedSpecialization]:
    models_by_import_path = {model.import_path: model for model in snapshot.models}
    structured_by_import_path = {
        spec.import_path: spec for spec in _iter_structured_specs(snapshot)
    }
    pending: list[_PendingTypeUse] = []
    for operation in snapshot.operations:
        if has_executable_response_type(operation):
            pending.append(
                _PendingTypeUse(
                    operation.logical_return_type,
                    ResolutionScope("operation_response", operation.operation_id),
                )
            )
        pending.extend(
            _PendingTypeUse(
                parameter.java_type,
                ResolutionScope("operation_request", operation.operation_id),
            )
            for parameter in operation.parameters
            if is_client_supplied_parameter(parameter)
        )
    occurrences: dict[str, set[_ResolvedSpecialization]] = defaultdict(set)
    visited: set[_PendingTypeUse] = set()
    while pending:
        use = _apply_direct_substitution(pending.pop())
        if use in visited:
            continue
        visited.add(use)
        java_type = use.java_type
        generic_args = generic_inner_types(java_type)
        base_import_path = _resolve_pending_reference(
            resolver,
            generic_base_type(java_type),
            use,
        )
        base_spec = (
            structured_by_import_path.get(base_import_path)
            if base_import_path is not None
            else None
        )
        base_model = (
            models_by_import_path.get(base_import_path)
            if base_import_path is not None
            else None
        )
        if generic_args and base_model is not None and base_import_path is not None:
            rendered_java_type = _materialize_pending_type(use)
            argument_resolutions = _generic_argument_resolutions(
                resolver,
                use,
                specialized_java_type=rendered_java_type,
            )
            occurrences[rendered_java_type].add(
                _ResolvedSpecialization(
                    base_import_path=base_import_path,
                    reference_import_paths=tuple(sorted(argument_resolutions.items())),
                )
            )
            substitutions = (
                (("T", _nested_type_use(generic_args[0], use)),)
                if len(generic_args) == 1
                else ()
            )
            nested_scope = ResolutionScope("structured_type", base_import_path)
            pending.extend(
                _PendingTypeUse(
                    field.java_type,
                    nested_scope,
                    substitutions,
                )
                for field in base_model.fields
            )
            continue
        if base_spec is not None and base_import_path is not None:
            nested_scope = ResolutionScope("structured_type", base_import_path)
            if base_spec.extends is not None:
                pending.append(_PendingTypeUse(base_spec.extends, nested_scope))
            pending.extend(
                _PendingTypeUse(field.java_type, nested_scope)
                for field in base_spec.fields
            )
            continue
        if java_type.endswith("[]"):
            pending.append(
                _PendingTypeUse(
                    java_type[:-2],
                    use.scope,
                    use.substitutions,
                )
            )
            continue
        pending.extend(
            _PendingTypeUse(
                generic_arg,
                use.scope,
                use.substitutions,
            )
            for generic_arg in generic_args
        )

    resolved: dict[str, _ResolvedSpecialization] = {}
    for java_type, candidates in occurrences.items():
        if len(candidates) != 1:
            rendered = sorted(
                (item.base_import_path, item.reference_import_paths)
                for item in candidates
            )
            message = (
                f"specialized type {java_type} has conflicting exact identities: "
                f"{rendered!r}"
            )
            raise ValueError(message)
        resolved[java_type] = next(iter(candidates))
    return resolved


def _generic_argument_resolutions(
    resolver: SnapshotTypeResolver,
    use: _PendingTypeUse,
    *,
    specialized_java_type: str,
) -> dict[str, str]:
    resolutions: dict[str, str] = {}
    for generic_arg in generic_inner_types(use.java_type):
        _collect_pending_resolutions(
            resolver,
            _nested_type_use(generic_arg, use),
            resolutions=resolutions,
            specialized_java_type=specialized_java_type,
        )
    return resolutions


def _collect_pending_resolutions(
    resolver: SnapshotTypeResolver,
    use: _PendingTypeUse,
    *,
    resolutions: dict[str, str],
    specialized_java_type: str,
) -> None:
    normalized = _apply_direct_substitution(use)
    java_type = normalized.java_type
    if java_type.endswith("[]"):
        _collect_pending_resolutions(
            resolver,
            _PendingTypeUse(
                java_type[:-2],
                normalized.scope,
                normalized.substitutions,
            ),
            resolutions=resolutions,
            specialized_java_type=specialized_java_type,
        )
        return
    reference_name = generic_base_type(java_type)
    import_path = _resolve_pending_reference(resolver, reference_name, normalized)
    if import_path is not None:
        previous = resolutions.setdefault(reference_name, import_path)
        if previous != import_path:
            message = (
                f"specialized type {specialized_java_type} resolves "
                f"{reference_name!r} to conflicting targets: "
                f"{sorted({previous, import_path})!r}"
            )
            raise ValueError(message)
    for generic_arg in generic_inner_types(java_type):
        _collect_pending_resolutions(
            resolver,
            _nested_type_use(generic_arg, normalized),
            resolutions=resolutions,
            specialized_java_type=specialized_java_type,
        )


def _nested_type_use(java_type: str, parent: _PendingTypeUse) -> _PendingTypeUse:
    return _PendingTypeUse(java_type, parent.scope, parent.substitutions)


def _apply_direct_substitution(use: _PendingTypeUse) -> _PendingTypeUse:
    resolved = dict(use.substitutions).get(use.java_type, use)
    canonical_java_type = canonicalize_builtin_type_expression(resolved.java_type)
    if canonical_java_type == resolved.java_type:
        return resolved
    return _PendingTypeUse(
        canonical_java_type,
        resolved.scope,
        resolved.substitutions,
    )


def _materialize_pending_type(
    use: _PendingTypeUse,
    *,
    active: frozenset[str] = frozenset(),
) -> str:
    substitutions: dict[str, str] = {}
    for name, replacement in use.substitutions:
        if name in active:
            continue
        substitutions[name] = _materialize_pending_type(
            replacement,
            active=active | {name},
        )
    return canonicalize_builtin_type_expression(
        substitute_type_parameters(use.java_type, substitutions)
    )


def _resolve_pending_reference(
    resolver: SnapshotTypeResolver,
    reference_name: str,
    use: _PendingTypeUse,
) -> str | None:
    if resolver.state(reference_name) == "missing":
        return None
    return resolver.resolve(reference_name, scope=use.scope)


def _iter_structured_specs(
    snapshot: ContractSnapshot,
) -> Iterator[DtoSpec | ModelSpec]:
    yield from snapshot.dtos
    yield from snapshot.models


def python_class_name(logical_name: str) -> str:
    parts = [part for part in re.split(r"[._]+", logical_name) if part]
    normalized_parts: list[str] = []
    for part in parts:
        if part[:1].islower():
            normalized_parts.append(part[:1].upper() + part[1:])
        else:
            normalized_parts.append(part)
    return "".join(normalized_parts)


def _assign_spec_module(
    import_path: str,
    *,
    fields: list[DtoFieldSpec] | None = None,
) -> AssignedType:
    if import_path.startswith("generated.view."):
        logical_name = import_path.split(".", 2)[-1]
        shared_assignment = _shared_generated_view_assignment(
            logical_name,
            fields or [],
        )
        if shared_assignment is not None:
            return shared_assignment
        module_name = _generated_view_module_name(logical_name)
        return AssignedType(
            import_path=import_path,
            class_name=_generated_view_class_name(logical_name, fields or []),
            module_parts=("api", "views", module_name),
        )

    package_parts, type_parts = _split_import_path(import_path)
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "api", "vo"]:
        return AssignedType(
            import_path=import_path,
            class_name=python_class_name(".".join(type_parts)),
            module_parts=(
                "api",
                "views",
                _api_view_module_name(package_parts, type_parts),
            ),
        )
    module_parts = (*_map_package_parts(package_parts), snake_case(type_parts[0]))
    return AssignedType(
        import_path=import_path,
        class_name=python_class_name(".".join(type_parts)),
        module_parts=module_parts,
    )


def _shared_generated_view_assignment(
    logical_name: str,
    fields: list[DtoFieldSpec],
) -> AssignedType | None:
    page_window_shape = tuple((field.wire_name, field.java_type) for field in fields)
    if logical_name.endswith("_PageInfo_created") and page_window_shape in {
        (("currentPage", "Integer"), ("pageSize", "Integer")),
        (("currentPage", "int"), ("pageSize", "int")),
    }:
        return AssignedType(
            import_path="generated.view.PaginationWindow",
            class_name="PaginationWindow",
            module_parts=("api", "views", "pagination"),
        )
    return None


def _api_view_module_name(
    package_parts: list[str],
    type_parts: list[str],
) -> str:
    nested_view_parts = package_parts[5:]
    if nested_view_parts:
        return snake_case(nested_view_parts[-1])
    type_name = type_parts[0].removesuffix("VO")
    return snake_case(type_name)


def _dedupe_assigned_type_names(
    assignments_by_import_path: dict[str, AssignedType],
) -> dict[str, AssignedType]:
    seen_by_module: dict[tuple[str, ...], dict[str, str]] = defaultdict(dict)
    updated_assignments: dict[str, AssignedType] = {}
    for import_path, assignment in sorted(assignments_by_import_path.items()):
        seen_names = seen_by_module[assignment.module_parts]
        if assignment.class_name not in seen_names:
            seen_names[assignment.class_name] = import_path
            updated_assignments[import_path] = assignment
            continue
        if seen_names[assignment.class_name] == import_path:
            updated_assignments[import_path] = assignment
            continue
        suffix_index = 2
        while f"{assignment.class_name}{suffix_index}" in seen_names:
            suffix_index += 1
        unique_name = f"{assignment.class_name}{suffix_index}"
        seen_names[unique_name] = import_path
        updated_assignments[import_path] = AssignedType(
            import_path=assignment.import_path,
            class_name=unique_name,
            module_parts=assignment.module_parts,
        )
    return updated_assignments


def _dedupe_module_package_collisions(
    assignments_by_import_path: dict[str, AssignedType],
) -> dict[str, AssignedType]:
    updated_assignments = dict(assignments_by_import_path)
    while True:
        module_parts_set = {
            assignment.module_parts for assignment in updated_assignments.values()
        }
        package_prefixes = {
            module_parts[:index]
            for module_parts in module_parts_set
            for index in range(1, len(module_parts))
        }
        collided_import_paths = [
            import_path
            for import_path, assignment in sorted(updated_assignments.items())
            if assignment.module_parts in package_prefixes
        ]
        if not collided_import_paths:
            return updated_assignments
        for import_path in collided_import_paths:
            assignment = updated_assignments[import_path]
            base_parts = assignment.module_parts[:-1]
            base_name = assignment.module_parts[-1]
            semantic_suffix = _module_collision_suffix(base_parts)
            suffix_index = 0
            while True:
                suffix = (
                    semantic_suffix
                    if suffix_index == 0
                    else f"{semantic_suffix}{suffix_index + 1}"
                )
                candidate_parts = (*base_parts, f"{base_name}{suffix}")
                if (
                    candidate_parts not in module_parts_set
                    and candidate_parts not in package_prefixes
                ):
                    updated_assignments[import_path] = AssignedType(
                        import_path=assignment.import_path,
                        class_name=assignment.class_name,
                        module_parts=candidate_parts,
                    )
                    break
                suffix_index += 1


def _module_collision_suffix(base_parts: tuple[str, ...]) -> str:
    if not base_parts:
        return "_types"
    match base_parts[-1]:
        case "views":
            return "_views"
        case "contracts":
            return "_contracts"
        case "operations":
            return "_operations"
        case "entities":
            return "_entities"
        case "enums":
            return "_enums"
        case _:
            return "_types"


def _split_import_path(import_path: str) -> tuple[list[str], list[str]]:
    parts = import_path.split(".")
    split_index = next(
        (index for index, part in enumerate(parts) if part and part[0].isupper()),
        len(parts) - 1,
    )
    return parts[:split_index], parts[split_index:]


def _map_package_parts(package_parts: list[str]) -> tuple[str, ...]:
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "api", "dto"]:
        return ("api", "contracts", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "api", "vo"]:
        return ("api", "views", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "api", "utils"]:
        return ("api", "contracts", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "api", "enums"]:
        return ("api", "enums", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == [
        "org",
        "apache",
        "dolphinscheduler",
        "api",
        "configuration",
    ]:
        return ("api", "configuration", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "common", "enums"]:
        return ("common", "enums", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "common", "model"]:
        return ("common", "model", *_snake_case_parts(package_parts[5:]))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "common"]:
        return ("common", *_snake_case_parts(package_parts[4:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "dao", "entity"]:
        return ("dao", "entities", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "dao", "model"]:
        return ("dao", "model", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "dao", "vo"]:
        return ("dao", "views", *_snake_case_parts(package_parts[5:]))
    if package_parts[:7] == [
        "org",
        "apache",
        "dolphinscheduler",
        "dao",
        "plugin",
        "api",
        "monitor",
    ]:
        return ("dao", "plugin_api", "monitor", *_snake_case_parts(package_parts[7:]))
    if package_parts[:6] == [
        "org",
        "apache",
        "dolphinscheduler",
        "dao",
        "plugin",
        "api",
    ]:
        return ("dao", "plugin_api", *_snake_case_parts(package_parts[6:]))
    if package_parts[:6] == [
        "org",
        "apache",
        "dolphinscheduler",
        "registry",
        "api",
        "enums",
    ]:
        return ("registry", "api", "enums", *_snake_case_parts(package_parts[6:]))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "spi"]:
        return ("spi", *_snake_case_parts(package_parts[4:]))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "plugin"]:
        plugin_name = snake_case(package_parts[4])
        remaining = package_parts[5:]
        if remaining and remaining[0] == "api":
            remaining = remaining[1:]
        return ("plugin", f"{plugin_name}_api", *_snake_case_parts(remaining))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "extract"]:
        return ("extract", *_snake_case_parts(package_parts[4:]))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "task"]:
        return ("task", *_snake_case_parts(package_parts[4:]))
    if package_parts[:4] == ["org", "apache", "dolphinscheduler", "remote"]:
        return ("remote", *_snake_case_parts(package_parts[4:]))
    if package_parts[:4] == ["com", "baomidou", "mybatisplus", "annotation"]:
        return ("external", "mybatisplus", "annotation")
    if package_parts[:2] == ["org", "springframework"]:
        return (
            "external",
            "springframework",
            *_snake_case_parts(package_parts[2:]),
        )
    package_path = ".".join(package_parts)
    message = f"Unsupported package mapping for {package_path}"
    raise ValueError(message)


def _snake_case_parts(parts: list[str]) -> tuple[str, ...]:
    return tuple(snake_case(part) for part in parts)


def _generated_view_module_name(logical_name: str) -> str:
    owner = logical_name.split("_", 1)[0]
    for suffix in ("V2Controller", "Controller", "ServiceImpl"):
        if owner.endswith(suffix):
            owner = owner[: -len(suffix)]
            break
    if not owner:
        owner = logical_name
    return snake_case(owner)


def _generated_view_class_name(
    logical_name: str,
    fields: list[DtoFieldSpec],
) -> str:
    owner, _, remainder = logical_name.partition("_")
    owner_base = owner
    for suffix in ("V2Controller", "Controller", "ServiceImpl"):
        if owner_base.endswith(suffix):
            owner_base = owner_base[: -len(suffix)]
            break
    owner_name = python_class_name(owner_base or owner)
    field_names = [field.wire_name for field in fields]

    named_patterns = {
        "queryWorkflowDefinitionSimpleList_arrayNodeItem": (
            "WorkflowDefinitionSimpleItem"
        ),
        "viewVariables_resultMap": f"{owner_name}VariablesView",
        "getLocalParams_localUserDefParamsValue": f"{owner_name}LocalParamsEntry",
        "queryTaskListByWorkflowInstanceId_resultMap": f"{owner_name}TaskListView",
        "queryParentInstanceBySubId_dataMap": f"{owner_name}ParentInstanceView",
        "querySubWorkflowInstanceByTaskId_dataMap": (
            f"{owner_name}SubWorkflowInstanceView"
        ),
        "queryWorkFlowLineage_result": "WorkflowLineageResult",
        "queryWorkFlowLineageByCode_result": "WorkflowLineageByCodeResult",
        "queryDependentTasks_result": "WorkflowLineageDependentTasksResult",
        "batchActivateUser_res": "UsersBatchActivateResult",
        "batchActivateUser_resValue": "UsersBatchActivateSuccess",
        "batchActivateUser_resValue_2": "UsersBatchActivateFailed",
        "grantDataSource_result": "UsersGrantDataSourceResult",
        "grantNamespaces_result": "UsersGrantNamespacesResult",
        "grantProjectByCode_result": "UsersGrantProjectByCodeResult",
        "grantProjectWithReadPerm_result": "UsersGrantProjectWithReadPermResult",
        "grantProject_result": "UsersGrantProjectResult",
        "revokeProjectById_result": "UsersRevokeProjectByIdResult",
        "revokeProject_result": "UsersRevokeProjectResult",
        "createDagDefine_result": "WorkflowDefinitionCreateResult",
        "updateDagDefine_result": "WorkflowDefinitionUpdateResult",
        "ParamsOptions_created": f"{owner_name}Option",
        "Queue_created": "QueueView",
        "Queue_created_2": "QueueRecord",
        "Tenant_created": "TenantView",
        "WorkflowTaskRelation_created": "WorkflowTaskRelationPayload",
        "PageInfo_created": f"{owner_name}PageWindow",
    }
    if remainder in named_patterns:
        return named_patterns[remainder]
    if remainder.endswith("_result") and field_names == ["data"]:
        action_name = python_class_name(remainder[: -len("_result")])
        action_name = action_name.removeprefix(owner_name)
        return f"{owner_name}{action_name}Result"
    return python_class_name(logical_name)


def _specialized_class_name(java_type: str) -> str:
    generic_base = generic_base_type(java_type)
    name_parts = [_specialized_reference_name_part(generic_base)]
    name_parts.extend(
        _specialized_type_name_part(generic_arg)
        for generic_arg in generic_inner_types(java_type)
    )
    return "".join(name_parts)


def _specialized_type_name_part(java_type: str) -> str:
    if java_type.endswith("[]"):
        return _specialized_type_name_part(java_type[:-2]) + "List"
    if "<" not in java_type or not java_type.endswith(">"):
        return _specialized_reference_name_part(java_type)
    generic_base = generic_base_type(java_type)
    parts = [_specialized_reference_name_part(generic_base)]
    parts.extend(
        _specialized_type_name_part(item) for item in generic_inner_types(java_type)
    )
    return "".join(parts)


def _specialized_reference_name_part(reference_name: str) -> str:
    """Return the declaration name without embedding its Java package."""
    _, type_parts = _split_import_path(reference_name)
    return python_class_name(".".join(type_parts))


def _specialized_class_names(
    specializations: dict[str, _ResolvedSpecialization],
    *,
    assignments_by_import_path: dict[str, AssignedType],
) -> dict[str, str]:
    """Allocate deterministic short specialization names within each module."""
    grouped: dict[tuple[tuple[str, ...], str], list[str]] = defaultdict(list)
    for java_type, specialization in specializations.items():
        module_parts = assignments_by_import_path[
            specialization.base_import_path
        ].module_parts
        short_name = _specialized_class_name(java_type)
        grouped[(module_parts, short_name)].append(java_type)

    resolved: dict[str, str] = {}
    occupied_by_module: dict[tuple[str, ...], set[str]] = defaultdict(set)
    for assignment in assignments_by_import_path.values():
        occupied_by_module[assignment.module_parts].add(assignment.class_name)
    collision_groups: list[tuple[tuple[str, ...], str, list[str]]] = []
    for (module_parts, short_name), java_types in sorted(grouped.items()):
        ordered_java_types = sorted(java_types)
        if (
            len(ordered_java_types) == 1
            and short_name not in occupied_by_module[module_parts]
        ):
            resolved[ordered_java_types[0]] = short_name
            occupied_by_module[module_parts].add(short_name)
        else:
            collision_groups.append((module_parts, short_name, ordered_java_types))

    for module_parts, short_name, java_types in collision_groups:
        occupied = occupied_by_module[module_parts]
        candidates = _shortest_package_qualified_names(
            short_name,
            java_types,
            occupied=occupied,
        )
        if candidates is None:
            candidates = _hashed_specialization_names(
                short_name,
                java_types,
                occupied=occupied,
            )
        resolved.update(candidates)
        occupied.update(candidates.values())

    return resolved


def _shortest_package_qualified_names(
    short_name: str,
    java_types: list[str],
    *,
    occupied: set[str],
) -> dict[str, str] | None:
    references_by_type = {
        java_type: _fully_qualified_references(java_type) for java_type in java_types
    }
    common_references = set.intersection(
        *(set(references) for references in references_by_type.values())
    )
    differing_packages = {
        java_type: [
            _split_import_path(reference)[0]
            for reference in references
            if reference not in common_references
        ]
        for java_type, references in references_by_type.items()
    }
    max_depth = max(
        (
            len(package_parts)
            for packages in differing_packages.values()
            for package_parts in packages
        ),
        default=0,
    )
    for depth in range(1, max_depth + 1):
        candidates = {
            java_type: short_name
            + "".join(
                python_class_name(".".join(package_parts[-depth:]))
                for package_parts in packages
            )
            for java_type, packages in differing_packages.items()
        }
        if (
            len(set(candidates.values())) == len(java_types)
            and not set(candidates.values()) & occupied
        ):
            return candidates
    return None


def _fully_qualified_references(java_type: str) -> list[str]:
    references: list[str] = []
    pending = [java_type]
    while pending:
        current = pending.pop()
        if current.endswith("[]"):
            pending.append(current[:-2])
            continue
        base = generic_base_type(current)
        package_parts, _ = _split_import_path(base)
        if package_parts:
            references.append(base)
        pending.extend(generic_inner_types(current))
    return sorted(set(references))


def _hashed_specialization_names(
    short_name: str,
    java_types: list[str],
    *,
    occupied: set[str],
) -> dict[str, str]:
    digests = {
        java_type: sha256(java_type.encode("utf-8")).hexdigest()
        for java_type in java_types
    }
    for length in range(8, 65, 4):
        candidates = {
            java_type: f"{short_name}H{digest[:length]}"
            for java_type, digest in digests.items()
        }
        if (
            len(set(candidates.values())) == len(java_types)
            and not set(candidates.values()) & occupied
        ):
            return candidates
    message = f"could not allocate unique specialization names for {java_types!r}"
    raise ValueError(message)
