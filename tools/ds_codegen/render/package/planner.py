"""Stable planning helpers for generated package layout and type naming."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.render.requests_client import _collect_specialized_model_types
from ds_codegen.render.requests_example import (
    _generic_base_type,
    _generic_inner_types,
    _RenderContext,
    _snake_case,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot, DtoFieldSpec


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


@dataclass(frozen=True)
class PackageRenderContext:
    repo_root: Path
    snapshot: ContractSnapshot
    assignments_by_import_path: dict[str, AssignedType]
    import_paths_by_name: dict[str, set[str]]
    specialized_by_java_type: dict[str, SpecializedModel]
    parse_cache: dict[
        str,
        tuple[object, object, dict[str, str], str | None] | None,
    ]


def build_package_context(
    repo_root: Path,
    snapshot: ContractSnapshot,
) -> PackageRenderContext:
    """Plan stable module/class assignments before rendering any file content."""
    assignments_by_import_path: dict[str, AssignedType] = {}
    import_paths_by_name: dict[str, set[str]] = defaultdict(set)

    for enum_spec in snapshot.enums:
        assignment = _assign_spec_module(enum_spec.import_path)
        assignments_by_import_path[enum_spec.import_path] = assignment
        import_paths_by_name[enum_spec.name].add(enum_spec.import_path)

    for dto_spec in snapshot.dtos:
        assignment = _assign_spec_module(dto_spec.import_path)
        assignments_by_import_path[dto_spec.import_path] = assignment
        import_paths_by_name[dto_spec.name].add(dto_spec.import_path)

    for model_spec in snapshot.models:
        assignment = _assign_spec_module(
            model_spec.import_path,
            fields=model_spec.fields,
        )
        assignments_by_import_path[model_spec.import_path] = assignment
        import_paths_by_name[model_spec.name].add(model_spec.import_path)

    assignments_by_import_path = _dedupe_assigned_type_names(assignments_by_import_path)
    assignments_by_import_path = _dedupe_module_package_collisions(
        assignments_by_import_path
    )

    name_context = _RenderContext(
        dtos_by_name={dto.name: dto for dto in snapshot.dtos},
        models_by_name={model.name: model for model in snapshot.models},
        enums_by_name={enum_spec.name: enum_spec for enum_spec in snapshot.enums},
    )
    specialized_model_types = _collect_specialized_model_types(
        snapshot,
        name_context,
        set(name_context.dtos_by_name),
        set(name_context.models_by_name),
    )
    specialized_by_java_type: dict[str, SpecializedModel] = {}
    for specialized_java_type in sorted(specialized_model_types):
        base_java_type = _generic_base_type(specialized_java_type)
        base_candidates = import_paths_by_name.get(base_java_type)
        if not base_candidates:
            continue
        base_import_path = sorted(base_candidates)[0]
        base_assignment = assignments_by_import_path[base_import_path]
        specialized_by_java_type[specialized_java_type] = SpecializedModel(
            java_type=specialized_java_type,
            class_name=_specialized_class_name(specialized_java_type),
            base_import_path=base_import_path,
            module_parts=base_assignment.module_parts,
        )

    return PackageRenderContext(
        repo_root=repo_root,
        snapshot=snapshot,
        assignments_by_import_path=assignments_by_import_path,
        import_paths_by_name=dict(import_paths_by_name),
        specialized_by_java_type=specialized_by_java_type,
        parse_cache={},
    )


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
    module_parts = (*_map_package_parts(package_parts), _snake_case(type_parts[0]))
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
        return _snake_case(nested_view_parts[-1])
    type_name = type_parts[0].removesuffix("VO")
    return _snake_case(type_name)


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
        if seen_names[assignment.class_name] != import_path:
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
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "dao", "entity"]:
        return ("dao", "entities", *_snake_case_parts(package_parts[5:]))
    if package_parts[:5] == ["org", "apache", "dolphinscheduler", "dao", "model"]:
        return ("dao", "model", *_snake_case_parts(package_parts[5:]))
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
        plugin_name = _snake_case(package_parts[4])
        remaining = package_parts[5:]
        if remaining and remaining[0] == "api":
            remaining = remaining[1:]
        return ("plugin", f"{plugin_name}_api", *_snake_case_parts(remaining))
    if package_parts[:4] == ["com", "baomidou", "mybatisplus", "annotation"]:
        return ("external", "mybatisplus", "annotation")
    package_path = ".".join(package_parts)
    message = f"Unsupported package mapping for {package_path}"
    raise ValueError(message)


def _snake_case_parts(parts: list[str]) -> tuple[str, ...]:
    return tuple(_snake_case(part) for part in parts)


def _generated_view_module_name(logical_name: str) -> str:
    owner = logical_name.split("_", 1)[0]
    for suffix in ("V2Controller", "Controller", "ServiceImpl"):
        if owner.endswith(suffix):
            owner = owner[: -len(suffix)]
            break
    if not owner:
        owner = logical_name
    return _snake_case(owner)


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
        "insertSchedule_result": "ScheduleInsertResult",
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
    generic_base = _generic_base_type(java_type)
    name_parts = [python_class_name(generic_base)]
    name_parts.extend(
        _specialized_type_name_part(generic_arg)
        for generic_arg in _generic_inner_types(java_type)
    )
    return "".join(name_parts)


def _specialized_type_name_part(java_type: str) -> str:
    if java_type.endswith("[]"):
        return _specialized_type_name_part(java_type[:-2]) + "List"
    if "<" not in java_type or not java_type.endswith(">"):
        return python_class_name(java_type)
    generic_base = _generic_base_type(java_type)
    parts = [python_class_name(generic_base)]
    parts.extend(
        _specialized_type_name_part(item) for item in _generic_inner_types(java_type)
    )
    return "".join(parts)
