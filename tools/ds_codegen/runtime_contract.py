"""Compile reviewed semantic bindings into minimal generated runtime contracts."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

from ds_codegen.compatibility_impact import (
    READ_OPERATION_BINDINGS,
    RUNTIME_OPERATION_BINDINGS,
    EvidenceSource,
    ReviewedBinding,
    SelectorSemantics,
    WireTypeRef,
    WireTypeSurface,
    validate_reviewed_bindings,
)
from ds_codegen.contract_visibility import (
    has_executable_response_type,
    is_client_supplied_parameter,
)
from ds_codegen.security_contract import (
    security_contract,
    user_identity_read_operations,
)
from ds_codegen.snapshot_resolution import (
    ResolutionScope,
    ScopedTypeUse,
    SnapshotTypeResolver,
)
from ds_codegen.task_definition_cleanup_contract import (
    TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION,
    TASK_DEFINITION_CLEANUP_VERSIONS,
    cleanup_source_operations,
    cleanup_type_roots,
)

_LEGACY_TASK_UI = "dolphinscheduler-ui/src/js/conf/home/store/dag/actions.js"
_TASK_DEFINITION_UI_20 = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/taskDefinition/index.vue"
)
_TASK_DEFINITION_UI = "dolphinscheduler-ui/src/service/modules/task-definition/index.ts"
PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION = "project-preference.read"
PROJECT_PREFERENCE_READ_VERSIONS = frozenset(
    {"3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot


def runtime_semantic_operations(version: str) -> tuple[str, ...]:
    """Return every reviewed operation reachable in one exact runtime package."""
    return tuple(
        sorted(
            {
                *runtime_operation_bindings(version),
                *runtime_auxiliary_operation_bindings(version),
            }
        )
    )


def configured_runtime_contract_slice(
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver | None = None,
) -> ContractSnapshot:
    """Return reviewed operations plus the exact local enum catalog."""
    semantic_operations = runtime_semantic_operations(snapshot.ds_version)
    version_bindings = {
        **runtime_operation_bindings(snapshot.ds_version),
        **runtime_auxiliary_operation_bindings(snapshot.ds_version),
    }
    return slice_contract_for_bindings(
        snapshot,
        {
            semantic_operation: version_bindings[semantic_operation]
            for semantic_operation in semantic_operations
        },
        additional_type_refs={
            _type_ref("enums", enum.import_path) for enum in snapshot.enums
        },
        resolver=resolver,
    )


def runtime_operation_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Return all reviewed bindings compiled into one exact runtime slice."""
    read_bindings = READ_OPERATION_BINDINGS.get(version)
    if read_bindings is None:
        message = f"No reviewed semantic bindings exist for DS {version}"
        raise ValueError(message)
    bindings = {
        **read_bindings,
        **RUNTIME_OPERATION_BINDINGS.get(version, {}),
    }
    validate_reviewed_bindings({version: bindings})
    return bindings


def runtime_auxiliary_operation_bindings(
    version: str,
) -> dict[str, ReviewedBinding]:
    """Return private generated roots that never enter the stable action ledger."""
    bindings: dict[str, ReviewedBinding] = {}
    if security_contract(version).user.simple_user_list:
        identity_sources = user_identity_read_operations(version)
        bindings["user.identity"] = ReviewedBinding(
            source_operations=identity_sources,
            type_closure=(
                WireTypeRef("models", "org.apache.dolphinscheduler.dao.entity.User"),
                WireTypeRef(
                    "models", "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO"
                ),
            ),
            selector_semantics=(
                SelectorSemantics(
                    resource="user",
                    consumed_selectors=("name", "id"),
                    exposed_identities=("name", "id"),
                    native_identity="id",
                    resolution="current-user-or-exact-identity-via-user-simple-list",
                ),
            ),
            evidence_sources=(
                *(EvidenceSource("controller", source) for source in identity_sources),
                EvidenceSource(
                    "ui", "dolphinscheduler-ui/src/service/modules/users/index.ts"
                ),
            ),
        )
    if version in PROJECT_PREFERENCE_READ_VERSIONS:
        source = "ProjectPreferenceController.queryProjectPreferenceByProjectCode"
        bindings[PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION] = ReviewedBinding(
            source_operations=(source,),
            type_closure=(
                WireTypeRef(
                    "models", "org.apache.dolphinscheduler.dao.entity.ProjectPreference"
                ),
            ),
            selector_semantics=(
                SelectorSemantics(
                    resource="project-preference",
                    consumed_selectors=("project_code",),
                    exposed_identities=("project_code",),
                    native_identity="project_code",
                    resolution="singleton-by-resolved-project-code",
                    parent_identity="project_code",
                ),
            ),
            evidence_sources=(
                EvidenceSource("controller", source),
                EvidenceSource(
                    "ui",
                    "dolphinscheduler-ui/src/service/modules/projects-preference/index.ts",
                ),
            ),
        )
    if version == "3.4.2":
        workflow_get = READ_OPERATION_BINDINGS[version]["workflow.get"]
        bindings["workflow.inspect"] = ReviewedBinding(
            source_operations=workflow_get.source_operations,
            type_closure=workflow_get.type_closure,
            selector_semantics=workflow_get.selector_semantics,
            evidence_sources=workflow_get.evidence_sources,
        )
    if version in TASK_DEFINITION_CLEANUP_VERSIONS:
        ui_source = (
            _LEGACY_TASK_UI
            if version in {"2.0.0", "2.0.1"}
            else _TASK_DEFINITION_UI_20
            if version
            in {"2.0.2", "2.0.3", "2.0.4", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9"}
            else _TASK_DEFINITION_UI
        )
        source_operations = cleanup_source_operations(version)
        bindings[TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION] = ReviewedBinding(
            source_operations=source_operations,
            type_closure=tuple(
                WireTypeRef("models", root) for root in cleanup_type_roots(version)
            ),
            selector_semantics=(
                SelectorSemantics(
                    resource="project",
                    consumed_selectors=("code",),
                    exposed_identities=("code",),
                    native_identity="code",
                    resolution="direct-gate-owned-native-code",
                ),
                SelectorSemantics(
                    resource="task",
                    consumed_selectors=("code",),
                    exposed_identities=("name", "code"),
                    native_identity="code",
                    resolution="exhaustive-project-page-plus-direct-detail",
                    parent_identity="project_code",
                ),
            ),
            evidence_sources=(
                *(
                    EvidenceSource("controller", operation_id)
                    for operation_id in source_operations
                ),
                EvidenceSource("ui", ui_source),
            ),
        )
    validate_reviewed_bindings({version: bindings})
    return bindings


def slice_contract_for_bindings(
    snapshot: ContractSnapshot,
    bindings: Mapping[str, ReviewedBinding],
    *,
    additional_type_refs: set[WireTypeRef] | None = None,
    resolver: SnapshotTypeResolver | None = None,
    execution_operation_ids: set[str] | None = None,
    execution_type_refs: set[WireTypeRef] | None = None,
) -> ContractSnapshot:
    """Keep the operation closure plus explicitly requested metadata roots."""
    # A fully compiled runtime has no executable legacy owner. Only an
    # explicitly empty execution projection may retain metadata-only roots.
    if not bindings and (
        execution_operation_ids != set() or execution_type_refs != set()
    ):
        message = "runtime contract slice requires at least one semantic binding"
        raise ValueError(message)
    validate_reviewed_bindings({snapshot.ds_version: bindings})
    active_resolver = resolver or SnapshotTypeResolver.compile(snapshot)

    operation_ids = {
        operation_id
        for binding in bindings.values()
        for operation_id in binding.source_operations
    }
    declared_type_refs = {
        type_ref for binding in bindings.values() for type_ref in binding.type_closure
    }
    reviewed_type_refs = set(declared_type_refs)
    if additional_type_refs is not None:
        declared_type_refs.update(additional_type_refs)
    available_operation_ids = {item.operation_id for item in snapshot.operations}
    available_type_refs = _available_type_refs(snapshot)
    missing_operations = sorted(operation_ids - available_operation_ids)
    missing_types = sorted(
        declared_type_refs - available_type_refs,
        key=lambda item: (item.surface, item.key),
    )
    if missing_operations or missing_types:
        missing = [
            *missing_operations,
            *(f"{item.surface}:{item.key}" for item in missing_types),
        ]
        message = f"runtime contract slice is incomplete: {', '.join(missing)}"
        raise ValueError(message)

    if (execution_operation_ids is None) != (execution_type_refs is None):
        message = "runtime execution projection requires operation and type roots"
        raise ValueError(message)
    if execution_operation_ids is not None and execution_type_refs is not None:
        if (
            not execution_operation_ids <= operation_ids
            or not execution_type_refs <= reviewed_type_refs
        ):
            message = "runtime execution projection exceeds its reviewed source closure"
            raise ValueError(message)
        operation_ids = set(execution_operation_ids)
        declared_type_refs = set(execution_type_refs)
        if additional_type_refs is not None:
            declared_type_refs.update(additional_type_refs)

    type_refs = _derive_type_closure(
        snapshot,
        operation_ids=operation_ids,
        declared_type_refs=declared_type_refs,
        resolver=active_resolver,
    )

    operations = [
        item for item in snapshot.operations if item.operation_id in operation_ids
    ]
    dtos = [
        item
        for item in snapshot.dtos
        if _type_ref("dtos", item.import_path) in type_refs
    ]
    models = [
        item
        for item in snapshot.models
        if _type_ref("models", item.import_path) in type_refs
    ]
    enums = [
        item
        for item in snapshot.enums
        if _type_ref("enums", item.import_path) in type_refs
    ]
    sliced = replace(
        snapshot,
        operation_count=len(operations),
        enum_count=len(enums),
        dto_count=len(dtos),
        model_count=len(models),
        operations=operations,
        enums=enums,
        dtos=dtos,
        models=models,
    )
    SnapshotTypeResolver.compile(sliced)
    return sliced


def _available_type_refs(snapshot: ContractSnapshot) -> set[WireTypeRef]:
    return {
        *(_type_ref("dtos", item.import_path) for item in snapshot.dtos),
        *(_type_ref("models", item.import_path) for item in snapshot.models),
        *(_type_ref("enums", item.import_path) for item in snapshot.enums),
    }


def _derive_type_closure(
    snapshot: ContractSnapshot,
    *,
    operation_ids: set[str],
    declared_type_refs: set[WireTypeRef],
    resolver: SnapshotTypeResolver,
) -> set[WireTypeRef]:
    """Expand reviewed roots through exact operation and structured-type shapes."""
    refs_by_import_path: dict[str, WireTypeRef] = {}
    for surface_name, specs in (
        ("dtos", snapshot.dtos),
        ("models", snapshot.models),
        ("enums", snapshot.enums),
    ):
        surface = cast("WireTypeSurface", surface_name)
        for spec in specs:
            type_ref = _type_ref(surface, spec.import_path)
            refs_by_import_path[spec.import_path] = type_ref

    uses: list[ScopedTypeUse] = []
    for operation in snapshot.operations:
        if operation.operation_id not in operation_ids:
            continue
        if has_executable_response_type(operation):
            uses.append(
                ScopedTypeUse(
                    operation.logical_return_type,
                    ResolutionScope("operation_response", operation.operation_id),
                )
            )
        uses.extend(
            ScopedTypeUse(
                parameter.java_type,
                ResolutionScope("operation_request", operation.operation_id),
            )
            for parameter in operation.parameters
            if is_client_supplied_parameter(parameter)
        )

    type_graph = resolver.resolve_type_graph(
        uses,
        root_import_paths=(type_ref.key for type_ref in declared_type_refs),
    )
    return {refs_by_import_path[import_path] for import_path in type_graph.import_paths}


def _type_ref(surface: WireTypeSurface, key: str) -> WireTypeRef:
    return WireTypeRef(surface, key)


__all__ = [
    "PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION",
    "PROJECT_PREFERENCE_READ_VERSIONS",
    "configured_runtime_contract_slice",
    "runtime_auxiliary_operation_bindings",
    "runtime_operation_bindings",
    "runtime_semantic_operations",
    "slice_contract_for_bindings",
]
