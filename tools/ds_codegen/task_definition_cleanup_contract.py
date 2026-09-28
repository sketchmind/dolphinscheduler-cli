"""Reviewed auxiliary task-definition cleanup contracts for release gates.

This module is intentionally outside the stable task semantic surface. It
selects only exact releases that require private task-definition reconciliation
around release-gate workflow deletion, whether by direct deletion or by proving
the exact workflow-cascade lineage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ds_codegen.profile_ledger import exact_versions

TASK_DEFINITION_CLEANUP_VERSIONS = (
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
)
TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION = "release-gate.task-definition.cleanup"
TASK_DEFINITION_CLEANUP_PROFILE_SCHEMA_VERSION = 4
GENERATED_TASK_DEFINITION_CLEANUP_PROFILE_PATH = Path(
    "generated/task_definition_cleanup_profiles.py"
)

_PAGE_OPERATION = "TaskDefinitionController.queryTaskDefinitionListPaging"
_DETAIL_OPERATION = "TaskDefinitionController.queryTaskDefinitionDetail"
_DELETE_OPERATION = "TaskDefinitionController.deleteTaskDefinitionByCode"
_RELEASE_OPERATION = "TaskDefinitionController.releaseTaskDefinition"
_WORKFLOW_PAGE_OPERATION = (
    "ProcessDefinitionController.queryProcessDefinitionListPaging"
)
_HISTORY_OPERATION = "TaskDefinitionController.queryTaskDefinitionVersions"
_RESULT_MODEL = "org.apache.dolphinscheduler.api.utils.Result"
_PAGE_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_PROCESS_DEFINITION = "org.apache.dolphinscheduler.dao.entity.ProcessDefinition"
_TASK_DEFINITION = "org.apache.dolphinscheduler.dao.entity.TaskDefinition"
_TASK_DEFINITION_LOG = "org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog"
_TASK_MAIN_INFO = "org.apache.dolphinscheduler.dao.entity.TaskMainInfo"
_STRICT_INTEGER_FIELDS = {
    _PAGE_MODEL: ("total", "totalPage", "pageSize", "currentPage", "pageNo"),
    _TASK_DEFINITION: ("code", "version", "projectCode"),
    _TASK_MAIN_INFO: ("taskCode", "taskVersion"),
}

PageParamsEpoch = Literal["task-search", "workflow-task-search", "execute-type"]
CleanupStrategy = Literal["direct-delete", "workflow-cascade-proof-only"]
DeleteResponse = Literal["void", "optional-process-definition", "unavailable"]
PreDeleteRelease = Literal["none", "offline"]


@dataclass(frozen=True)
class TaskDefinitionCleanupContract:
    """One exact auxiliary page/detail/delete-or-history recipe."""

    version: str
    strategy: CleanupStrategy
    page_model: str
    page_params_epoch: PageParamsEpoch
    row_fields: tuple[str, str, str]
    execute_types: tuple[str, ...]
    workflow_binding_fields: tuple[str, str, str, str] | None
    history_model: str | None
    delete_response: DeleteResponse
    pre_delete_release: PreDeleteRelease
    full_core_applicable: bool
    full_core_reconciliation_applicable: bool
    cross_process_recovery: bool


_LEGACY = TaskDefinitionCleanupContract(
    version="2.0.0",
    strategy="direct-delete",
    page_model=_TASK_DEFINITION,
    page_params_epoch="task-search",
    row_fields=("code", "name", "version"),
    execute_types=(),
    workflow_binding_fields=None,
    history_model=None,
    delete_response="void",
    pre_delete_release="none",
    full_core_applicable=True,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=True,
)
_TRANSITIONAL = TaskDefinitionCleanupContract(
    version="2.0.9",
    strategy="direct-delete",
    page_model=_TASK_DEFINITION,
    page_params_epoch="task-search",
    row_fields=("code", "name", "version"),
    execute_types=(),
    workflow_binding_fields=None,
    history_model=None,
    delete_response="optional-process-definition",
    pre_delete_release="none",
    full_core_applicable=True,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=True,
)
_JOINED_300 = TaskDefinitionCleanupContract(
    version="3.0.0",
    strategy="direct-delete",
    page_model=_TASK_MAIN_INFO,
    page_params_epoch="workflow-task-search",
    row_fields=("taskCode", "taskName", "taskVersion"),
    execute_types=(),
    workflow_binding_fields=None,
    history_model=None,
    delete_response="optional-process-definition",
    pre_delete_release="none",
    full_core_applicable=True,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=False,
)
_JOINED_306 = TaskDefinitionCleanupContract(
    version="3.0.6",
    strategy="direct-delete",
    page_model=_TASK_MAIN_INFO,
    page_params_epoch="workflow-task-search",
    row_fields=("taskCode", "taskName", "taskVersion"),
    execute_types=(),
    workflow_binding_fields=None,
    history_model=None,
    delete_response="optional-process-definition",
    pre_delete_release="none",
    full_core_applicable=True,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=False,
)
_EXECUTE_TYPED = TaskDefinitionCleanupContract(
    version="3.1.0",
    strategy="direct-delete",
    page_model=_TASK_MAIN_INFO,
    page_params_epoch="execute-type",
    row_fields=("taskCode", "taskName", "taskVersion"),
    execute_types=("BATCH", "STREAM"),
    workflow_binding_fields=None,
    history_model=None,
    delete_response="optional-process-definition",
    pre_delete_release="none",
    full_core_applicable=True,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=False,
)
_WORKFLOW_CASCADE_PROOF_319 = TaskDefinitionCleanupContract(
    version="3.1.9",
    strategy="workflow-cascade-proof-only",
    page_model=_TASK_MAIN_INFO,
    page_params_epoch="execute-type",
    row_fields=("taskCode", "taskName", "taskVersion"),
    execute_types=("BATCH", "STREAM"),
    workflow_binding_fields=(
        "processDefinitionCode",
        "processDefinitionVersion",
        "processDefinitionName",
        "processReleaseState",
    ),
    history_model=_TASK_DEFINITION_LOG,
    delete_response="unavailable",
    pre_delete_release="none",
    full_core_applicable=False,
    full_core_reconciliation_applicable=True,
    cross_process_recovery=True,
)
_CONTRACTS = {
    "2.0.0": _LEGACY,
    "2.0.1": replace(
        _LEGACY,
        version="2.0.1",
        pre_delete_release="offline",
    ),
    "2.0.2": replace(
        _TRANSITIONAL,
        version="2.0.2",
        delete_response="void",
        pre_delete_release="offline",
    ),
    "2.0.3": replace(
        _TRANSITIONAL,
        version="2.0.3",
        delete_response="void",
        pre_delete_release="offline",
    ),
    "2.0.4": replace(_TRANSITIONAL, version="2.0.4"),
    "2.0.5": replace(_TRANSITIONAL, version="2.0.5"),
    "2.0.6": replace(_TRANSITIONAL, version="2.0.6"),
    "2.0.7": replace(_TRANSITIONAL, version="2.0.7"),
    "2.0.8": replace(_TRANSITIONAL, version="2.0.8"),
    "2.0.9": _TRANSITIONAL,
    "3.0.0": _JOINED_300,
    "3.0.1": replace(_JOINED_300, version="3.0.1"),
    "3.0.2": replace(_JOINED_306, version="3.0.2"),
    "3.0.3": replace(_JOINED_306, version="3.0.3"),
    "3.0.4": replace(_JOINED_306, version="3.0.4"),
    "3.0.5": replace(_JOINED_306, version="3.0.5"),
    "3.0.6": _JOINED_306,
    "3.1.0": _EXECUTE_TYPED,
    "3.1.1": replace(_EXECUTE_TYPED, version="3.1.1"),
    "3.1.2": replace(_EXECUTE_TYPED, version="3.1.2"),
    "3.1.3": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.3"),
    "3.1.4": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.4"),
    "3.1.5": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.5"),
    "3.1.6": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.6"),
    "3.1.7": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.7"),
    "3.1.8": replace(_WORKFLOW_CASCADE_PROOF_319, version="3.1.8"),
    "3.1.9": _WORKFLOW_CASCADE_PROOF_319,
}
FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS = tuple(
    version
    for version in TASK_DEFINITION_CLEANUP_VERSIONS
    if _CONTRACTS[version].full_core_applicable
)
FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS = tuple(
    version
    for version in TASK_DEFINITION_CLEANUP_VERSIONS
    if _CONTRACTS[version].full_core_reconciliation_applicable
)
CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS = tuple(
    version
    for version in TASK_DEFINITION_CLEANUP_VERSIONS
    if _CONTRACTS[version].cross_process_recovery
)


def task_definition_cleanup_contract(version: str) -> TaskDefinitionCleanupContract:
    """Return one reviewed auxiliary recipe without version inheritance."""
    try:
        return _CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no auxiliary task-definition cleanup contract"
        raise ValueError(message) from exc


def cleanup_source_operations(version: str) -> tuple[str, ...]:
    """Return the exact generated operation closure for cleanup."""
    recipe = task_definition_cleanup_contract(version)
    if recipe.strategy == "workflow-cascade-proof-only":
        return (_PAGE_OPERATION, _DETAIL_OPERATION, _HISTORY_OPERATION)
    operations = [_PAGE_OPERATION, _DETAIL_OPERATION]
    if recipe.pre_delete_release == "offline":
        operations.extend((_WORKFLOW_PAGE_OPERATION, _RELEASE_OPERATION))
    operations.append(_DELETE_OPERATION)
    return tuple(operations)


def cleanup_type_roots(version: str) -> tuple[str, ...]:
    """Return explicit response roots needed by the auxiliary slice."""
    recipe = task_definition_cleanup_contract(version)
    roots = {
        _RESULT_MODEL,
        _PAGE_MODEL,
        _TASK_DEFINITION,
        recipe.page_model,
    }
    if recipe.delete_response == "optional-process-definition":
        roots.add(_PROCESS_DEFINITION)
    if recipe.pre_delete_release == "offline":
        roots.add(_PROCESS_DEFINITION)
    if recipe.history_model is not None:
        roots.add(recipe.history_model)
    return tuple(sorted(roots))


def cleanup_strict_integer_fields(version: str) -> dict[str, tuple[str, ...]]:
    """Return cleanup-owned response integers that must not be coerced."""
    if version not in TASK_DEFINITION_CLEANUP_VERSIONS:
        return {}
    fields = dict(_STRICT_INTEGER_FIELDS)
    if task_definition_cleanup_contract(version).workflow_binding_fields is not None:
        fields[_TASK_MAIN_INFO] = (
            *fields[_TASK_MAIN_INFO],
            "processDefinitionCode",
            "processDefinitionVersion",
        )
    return fields


def task_definition_cleanup_profile_data(
    versions: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Project exact cleanup facts into the installed runtime seam."""
    # Cleanup is a reviewed auxiliary surface on only some packaged releases.
    selected = (
        TASK_DEFINITION_CLEANUP_VERSIONS
        if versions is None
        else tuple(
            version
            for version in exact_versions(versions, label="selected runtime versions")
            if version in TASK_DEFINITION_CLEANUP_VERSIONS
        )
    )
    return {
        "schema_version": TASK_DEFINITION_CLEANUP_PROFILE_SCHEMA_VERSION,
        "semantic_operation": TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION,
        "target_versions": list(selected),
        "full_core_reconciliation_versions": [
            version
            for version in FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
            if version in selected
        ],
        "full_core_versions": [
            version
            for version in FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS
            if version in selected
        ],
        "cross_process_recovery_versions": [
            version
            for version in CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS
            if version in selected
        ],
        "profiles": {
            version: {
                "strategy": recipe.strategy,
                "page_params_epoch": recipe.page_params_epoch,
                "page_model": recipe.page_model,
                "row_fields": list(recipe.row_fields),
                "execute_types": list(recipe.execute_types),
                "workflow_binding_fields": (
                    list(recipe.workflow_binding_fields)
                    if recipe.workflow_binding_fields is not None
                    else None
                ),
                "history_model": recipe.history_model,
                "delete_response": recipe.delete_response,
                "pre_delete_release": recipe.pre_delete_release,
            }
            for version in selected
            for recipe in (task_definition_cleanup_contract(version),)
        },
    }


def render_task_definition_cleanup_profiles(
    data: dict[str, object] | None = None,
) -> str:
    """Render the generated auxiliary cleanup profile module."""
    projected = task_definition_cleanup_profile_data() if data is None else data
    payload = json.dumps(projected, indent=2, ensure_ascii=True)
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "import json as _json",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "_TASK_DEFINITION_CLEANUP_PROFILE_JSON = r'''",
            payload,
            "'''",
            "TASK_DEFINITION_CLEANUP_PROFILE_DATA = _json.loads(",
            "    _TASK_DEFINITION_CLEANUP_PROFILE_JSON",
            ")",
            "TASK_DEFINITION_CLEANUP_PROFILE_SCHEMA_VERSION = (",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA['schema_version']",
            ")",
            "TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION = (",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA['semantic_operation']",
            ")",
            "TARGET_TASK_DEFINITION_CLEANUP_VERSIONS = tuple(",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA['target_versions']",
            ")",
            "FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS = tuple(",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA[",
            "        'full_core_reconciliation_versions'",
            "    ]",
            ")",
            "FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS = tuple(",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA['full_core_versions']",
            ")",
            "CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS = tuple(",
            "    TASK_DEFINITION_CLEANUP_PROFILE_DATA[",
            "        'cross_process_recovery_versions'",
            "    ]",
            ")",
            "TASK_DEFINITION_CLEANUP_PROFILES = {",
            "    _version: dict(_profile)",
            "    for _version, _profile in (",
            "        TASK_DEFINITION_CLEANUP_PROFILE_DATA['profiles'].items()",
            "    )",
            "}",
            "",
        )
    )


def write_task_definition_cleanup_profiles(
    output_root: Path, *, versions: tuple[str, ...] | None = None
) -> Path:
    """Write the generated auxiliary cleanup profiles."""
    output_path = output_root / GENERATED_TASK_DEFINITION_CLEANUP_PROFILE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_task_definition_cleanup_profiles(
            task_definition_cleanup_profile_data(versions)
        ),
        encoding="utf-8",
    )
    return output_path


__all__ = [
    "CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS",
    "FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS",
    "FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS",
    "TASK_DEFINITION_CLEANUP_PROFILE_SCHEMA_VERSION",
    "TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION",
    "TASK_DEFINITION_CLEANUP_VERSIONS",
    "TaskDefinitionCleanupContract",
    "cleanup_source_operations",
    "cleanup_strict_integer_fields",
    "cleanup_type_roots",
    "render_task_definition_cleanup_profiles",
    "task_definition_cleanup_contract",
    "task_definition_cleanup_profile_data",
    "write_task_definition_cleanup_profiles",
]
