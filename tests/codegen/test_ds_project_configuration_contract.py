from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _codegen_module(name: str) -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


_COMPATIBILITY_IMPACT = _codegen_module("ds_codegen.compatibility_impact")
_RETURN_TYPE_RESOLUTION = _codegen_module("ds_codegen.extract.return_type_resolution")
RUNTIME_OPERATION_BINDINGS = _COMPATIBILITY_IMPACT.RUNTIME_OPERATION_BINDINGS
TARGET_DS_VERSIONS = _COMPATIBILITY_IMPACT.REVIEWED_DS_VERSIONS
validate_reviewed_bindings = _COMPATIBILITY_IMPACT.validate_reviewed_bindings
LogicalReturnTypeOverrideScope = _RETURN_TYPE_RESOLUTION.LogicalReturnTypeOverrideScope

_PARAMETER_VERSIONS = frozenset(TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("3.2.0") :])
_WORKER_GROUP_VERSIONS = frozenset(
    TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("3.2.2") :]
)


def test_project_configuration_bindings_have_exact_support_boundaries() -> None:
    for version in TARGET_DS_VERSIONS:
        bindings = RUNTIME_OPERATION_BINDINGS[version]
        parameter_operations = {
            item for item in bindings if item.startswith("project-parameter.")
        }
        preference_operations = {
            item for item in bindings if item.startswith("project-preference.")
        }
        worker_group_operations = {
            item for item in bindings if item.startswith("project-worker-group.")
        }
        assert bool(parameter_operations) is (version in _PARAMETER_VERSIONS)
        assert bool(preference_operations) is (version in _PARAMETER_VERSIONS)
        assert bool(worker_group_operations) is (version in _WORKER_GROUP_VERSIONS)


def test_project_configuration_bindings_are_fail_closed_and_complete() -> None:
    selected = {
        version: {
            operation: binding
            for operation, binding in bindings.items()
            if operation.startswith(
                (
                    "project-parameter.",
                    "project-preference.",
                    "project-worker-group.",
                )
            )
        }
        for version, bindings in RUNTIME_OPERATION_BINDINGS.items()
        if any(operation.startswith("project-") for operation in bindings)
    }
    validate_reviewed_bindings(selected)
    assert all(
        binding.source_operations[0] == "ProjectController.queryProjectListPaging"
        for bindings in selected.values()
        for binding in bindings.values()
    )


@pytest.mark.parametrize("version", sorted(_PARAMETER_VERSIONS))
def test_project_preference_nullability_override_is_exact_version_scoped(
    version: str,
) -> None:
    scope = LogicalReturnTypeOverrideScope(version)
    assert (
        scope.resolve(
            operation_id=(
                "ProjectPreferenceController.queryProjectPreferenceByProjectCode"
            ),
            repo_root=Path(),
            raw_return_type="Result",
            inferred_return_type="ProjectPreference",
            import_map={},
        )
        == "Optional<ProjectPreference>"
    )


def test_342_project_worker_group_override_removes_double_data_projection() -> None:
    scope = LogicalReturnTypeOverrideScope("3.4.2")
    assert (
        scope.resolve(
            operation_id=("ProjectWorkerGroupController.queryAssignedWorkerGroups"),
            repo_root=Path(),
            raw_return_type="Map<String, Object>",
            inferred_return_type=(
                "ProjectWorkerGroupController_queryAssignedWorkerGroups_result"
            ),
            import_map={},
        )
        == "List<ProjectWorkerGroup>"
    )
