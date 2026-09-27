from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _cleanup_contract() -> Any:
    tools_dir = _REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.task_definition_cleanup_contract")


def _runtime_contract() -> Any:
    _cleanup_contract()
    return importlib.import_module("ds_codegen.runtime_contract")


def test_cleanup_contract_is_auxiliary_only_for_exact_orphaning_releases() -> None:
    contract = _cleanup_contract()

    assert contract.TASK_DEFINITION_CLEANUP_VERSIONS == (
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


def test_319_cleanup_contract_is_workflow_cascade_proof_only() -> None:
    contract = _cleanup_contract()

    recipe = contract.task_definition_cleanup_contract("3.1.9")

    assert recipe.strategy == "workflow-cascade-proof-only"
    assert recipe.page_params_epoch == "execute-type"
    assert recipe.execute_types == ("BATCH", "STREAM")
    assert recipe.workflow_binding_fields == (
        "processDefinitionCode",
        "processDefinitionVersion",
        "processDefinitionName",
        "processReleaseState",
    )
    assert recipe.history_model == (
        "org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog"
    )
    assert recipe.delete_response == "unavailable"
    assert contract.cleanup_source_operations("3.1.9") == (
        "TaskDefinitionController.queryTaskDefinitionListPaging",
        "TaskDefinitionController.queryTaskDefinitionDetail",
        "TaskDefinitionController.queryTaskDefinitionVersions",
    )


def test_cleanup_contract_records_exact_page_and_nullable_delete_epochs() -> None:
    contract = _cleanup_contract()

    legacy = contract.task_definition_cleanup_contract("2.0.0")
    transitional = contract.task_definition_cleanup_contract("2.0.9")
    joined = contract.task_definition_cleanup_contract("3.0.0")
    execute_typed = contract.task_definition_cleanup_contract("3.1.0")

    assert (
        legacy.page_model,
        legacy.page_params_epoch,
        legacy.row_fields,
        legacy.execute_types,
        legacy.delete_response,
    ) == (
        "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
        "task-search",
        ("code", "name", "version"),
        (),
        "void",
    )
    assert transitional.delete_response == "optional-process-definition"
    assert legacy.pre_delete_release == "none"
    legacy_201 = contract.task_definition_cleanup_contract("2.0.1")
    assert legacy_201.pre_delete_release == "offline"
    assert not legacy_201.full_core_applicable
    assert not legacy_201.full_core_reconciliation_applicable
    assert not legacy_201.cross_process_recovery
    assert contract.task_definition_cleanup_contract("2.0.2").pre_delete_release == (
        "offline"
    )
    assert contract.task_definition_cleanup_contract("2.0.3").pre_delete_release == (
        "offline"
    )
    assert contract.task_definition_cleanup_contract("2.0.4").pre_delete_release == (
        "none"
    )
    assert (
        joined.page_model,
        joined.page_params_epoch,
        joined.row_fields,
        joined.execute_types,
    ) == (
        "org.apache.dolphinscheduler.dao.entity.TaskMainInfo",
        "workflow-task-search",
        ("taskCode", "taskName", "taskVersion"),
        (),
    )
    assert execute_typed.page_params_epoch == "execute-type"
    assert execute_typed.execute_types == ("BATCH", "STREAM")
    with pytest.raises(ValueError, match="no auxiliary task-definition cleanup"):
        contract.task_definition_cleanup_contract("3.2.0")


def test_cleanup_contract_exposes_one_exact_generated_auxiliary_closure() -> None:
    contract = _cleanup_contract()

    assert contract.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION == (
        "release-gate.task-definition.cleanup"
    )
    for ds_version in contract.TASK_DEFINITION_CLEANUP_VERSIONS:
        recipe = contract.task_definition_cleanup_contract(ds_version)
        if recipe.strategy == "workflow-cascade-proof-only":
            expected_operations = [
                "TaskDefinitionController.queryTaskDefinitionListPaging",
                "TaskDefinitionController.queryTaskDefinitionDetail",
                "TaskDefinitionController.queryTaskDefinitionVersions",
            ]
        else:
            expected_operations = [
                "TaskDefinitionController.queryTaskDefinitionListPaging",
                "TaskDefinitionController.queryTaskDefinitionDetail",
            ]
            if recipe.pre_delete_release == "offline":
                expected_operations.extend(
                    (
                        "ProcessDefinitionController.queryProcessDefinitionListPaging",
                        "TaskDefinitionController.releaseTaskDefinition",
                    )
                )
            expected_operations.append(
                "TaskDefinitionController.deleteTaskDefinitionByCode"
            )
        assert contract.cleanup_source_operations(ds_version) == tuple(
            expected_operations
        )
        expected_roots = {
            "org.apache.dolphinscheduler.api.utils.Result",
            "org.apache.dolphinscheduler.api.utils.PageInfo",
            "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
            recipe.page_model,
        }
        if recipe.delete_response == "optional-process-definition":
            expected_roots.add(
                "org.apache.dolphinscheduler.dao.entity.ProcessDefinition"
            )
        if recipe.pre_delete_release == "offline":
            expected_roots.add(
                "org.apache.dolphinscheduler.dao.entity.ProcessDefinition"
            )
        if recipe.history_model is not None:
            expected_roots.add(recipe.history_model)
        assert set(contract.cleanup_type_roots(ds_version)) == expected_roots


def test_cleanup_contract_owns_exact_wire_integer_fields() -> None:
    contract = _cleanup_contract()
    expected = {
        "org.apache.dolphinscheduler.api.utils.PageInfo": (
            "total",
            "totalPage",
            "pageSize",
            "currentPage",
            "pageNo",
        ),
        "org.apache.dolphinscheduler.dao.entity.TaskDefinition": (
            "code",
            "version",
            "projectCode",
        ),
        "org.apache.dolphinscheduler.dao.entity.TaskMainInfo": (
            "taskCode",
            "taskVersion",
        ),
    }

    for ds_version in contract.TASK_DEFINITION_CLEANUP_VERSIONS:
        projected = contract.cleanup_strict_integer_fields(ds_version)
        if ds_version in {
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.8",
            "3.1.9",
        }:
            assert projected == {
                **expected,
                "org.apache.dolphinscheduler.dao.entity.TaskMainInfo": (
                    "taskCode",
                    "taskVersion",
                    "processDefinitionCode",
                    "processDefinitionVersion",
                ),
            }
        else:
            assert projected == expected
    assert contract.cleanup_strict_integer_fields("3.2.0") == {}


def test_cleanup_profile_projects_exact_runtime_invocation_facts() -> None:
    contract = _cleanup_contract()

    data = contract.task_definition_cleanup_profile_data()

    assert data["schema_version"] == 4
    assert data["semantic_operation"] == ("release-gate.task-definition.cleanup")
    assert data["target_versions"] == list(contract.TASK_DEFINITION_CLEANUP_VERSIONS)
    assert data["full_core_reconciliation_versions"] == [
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
    ]
    assert data["full_core_versions"] == [
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
    ]
    assert data["cross_process_recovery_versions"] == [
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    ]
    profiles = data["profiles"]
    assert profiles["2.0.0"] == {
        "strategy": "direct-delete",
        "page_params_epoch": "task-search",
        "page_model": "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
        "row_fields": ["code", "name", "version"],
        "execute_types": [],
        "workflow_binding_fields": None,
        "history_model": None,
        "delete_response": "void",
        "pre_delete_release": "none",
    }
    assert profiles["2.0.2"]["pre_delete_release"] == "offline"
    assert profiles["2.0.1"]["pre_delete_release"] == "offline"
    assert profiles["2.0.3"]["pre_delete_release"] == "offline"
    assert profiles["2.0.4"]["pre_delete_release"] == "none"
    assert profiles["3.1.0"]["execute_types"] == ["BATCH", "STREAM"]
    assert profiles["3.1.9"] == {
        "strategy": "workflow-cascade-proof-only",
        "page_params_epoch": "execute-type",
        "page_model": "org.apache.dolphinscheduler.dao.entity.TaskMainInfo",
        "row_fields": ["taskCode", "taskName", "taskVersion"],
        "execute_types": ["BATCH", "STREAM"],
        "workflow_binding_fields": [
            "processDefinitionCode",
            "processDefinitionVersion",
            "processDefinitionName",
            "processReleaseState",
        ],
        "history_model": ("org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog"),
        "delete_response": "unavailable",
        "pre_delete_release": "none",
    }
    rendered = contract.render_task_definition_cleanup_profiles(data)
    compile(rendered, "task_definition_cleanup_profiles.py", "exec")
    assert "TASK_DEFINITION_CLEANUP_PROFILES" in rendered
    assert "TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION" in rendered
    assert "FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS" in rendered
    assert "FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS" in rendered
    assert "CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS" in rendered


def test_cleanup_root_is_generated_but_never_enters_stable_runtime_bindings() -> None:
    cleanup = _cleanup_contract()
    runtime = _runtime_contract()
    operation = cleanup.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION

    for ds_version in cleanup.TASK_DEFINITION_CLEANUP_VERSIONS:
        auxiliary = runtime.runtime_auxiliary_operation_bindings(ds_version)
        assert set(auxiliary) == {operation}
        assert [
            evidence.reference
            for evidence in auxiliary[operation].evidence_sources
            if evidence.kind == "controller"
        ] == list(cleanup.cleanup_source_operations(ds_version))
        assert operation in runtime.runtime_semantic_operations(ds_version)
        assert operation not in runtime.runtime_operation_bindings(ds_version)
    assert runtime.runtime_auxiliary_operation_bindings("1.3.9") == {}
    assert operation not in runtime.runtime_semantic_operations("1.3.9")
    for ds_version in ("3.4.2",):
        assert set(runtime.runtime_auxiliary_operation_bindings(ds_version)) == {
            "workflow.inspect",
            "project-preference.read",
        }
        assert operation not in runtime.runtime_semantic_operations(ds_version)
