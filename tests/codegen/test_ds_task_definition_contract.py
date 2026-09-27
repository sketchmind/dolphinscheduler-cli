from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _task_contract() -> Any:
    tools_dir = _REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.task_definition_contract")


_CODE_NATIVE_VERSIONS = (
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
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_DEPENDENCE_GETTER_VERSIONS = (
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
    "3.2.0",
    "3.2.1",
    "3.2.2",
)


@pytest.mark.parametrize("ds_version", _CODE_NATIVE_VERSIONS)
@pytest.mark.source_contract
def test_task_contract_closure_exists_in_exact_snapshot(
    ds_version: str,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _task_contract()
    snapshot = exact_contract_corpus.snapshot(ds_version)
    operation_ids = {
        item.operation_id
        for item in snapshot.operations
        if item.operation_id is not None
    }
    model_keys = {item.import_path for item in snapshot.models}

    sources = contract.semantic_operation_sources(ds_version)
    roots = contract.semantic_operation_type_roots(ds_version)
    assert set(sources) == set(contract.TASK_SEMANTIC_OPERATIONS)
    assert set(roots) == set(contract.TASK_SEMANTIC_OPERATIONS)
    assert all(set(operations) <= operation_ids for operations in sources.values())
    assert all(set(type_roots) <= model_keys for type_roots in roots.values())


@pytest.mark.source_contract
def test_139_is_an_evidence_backed_graph_recipe_without_invented_codes(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract("1.3.9").task
    snapshot = exact_contract_corpus.snapshot("1.3.9")
    operation_ids = {
        item.operation_id
        for item in snapshot.operations
        if item.operation_id is not None
    }
    task_node = next(
        item
        for item in snapshot.models
        if item.import_path == "org.apache.dolphinscheduler.common.model.TaskNode"
    )
    task_node_id = next(field for field in task_node.fields if field.wire_name == "id")

    assert recipe.executable is False
    assert recipe.graph_backed is True
    assert recipe.identity_wire == "legacy-string"
    assert recipe.detail_operation is None
    assert recipe.update_operation is None
    assert not any(
        operation.startswith("TaskDefinitionController.") for operation in operation_ids
    )
    assert task_node_id.java_type == "String"
    assert contract.semantic_operation_sources("1.3.9") == {
        "task.list": ("ProcessDefinitionController.queryProcessDefinitionById",),
        "task.get": ("ProcessDefinitionController.queryProcessDefinitionById",),
        "task.update": (
            "ProcessDefinitionController.queryProcessDefinitionById",
            "ProcessDefinitionController.updateProcessDefinition",
        ),
    }
    facets = contract.semantic_operation_facets("1.3.9")
    assert "identity:legacy-string" in facets["task.get"]
    assert "selector:enumeration" in facets["task.list"]
    assert "mutation:whole-workflow" not in facets["task.list"]
    assert "mutation:whole-workflow" not in facets["task.get"]
    assert "mutation:whole-workflow" in facets["task.update"]
    assert "execution:supported" in facets["task.update"]


def test_task_wire_epochs_are_explicit_at_reviewed_boundaries() -> None:
    contract = _task_contract()
    v200 = contract.task_definition_contract("2.0.0").task
    v209 = contract.task_definition_contract("2.0.9").task
    v300 = contract.task_definition_contract("3.0.0").task
    v310 = contract.task_definition_contract("3.1.0").task
    v319 = contract.task_definition_contract("3.1.9").task
    v320 = contract.task_definition_contract("3.2.0").task
    v321 = contract.task_definition_contract("3.2.1").task
    v331 = contract.task_definition_contract("3.3.1").task

    assert v200.detail_model.endswith(".TaskDefinition")
    assert v200.update_operation.endswith(".updateTaskDefinition")
    assert v200.update_executable is False
    assert v200.whole_workflow_update is True
    assert v200.dependency_update is True
    assert v209.update_operation == v200.update_operation
    assert v209.update_executable is True
    assert v300.update_operation.endswith(".updateTaskDefinition")
    assert v300.dependency_update is False
    assert "taskGroupId" not in v200.request_fields
    assert "taskGroupId" in v300.request_fields
    assert "cpuQuota" not in v300.request_fields
    assert "cpuQuota" in v310.request_fields
    assert v310.update_operation.endswith(".updateTaskDefinition")
    assert v310.dependency_update is False
    assert v310.requires_unique_workflow_binding is True
    assert v319.detail_model.endswith(".TaskDefinitionVo")
    assert v319.relation_field == "processTaskRelationList"
    assert v319.update_operation.endswith(".updateTaskDefinition")
    assert v319.dependency_update is False
    assert v319.requires_unique_workflow_binding is True
    assert v320.preserve_is_cache is True
    assert v320.update_operation.endswith(".updateTaskDefinition")
    assert v320.dependency_update is False
    assert v320.requires_unique_workflow_binding is True
    assert v321.detail_model.endswith(".TaskDefinitionVO")
    assert v321.update_operation.endswith(".updateTaskWithUpstream")
    assert v321.dependency_update is True
    assert v321.requires_unique_workflow_binding is False
    assert v331.relation_field == "workflowTaskRelationList"
    assert v331.dag_family == "workflow"


@pytest.mark.parametrize("ds_version", _DEPENDENCE_GETTER_VERSIONS)
def test_task_dependence_getter_is_an_exact_computed_response_field(
    ds_version: str,
) -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract(ds_version).task
    policy = contract.task_top_level_field_policy(ds_version)

    assert recipe.computed_response_fields == frozenset({"dependence"})
    assert "dependence" in policy.response_derived
    assert "dependence" not in policy.request_payload
    assert "dependence" not in policy.opaque_preservation


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"],
)
def test_task_dependence_getter_is_absent_outside_its_exact_source_epoch(
    ds_version: str,
) -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract(ds_version).task

    assert not recipe.computed_response_fields
    if recipe.executable:
        assert (
            "dependence"
            not in contract.task_top_level_field_policy(ds_version).classified_fields
        )


def test_200_update_uses_the_safe_whole_workflow_mutation_seam() -> None:
    contract = _task_contract()

    assert contract.semantic_operation_sources("2.0.0")["task.update"] == (
        "ProcessDefinitionController.queryProcessDefinitionByCode",
        "TaskDefinitionController.queryTaskDefinitionDetail",
        "ProcessDefinitionController.updateProcessDefinition",
    )
    assert contract.semantic_operation_type_roots("2.0.0")["task.update"]
    evidence = contract.semantic_operation_evidence("2.0.0")["task.update"]
    assert {item.kind for item in evidence} == {"controller", "service", "ui"}
    assert any(
        item.kind == "controller" and item.symbol == "updateProcessDefinition"
        for item in evidence
    )
    assert any(
        item.kind == "service"
        and item.symbol == "updateDagDefine"
        and "task and relation versions" in item.conclusion
        for item in evidence
    )
    facets = contract.semantic_operation_facets("2.0.0")["task.update"]
    assert "execution:supported" in facets
    assert "mutation:whole-workflow" in facets
    assert "standalone-task-update:unsafe" in facets


def test_343_update_selects_whole_workflow_when_standalone_update_is_absent() -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract("3.4.3").task

    assert recipe.update_operation is None
    assert recipe.update_executable is False
    assert recipe.whole_workflow_update is True
    assert recipe.dependency_update is True
    assert contract.semantic_operation_sources("3.4.3")["task.update"] == (
        "WorkflowDefinitionController.queryWorkflowDefinitionByCode",
        "TaskDefinitionController.queryTaskDefinitionDetail",
        "WorkflowDefinitionController.updateWorkflowDefinition",
    )
    evidence = contract.semantic_operation_evidence("3.4.3")["task.update"]
    assert any(
        item.kind == "controller" and item.symbol == "updateWorkflowDefinition"
        for item in evidence
    )
    assert any(
        item.kind == "service"
        and item.symbol == "updateDagDefine"
        and "task and relation versions" in item.conclusion
        for item in evidence
    )
    facets = contract.semantic_operation_facets("3.4.3")["task.update"]
    assert "execution:supported" in facets
    assert "mutation:whole-workflow" in facets
    assert "standalone-task-update:absent" in facets
    assert "standalone-task-update:unsafe" not in facets


def test_209_update_is_backed_by_relation_version_rewrite_evidence() -> None:
    contract = _task_contract()

    evidence = contract.semantic_operation_evidence("2.0.9")["task.update"]

    assert contract.semantic_operation_sources("2.0.9")["task.update"][-2] == (
        "TaskDefinitionController.updateTaskDefinition"
    )
    assert {item.kind for item in evidence} == {"controller", "service", "ui"}
    assert any(
        item.symbol == "updateTaskDefinition/updateDag"
        and "first upstream relation" in item.conclusion
        for item in evidence
    )


@pytest.mark.parametrize(
    ("ds_version", "rejected_symbol", "rejected_conclusion"),
    [
        (
            "3.1.9",
            "updateTaskWithUpstream",
            "now-empty set guards updateDag",
        ),
        (
            "3.2.0",
            "updateTaskWithUpstream/updateUpstreamTask",
            "does not call updateDag",
        ),
    ],
)
def test_guarded_standalone_update_records_its_binding_precondition(
    ds_version: str,
    rejected_symbol: str,
    rejected_conclusion: str,
) -> None:
    contract = _task_contract()

    sources = contract.semantic_operation_sources(ds_version)["task.update"]
    evidence = contract.semantic_operation_evidence(ds_version)["task.update"]
    facets = contract.semantic_operation_facets(ds_version)["task.update"]

    assert sources[-2:] == (
        "TaskDefinitionController.updateTaskDefinition",
        "ProcessDefinitionController.queryProcessDefinitionSimpleList",
    )
    assert "dependency-update:false" in facets
    assert "workflow-binding:single" in facets
    assert any(
        item.symbol == "updateTaskDefinition/updateDag"
        and "first upstream relation" in item.conclusion
        for item in evidence
    )
    assert any(
        item.symbol == rejected_symbol and rejected_conclusion in item.conclusion
        for item in evidence
    )
    assert any(
        item.symbol == ("queryProcessDefinitionSimpleList/queryProcessDefinitionByCode")
        and "exhausts the project workflow inventory" in item.conclusion
        for item in evidence
    )


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.2.2"])
def test_32x_is_cache_is_an_owned_opaque_preservation_field(
    ds_version: str,
) -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract(ds_version).task
    policy = contract.task_top_level_field_policy(ds_version)

    assert recipe.preserve_is_cache is True
    assert "isCache" in policy.request_payload
    assert "isCache" in policy.opaque_preservation


@pytest.mark.parametrize(
    "ds_version",
    [
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
    ],
)
def test_entity_detail_epoch_has_no_relation_projection(ds_version: str) -> None:
    contract = _task_contract()
    assert contract.task_definition_contract(ds_version).task.relation_field is None
    assert not contract.task_top_level_field_policy(ds_version).relation_projection


@pytest.mark.parametrize("ds_version", _CODE_NATIVE_VERSIONS)
@pytest.mark.source_contract
def test_explicit_field_policy_matches_exact_detail_model(
    ds_version: str,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _task_contract()
    recipe = contract.task_definition_contract(ds_version).task
    policy = contract.task_top_level_field_policy(ds_version)
    snapshot = exact_contract_corpus.snapshot(ds_version)
    model = next(
        item for item in snapshot.models if item.import_path == recipe.detail_model
    )
    generated_fields = {field.wire_name for field in model.fields}

    assert generated_fields.isdisjoint(recipe.computed_response_fields)
    assert policy.classified_fields == (
        generated_fields | recipe.computed_response_fields
    )


def test_unknown_task_contract_fails_closed() -> None:
    contract = _task_contract()
    with pytest.raises(ValueError, match="no reviewed task-definition contract"):
        contract.task_definition_contract("9.9.9")


def test_generated_runtime_profiles_are_the_exact_contract_projection() -> None:
    contract = _task_contract()
    generated = importlib.import_module("dsctl.generated.task_definition_profiles")
    projected = contract.task_definition_profile_data()

    assert generated.TASK_DEFINITION_PROFILE_SCHEMA_VERSION == 5
    assert (
        *contract.TARGET_TASK_VERSIONS,
    ) == generated.TARGET_TASK_DEFINITION_VERSIONS
    assert projected["profiles"] == generated.TASK_DEFINITION_PROFILES
    generated_path = _REPO_ROOT / "src/dsctl/generated/task_definition_profiles.py"
    assert generated_path.read_text(encoding="utf-8") == (
        contract.render_task_definition_profiles()
    )


@pytest.mark.parametrize("ds_version", _CODE_NATIVE_VERSIONS)
def test_runtime_wire_spec_is_derived_from_the_reviewed_profile(
    ds_version: str,
) -> None:
    contract = _task_contract()
    runtime = importlib.import_module("dsctl.upstream.task_definition_wire")
    recipe = contract.task_definition_contract(ds_version).task
    policy = contract.task_top_level_field_policy(ds_version)
    spec = runtime._TASK_SPEC_BY_VERSION[ds_version]

    assert spec.dag_source_operation == recipe.dag_operation
    assert spec.detail_model_name == recipe.detail_model.rpartition(".")[2]
    assert spec.update_source_operation == recipe.update_operation
    assert spec.update_response_optional is recipe.update_response_optional
    assert spec.update_policy.update_available is recipe.update_executable
    assert spec.update_policy.dependency_update is recipe.dependency_update
    assert (
        spec.update_policy.requires_unique_workflow_binding
        is recipe.requires_unique_workflow_binding
    )
    assert spec.computed_response_fields == recipe.computed_response_fields
    assert spec.field_policy.request_payload == policy.request_payload
    assert spec.field_policy.relation_projection == policy.relation_projection
    assert spec.field_policy.opaque_preservation == policy.opaque_preservation
    assert "1.3.9" not in runtime._TASK_SPEC_BY_VERSION


@pytest.mark.parametrize("ds_version", _CODE_NATIVE_VERSIONS)
def test_runtime_slice_binds_every_task_action_to_the_exact_contract(
    ds_version: str,
) -> None:
    contract = _task_contract()
    impact = importlib.import_module("ds_codegen.compatibility_impact")
    bindings = impact.RUNTIME_OPERATION_BINDINGS[ds_version]
    contract_sources = contract.semantic_operation_sources(ds_version)
    contract_roots = contract.semantic_operation_type_roots(ds_version)

    for semantic_operation in contract.TASK_SEMANTIC_OPERATIONS:
        if not contract_sources[semantic_operation]:
            assert semantic_operation not in bindings
            continue
        binding = bindings[semantic_operation]
        assert set(contract_sources[semantic_operation]) <= set(
            binding.source_operations
        )
        assert "SchedulerController.queryScheduleListPaging" not in (
            binding.source_operations
        )
        assert {
            item.key for item in binding.type_closure if item.surface == "models"
        } == set(contract_roots[semantic_operation])
        assert {item.resource for item in binding.selector_semantics} == {
            "project",
            "workflow",
            "task",
        }


def test_139_runtime_slice_binds_graph_backed_tasks_by_name_and_id() -> None:
    impact = importlib.import_module("ds_codegen.compatibility_impact")
    bindings = impact.RUNTIME_OPERATION_BINDINGS["1.3.9"]

    assert {"task.list", "task.get", "task.update"} <= bindings.keys()
    for action in ("task.list", "task.get", "task.update"):
        task_selector = next(
            item
            for item in bindings[action].selector_semantics
            if item.resource == "task"
        )
        assert task_selector.native_identity == "id"
        assert task_selector.exposed_identities == ("name", "id")
        assert "code" not in task_selector.consumed_selectors


def test_version_profile_ledger_materializes_exact_task_terminal_seams() -> None:
    contract = _task_contract()
    version_profiles = importlib.import_module("ds_codegen.version_profiles")
    cli_surface = importlib.import_module("dsctl.cli_surface")
    data = version_profiles.compile_version_profile_data(
        stable_actions=cli_surface.stable_leaf_actions()
    )

    for ds_version in contract.TARGET_TASK_VERSIONS:
        profile = data["profiles"][ds_version]
        actions = profile["actions"]
        decisions = profile["build_decisions"]
        recipe = contract.task_definition_contract(ds_version).task

        for operation in contract.TASK_SEMANTIC_OPERATIONS:
            capability = actions[operation]
            decision = decisions[operation]
            if (not recipe.executable and not recipe.graph_backed) or (
                operation == "task.update"
                and not recipe.update_executable
                and not recipe.whole_workflow_update
                and not recipe.graph_backed
            ):
                assert capability["availability"] == "limited"
                assert capability["execution_mode"] == "not_executable"
                assert capability["reason"] == "upstream_capability_limited"
                assert decision["build_status"] == "blocked"
                assert decision["source_operations"] == []
                continue

            assert capability["availability"] == "supported"
            assert decision["build_status"] == "accepted"
            assert decision["source_operations"]

    for ds_version, expected_verification in (
        ("3.4.1", "live_smoke"),
        ("3.4.2", "live_smoke"),
    ):
        actions = data["profiles"][ds_version]["actions"]
        for operation in contract.TASK_SEMANTIC_OPERATIONS:
            assert actions[operation]["execution_mode"] == "wire_program"
            assert actions[operation]["verification"] == expected_verification
