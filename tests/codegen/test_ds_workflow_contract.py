from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

REPO_ROOT = Path(__file__).resolve().parents[2]


def _contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.workflow_contract")


def test_workflow_contract_covers_every_exact_target_without_inference() -> None:
    contract = _contract()

    assert tuple(contract.WORKFLOW_CONTRACTS) == contract.TARGET_WORKFLOW_VERSIONS
    assert "3.4.3" in contract.TARGET_WORKFLOW_VERSIONS
    for version in contract.TARGET_WORKFLOW_VERSIONS:
        assert contract.workflow_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed workflow-family contract"):
        contract.workflow_contract("3.4.4")


def test_workflow_terminal_boundaries_are_explicit_and_close_the_surface() -> None:
    contract = _contract()

    legacy = contract.terminal_decisions("1.3.9")
    assert set(legacy) == {
        "workflow.lineage.list",
        "workflow.lineage.get",
        "workflow.lineage.dependent-tasks",
    }
    assert legacy["workflow.lineage.list"].reason == "upstream_capability_absent"

    legacy_sources = contract.semantic_operation_sources("1.3.9")
    assert legacy_sources["workflow.create"][-1] == (
        "ProcessDefinitionController.createProcessDefinition"
    )
    assert legacy_sources["workflow.edit"][-1] == (
        "ProcessDefinitionController.updateProcessDefinition"
    )
    assert (
        "ProcessDefinitionController.queryProcessDefinitionById"
        in (legacy_sources["workflow.describe"])
    )
    assert legacy_sources["workflow.run-task"][-1] == (
        "ExecutorController.startProcessInstance"
    )

    for version in (
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
    ):
        decisions = contract.terminal_decisions(version)
        assert set(decisions) == {"workflow.lineage.dependent-tasks"}
        assert (
            decisions["workflow.lineage.dependent-tasks"].reason
            == "upstream_capability_absent"
        )

    for version in contract.TARGET_WORKFLOW_VERSIONS[
        contract.TARGET_WORKFLOW_VERSIONS.index("3.2.2") :
    ]:
        assert contract.terminal_decisions(version) == {}

    for version in contract.TARGET_WORKFLOW_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        terminal = contract.terminal_decisions(version)
        assert set(sources).isdisjoint(terminal)
        assert set(sources) | set(terminal) == set(
            contract.WORKFLOW_SEMANTIC_OPERATIONS
        )


def test_workflow_real_wire_transitions_remain_visible() -> None:
    contract = _contract()

    legacy = contract.workflow_contract("1.3.9")
    two_zero = contract.workflow_contract("2.0.0")
    three_one = contract.workflow_contract("3.1.0")
    three_two_zero = contract.workflow_contract("3.2.0")
    three_two_one = contract.workflow_contract("3.2.1")
    three_two_two = contract.workflow_contract("3.2.2")
    three_three = contract.workflow_contract("3.3.1")

    assert (
        legacy.definition.family,
        legacy.definition.project_route,
        legacy.definition.native_identity,
    ) == ("legacy-json", "name", "id")
    assert legacy.definition.dag_support == "supported"
    assert legacy.definition.authoring_support == "supported"
    assert legacy.execution.task_scope == "supported"
    assert legacy.execution.start_node_identity == "name"
    assert legacy.execution.result == "none"

    assert two_zero.definition.tenant_code is True
    assert two_zero.definition.execution_type is False
    assert three_one.definition.create_other_params is True
    assert three_one.definition.update_other_params is True
    assert three_two_zero.definition.tenant_code is False
    assert three_two_zero.execution.tenant_code is True
    assert three_two_zero.execution.result == "trigger-code"
    assert three_two_one.definition.update_other_params is False
    assert three_two_one.definition.release_result == "boolean"
    assert three_two_two.lineage.dependent_projection == "task-main-info"
    assert three_three.definition.family == "workflow"
    assert three_three.definition.dag_wire == "workflow"
    assert three_three.execution.result == "id-list"
    assert three_three.lineage.dependent_projection == "dependent-lineage-task"


def test_definition_delete_lineage_guard_is_an_exact_generated_fact() -> None:
    contract = _contract()
    guarded = {
        version
        for version, profile in contract.WORKFLOW_CONTRACTS.items()
        if profile.definition.definition_delete_lineage_guard
    }

    assert guarded == {"3.3.1", "3.3.2"}
    for version in guarded:
        lineage_get = contract.workflow_contract(version).lineage.get_operation
        delete_sources = contract.semantic_operation_sources(version)["workflow.delete"]
        assert lineage_get in delete_sources
        assert delete_sources.index(lineage_get) < len(delete_sources) - 1
        assert contract.semantic_operation_facets(version)["workflow.delete"] == {
            "definition_family": "workflow",
            "project_route": "code",
            "native_identity": "code",
            "definition_delete_lineage_guard": True,
        }

    assert (
        "WorkflowLineageController.queryWorkFlowLineageByCode"
        not in contract.semantic_operation_sources("3.4.0")["workflow.delete"]
    )
    assert (
        contract.semantic_operation_facets("3.4.0")["workflow.delete"][
            "definition_delete_lineage_guard"
        ]
        is False
    )

    facts = contract.runtime_profile_facts()
    assert {
        version
        for version, profile in facts.items()
        if profile["definition_delete_lineage_guard"]
    } == guarded


def test_runtime_profile_facts_preserve_independent_tenant_contracts() -> None:
    contract = _contract()
    facts = contract.runtime_profile_facts()

    assert tuple(facts) == contract.TARGET_WORKFLOW_VERSIONS
    for version in ("3.2.0", "3.2.1", "3.2.2"):
        assert facts[version]["definition_tenant_code"] is False
        assert facts[version]["execution_tenant_code"] is True


def test_generated_workflow_profiles_are_the_exact_contract_projection() -> None:
    contract = _contract()
    generated = importlib.import_module("dsctl.generated.workflow_profiles")
    projected = contract.workflow_profile_data()

    assert generated.WORKFLOW_PROFILE_SCHEMA_VERSION == 4
    assert (*contract.TARGET_WORKFLOW_VERSIONS,) == generated.TARGET_WORKFLOW_VERSIONS
    assert projected["profiles"] == generated.WORKFLOW_PROFILE_FACTS
    generated_path = REPO_ROOT / "src/dsctl/generated/workflow_profiles.py"
    assert generated_path.read_text(encoding="utf-8") == (
        contract.render_workflow_profiles(projected)
    )


def test_runtime_profile_facts_preserve_exact_executor_omission_semantics() -> None:
    contract = _contract()
    facts = contract.runtime_profile_facts()

    assert {
        version
        for version, profile in facts.items()
        if profile["warning_group_omission"] == "zero"
    } == contract.WARNING_GROUP_PRIMITIVE_VERSIONS
    assert {
        "3.0.0",
        "3.0.1",
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
    } == contract.WARNING_GROUP_DEFAULT_ZERO_VERSIONS
    assert {
        version
        for version, profile in facts.items()
        if profile["execution_schedule_time_shape"] == "comma-range"
    } == contract.COMMA_RANGE_EXECUTION_SCHEDULE_VERSIONS


def test_generated_legacy_workflow_slice_exposes_exact_string_native_wire() -> None:
    programs = importlib.import_module(
        "dsctl.upstream._compiled_workflow_runtime"
    ).WORKFLOW_PROGRAMS.profile("1.3.9")
    profiles = importlib.import_module("dsctl.generated.version_profiles")

    create_params = programs.program("definition_create").codec.params_model
    update_params = programs.program("definition_update").codec.params_model
    assert set(create_params.model_fields) == {
        "projectName",
        "name",
        "processDefinitionJson",
        "locations",
        "connects",
        "description",
    }
    assert set(update_params.model_fields) == {
        "projectName",
        "name",
        "id",
        "processDefinitionJson",
        "locations",
        "connects",
        "description",
    }
    for params in (create_params, update_params):
        assert params.model_fields["projectName"].annotation is str
        assert params.model_fields["projectName"].is_required()

    legacy_profile = profiles.VERSION_PROFILES["1.3.9"]
    assert legacy_profile["support_level"] == "experimental"
    assert legacy_profile["tested"] is False
    for action in (
        "workflow.create",
        "workflow.edit",
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
        "workflow.run-task",
    ):
        assert legacy_profile["actions"][action] == {
            "availability": "supported",
            "execution_mode": "generated_adapter",
            "verification": "contract_tested",
        }
        assert legacy_profile["build_decisions"][action]["build_status"] == ("accepted")


@pytest.mark.source_contract
def test_workflow_source_closure_exists_in_each_exact_snapshot(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_WORKFLOW_VERSIONS:
        operation_ids = {
            operation.operation_id
            for operation in exact_contract_corpus.snapshot(version).operations
        }
        sources = contract.semantic_operation_sources(version)
        type_roots = contract.semantic_operation_type_roots(version)
        enum_roots = contract.semantic_operation_enum_roots(version)
        assert type_roots.keys() == sources.keys()
        assert enum_roots.keys() == sources.keys()
        for operation, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, operation)


@pytest.mark.source_contract
def test_workflow_evidence_and_facets_close_over_executable_actions(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_WORKFLOW_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        facets = contract.semantic_operation_facets(version)
        evidence = contract.semantic_operation_evidence(version)
        assert facets.keys() == sources.keys()
        assert evidence.keys() == sources.keys()
        for operation, action_evidence in evidence.items():
            expected_kinds = {"controller", "ui"}
            if operation == "workflow.delete" and version in {"3.3.1", "3.3.2"}:
                expected_kinds |= {"mapper", "service"}
            elif operation == "workflow.delete" and version == "3.4.0":
                expected_kinds.add("service")
            assert {item.kind for item in action_evidence} == expected_kinds
            for item in action_evidence:
                assert item.version == version
                assert exact_contract_corpus.source_file(
                    version,
                    item.source,
                ).is_file()


@pytest.mark.source_contract
def test_definition_delete_lineage_guard_matches_exact_upstream_cleanup(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in ("3.3.1", "3.3.2"):
        evidence = tuple(
            item
            for item in contract.semantic_operation_evidence(version)["workflow.delete"]
            if item.kind in {"mapper", "service"}
        )
        assert {item.kind for item in evidence} == {"mapper", "service"}
        assert all(item.version == version for item in evidence)
        sources = {
            item.symbol: exact_contract_corpus.source_file(
                version, item.source
            ).read_text(encoding="utf-8")
            for item in evidence
        }

        delete_method = (
            sources["deleteWorkflowDefinitionByCode"]
            .split("public void deleteWorkflowDefinitionByCode", 1)[1]
            .split("\n    /**", 1)[0]
        )
        assert "deleteWorkflowLineage(" not in delete_method

        lineage_get = (
            sources["queryWorkFlowLineageByCode"]
            .split("public WorkFlowLineage queryWorkFlowLineageByCode", 1)[1]
            .split("\n    @Override", 1)[0]
        )
        assert "queryByWorkflowDefinitionCode(workflowDefinitionCode)" in lineage_get

        relation_projection = (
            sources["queryWorkFlowLineageByCode"]
            .split("private List<WorkFlowRelation> getWorkFlowRelations", 1)[1]
            .split("\n    private ", 1)[0]
        )
        assert (
            "workflowTaskLineage.getDeptWorkflowDefinitionCode(),\n"
            "                    workflowTaskLineage.getWorkflowDefinitionCode()"
            in relation_projection
        )

        owner_query = (
            sources["queryByWorkflowDefinitionCode"]
            .split('<select id="queryByWorkflowDefinitionCode"', 1)[1]
            .split("</select>", 1)[0]
        )
        assert "workflow_definition_code = #{workflowDefinitionCode}" in owner_query
        assert "workflow_definition_version" not in owner_query

    cleanup_evidence = tuple(
        item
        for item in contract.semantic_operation_evidence("3.4.0")["workflow.delete"]
        if item.kind == "service"
    )
    assert len(cleanup_evidence) == 1
    cleanup_source = exact_contract_corpus.source_file(
        "3.4.0", cleanup_evidence[0].source
    ).read_text(encoding="utf-8")
    cleanup_method = cleanup_source.split(
        "public void deleteWorkflowDefinitionByCode", 1
    )[1].split("\n    /**", 1)[0]
    assert "deleteWorkflowLineage(" in cleanup_method
