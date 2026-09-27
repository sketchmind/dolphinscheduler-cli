from __future__ import annotations

import hashlib
import importlib
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.ir import ContractSnapshot

REPO_ROOT = Path(__file__).resolve().parents[2]
_QUEUE_MAPPER = (
    "dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/"
    "dao/mapper/TaskGroupQueueMapper.xml"
)
_QUEUE_ENTITY = (
    "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/"
    "dao/entity/TaskGroupQueue.java"
)


def _contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.task_group_contract")


def _operation_ids(snapshot: ContractSnapshot) -> set[str]:
    return {operation.operation_id for operation in snapshot.operations}


def _structured_type_keys(snapshot: ContractSnapshot) -> set[str]:
    return (
        {model.import_path for model in snapshot.models}
        | {dto.import_path for dto in snapshot.dtos}
        | {enum.import_path for enum in snapshot.enums}
    )


def test_task_group_contract_covers_exact_targets_without_inference() -> None:
    contract = _contract()

    assert tuple(contract.TASK_GROUP_CONTRACTS) == contract.TARGET_TASK_GROUP_VERSIONS
    assert "3.4.3" in contract.TARGET_TASK_GROUP_VERSIONS
    for version in contract.TARGET_TASK_GROUP_VERSIONS:
        assert contract.task_group_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed task-group contract"):
        contract.task_group_contract("3.4.4")


def test_task_group_absence_before_3_0_is_explicit_and_terminal() -> None:
    contract = _contract()

    for version in (
        "1.3.9",
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
    ):
        assert set(contract.terminal_decisions(version)) == set(
            contract.TASK_GROUP_SEMANTIC_OPERATIONS
        )
        assert set(contract.action_support(version).values()) == {"absent"}
        assert contract.semantic_operation_sources(version) == {}

    for version in contract.TARGET_TASK_GROUP_VERSIONS[
        contract.TARGET_TASK_GROUP_VERSIONS.index("3.0.0") :
    ]:
        assert contract.terminal_decisions(version) == {}
        assert set(contract.action_support(version).values()) == {"supported"}


def test_task_group_real_wire_and_result_boundaries_are_preserved() -> None:
    contract = _contract()

    three_zero = contract.task_group_contract("3.0.0").task_group
    three_two_one = contract.task_group_contract("3.2.1").task_group
    three_two_two = contract.task_group_contract("3.2.2").task_group
    three_three = contract.task_group_contract("3.3.1").task_group
    three_four_two = contract.task_group_contract("3.4.2").task_group

    assert three_zero.queue_operation.endswith("queryTasksByGroupId")
    assert three_zero.queue_workflow_filter == "processInstanceName"
    assert three_zero.queue_identity == "process"
    assert three_zero.numeric_group_status is True
    assert (three_zero.create_result, three_zero.update_result) == ("none", "none")
    assert three_two_one.queue_operation.endswith("queryTaskGroupQueues")
    assert three_two_one.numeric_group_status is False
    assert (three_two_one.create_result, three_two_one.update_result) == (
        "none",
        "none",
    )
    assert (three_two_two.create_result, three_two_two.update_result) == (
        "entity",
        "entity",
    )
    assert three_three.queue_workflow_filter == "workflowInstanceName"
    assert three_three.queue_identity == "workflow"
    assert three_four_two.typed_results is True
    assert contract.semantic_operation_sources("3.4.2")["task-group.update"] == (
        "TaskGroupController.queryAllTaskGroup",
        "TaskGroupController.queryTaskGroupByCode",
        "TaskGroupController.updateTaskGroup",
    )


@pytest.mark.source_contract
def test_task_group_source_and_type_closure_exists_in_each_exact_snapshot(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_TASK_GROUP_VERSIONS[
        contract.TARGET_TASK_GROUP_VERSIONS.index("3.0.0") :
    ]:
        snapshot = exact_contract_corpus.snapshot(version)
        operation_ids = _operation_ids(snapshot)
        type_keys = _structured_type_keys(snapshot)
        sources = contract.semantic_operation_sources(version)
        type_roots = contract.semantic_operation_type_roots(version)
        enum_roots = contract.semantic_operation_enum_roots(version)

        assert type_roots.keys() == sources.keys()
        assert enum_roots.keys() == sources.keys()
        for operation, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, operation)
            assert set(type_roots[operation]) <= type_keys, (version, operation)
            assert set(enum_roots[operation]) <= type_keys, (version, operation)


@pytest.mark.source_contract
def test_task_group_evidence_and_facets_close_over_executable_operations(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_TASK_GROUP_VERSIONS[
        contract.TARGET_TASK_GROUP_VERSIONS.index("3.0.0") :
    ]:
        sources = contract.semantic_operation_sources(version)
        facets = contract.semantic_operation_facets(version)
        evidence = contract.semantic_operation_evidence(version)

        assert facets.keys() == sources.keys()
        assert evidence.keys() == sources.keys()
        for operation, operation_evidence in evidence.items():
            kinds = {"controller", "ui"}
            if operation == "task-group.queue.page":
                kinds.add("mapper")
            assert {item.kind for item in operation_evidence} == kinds
            for item in operation_evidence:
                assert item.version == version
                assert exact_contract_corpus.source_file(
                    version,
                    item.source,
                ).is_file()

        queue_facets = facets["task-group.queue.page"]
        expected_filter = contract.task_group_contract(
            version
        ).task_group.queue_workflow_filter
        assert queue_facets["workflow_instance_filter"] == expected_filter
        projection = contract.task_group_contract(version).queue_page_projection
        assert projection is not None
        assert queue_facets["paging_projection"] == {
            "taskId": projection.task_id_projected,
            "inQueue": projection.in_queue_projected,
        }


@pytest.mark.source_contract
def test_queue_page_projection_matches_each_exact_mapper_select(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_TASK_GROUP_VERSIONS:
        reviewed = contract.task_group_contract(version)
        if reviewed.task_group.support == "absent":
            assert reviewed.queue_page_projection is None
            continue

        projection = reviewed.queue_page_projection
        assert projection is not None
        mapper = exact_contract_corpus.source_file(version, _QUEUE_MAPPER).read_text()
        matches = re.findall(
            r'<select\s+id="queryTaskGroupQueueByTaskGroupIdPaging"[^>]*>(.*?)</select>',
            mapper,
            flags=re.DOTALL,
        )
        assert len(matches) == 1, version
        selected, separator, _ = matches[0].partition("from t_ds_task_group_queue")
        assert separator, version
        assert (
            hashlib.sha256(selected.encode()).hexdigest() == projection.select_sha256
        ), version
        columns = set(re.findall(r"\bqueue\.([a-z_]+)\b", selected))
        assert projection.task_id_projected == ("task_id" in columns), version
        assert projection.in_queue_projected == ("in_queue" in columns), version

        entity = exact_contract_corpus.source_file(version, _QUEUE_ENTITY).read_text()
        assert re.search(r"\bprivate\s+int\s+taskId\s*;", entity), version
        assert re.search(r"\bprivate\s+int\s+inQueue\s*;", entity), version
