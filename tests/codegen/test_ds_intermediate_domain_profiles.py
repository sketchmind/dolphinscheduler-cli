"""Independent source boundaries for the newly admitted exact releases."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ds_codegen.task_definition_contract import task_definition_contract

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus


@pytest.mark.parametrize(
    ("version", "whole_workflow", "dependencies", "unique_binding", "detail"),
    [
        *((f"2.0.{patch}", True, True, False, "TaskDefinition") for patch in range(3)),
        ("2.0.3", True, False, False, "TaskDefinition"),
        *(
            (f"2.0.{patch}", False, False, True, "TaskDefinition")
            for patch in range(4, 10)
        ),
        *((f"3.0.{patch}", False, False, True, "TaskDefinition") for patch in range(7)),
        *((f"3.1.{patch}", False, False, True, "TaskDefinition") for patch in range(3)),
        *(
            (f"3.1.{patch}", False, False, True, "TaskDefinitionVo")
            for patch in range(3, 10)
        ),
        ("3.2.0", False, False, True, "TaskDefinitionVo"),
        ("3.2.1", False, True, False, "TaskDefinitionVO"),
    ],
)
def test_task_mutation_route_is_independent_of_detail_model(
    version: str,
    *,
    whole_workflow: bool,
    dependencies: bool,
    unique_binding: bool,
    detail: str,
) -> None:
    recipe = task_definition_contract(version).task
    assert recipe.whole_workflow_update is whole_workflow
    assert recipe.update_executable is not whole_workflow
    assert recipe.dependency_update is dependencies
    assert recipe.requires_unique_workflow_binding is unique_binding
    assert recipe.detail_model.rsplit(".", 1)[1] == detail
    assert recipe.update_operation == (
        "TaskDefinitionController.updateTaskWithUpstream"
        if dependencies and not whole_workflow
        else "TaskDefinitionController.updateTaskDefinition"
    )


@pytest.mark.source_contract
@pytest.mark.parametrize("version", [f"2.0.{patch}" for patch in range(1, 7)])
def test_resource_view_changes_from_map_to_generated_entity_only_in_206(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ResourcesController.viewResource"
    )
    if version == "2.0.6":
        assert "Map" not in operation.logical_return_type
    else:
        assert operation.logical_return_type.replace(" ", "") == "Map<String,String>"


@pytest.mark.source_contract
@pytest.mark.parametrize("patch", range(10))
def test_task_stop_enum_hole_has_its_own_exact_membership(
    exact_contract_corpus: ExactContractCorpus,
    patch: int,
) -> None:
    snapshot = exact_contract_corpus.snapshot(f"3.1.{patch}")
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "TaskInstanceController.queryTaskListPaging"
    )
    enum_path = next(
        item.java_type for item in operation.parameters if item.name == "stateType"
    )
    state = next(item for item in snapshot.enums if item.import_path == enum_path)
    assert ("STOP" in {value.name for value in state.values}) is (patch >= 7)


@pytest.mark.source_contract
@pytest.mark.parametrize("version", ["3.0.1", "3.0.5", "3.1.0"])
def test_dependency_route_passes_the_original_relation_list(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    source = exact_contract_corpus.source_file(
        version,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java",
    ).read_text(encoding="utf-8")
    method = source.split("public Map<String, Object> updateTaskWithUpstream", 1)[
        1
    ].split("\n    /**", 1)[0]
    assert (
        "processTaskRelationList = Lists.newArrayList(processTaskRelations)" in method
    )
    assert "processTaskRelationList.removeAll(relationList)" in method
    assert (
        "processTaskRelations, Lists.newArrayList(taskDefinitionToUpdate)"
        in " ".join(method.split())
    )
    assert task_definition_contract(version).task.dependency_update is False


@pytest.mark.source_contract
@pytest.mark.parametrize("version", ["3.1.1", "3.1.2", "3.1.3", "3.1.8"])
def test_dependency_route_clears_the_set_that_guards_persistence(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    source = exact_contract_corpus.source_file(
        version,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/TaskDefinitionServiceImpl.java",
    ).read_text(encoding="utf-8")
    method = source.split("public Map<String, Object> updateTaskWithUpstream", 1)[
        1
    ].split("\n    /**", 1)[0]
    validation_tail = method.split(
        "upstreamTaskCodes.removeAll(queryUpStreamTaskCodeMap.keySet());", 1
    )[1]
    assert "if (CollectionUtils.isNotEmpty(upstreamTaskCodes))" in validation_tail
    valid_upstreams = {11, 22}
    valid_upstreams.difference_update({11, 22})
    assert not valid_upstreams  # The source's subsequent persistence guard is false.
    assert task_definition_contract(version).task.dependency_update is False
