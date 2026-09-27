from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_queue_page_does_not_expose_unselected_task_identity() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    contract = _load_module("ds_codegen.task_group_contract")
    for version in contract.TARGET_TASK_GROUP_VERSIONS:
        if contract.task_group_contract(version).task_group.support == "absent":
            continue
        binding = impact.RUNTIME_OPERATION_BINDINGS[version]["task-group.queue.page"]
        queue = next(
            item
            for item in binding.selector_semantics
            if item.resource == "task-group-queue"
        )
        assert "taskId" not in queue.exposed_identities, version
        assert {"id", "groupId"} <= set(queue.exposed_identities), version
        projection = contract.task_group_contract(version).queue_page_projection
        assert projection is not None
        assert binding.paging_projection == (
            ("taskId", projection.task_id_projected),
            ("inQueue", projection.in_queue_projected),
        )
        assert {source.kind for source in binding.evidence_sources} == {
            "controller",
            "ui",
            "mapper",
        }
        for operation, other in impact.RUNTIME_OPERATION_BINDINGS[version].items():
            if operation != "task-group.queue.page":
                assert other.paging_projection == (), (version, operation)


def test_impact_groups_exact_versions_only_when_bound_operation_matches() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    first: Any = _target(
        "3.4.1",
        "ProjectController.queryProjectListPaging",
        "same",
    )
    second: Any = _target(
        "3.4.2",
        "ProjectController.queryProjectListPaging",
        "same",
    )
    binding = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
    )
    bindings = {
        version: {
            "project.page": binding,
        }
        for version in ("3.4.1", "3.4.2")
    }

    report = impact.analyze_semantic_bindings(
        _inventory(first, second),
        bindings=bindings,
    )
    reversed_report = impact.analyze_semantic_bindings(
        _inventory(second, first),
        bindings=bindings,
    )

    assert report["complete"] is True
    assert report["schema_version"] == 2
    assert report["diagnostics"] == []
    assert report == reversed_report
    groups = report["semantic_operations"]["project.page"]["contract_groups"]
    assert len(groups) == 1
    assert groups[0]["versions"] == ["3.4.1", "3.4.2"]
    assert groups[0]["closure_fingerprint"].startswith("sha256:")


def test_impact_group_fingerprint_covers_consumed_nested_type_closure() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    first: Any = _target(
        "3.4.1",
        "ProjectController.queryProjectListPaging",
        "same-operation",
    )
    second: Any = _target(
        "3.4.2",
        "ProjectController.queryProjectListPaging",
        "same-operation",
    )
    first["surfaces"]["enums"] = [
        {"key": "example.ProjectState", "fingerprint": "old-enum"}
    ]
    second["surfaces"]["enums"] = [
        {"key": "example.ProjectState", "fingerprint": "new-enum"}
    ]
    binding = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
        type_closure=(impact.WireTypeRef("enums", "example.ProjectState"),),
    )

    report = impact.analyze_semantic_bindings(
        _inventory(first, second),
        bindings={
            "3.4.1": {"project.page": binding},
            "3.4.2": {"project.page": binding},
        },
    )

    groups = report["semantic_operations"]["project.page"]["contract_groups"]
    assert sorted(group["versions"] for group in groups) == [["3.4.1"], ["3.4.2"]]


def test_impact_group_fingerprint_covers_every_operation_in_resolution_chain() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    first: Any = _target(
        "3.4.1",
        "ProjectController.queryProjectListPaging",
        "same-page",
    )
    second: Any = _target(
        "3.4.2",
        "ProjectController.queryProjectListPaging",
        "same-page",
    )
    for target, detail_fingerprint in (
        (first, "old-detail"),
        (second, "new-detail"),
    ):
        target["surfaces"]["operations"].append(
            {
                "key": "ProjectController.queryProjectByCode",
                "fingerprint": detail_fingerprint,
                "http_method": "GET",
                "path": "projects/{code}",
            }
        )
    binding = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
    )

    report = impact.analyze_semantic_bindings(
        _inventory(first, second),
        bindings={
            "3.4.1": {"project.get": binding},
            "3.4.2": {"project.get": binding},
        },
    )

    groups = report["semantic_operations"]["project.get"]["contract_groups"]
    assert sorted(group["versions"] for group in groups) == [["3.4.1"], ["3.4.2"]]


def test_impact_reports_missing_bound_operation_without_hiding_other_versions() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    inventory = _inventory(
        _target("3.4.1", "ProjectController.queryProjectListPaging", "old"),
        _target("3.4.2", "ProjectController.queryAllProjectList", "new"),
    )
    binding = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
    )
    bindings = {
        version: {
            "project.page": binding,
        }
        for version in ("3.4.1", "3.4.2")
    }

    report = impact.analyze_semantic_bindings(inventory, bindings=bindings)

    assert report["complete"] is False
    successful = report["semantic_operations"]["project.page"]["bindings"]
    assert len(successful) == 1
    assert successful[0]["version"] == "3.4.1"
    assert successful[0]["source_operations"] == [
        {
            "key": "ProjectController.queryProjectListPaging",
            "fingerprint": "old",
            "http_method": "GET",
            "path": "projects",
        }
    ]
    assert successful[0]["type_closure"] == [
        {
            "surface": "models",
            "key": "example.Payload",
            "fingerprint": "same-payload",
        }
    ]
    assert report["diagnostics"] == [
        {
            "version": "3.4.2",
            "semantic_operation": "project.page",
            "source_operation": "ProjectController.queryProjectListPaging",
            "error": "source_operation_not_found",
        }
    ]


def test_impact_reports_a_missing_member_of_the_reviewed_type_closure() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    binding = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
        type_closure=(impact.WireTypeRef("models", "example.Project"),),
    )

    report = impact.analyze_semantic_bindings(
        _inventory(
            _target(
                "3.4.2",
                "ProjectController.queryProjectListPaging",
                "operation",
            )
        ),
        bindings={"3.4.2": {"project.page": binding}},
    )

    assert report["complete"] is False
    assert report["semantic_operations"]["project.page"]["bindings"] == []
    assert report["diagnostics"] == [
        {
            "version": "3.4.2",
            "semantic_operation": "project.page",
            "source_type": "models:example.Project",
            "error": "source_type_not_found",
        }
    ]


def test_reviewed_read_bindings_cover_every_exact_target() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")

    assert tuple(impact.READ_OPERATION_BINDINGS) == impact.REVIEWED_DS_VERSIONS
    assert {
        tuple(sorted(version_bindings))
        for version_bindings in impact.READ_OPERATION_BINDINGS.values()
    } == {
        (
            "project.get",
            "project.page",
            "workflow.get",
            "workflow.page",
        )
    }
    for version_bindings in impact.READ_OPERATION_BINDINGS.values():
        for semantic_operation, binding in version_bindings.items():
            assert isinstance(binding, impact.ReviewedBinding)
            assert binding.source_operations
            assert len(binding.source_operations) == len(set(binding.source_operations))
            assert binding.type_closure
            assert all(
                isinstance(type_ref, impact.WireTypeRef)
                for type_ref in binding.type_closure
            )
            assert binding.selector_semantics
            assert all(
                selector.exposed_identities and selector.resolution
                for selector in binding.selector_semantics
            )
            assert {source.kind for source in binding.evidence_sources} == {
                "controller",
                "ui",
            }
            assert all(source.reference for source in binding.evidence_sources)
            controller_evidence = next(
                source
                for source in binding.evidence_sources
                if source.kind == "controller"
            )
            assert ".java#" in controller_evidence.reference
            assert (
                len(binding.source_operations)
                == {
                    "project.page": 1,
                    "project.get": 2,
                    "workflow.page": 3,
                    "workflow.get": 5,
                }[semantic_operation]
            )
    assert (
        impact.WireTypeRef(
            "models",
            "org.apache.dolphinscheduler.api.utils.PageInfo",
        )
        in impact.READ_OPERATION_BINDINGS["1.3.9"]["project.page"].type_closure
    )
    assert (
        impact.WireTypeRef(
            "enums",
            "org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy",
        )
        in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].type_closure
    )
    assert (
        "SchedulerController.queryScheduleListPaging"
        in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].source_operations
    )
    assert (
        impact.WireTypeRef(
            "models",
            "org.apache.dolphinscheduler.api.vo.ScheduleVO",
        )
        in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].type_closure
    )
    assert (
        impact.WireTypeRef(
            "models",
            "org.apache.dolphinscheduler.api.vo.ScheduleVo",
        )
        in impact.READ_OPERATION_BINDINGS["3.2.0"]["workflow.get"].type_closure
    )
    assert (
        impact.WireTypeRef(
            "models",
            "org.apache.dolphinscheduler.api.vo.ScheduleVo",
        )
        not in impact.READ_OPERATION_BINDINGS["3.2.1"]["workflow.get"].type_closure
    )
    legacy_workflow_selector = next(
        selector
        for selector in impact.READ_OPERATION_BINDINGS["1.3.9"][
            "workflow.get"
        ].selector_semantics
        if selector.resource == "workflow"
    )
    current_workflow_selector = next(
        selector
        for selector in impact.READ_OPERATION_BINDINGS["3.4.2"][
            "workflow.get"
        ].selector_semantics
        if selector.resource == "workflow"
    )
    assert legacy_workflow_selector.parent_identity == "project_name"
    assert current_workflow_selector.parent_identity == "project_code"
    assert (
        legacy_workflow_selector.resolution
        == "direct-native-or-exact-name-via-workflow.refs"
    )
    assert (
        "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList"
        in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].source_operations
    )


def test_legacy_workflow_run_task_selector_is_name_native() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")

    legacy_task = next(
        selector
        for selector in impact._workflow_family_selectors("1.3.9", "workflow.run-task")
        if selector.resource == "task"
    )
    modern_task = next(
        selector
        for selector in impact._workflow_family_selectors("2.0.0", "workflow.run-task")
        if selector.resource == "task"
    )

    assert legacy_task.consumed_selectors == ("name",)
    assert legacy_task.exposed_identities == ("name",)
    assert legacy_task.native_identity == "name"
    assert legacy_task.resolution == "exact-name-within-legacy-workflow-graph"
    assert legacy_task.parent_identity == "process_definition_id"
    assert modern_task.consumed_selectors == ("name", "code")
    assert modern_task.native_identity == "code"
    assert (
        "WorkflowDefinitionController.queryWorkflowDefinitionListPaging"
        not in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].source_operations
    )
    assert (
        impact.WireTypeRef(
            "models",
            "generated.view."
            "WorkflowDefinitionServiceImpl_"
            "queryWorkflowDefinitionSimpleList_arrayNodeItem",
        )
        in impact.READ_OPERATION_BINDINGS["3.4.2"]["workflow.get"].type_closure
    )
    assert (
        "ProcessDefinitionController.queryProcessDefinitionSimpleList"
        in impact.READ_OPERATION_BINDINGS["3.2.2"]["workflow.get"].source_operations
    )
    assert (
        impact.WireTypeRef(
            "models",
            "generated.view."
            "ProcessDefinitionServiceImpl_"
            "queryProcessDefinitionSimpleList_arrayNodeItem",
        )
        in impact.READ_OPERATION_BINDINGS["3.2.2"]["workflow.get"].type_closure
    )
    assert all(
        impact.WireTypeRef(
            "models",
            "org.apache.dolphinscheduler.dao.entity.DependentSimplifyDefinition",
        )
        not in impact.READ_OPERATION_BINDINGS[version]["workflow.get"].type_closure
        for version in impact.REVIEWED_DS_VERSIONS[
            impact.REVIEWED_DS_VERSIONS.index("2.0.0") :
        ]
    )
    assert (
        impact.READ_OPERATION_BINDINGS["3.4.2"]["project.page"]
        .selector_semantics[0]
        .consumed_selectors
        == ()
    )


def test_impact_rejects_incomplete_reviewed_bindings() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    valid = _reviewed_binding(
        impact,
        "ProjectController.queryProjectListPaging",
    )
    duplicate_selector = replace(
        valid.selector_semantics[0],
        consumed_selectors=("name", "name"),
    )
    invalid_cases = (
        (
            replace(valid, source_operations=()),
            r"source_operations must contain at least 1",
        ),
        (replace(valid, type_closure=()), r"type_closure must not be empty"),
        (
            replace(valid, selector_semantics=()),
            r"selector_semantics must not be empty",
        ),
        (
            replace(valid, evidence_sources=()),
            r"evidence_sources must not be empty",
        ),
        (
            replace(
                valid,
                type_closure=(impact.WireTypeRef("invalid", "example.Payload"),),
            ),
            r"type_closure must contain non-empty WireTypeRef values",
        ),
        (
            replace(valid, selector_semantics=(duplicate_selector,)),
            r"consumed_selectors must not contain duplicates",
        ),
    )
    inventory = _inventory(
        _target(
            "3.4.2",
            "ProjectController.queryProjectListPaging",
            "operation",
        )
    )

    for binding, match in invalid_cases:
        with pytest.raises(ValueError, match=match):
            impact.analyze_semantic_bindings(
                inventory,
                bindings={"3.4.2": {"project.page": binding}},
            )


def test_impact_rejects_malformed_inventory_fingerprint() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    target: Any = _target(
        "3.4.1",
        "ProjectController.queryProjectListPaging",
        "fingerprint",
    )
    target["surfaces"]["operations"][0]["fingerprint"] = None

    with pytest.raises(
        TypeError,
        match=r"source operation\.fingerprint must be text",
    ):
        impact.analyze_semantic_bindings(
            _inventory(target),
            bindings={
                "3.4.1": {
                    "project.page": _reviewed_binding(
                        impact,
                        "ProjectController.queryProjectListPaging",
                    ),
                }
            },
        )


def _inventory(*targets: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "dolphinscheduler-source-contract-inventory",
        "complete": True,
        "diagnostics": [],
        "targets": list(targets),
    }


def _target(
    version: str,
    operation: str,
    fingerprint: str,
) -> dict[str, object]:
    return {
        "ds_version": version,
        "surfaces": {
            "dtos": [],
            "models": [
                {
                    "key": "example.Payload",
                    "fingerprint": "same-payload",
                }
            ],
            "enums": [],
            "operations": [
                {
                    "key": operation,
                    "fingerprint": fingerprint,
                    "http_method": "GET",
                    "path": "projects",
                }
            ],
        },
    }


def _reviewed_binding(
    impact: Any,
    *source_operations: str,
    type_closure: tuple[Any, ...] | None = None,
) -> Any:
    return impact.ReviewedBinding(
        source_operations=source_operations,
        type_closure=(
            (impact.WireTypeRef("models", "example.Payload"),)
            if type_closure is None
            else type_closure
        ),
        selector_semantics=(
            impact.SelectorSemantics(
                resource="project",
                consumed_selectors=("name", "code"),
                exposed_identities=("name", "code"),
                native_identity="code",
                resolution="test-resolution",
            ),
        ),
        evidence_sources=(
            impact.EvidenceSource("controller", "ProjectController.java#method"),
            impact.EvidenceSource("ui", "projects/index.ts"),
        ),
    )
