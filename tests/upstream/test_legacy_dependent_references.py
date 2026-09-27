from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from dsctl.models.workflow_spec import WorkflowSpec, validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.legacy_dependent_references import (
    LegacyDependentReferenceResolver,
    LegacyDependentTargetResolutionError,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyDependentRefIndex,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot


def test_authoring_resolves_cross_project_workflow_and_task_names() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        task_names={202: ("extract", "publish")},
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.create",
    ).resolve_authoring(_dependent_spec())

    assert refs.native_for_selector(
        "analytics",
        "upstream-daily",
        None,
    ) == (7, 101, "ALL")
    assert refs.native_for_selector(
        "warehouse",
        "load-dimensions",
        "publish",
    ) == (8, 202, "publish")
    assert operations.project_name_reads == ["analytics", "warehouse"]
    assert operations.workflow_name_reads == [
        (7, "upstream-daily"),
        (8, "load-dimensions"),
    ]
    assert operations.loaded_workflow_ids == [202]


def test_authoring_rejects_missing_task_before_compilation() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        task_names={202: ("extract",)},
    )

    with pytest.raises(LegacyDependentTargetResolutionError) as raised:
        LegacyDependentReferenceResolver(
            operations,
            action="workflow.create",
        ).resolve_authoring(_dependent_spec())

    assert raised.value.project_name == "warehouse"
    assert raised.value.workflow_name == "load-dimensions"
    assert raised.value.task_name == "publish"
    assert "does not exist" in str(raised.value.cause)


def test_authoring_keeps_numeric_looking_project_and_workflow_names_literal() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"123": 7, "789": 8},
        workflows={(7, "456"): 101, (8, "987"): 202},
        task_names={202: ("publish",)},
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.create",
    ).resolve_authoring(
        _dependent_spec(
            workflow_project="123",
            workflow_name="456",
            task_project="789",
            task_workflow="987",
        )
    )

    assert refs.native_for_selector("123", "456", None) == (7, 101, "ALL")
    assert refs.native_for_selector("789", "987", "publish") == (
        8,
        202,
        "publish",
    )


def test_reverse_binding_returns_only_targets_proved_by_visible_inventory() -> None:
    graph = _decoded_dependent_graph()
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        task_names={202: ("extract",)},
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.read",
    ).reverse_bind(graph)

    assert refs.selector_for_native(7, 101, "ALL") == (
        "analytics",
        "upstream-daily",
        None,
    )
    assert refs.selector_for_native(8, 202, "publish") is None


def test_reverse_binding_rejects_unrequested_project_name_collision() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        task_names={202: ("publish",)},
        project_inventory=(
            ProjectRef(NativeId(7), "analytics", None),
            ProjectRef(NativeId(70), "analytics", None),
            ProjectRef(NativeId(8), "warehouse", None),
        ),
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.read",
    ).reverse_bind(_decoded_dependent_graph())

    assert refs.selector_for_native(7, 101, "ALL") is None
    assert refs.selector_for_native(8, 202, "publish") == (
        "warehouse",
        "load-dimensions",
        "publish",
    )


def test_reverse_binding_rejects_unrequested_workflow_name_collision() -> None:
    analytics = ProjectRef(NativeId(7), "analytics", None)
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        workflow_inventories={
            7: (
                _scope(analytics, 101, "upstream-daily").workflow,
                _scope(analytics, 999, "upstream-daily").workflow,
            )
        },
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.read",
    ).reverse_bind(_decoded_dependent_graph())

    assert refs.selector_for_native(7, 101, "ALL") is None


def test_reverse_binding_rejects_project_identity_change_during_read() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        resolved_projects={"analytics": 70, "warehouse": 8},
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.read",
    ).reverse_bind(_decoded_dependent_graph())

    assert refs.selector_for_native(7, 101, "ALL") is None


def test_reverse_binding_rejects_whole_workflow_identity_change_during_read() -> None:
    operations = FakeLegacyDependentOperations(
        projects={"analytics": 7, "warehouse": 8},
        workflows={
            (7, "upstream-daily"): 101,
            (8, "load-dimensions"): 202,
        },
        resolved_workflows={
            (7, "upstream-daily"): 999,
            (8, "load-dimensions"): 202,
        },
    )

    refs = LegacyDependentReferenceResolver(
        operations,
        action="workflow.read",
    ).reverse_bind(_decoded_dependent_graph())

    assert refs.selector_for_native(7, 101, "ALL") is None


def _dependent_spec(
    *,
    workflow_project: str = "analytics",
    workflow_name: str = "upstream-daily",
    task_project: str = "warehouse",
    task_workflow: str = "load-dimensions",
) -> WorkflowSpec:
    return validate_workflow_document(
        {
            "workflow": {"name": "downstream", "project": "orchestration"},
            "tasks": [
                {
                    "name": "wait-upstream",
                    "type": "DEPENDENT",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "AND",
                                    "dependItemList": [
                                        {
                                            "dependentType": "DEPENDENT_ON_WORKFLOW",
                                            "projectName": workflow_project,
                                            "workflowName": workflow_name,
                                            "cycle": "day",
                                            "dateValue": "today",
                                        },
                                        {
                                            "dependentType": "DEPENDENT_ON_TASK",
                                            "projectName": task_project,
                                            "workflowName": task_workflow,
                                            "taskName": "publish",
                                            "cycle": "day",
                                            "dateValue": "last1Days",
                                        },
                                    ],
                                }
                            ],
                        }
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("1.3.9"),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _decoded_dependent_graph() -> DecodedLegacyWorkflowGraph:
    native_refs = LegacyDependentRefIndex.from_native_by_selector(
        {
            ("analytics", "upstream-daily", None): (7, 101, "ALL"),
            ("warehouse", "load-dimensions", "publish"): (
                8,
                202,
                "publish",
            ),
        }
    )
    payload = prepare_legacy_workflow_graph(
        _dependent_spec(),
        task_id_factory=lambda _name: "tasks-dependent",
        dependent_refs=native_refs,
    ).materialize()
    return decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
    )


@dataclass
class FakeLegacyDependentOperations:
    projects: dict[str, int]
    workflows: dict[tuple[int, str], int]
    task_names: dict[int, tuple[str, ...]] = field(default_factory=dict)
    project_inventory: tuple[ProjectRef, ...] | None = None
    workflow_inventories: dict[int, tuple[WorkflowRef, ...]] = field(
        default_factory=dict
    )
    resolved_projects: dict[str, int] | None = None
    resolved_workflows: dict[tuple[int, str], int] | None = None
    project_name_reads: list[str] = field(default_factory=list)
    workflow_name_reads: list[tuple[int, str]] = field(default_factory=list)
    loaded_workflow_ids: list[int] = field(default_factory=list)

    def resolve_project_by_name(self, project_name: str) -> ProjectRef:
        self.project_name_reads.append(project_name)
        project_id = (self.resolved_projects or self.projects)[project_name]
        return ProjectRef(NativeId(project_id), project_name, None)

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        if self.project_inventory is not None:
            return self.project_inventory
        return tuple(
            ProjectRef(NativeId(project_id), project_name, None)
            for project_name, project_id in self.projects.items()
        )

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        self.workflow_name_reads.append((project.native.value, workflow_name))
        workflow_id = (self.resolved_workflows or self.workflows)[
            (project.native.value, workflow_name)
        ]
        return _scope(project, workflow_id, workflow_name)

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        if project.native.value in self.workflow_inventories:
            return self.workflow_inventories[project.native.value]
        return tuple(
            _scope(project, workflow_id, workflow_name).workflow
            for (project_id, workflow_name), workflow_id in self.workflows.items()
            if project_id == project.native.value
        )

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        del action
        workflow_id = scope.workflow.native.value
        self.loaded_workflow_ids.append(workflow_id)
        tasks = [
            {
                "id": f"tasks-{index}",
                "name": task_name,
                "type": "SHELL",
                "params": {"rawScript": "true", "resourceList": []},
                "preTasks": [],
            }
            for index, task_name in enumerate(
                self.task_names.get(workflow_id, ()),
                start=1,
            )
        ]
        return LegacyWorkflowDefinitionSnapshot(
            scope=scope,
            name=scope.workflow.name,
            description=None,
            release_state="ONLINE",
            process_definition_json=json.dumps({"tasks": tasks}),
            locations="{}",
            connects="[]",
        )


def _scope(
    project: ProjectRef,
    workflow_id: int,
    workflow_name: str,
) -> WorkflowScope:
    ref = WorkflowRef(NativeId(workflow_id), workflow_name, 1)
    view = WorkflowView(
        ref=ref,
        project_native=project.native,
        id=workflow_id,
        description=None,
        global_params=None,
        global_param_map=None,
        create_time=None,
        update_time=None,
        user_id=1,
        user_name="alice",
        project_name=project.name,
        timeout=0,
        release_state="ONLINE",
        execution_type=None,
        include_execution_type=False,
    )
    return WorkflowScope(project=project, workflow=ref, view=view)
