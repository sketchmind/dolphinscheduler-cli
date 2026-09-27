from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import NotFoundError, ResolutionError
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    PreparedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.legacy_workflow_references import (
    MAX_LEGACY_DESCENDANT_WORKFLOWS,
    LegacyDescendantWorkflowError,
    LegacyNestedWorkflowCycleError,
    LegacyNestedWorkflowLimitError,
    LegacyWorkflowAuthoringResolution,
    LegacyWorkflowReferenceResolver,
)
from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue

_PROJECT = ProjectRef(native=NativeId(7), name="etl", description=None)


def test_runtime_audit_follows_process_definition_id_in_any_task_params() -> None:
    operations = FakeLegacyWorkflowReferenceOperations(adjacency={101: ()})
    resolver = _resolver(operations)

    resolver.audit_compilation(
        _compilation_with_runtime_edge(101, task_type="SHELL"),
        containing_workflow_id=None,
        resolution=LegacyWorkflowAuthoringResolution.empty(),
    )

    assert operations.resolved_ids == [101]
    assert operations.loaded_ids == [101]


@pytest.mark.parametrize(
    ("wire_value", "workflow_id"),
    [
        ("1", 1),
        ("+1", 1),
        ("1,000", 1_000),
        ("1.000", 1),
        (True, 1),
        (4_294_967_297, 1),
        (1.2345678901234568e18, 2_112_454_944),
        ("\u0661", 1),
        ({"andIncrement": 1, "andDecrement": 2}, 2),
        ({"andDecrement": 2, "andIncrement": 1}, 1),
        ({"andIncrement": 0, "andDecrement": 2}, 2),
    ],
)
def test_runtime_audit_accepts_fastjson_integer_coercions(
    wire_value: JsonValue,
    workflow_id: int,
) -> None:
    operations = FakeLegacyWorkflowReferenceOperations(adjacency={workflow_id: ()})

    _resolver(operations).audit_compilation(
        _compilation_with_runtime_edge(wire_value),
        containing_workflow_id=None,
        resolution=LegacyWorkflowAuthoringResolution.empty(),
    )

    assert operations.resolved_ids == [workflow_id]


@pytest.mark.parametrize(
    "workflow_id",
    [
        " 1 ",
        "1_0",
        "1.1",
        "\U0001d7d9",
        False,
        {"andIncrement": 1, "andDecrement": None},
    ],
)
def test_runtime_audit_rejects_fastjson_invalid_or_nonpositive_coercions(
    workflow_id: JsonValue,
) -> None:
    operations = FakeLegacyWorkflowReferenceOperations(adjacency={1: ()})

    with pytest.raises(LegacyDescendantWorkflowError) as raised:
        _resolver(operations).audit_compilation(
            _compilation_with_runtime_edge(workflow_id),
            containing_workflow_id=None,
            resolution=LegacyWorkflowAuthoringResolution.empty(),
        )

    assert raised.value.workflow_id is None
    assert isinstance(raised.value.cause, LegacyWorkflowGraphError)
    assert operations.resolved_ids == []


@pytest.mark.parametrize(
    "wire_value",
    [None, "", "null", "NULL", {"andIncrement": None, "andDecrement": 2}],
)
def test_runtime_audit_ignores_fastjson_null_integer_coercions(
    wire_value: JsonValue,
) -> None:
    operations = FakeLegacyWorkflowReferenceOperations(adjacency={1: ()})

    _resolver(operations).audit_compilation(
        _compilation_with_runtime_edge(wire_value),
        containing_workflow_id=None,
        resolution=LegacyWorkflowAuthoringResolution.empty(),
    )

    assert operations.resolved_ids == []


def test_runtime_audit_reports_structured_limit_for_1001_deep_chain() -> None:
    operations = FakeLegacyWorkflowReferenceOperations(chain_length=1_001)
    resolver = _resolver(operations)

    with pytest.raises(LegacyNestedWorkflowLimitError) as raised:
        resolver.audit_compilation(
            _compilation_with_runtime_edge(1),
            containing_workflow_id=None,
            resolution=LegacyWorkflowAuthoringResolution.empty(),
        )

    assert raised.value.limit == MAX_LEGACY_DESCENDANT_WORKFLOWS


def test_runtime_audit_rejects_indirect_cycle() -> None:
    operations = FakeLegacyWorkflowReferenceOperations(adjacency={1: (2,), 2: (1,)})
    resolver = _resolver(operations)

    with pytest.raises(LegacyNestedWorkflowCycleError) as raised:
        resolver.audit_compilation(
            _compilation_with_runtime_edge(1),
            containing_workflow_id=None,
            resolution=LegacyWorkflowAuthoringResolution.empty(),
        )

    assert raised.value.cycle == (1, 2, 1)


def test_runtime_audit_identifies_missing_descendant() -> None:
    operations = FakeLegacyWorkflowReferenceOperations(
        adjacency={1: (999,)},
        missing_ids={999},
    )
    resolver = _resolver(operations)

    with pytest.raises(LegacyDescendantWorkflowError) as raised:
        resolver.audit_compilation(
            _compilation_with_runtime_edge(1),
            containing_workflow_id=None,
            resolution=LegacyWorkflowAuthoringResolution.empty(),
        )

    assert raised.value.workflow_id == 999
    assert isinstance(raised.value.cause, NotFoundError)


def test_reverse_binding_fails_closed_for_duplicate_child_names() -> None:
    graph = _decoded_graph(0, (101, 202))
    operations = FakeLegacyWorkflowReferenceOperations(
        adjacency={101: (), 202: ()},
        names_by_id={101: "duplicate", 202: "duplicate"},
    )

    refs = _resolver(operations).reverse_bind_immediate(
        graph,
        containing_workflow_id=None,
    )

    assert refs.name_for_id(101) is None
    assert refs.name_for_id(202) is None
    assert operations.inventory_reads == 1
    assert operations.resolved_ids == []


def test_reverse_binding_fails_closed_for_placeholder_child_name() -> None:
    graph = _decoded_graph(0, (101,))
    operations = FakeLegacyWorkflowReferenceOperations(
        adjacency={101: ()},
        names_by_id={101: "${child}"},
    )

    refs = _resolver(operations).reverse_bind_immediate(
        graph,
        containing_workflow_id=None,
    )

    assert refs.name_for_id(101) is None
    assert operations.inventory_reads == 1


@dataclass
class FakeLegacyWorkflowReferenceOperations:
    adjacency: dict[int, tuple[int, ...]] = field(default_factory=dict)
    names_by_id: dict[int, str] = field(default_factory=dict)
    missing_ids: set[int] = field(default_factory=set)
    chain_length: int | None = None
    resolved_ids: list[int] = field(default_factory=list)
    loaded_ids: list[int] = field(default_factory=list)
    inventory_reads: int = 0

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        matches = [
            workflow_id
            for workflow_id, name in self.names_by_id.items()
            if name == workflow_name
        ]
        if len(matches) != 1:
            message = f"Workflow name {workflow_name!r} is ambiguous"
            raise ResolutionError(message)
        return _scope(project, matches[0], workflow_name)

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        self.inventory_reads += 1
        workflow_ids = self._known_ids()
        return tuple(
            _scope(
                project,
                workflow_id,
                self.names_by_id.get(workflow_id, f"workflow-{workflow_id}"),
            ).workflow
            for workflow_id in sorted(workflow_ids)
            if workflow_id not in self.missing_ids
        )

    def resolve_workflow_by_id(
        self,
        project: ProjectRef,
        workflow_id: int,
    ) -> WorkflowScope:
        self.resolved_ids.append(workflow_id)
        if workflow_id in self.missing_ids or not self._is_known(workflow_id):
            message = f"Workflow id {workflow_id} was not found"
            raise NotFoundError(message)
        return _scope(
            project,
            workflow_id,
            self.names_by_id.get(workflow_id, f"workflow-{workflow_id}"),
        )

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        del action
        workflow_id = scope.workflow.native.value
        self.loaded_ids.append(workflow_id)
        process_definition_json, locations, connects = _legacy_payload(
            workflow_id,
            self._children(workflow_id),
        )
        return LegacyWorkflowDefinitionSnapshot(
            scope=scope,
            name=scope.workflow.name,
            description=None,
            release_state="ONLINE",
            process_definition_json=process_definition_json,
            locations=locations,
            connects=connects,
        )

    def _is_known(self, workflow_id: int) -> bool:
        if self.chain_length is not None:
            return 1 <= workflow_id <= self.chain_length
        return workflow_id in (self._known_ids())

    def _known_ids(self) -> set[int]:
        if self.chain_length is not None:
            return set(range(1, self.chain_length + 1))
        return (
            set(self.adjacency)
            | {child for children in self.adjacency.values() for child in children}
            | set(self.names_by_id)
        )

    def _children(self, workflow_id: int) -> tuple[int, ...]:
        if self.chain_length is not None:
            if workflow_id < self.chain_length:
                return (workflow_id + 1,)
            return ()
        return self.adjacency.get(workflow_id, ())


def _resolver(
    operations: FakeLegacyWorkflowReferenceOperations,
) -> LegacyWorkflowReferenceResolver:
    return LegacyWorkflowReferenceResolver(
        operations,
        project=_PROJECT,
        action="workflow edit",
    )


def _compilation_with_runtime_edge(
    child_id: JsonValue,
    *,
    task_type: str = "SUB_PROCESS",
) -> PreparedLegacyWorkflowGraph:
    graph = _decoded_graph(0, (child_id,), task_type=task_type)
    return prepare_legacy_workflow_graph(
        graph.to_workflow_spec(name="parent", project="etl"),
        baseline=graph,
    )


def _decoded_graph(
    workflow_id: int,
    child_ids: tuple[JsonValue, ...],
    *,
    task_type: str = "SUB_PROCESS",
) -> DecodedLegacyWorkflowGraph:
    process_definition_json, locations, connects = _legacy_payload(
        workflow_id,
        child_ids,
        task_type=task_type,
    )
    return decode_legacy_workflow_graph(
        process_definition_json,
        locations,
        connects,
    )


def _legacy_payload(
    workflow_id: int,
    child_ids: tuple[JsonValue, ...],
    *,
    task_type: str = "SUB_PROCESS",
) -> tuple[str, str, str]:
    tasks: list[dict[str, object]] = []
    locations: dict[str, dict[str, object]] = {}
    for index, child_id in enumerate(child_ids):
        task_id = f"task-{workflow_id}-{index}"
        task_name = f"child-{workflow_id}-{index}"
        params: dict[str, object] = {"processDefinitionId": child_id}
        if task_type == "SHELL":
            params.update(
                {
                    "rawScript": "echo opaque",
                    "resourceList": [],
                    "localParams": [],
                }
            )
        tasks.append(
            {
                "id": task_id,
                "name": task_name,
                "type": task_type,
                "params": params,
                "preTasks": [],
            }
        )
        locations[task_id] = {
            "name": task_name,
            "targetarr": "",
            "nodenumber": 0,
            "x": index * 300,
            "y": 0,
        }
    if not tasks:
        task_id = f"task-{workflow_id}-terminal"
        task_name = f"terminal-{workflow_id}"
        tasks.append(
            {
                "id": task_id,
                "name": task_name,
                "type": "SHELL",
                "params": {
                    "rawScript": "echo done",
                    "resourceList": [],
                    "localParams": [],
                },
                "preTasks": [],
            }
        )
        locations[task_id] = {
            "name": task_name,
            "targetarr": "",
            "nodenumber": 0,
            "x": 0,
            "y": 0,
        }
    return (
        json.dumps(
            {
                "globalParams": [],
                "tasks": tasks,
                "timeout": 0,
                "tenantId": -1,
            },
            separators=(",", ":"),
        ),
        json.dumps(locations, separators=(",", ":")),
        "[]",
    )


def _scope(project: ProjectRef, workflow_id: int, name: str) -> WorkflowScope:
    ref = WorkflowRef(native=NativeId(workflow_id), name=name, version=1)
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
