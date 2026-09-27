from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import NotFoundError
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)
from dsctl.upstream.dynamic_workflow_references import (
    MAX_DYNAMIC_DESCENDANT_WORKFLOWS,
    DynamicChildWorkflowResolutionError,
    DynamicWorkflowCycleError,
    DynamicWorkflowLimitError,
    DynamicWorkflowReadError,
    DynamicWorkflowReferenceAuditor,
    DynamicWorkflowReferenceResolver,
    DynamicWorkflowShapeError,
    dynamic_workflow_codes_from_dag,
)

if TYPE_CHECKING:
    from dsctl.upstream.definition_models import WorkflowView
    from dsctl.upstream.protocol import WorkflowDagRecord


_PROJECT = ProjectRef(NativeCode(7), "etl", None)


@dataclass(frozen=True)
class _Task:
    taskType: str  # noqa: N815
    taskParams: object  # noqa: N815


@dataclass(frozen=True)
class _Dag:
    taskDefinitionList: tuple[_Task, ...] = ()  # noqa: N815


@dataclass
class _FakeOperations:
    dags: dict[int, _Dag]
    names_by_code: dict[int, str] = field(default_factory=dict)
    inventory: tuple[WorkflowRef, ...] | None = None
    missing: set[int] = field(default_factory=set)
    resolved_codes: list[int] = field(default_factory=list)
    dag_codes: list[int] = field(default_factory=list)
    inventory_reads: int = 0

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        assert project == _PROJECT
        self.inventory_reads += 1
        if self.inventory is not None:
            return self.inventory
        return tuple(
            WorkflowRef(NativeCode(code), name, 1)
            for code, name in self.names_by_code.items()
        )

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        assert project == _PROJECT
        self.resolved_codes.append(workflow_code)
        if workflow_code in self.missing:
            message = f"workflow {workflow_code} missing"
            raise NotFoundError(message)
        return self._scope(
            workflow_code,
            self.names_by_code.get(workflow_code, f"workflow-{workflow_code}"),
        )

    def _scope(self, workflow_code: int, workflow_name: str) -> WorkflowScope:
        return WorkflowScope(
            project=_PROJECT,
            workflow=WorkflowRef(
                NativeCode(workflow_code),
                workflow_name,
                1,
            ),
            view=cast("WorkflowView", object()),
        )

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        assert action == "workflow.edit"
        native = scope.workflow.native
        assert isinstance(native, NativeCode)
        self.dag_codes.append(native.value)
        return cast("WorkflowDagRecord", self.dags.get(native.value, _Dag()))


def _nested(task_type: str, code: int) -> _Task:
    return _Task(task_type, json.dumps({"processDefinitionCode": code}))


def test_auditor_proves_mixed_nested_closure_once_in_runtime_order() -> None:
    operations = _FakeOperations(
        dags={
            101: _Dag((_nested("DYNAMIC", 102), _nested("SUB_PROCESS", 103))),
            102: _Dag((_nested("DYNAMIC", 104),)),
            103: _Dag((_nested("SUB_PROCESS", 104),)),
            104: _Dag(),
        }
    )

    DynamicWorkflowReferenceAuditor(
        operations,
        project=_PROJECT,
        action="workflow.edit",
    ).audit((101, 103), containing_workflow_code=99)

    assert operations.resolved_codes == [101, 102, 104, 103]
    assert operations.dag_codes == [101, 102, 104, 103]


def test_auditor_rejects_direct_reference_to_containing_workflow_without_read() -> None:
    operations = _FakeOperations(dags={})

    with pytest.raises(DynamicWorkflowCycleError) as raised:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=_PROJECT,
            action="workflow.edit",
        ).audit((99,), containing_workflow_code=99)

    assert raised.value.cycle == (99, 99)
    assert str(raised.value) == "99 -> 99"
    assert operations.resolved_codes == []


def test_auditor_reports_limit_before_reading_the_1001st_workflow() -> None:
    operations = _FakeOperations(
        dags={
            code: _Dag((_nested("DYNAMIC", code + 1),))
            for code in range(1, MAX_DYNAMIC_DESCENDANT_WORKFLOWS + 1)
        }
    )

    with pytest.raises(DynamicWorkflowLimitError) as raised:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=_PROJECT,
            action="workflow.edit",
        ).audit((1,), containing_workflow_code=2_000)

    assert raised.value.limit == MAX_DYNAMIC_DESCENDANT_WORKFLOWS
    assert len(operations.resolved_codes) == MAX_DYNAMIC_DESCENDANT_WORKFLOWS
    assert len(operations.dag_codes) == MAX_DYNAMIC_DESCENDANT_WORKFLOWS


def test_auditor_keeps_the_native_cause_for_a_failed_descendant_read() -> None:
    operations = _FakeOperations(
        dags={101: _Dag((_nested("SUB_PROCESS", 404),))},
        missing={404},
    )

    with pytest.raises(DynamicWorkflowReadError) as raised:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=_PROJECT,
            action="workflow.edit",
        ).audit((101,), containing_workflow_code=99)

    assert raised.value.workflow_code == 404
    assert isinstance(raised.value.cause, NotFoundError)


def test_auditor_reports_the_containing_child_for_an_invalid_nested_code() -> None:
    operations = _FakeOperations(
        dags={101: _Dag((_Task("SUB_PROCESS", {"processDefinitionCode": False}),))}
    )

    with pytest.raises(DynamicWorkflowShapeError) as raised:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=_PROJECT,
            action="workflow.edit",
        ).audit((101,), containing_workflow_code=99)

    assert raised.value.workflow_code == 101
    assert raised.value.reason == (
        "SUB_PROCESS.processDefinitionCode is not a positive workflow code"
    )


@pytest.mark.parametrize("task_params", ["[]", {1: "not-a-string-key"}])
def test_auditor_classifies_non_object_task_params_as_shape_error(
    task_params: object,
) -> None:
    operations = _FakeOperations(dags={101: _Dag((_Task("DYNAMIC", task_params),))})

    with pytest.raises(DynamicWorkflowShapeError) as raised:
        DynamicWorkflowReferenceAuditor(
            operations,
            project=_PROJECT,
            action="workflow.edit",
        ).audit((101,), containing_workflow_code=99)

    assert raised.value.workflow_code == 101
    assert raised.value.reason == "DYNAMIC.taskParams is not a JSON object"


def test_resolve_authoring_keeps_numeric_names_literal_with_one_inventory_read() -> (
    None
):
    operations = _FakeOperations(
        dags={},
        names_by_code={101: "child-orders", 202: "123"},
    )

    refs = DynamicWorkflowReferenceResolver(
        operations,
        project=_PROJECT,
    ).resolve_authoring(("123", "child-orders", "123"))

    assert dict(refs.code_by_name) == {"123": 202, "child-orders": 101}
    assert dict(refs.name_by_code) == {202: "123", 101: "child-orders"}
    assert operations.inventory_reads == 1
    assert operations.resolved_codes == []


def test_resolve_authoring_keeps_selector_and_cause_for_strict_failure() -> None:
    operations = _FakeOperations(dags={}, names_by_code={})

    with pytest.raises(DynamicChildWorkflowResolutionError) as raised:
        DynamicWorkflowReferenceResolver(
            operations,
            project=_PROJECT,
        ).resolve_authoring(("missing-child",))

    assert raised.value.selector == "missing-child"
    assert isinstance(raised.value.cause, ValueError)


@pytest.mark.parametrize(
    "inventory",
    [
        (
            WorkflowRef(NativeCode(101), "ambiguous", 1),
            WorkflowRef(NativeCode(202), "ambiguous", 1),
        ),
        (WorkflowRef(NativeId(101), "legacy-id", 1),),
    ],
)
def test_resolve_authoring_rejects_duplicate_names_and_non_code_identities(
    inventory: tuple[WorkflowRef, ...],
) -> None:
    selector = inventory[0].name
    assert selector is not None
    operations = _FakeOperations(dags={}, inventory=inventory)

    with pytest.raises(DynamicChildWorkflowResolutionError) as raised:
        DynamicWorkflowReferenceResolver(
            operations,
            project=_PROJECT,
        ).resolve_authoring((selector,))

    assert raised.value.selector == selector
    assert isinstance(raised.value.cause, ValueError)
    assert operations.inventory_reads == 1


def test_reverse_bind_uses_one_visible_inventory_without_detail_reads() -> None:
    operations = _FakeOperations(
        dags={},
        names_by_code={101: "child-orders", 202: "123", 303: "unrequested"},
    )

    refs = DynamicWorkflowReferenceResolver(
        operations,
        project=_PROJECT,
    ).reverse_bind((202, 101, 202))

    assert dict(refs.code_by_name) == {"child-orders": 101, "123": 202}
    assert dict(refs.name_by_code) == {101: "child-orders", 202: "123"}
    assert operations.inventory_reads == 1
    assert operations.resolved_codes == []


def test_dag_prefilter_collects_positive_dynamic_codes_without_decoding_policy() -> (
    None
):
    dag = _Dag(
        (
            _Task(
                "DYNAMIC",
                json.dumps({"processDefinitionCode": 202, "futureField": True}),
            ),
            _Task("DYNAMIC", {"processDefinitionCode": 101}),
            _Task("DYNAMIC", "not-json"),
            _Task("DYNAMIC", []),
            _Task("DYNAMIC", {"processDefinitionCode": False}),
            _Task("SUB_PROCESS", {"processDefinitionCode": 303}),
            _Task("DYNAMIC", {"processDefinitionCode": 202}),
            _Task("SHELL", {"processDefinitionCode": 404}),
        )
    )

    assert dynamic_workflow_codes_from_dag(cast("WorkflowDagRecord", dag)) == (202, 101)
