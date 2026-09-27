from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import ApiTransportError, NotFoundError, UserInputError
from dsctl.services._dynamic_workflow_references import (
    resolve_dynamic_authoring_workflow_refs,
    resolve_read_dynamic_workflow_refs,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)

if TYPE_CHECKING:
    from dsctl.upstream.definition_models import WorkflowView
    from dsctl.upstream.protocol import WorkflowDagRecord


_PROJECT = ProjectRef(NativeCode(7), "etl", None)


@dataclass(frozen=True)
class _Task:
    taskType: str  # noqa: N815
    taskParams: str  # noqa: N815


@dataclass(frozen=True)
class _Dag:
    taskDefinitionList: tuple[_Task, ...] = ()  # noqa: N815


@dataclass
class _FakeOperations:
    names_by_code: dict[int, str]
    dags: dict[int, _Dag]
    missing: set[int] = field(default_factory=set)
    inventory_reads: int = 0
    resolved_codes: list[int] = field(default_factory=list)
    events: list[str] = field(default_factory=list)

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        assert project == _PROJECT
        self.inventory_reads += 1
        self.events.append("inventory")
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
        self.events.append(f"resolve:{workflow_code}")
        if workflow_code in self.missing:
            message = f"workflow {workflow_code} missing"
            raise NotFoundError(
                message,
                details={
                    "resource": "workflow",
                    "action": "workflow.edit",
                    "reason": "lower-level-reason",
                    "native_detail": "kept",
                },
            )
        return WorkflowScope(
            project=project,
            workflow=WorkflowRef(
                NativeCode(workflow_code),
                self.names_by_code.get(workflow_code),
                1,
            ),
            view=cast("WorkflowView", object()),
        )

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        self.events.append(f"dag:{scope.workflow.native.value}:{action}")
        return cast(
            "WorkflowDagRecord",
            self.dags.get(scope.workflow.native.value, _Dag()),
        )


def _nested(task_type: str, code: int) -> _Task:
    return _Task(task_type, json.dumps({"processDefinitionCode": code}))


def test_dynamic_service_resolves_all_literal_names_from_one_inventory() -> None:
    operations = _FakeOperations(
        names_by_code={101: "child-orders", 202: "123"},
        dags={},
    )

    refs = resolve_dynamic_authoring_workflow_refs(
        operations,
        project=_PROJECT,
        child_workflow_names=("123", "child-orders"),
        containing_workflow_code=None,
        audit_descendants=True,
        action="workflow.create",
    )

    assert dict(refs.code_by_name) == {"123": 202, "child-orders": 101}
    assert operations.inventory_reads == 1
    assert operations.events == [
        "inventory",
        "resolve:202",
        "dag:202:workflow.create",
        "resolve:101",
        "dag:101:workflow.create",
    ]


def test_dynamic_no_change_resolves_names_without_auditing_descendants() -> None:
    operations = _FakeOperations(
        names_by_code={101: "child-orders"},
        dags={101: _Dag((_nested("DYNAMIC", 101),))},
    )

    refs = resolve_dynamic_authoring_workflow_refs(
        operations,
        project=_PROJECT,
        child_workflow_names=("child-orders",),
        containing_workflow_code=-1,
        audit_descendants=False,
        action="workflow.edit",
    )

    assert dict(refs.code_by_name) == {"child-orders": 101}
    assert operations.events == ["inventory"]


@pytest.mark.parametrize("audit_descendants", [False, True])
def test_dynamic_empty_names_require_neither_reads_nor_parent_validation(
    *,
    audit_descendants: bool,
) -> None:
    operations = _FakeOperations(names_by_code={}, dags={})
    for dependency in (operations, None):
        refs = resolve_dynamic_authoring_workflow_refs(
            dependency,
            project=_PROJECT,
            child_workflow_names=(),
            containing_workflow_code=-1,
            audit_descendants=audit_descendants,
            action="workflow.create",
        )

        assert dict(refs.code_by_name) == {}
    assert operations.events == []


def test_dynamic_resolution_failure_precedes_parent_validation_and_audit() -> None:
    operations = _FakeOperations(names_by_code={101: "child-orders"}, dags={})

    with pytest.raises(UserInputError, match="'missing-child' was not found") as raised:
        resolve_dynamic_authoring_workflow_refs(
            operations,
            project=_PROJECT,
            child_workflow_names=("child-orders", "missing-child"),
            containing_workflow_code=-1,
            audit_descendants=True,
            action="workflow.edit",
        )

    assert raised.value.details["reason"] == "dynamic-child-workflow-resolution"
    assert operations.events == ["inventory"]


def test_dynamic_parent_validation_follows_resolution_before_descendant_reads() -> None:
    operations = _FakeOperations(names_by_code={101: "child-orders"}, dags={})

    with pytest.raises(ValueError, match="containing workflow must use a positive"):
        resolve_dynamic_authoring_workflow_refs(
            operations,
            project=_PROJECT,
            child_workflow_names=("child-orders",),
            containing_workflow_code=-1,
            audit_descendants=True,
            action="workflow.edit",
        )

    assert operations.events == ["inventory"]


def test_dynamic_required_names_fail_when_definition_reads_are_unavailable() -> None:
    with pytest.raises(
        ApiTransportError, match="no workflow-definition reads"
    ) as raised:
        resolve_dynamic_authoring_workflow_refs(
            None,
            project=_PROJECT,
            child_workflow_names=("child-orders",),
            containing_workflow_code=-1,
            audit_descendants=True,
            action="workflow-instance.edit",
            boundary_resource="workflow-instance",
        )

    assert raised.value.details == {
        "child_workflow_names": ["child-orders"],
        "resource": "workflow-instance",
        "action": "workflow-instance.edit",
        "reason": "dynamic-workflow-reads-unavailable",
    }


def test_dynamic_service_read_binding_is_best_effort() -> None:
    operations = _FakeOperations(
        names_by_code={101: "child-orders"},
        dags={},
    )

    refs = resolve_read_dynamic_workflow_refs(
        operations,
        project=_PROJECT,
        workflow_codes=(101, 404),
    )

    assert dict(refs.code_by_name) == {"child-orders": 101}


def test_dynamic_service_audit_translates_a_mixed_native_cycle() -> None:
    operations = _FakeOperations(
        names_by_code={101: "child-orders", 102: "grandchild"},
        dags={
            101: _Dag((_nested("SUB_PROCESS", 102),)),
            102: _Dag((_nested("DYNAMIC", 99),)),
        },
    )
    with pytest.raises(UserInputError, match="99 -> 101 -> 102 -> 99") as raised:
        resolve_dynamic_authoring_workflow_refs(
            operations,
            project=_PROJECT,
            child_workflow_names=("child-orders",),
            containing_workflow_code=99,
            audit_descendants=True,
            action="workflow.edit",
        )

    assert raised.value.details["reason"] == "dynamic-child-workflow-cycle"


def test_dynamic_instance_boundary_overrides_real_lower_level_error_details() -> None:
    operations = _FakeOperations(
        names_by_code={404: "missing-child"},
        dags={},
        missing={404},
    )
    with pytest.raises(UserInputError) as raised:
        resolve_dynamic_authoring_workflow_refs(
            operations,
            project=_PROJECT,
            child_workflow_names=("missing-child",),
            containing_workflow_code=99,
            audit_descendants=True,
            action="workflow-instance.edit",
            boundary_resource="workflow-instance",
        )

    assert raised.value.details == {
        "resource": "workflow-instance",
        "action": "workflow-instance.edit",
        "reason": "dynamic-child-workflow-read",
        "native_detail": "kept",
        "project": "etl",
        "workflow_code": 404,
    }
