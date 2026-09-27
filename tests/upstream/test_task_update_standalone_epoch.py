"""Exact standalone task-update recipe epoch regression tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UnsupportedFeatureError,
)
from dsctl.upstream.definition_wire import CodeDefinitionWire
from dsctl.upstream.task_definition_wire import bind_task_definition_wire
from tests.fakes import (
    FakeDag,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.support import make_profile
from tests.upstream.test_task_definitions import (
    _dependency_module,
    _depends_intent,
    _FakeTaskWire,
    _intent,
    _module,
    _snapshot,
    _task,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import WorkflowDagRecord
    from dsctl.upstream.task_definitions import TaskDefinitions
    from dsctl.upstream.wire import WireExecution


@pytest.mark.parametrize(
    ("ds_version", "standalone"),
    [
        ("3.1.0", True),
        ("3.1.9", True),
        ("3.2.0", True),
        ("3.2.1", False),
    ],
)
def test_task_update_wire_locks_the_exact_standalone_epoch(
    ds_version: str,
    *,
    standalone: bool,
) -> None:
    client = DolphinSchedulerClient(make_profile(ds_version=ds_version))

    with client:
        exact_wire = bind_task_definition_wire(client)
        prepared = exact_wire.prepare_update(
            project_code=7,
            task_code=7001,
            task_definition_json='{"name":"report"}',
            upstream_codes=[6001],
        )

    expected_path = "/projects/7/task-definition/7001"
    if not standalone:
        expected_path += "/with-upstream"
    assert exact_wire.update_policy.dependency_update is not standalone
    assert exact_wire.update_policy.requires_unique_workflow_binding is standalone
    assert prepared.request.path == expected_path
    assert prepared.request.form == {
        "taskDefinitionJsonObj": '{"name":"report"}',
        **({} if standalone else {"upstreamCodes": "6001"}),
    }


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
def test_standalone_task_update_rejects_dependency_edits_before_transport(
    ds_version: str,
) -> None:
    client = DolphinSchedulerClient(make_profile(ds_version=ds_version))
    with client:
        exact_wire = bind_task_definition_wire(client)
    definitions, wire = _dependency_module(
        profile_version=ds_version,
        update_policy=exact_wire.update_policy,
        top_level_field_policy=exact_wire.top_level_field_policy,
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="cannot express the requested task update fields",
    ) as exc_info:
        definitions.prepare_update(_depends_intent(["transform"]))

    assert exc_info.value.details["unsupported_fields"] == ["depends_on"]
    assert exc_info.value.details["dependency_update"] is False
    assert exc_info.value.details["mutation_applied"] is False
    assert wire.get_calls == []
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
def test_standalone_task_update_accepts_one_exhaustively_proven_workflow_binding(
    ds_version: str,
) -> None:
    client = DolphinSchedulerClient(make_profile(ds_version=ds_version))
    with client:
        exact_wire = bind_task_definition_wire(client)
    definitions, wire = _dependency_module(
        profile_version=ds_version,
        update_policy=exact_wire.update_policy,
        top_level_field_policy=exact_wire.top_level_field_policy,
    )
    prepared = definitions.prepare_update(_intent())

    assert prepared.request.path == "/projects/7/task-definition/7001"
    assert prepared.request.form is not None
    assert "upstreamCodes" not in prepared.request.form
    assert wire.apply_calls == []


@dataclass
class _EnumeratedDagWire(_FakeTaskWire):
    enumerated_dags: dict[int, FakeDag] = field(default_factory=dict)
    describe_calls: int = 0

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        self.describe_calls += 1
        if self.describe_calls > 1:
            self.dag = self.enumerated_dags[workflow_code]
        return super().describe(
            project_code=project_code,
            workflow_code=workflow_code,
        )


def _guarded_module(
    ds_version: str,
    *,
    attack: str,
    top_level_extension: bool = False,
) -> tuple[TaskDefinitions, _EnumeratedDagWire]:
    client = DolphinSchedulerClient(make_profile(ds_version=ds_version))
    with client:
        exact_wire = bind_task_definition_wire(client)
    task = _task(description="current description")
    wire = _EnumeratedDagWire(
        [_snapshot(task, top_level_extension=top_level_extension)],
        ds_version=ds_version,
        update_policy_value=exact_wire.update_policy,
        top_level_field_policy=exact_wire.top_level_field_policy,
    )
    definitions, returned_wire, _compiler = _module(
        wire=wire,
        profile_version=ds_version,
    )
    assert isinstance(returned_wire, _EnumeratedDagWire)
    wire = returned_wire
    assert wire.dag is not None
    current_workflow = wire.dag.workflowDefinition
    assert current_workflow is not None
    root_relation = FakeWorkflowTaskRelation(
        pre_task_code_value=0,
        post_task_code_value=task.code,
        pre_task_version_value=0,
        post_task_version_value=task.version or 0,
    )
    current_dag = FakeDag(
        workflow_definition_value=current_workflow,
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[root_relation],
    )
    wire.dag = current_dag
    definition_wire = definitions.definitions.wire
    assert isinstance(definition_wire, CodeDefinitionWire)
    workflow_adapter = definition_wire.workflows
    assert isinstance(workflow_adapter, FakeWorkflowAdapter)

    wire.enumerated_dags = {101: current_dag}
    if attack == "none":
        pass
    elif attack == "zero-bindings":
        wire.enumerated_dags = {
            101: FakeDag(
                workflow_definition_value=current_workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        }
    elif attack == "duplicate-current-dag":
        wire.enumerated_dags = {
            101: FakeDag(
                workflow_definition_value=current_workflow,
                task_definition_list_value=[task, task],
                workflow_task_relation_list_value=[root_relation],
            )
        }
    elif attack == "duplicate-workflow-ref":
        workflow_adapter.workflows.append(current_workflow)
    elif attack == "duplicate-relation":
        wire.enumerated_dags = {
            101: FakeDag(
                workflow_definition_value=current_workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[root_relation, root_relation],
            )
        }
    elif attack == "outgoing-only-relation":
        wire.enumerated_dags = {
            101: FakeDag(
                workflow_definition_value=current_workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=task.code,
                        post_task_code_value=8001,
                        pre_task_version_value=task.version or 0,
                        post_task_version_value=1,
                    )
                ],
            )
        }
    elif attack == "dag-identity-drift":
        wire.enumerated_dags = {
            101: FakeDag(
                workflow_definition_value=FakeWorkflow(
                    code=202,
                    name="drifted-flow",
                    project_code_value=7,
                    user_id_value=11,
                ),
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[root_relation],
            )
        }
    else:
        assert attack == "second-workflow-root"
        second_workflow = FakeWorkflow(
            code=202,
            name="secondary-flow",
            project_code_value=7,
            user_id_value=11,
        )
        workflow_adapter.workflows.append(second_workflow)
        wire.enumerated_dags = {
            101: current_dag,
            202: FakeDag(
                workflow_definition_value=second_workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=task.code,
                        pre_task_version_value=0,
                        post_task_version_value=task.version or 0,
                    )
                ],
            ),
        }
    return definitions, wire


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
def test_standalone_task_update_rejects_local_shape_before_binding_reads(
    ds_version: str,
) -> None:
    definitions, wire = _guarded_module(
        ds_version,
        attack="none",
        top_level_extension=True,
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="unreviewed top-level fields",
    ) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.details["unexpected_fields"] == ["futureTopLevel"]
    assert exc_info.value.details["mutation_applied"] is False
    assert wire.describe_calls == 1
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_standalone_task_update_bounds_large_binding_diagnostics() -> None:
    definitions, wire = _guarded_module("3.1.9", attack="none")
    definition_wire = definitions.definitions.wire
    assert isinstance(definition_wire, CodeDefinitionWire)
    workflow_adapter = definition_wire.workflows
    assert isinstance(workflow_adapter, FakeWorkflowAdapter)
    task = cast("FakeTaskDefinition", wire.snapshots[0].record)
    for code in range(200, 220):
        workflow = FakeWorkflow(
            code=code,
            name=f"secondary-{code}",
            project_code_value=7,
            user_id_value=11,
        )
        workflow_adapter.workflows.append(workflow)
        wire.enumerated_dags[code] = FakeDag(
            workflow_definition_value=workflow,
            task_definition_list_value=[task],
            workflow_task_relation_list_value=[
                FakeWorkflowTaskRelation(
                    pre_task_code_value=0,
                    post_task_code_value=task.code,
                    pre_task_version_value=0,
                    post_task_version_value=task.version or 0,
                )
            ],
        )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        definitions.prepare_update(_intent())

    details = exc_info.value.details
    workflow_sample = details["workflow_inventory_codes_sample"]
    membership_sample = details["membership_workflow_codes_sample"]
    relation_sample = details["relation_workflow_codes_sample"]
    issues = details["issues"]
    assert isinstance(workflow_sample, list)
    assert isinstance(membership_sample, list)
    assert isinstance(relation_sample, list)
    assert isinstance(issues, list)
    assert details["workflow_inventory_count"] == 21
    assert len(workflow_sample) == 8
    assert details["workflow_inventory_truncated"] is True
    assert details["membership_workflow_count"] == 21
    assert len(membership_sample) == 8
    assert details["membership_workflow_truncated"] is True
    assert details["relation_workflow_count"] == 21
    assert len(relation_sample) == 8
    assert details["relation_workflow_truncated"] is True
    assert details["issue_count"] == 0
    assert issues == []
    assert details["issues_truncated"] is False


def test_standalone_task_update_bounds_large_issue_diagnostics() -> None:
    definitions, wire = _guarded_module("3.1.9", attack="none")
    task = cast("FakeTaskDefinition", wire.snapshots[0].record)
    current_dag = wire.enumerated_dags[101]
    workflow = current_dag.workflowDefinition
    assert isinstance(workflow, FakeWorkflow)
    relation = FakeWorkflowTaskRelation(
        pre_task_code_value=0,
        post_task_code_value=task.code,
        pre_task_version_value=0,
        post_task_version_value=task.version or 0,
    )
    wire.enumerated_dags[101] = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[relation] * 20,
    )

    with pytest.raises(ApiTransportError) as exc_info:
        definitions.prepare_update(_intent())

    details = exc_info.value.details
    issues = details["issues"]
    assert isinstance(issues, list)
    assert details["issue_count"] == 19
    assert len(issues) == 8
    assert details["issues_truncated"] is True


def test_standalone_task_update_preserves_permission_denied_inventory_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definitions, wire = _guarded_module("3.1.9", attack="none")
    suggestion = "Ask an administrator to grant workflow read permission."

    def deny_workflow_inventory(
        _definitions: object,
        _project_selector: str,
    ) -> tuple[()]:
        message = "Workflow inventory permission denied"
        raise PermissionDeniedError(
            message,
            details={"resource": "workflow"},
            suggestion=suggestion,
        )

    monkeypatch.setattr(
        type(definitions.definitions),
        "workflow_refs",
        deny_workflow_inventory,
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.error_type == "permission_denied"
    assert exc_info.value.details == {
        "resource": "workflow",
        "mutation_applied": False,
    }
    assert exc_info.value.suggestion == suggestion
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_standalone_task_update_preserves_not_found_inventory_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definitions, wire = _guarded_module("3.1.9", attack="none")
    suggestion = "List the project workflows and retry from a fresh read."

    def missing_workflow_inventory(
        _definitions: object,
        _project_selector: str,
    ) -> tuple[()]:
        message = "Workflow inventory no longer exists"
        raise NotFoundError(
            message,
            details={"resource": "workflow"},
            suggestion=suggestion,
        )

    monkeypatch.setattr(
        type(definitions.definitions),
        "workflow_refs",
        missing_workflow_inventory,
    )

    with pytest.raises(NotFoundError) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.error_type == "not_found"
    assert exc_info.value.details == {
        "resource": "workflow",
        "mutation_applied": False,
    }
    assert exc_info.value.suggestion == suggestion
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


def test_standalone_task_update_preserves_transport_inventory_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definitions, wire = _guarded_module("3.1.9", attack="none")
    suggestion = "Retry after verifying the DolphinScheduler API connection."
    source = {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "response",
    }

    def unreadable_workflow_inventory(
        _definitions: object,
        _project_selector: str,
    ) -> tuple[()]:
        message = "Workflow inventory response could not be decoded"
        raise ApiTransportError(
            message,
            details={"resource": "workflow"},
            source=source,
            suggestion=suggestion,
        )

    monkeypatch.setattr(
        type(definitions.definitions),
        "workflow_refs",
        unreadable_workflow_inventory,
    )

    with pytest.raises(ApiTransportError) as exc_info:
        definitions.prepare_update(_intent())

    assert exc_info.value.message == "Workflow inventory response could not be decoded"
    assert exc_info.value.details == {
        "resource": "workflow",
        "mutation_applied": False,
    }
    assert exc_info.value.source == source
    assert exc_info.value.suggestion == suggestion
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
@pytest.mark.parametrize(
    ("attack", "expected_error", "expected_reason"),
    [
        (
            "zero-bindings",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "duplicate-current-dag",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "duplicate-workflow-ref",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "duplicate-relation",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "outgoing-only-relation",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "dag-identity-drift",
            ApiTransportError,
            "standalone_task_update_workflow_inventory_inconsistent",
        ),
        (
            "second-workflow-root",
            UnsupportedFeatureError,
            "standalone_task_update_workflow_binding_not_unique",
        ),
    ],
)
def test_standalone_task_update_rejects_unproven_workflow_binding_before_mutation(
    ds_version: str,
    attack: str,
    expected_error: type[Exception],
    expected_reason: str,
) -> None:
    definitions, wire = _guarded_module(ds_version, attack=attack)

    with pytest.raises(expected_error) as exc_info:
        definitions.prepare_update(_intent())

    error = exc_info.value
    assert isinstance(error, (ApiTransportError, UnsupportedFeatureError))
    assert error.details["reason"] == expected_reason
    assert error.details["mutation_applied"] is False
    if attack == "outgoing-only-relation":
        assert error.details["issues"] == [
            {
                "kind": "missing_incoming_task_relation",
                "workflow_code": 101,
            }
        ]
    assert wire.prepare_calls == []
    assert wire.apply_calls == []


@pytest.mark.parametrize("ds_version", ["3.1.9", "3.2.0"])
def test_standalone_task_update_rechecks_binding_before_apply(
    ds_version: str,
) -> None:
    definitions, wire = _guarded_module(ds_version, attack="none")
    prepared = definitions.prepare_update(_intent())
    definition_wire = definitions.definitions.wire
    assert isinstance(definition_wire, CodeDefinitionWire)
    workflow_adapter = definition_wire.workflows
    assert isinstance(workflow_adapter, FakeWorkflowAdapter)
    task = cast("FakeTaskDefinition", wire.snapshots[0].record)
    second_workflow = FakeWorkflow(
        code=202,
        name="concurrent-flow",
        project_code_value=7,
        user_id_value=11,
    )
    workflow_adapter.workflows.append(second_workflow)
    wire.enumerated_dags[202] = FakeDag(
        workflow_definition_value=second_workflow,
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=0,
                post_task_code_value=task.code,
                pre_task_version_value=0,
                post_task_version_value=task.version or 0,
            )
        ],
    )

    with pytest.raises(ConflictError, match="workflow binding changed") as exc_info:
        definitions.apply(prepared)

    assert exc_info.value.details["reason"] == (
        "prepared_task_update_workflow_binding_changed"
    )
    assert exc_info.value.details["phase"] == "stale_check"
    assert exc_info.value.details["mutation_applied"] is False
    assert wire.apply_calls == []
