"""Legacy name-routed execution and exact unsupported execution options."""

from typing import TYPE_CHECKING

import pytest
from tests.fakes import (
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    NotFoundError,
    UnsupportedFeatureError,
)
from dsctl.models import WorkflowSpec
from dsctl.services import workflow as workflow_service
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph

if TYPE_CHECKING:
    from collections.abc import Callable


def test_139_run_task_dry_run_resolves_name_without_exposing_native_task_id(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "daily-sync", "project": "etl-prod"},
            "tasks": [
                {"name": "extract", "type": "SHELL", "command": "echo extract"},
                {
                    "name": "load",
                    "type": "SHELL",
                    "command": "echo load",
                    "depends_on": ["extract"],
                },
            ],
        }
    )
    prepared = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda name: f"tasks-{name}",
    )
    graph = prepared.materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )

    result = workflow_service.run_workflow_task_result(
        "daily-sync",
        task="load",
        project="etl-prod",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request["method"] == "POST"
    assert request["path"] == "/projects/etl-prod/executors/start-process-instance"
    request_form = _mapping(request["form"])
    assert request_form["processDefinitionId"] == 101
    assert request_form["startNodeList"] == "load"
    assert request_form["taskDependType"] == "TASK_ONLY"
    assert request_form["execType"] == "START_PROCESS"
    resolved_task = _mapping(result.resolved["task"])
    assert resolved_task == {"name": "load"}
    assert fake_workflow_adapter.run_calls == []
    assert len(operations.prepared_executions) == 1
    assert operations.applied_executions == []


def test_139_run_dry_run_uses_the_exact_name_routed_execution_request(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )

    result = workflow_service.run_workflow_result(
        "daily-sync",
        project="etl-prod",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request["method"] == "POST"
    assert request["path"] == "/projects/etl-prod/executors/start-process-instance"
    form = _mapping(request["form"])
    assert form["processDefinitionId"] == 101
    assert form["execType"] == "START_PROCESS"
    assert form["taskDependType"] == "TASK_POST"
    assert "tenantCode" not in form
    assert "environmentCode" not in form
    assert "dryRun" not in form
    assert len(operations.prepared_executions) == 1
    assert operations.applied_executions == []
    assert fake_workflow_adapter.run_calls == []


def test_139_run_rejects_explicit_execution_fields_that_legacy_wire_drops(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    invocations: list[Callable[[], object]] = [
        lambda: workflow_service.run_workflow_result(
            "daily-sync",
            project="etl-prod",
            tenant="analytics",
        ),
        lambda: workflow_service.run_workflow_result(
            "daily-sync",
            project="etl-prod",
            environment_code=7,
        ),
        lambda: workflow_service.run_workflow_result(
            "daily-sync",
            project="etl-prod",
            execution_dry_run=True,
        ),
    ]

    for invocation in invocations:
        with pytest.raises(UnsupportedFeatureError):
            invocation()

    assert operations.prepared_executions == []
    assert operations.applied_executions == []
    assert fake_workflow_adapter.run_calls == []


def test_139_run_task_unknown_name_fails_before_execution_prepare(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    prepared = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "extract",
                        "type": "SHELL",
                        "command": "echo extract",
                    },
                    {
                        "name": "load",
                        "type": "SHELL",
                        "command": "echo load",
                        "depends_on": ["extract"],
                    },
                ],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    )
    graph = prepared.materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )

    with pytest.raises(NotFoundError):
        workflow_service.run_workflow_task_result(
            "daily-sync",
            task="missing",
            project="etl-prod",
        )

    assert operations.prepared_executions == []
    assert operations.applied_executions == []
    assert fake_workflow_adapter.run_calls == []


def test_139_run_task_apply_uses_the_frozen_name_native_execution(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    prepared = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "extract",
                        "type": "SHELL",
                        "command": "echo extract",
                    },
                    {
                        "name": "load",
                        "type": "SHELL",
                        "command": "echo load",
                        "depends_on": ["extract"],
                    },
                ],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    )
    graph = prepared.materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )

    workflow_service.run_workflow_task_result(
        "daily-sync",
        task="load",
        project="etl-prod",
    )

    assert len(operations.prepared_executions) == 1
    assert len(operations.applied_executions) == 1
    assert operations.applied_executions[0] is operations.prepared_executions[0]
    assert operations.prepared_executions[0].request.path == (
        "/projects/etl-prod/executors/start-process-instance"
    )
    prepared_form = operations.prepared_executions[0].request.form
    assert prepared_form is not None
    assert prepared_form["startNodeList"] == "load"
    assert fake_workflow_adapter.run_calls[-1]["start_node_list"] == ["load"]


def test_139_backfill_task_dry_run_uses_exact_name_native_execution(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    prepared = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "extract",
                        "type": "SHELL",
                        "command": "echo extract",
                    },
                    {
                        "name": "load",
                        "type": "SHELL",
                        "command": "echo load",
                        "depends_on": ["extract"],
                    },
                ],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    )
    graph = prepared.materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )

    result = workflow_service.backfill_workflow_result(
        "daily-sync",
        project="etl-prod",
        start="2026-08-01 00:00:00",
        end="2026-08-02 00:00:00",
        task="load",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request["path"] == "/projects/etl-prod/executors/start-process-instance"
    form = _mapping(request["form"])
    assert form["processDefinitionId"] == 101
    assert form["execType"] == "COMPLEMENT_DATA"
    assert form["runMode"] == "RUN_MODE_SERIAL"
    assert form["startNodeList"] == "load"
    assert form["taskDependType"] == "TASK_ONLY"
    assert "expectedParallelismNumber" not in form
    assert "complementDependentMode" not in form
    assert "allLevelDependent" not in form
    assert "executionOrder" not in form
    assert len(operations.prepared_executions) == 1
    assert operations.applied_executions == []
    assert fake_workflow_adapter.backfill_calls == []


def test_139_backfill_range_dry_run_uses_exact_name_routed_execution(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )

    result = workflow_service.backfill_workflow_result(
        "daily-sync",
        project="etl-prod",
        start="2026-08-01 00:00:00",
        end="2026-08-02 00:00:00",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request["path"] == "/projects/etl-prod/executors/start-process-instance"
    form = _mapping(request["form"])
    assert form["processDefinitionId"] == 101
    assert form["scheduleTime"] == ("2026-08-01 00:00:00,2026-08-02 00:00:00")
    assert form["execType"] == "COMPLEMENT_DATA"
    assert form["runMode"] == "RUN_MODE_SERIAL"
    assert "startNodeList" not in form
    assert form["taskDependType"] == "TASK_POST"
    assert len(operations.prepared_executions) == 1
    assert operations.applied_executions == []
    assert fake_workflow_adapter.backfill_calls == []


def test_139_backfill_rejects_explicit_date_list_before_execution_prepare(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )

    with pytest.raises(UnsupportedFeatureError):
        workflow_service.backfill_workflow_result(
            "daily-sync",
            project="etl-prod",
            dates=["2026-08-01 00:00:00", "2026-08-02 00:00:00"],
        )

    assert operations.prepared_executions == []
    assert operations.applied_executions == []
    assert fake_workflow_adapter.backfill_calls == []


def test_139_backfill_rejects_explicit_complement_fields_the_wire_drops(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    invocations: list[Callable[[], object]] = [
        lambda: workflow_service.backfill_workflow_result(
            "daily-sync",
            project="etl-prod",
            start="2026-08-01 00:00:00",
            end="2026-08-02 00:00:00",
            complement_dependent_mode="all",
        ),
        lambda: workflow_service.backfill_workflow_result(
            "daily-sync",
            project="etl-prod",
            start="2026-08-01 00:00:00",
            end="2026-08-02 00:00:00",
            all_level_dependent=True,
        ),
        lambda: workflow_service.backfill_workflow_result(
            "daily-sync",
            project="etl-prod",
            start="2026-08-01 00:00:00",
            end="2026-08-02 00:00:00",
            execution_order="asc",
        ),
    ]

    for invocation in invocations:
        with pytest.raises(UnsupportedFeatureError):
            invocation()

    assert operations.prepared_executions == []
    assert operations.applied_executions == []
    assert fake_workflow_adapter.backfill_calls == []
