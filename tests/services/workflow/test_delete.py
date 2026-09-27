"""Workflow deletion confirmation and upstream failure translation."""

from dataclasses import replace

import pytest
from tests.fakes import (
    FakeScheduleAdapter,
    FakeWorkflowAdapter,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    InvalidStateError,
    UserInputError,
)
from dsctl.services import workflow as workflow_service
from dsctl.upstream.definition_models import WorkflowScope
from dsctl.upstream.workflows import WorkflowDeleteLineageError


def test_delete_workflow_result_requires_force() -> None:
    with pytest.raises(UserInputError, match="Workflow deletion requires --force"):
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=False,
        )


def test_delete_workflow_result_returns_deleted_payload(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )

    result = workflow_service.delete_workflow_result(
        "daily-sync",
        project="etl-prod",
        force=True,
    )
    data = _mapping(result.data)

    assert data["deleted"] is True
    workflow_data = _mapping(data["workflow"])
    assert workflow_data["name"] == "daily-sync"
    assert workflow_data["scheduleReleaseState"] == "ONLINE"
    assert _mapping(workflow_data["schedule"])["id"] == 23
    assert len(schedule_adapter.list_calls) == 1
    assert [workflow.name for workflow in fake_workflow_adapter.workflows] == [
        "adhoc-backfill"
    ]
    assert 101 not in fake_workflow_adapter.dags
    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": "daily jobs",
            "source": "flag",
        },
        "workflow": {
            "code": 101,
            "name": "daily-sync",
            "source": "flag",
            "version": 1,
        },
    }


def test_delete_workflow_result_maps_online_state_to_invalid_state(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=50021,
            result_message="workflow definition [daily-sync] is already online",
        )
    }
    workflow_harness.install()

    with pytest.raises(
        InvalidStateError,
        match="must be offline before deletion",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
        "then retry `dsctl workflow delete --force`."
    )


def test_delete_workflow_result_suggests_schedule_cleanup_for_online_schedule(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=50023,
            result_message="workflow definition [daily-sync] has online schedule",
        )
    }
    workflow_harness.install()

    with pytest.raises(
        InvalidStateError,
        match="online schedule",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schedule list --workflow WORKFLOW --project PROJECT` to find "
        "the attached schedule, take it offline with `dsctl schedule offline "
        "SCHEDULE_ID`, then retry `dsctl workflow delete --force`."
    )


def test_delete_workflow_result_suggests_instance_inspection_for_running_instances(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=10163,
            result_message="workflow definition [daily-sync] has running instances",
        )
    }
    workflow_harness.install()

    with pytest.raises(
        InvalidStateError,
        match="running workflow instances",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow-instance list --workflow WORKFLOW --project PROJECT` "
        "to inspect active instances, stop or wait for them to finish, then "
        "retry deletion."
    )


def test_delete_workflow_result_maps_dependencies_to_conflict(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=10193,
            result_message="delete workflow definition fail, cause used by other tasks",
        )
    }
    workflow_harness.install()

    with pytest.raises(ConflictError, match="referenced by other tasks") as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect references before retrying deletion."
    )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2"])
def test_delete_owner_lineage_conflict_preserves_definition_and_guides_inspection(
    version: str,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(profile=make_profile(ds_version=version))

    def blocked_delete(scope: WorkflowScope) -> None:
        assert scope.workflow.native.value == 101
        message = "Native deletion retains owner lineage"
        raise WorkflowDeleteLineageError(message)

    workflow_harness.monkeypatch.setattr(operations, "delete", blocked_delete)
    before = list(fake_workflow_adapter.workflows)
    with pytest.raises(ConflictError, match="No delete request was sent") as caught:
        workflow_service.delete_workflow_result(
            "daily-sync", project="etl-prod", force=True
        )
    assert fake_workflow_adapter.workflows == before
    error = caught.value
    assert error.details["ds_version"] == version
    assert error.details["reason"] == "upstream_lineage_cleanup_missing"
    assert error.suggestion is not None
    assert "dsctl workflow lineage get 101 --project 7" in error.suggestion
    assert "dsctl workflow offline 101 --project 7" in error.suggestion
    assert "retrying --force does not clear existing lineage" in error.suggestion
