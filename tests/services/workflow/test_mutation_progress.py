"""A partial workflow sequence must never look like an unapplied create/edit."""

from pathlib import Path

import pytest
from tests.services.workflow.harness import _WorkflowServiceHarness
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.errors import ApiTransportError, UserInputError
from dsctl.services import workflow as service
from dsctl.services.workflow._outcomes import WorkflowMutationProgress
from dsctl.upstream.definition_models import WorkflowScope
from dsctl.upstream.protocol import ScheduleCreateSpec, WorkflowPayloadRecord


def _create_file(tmp_path: Path, *, schedule: bool = False) -> Path:
    path = tmp_path / "create.yaml"
    path.write_text(
        """workflow:
  name: outcome-check
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: hello
    type: SHELL
    command: echo hello
"""
        + (
            """schedule:
  cron: '0 0 0 * * ?'
  timezone: UTC
  start: '2026-01-01 00:00:00'
  end: '2026-12-31 23:59:59'
  release_state: ONLINE
"""
            if schedule
            else ""
        )
    )
    return path


def test_create_identity_readback_failure_preserves_written_name(
    workflow_harness: _WorkflowServiceHarness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = workflow_harness.install()
    original = operations.resolve_workflow

    def resolve(project: str, workflow: str) -> WorkflowScope:
        if any(
            item.name == "outcome-check"
            for item in workflow_harness.workflow_adapter.workflows
        ):
            message = "readback unavailable"
            raise ApiTransportError(message)
        return original(project, workflow)

    monkeypatch.setattr(operations, "resolve_workflow", resolve)
    with pytest.raises(ApiTransportError) as caught:
        service.create_workflow_result(file=_create_file(tmp_path))
    details = caught.value.details
    assert details.get("mutation_applied") is True, str(caught.value)
    assert details["completed_stages"] == ["workflow_create"]
    assert details["failed_stage"] == "workflow_identity_readback"
    assert assert_mapping(details["known_resources"])["workflow"] == {
        "name": "outcome-check"
    }
    assert (
        sum(
            item.name == "outcome-check"
            for item in workflow_harness.workflow_adapter.workflows
        )
        == 1
    )


def test_schedule_failure_preserves_created_and_released_workflow(
    workflow_harness: _WorkflowServiceHarness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workflow_harness.install(schedule_adapter=workflow_harness.schedule_adapter)

    def fail_create(*, spec: ScheduleCreateSpec[int]) -> None:
        del spec
        message = "schedule rejected"
        raise UserInputError(message, suggestion="Correct the schedule parameters.")

    monkeypatch.setattr(workflow_harness.schedule_adapter, "create", fail_create)
    with pytest.raises(UserInputError) as caught:
        service.create_workflow_result(file=_create_file(tmp_path, schedule=True))
    details = caught.value.details
    assert details.get("mutation_applied") is True, str(caught.value)
    assert details["failed_stage"] == "schedule_create"
    assert caught.value.suggestion is not None
    assert caught.value.suggestion.count("Inspect known_resources") == 1
    assert caught.value.suggestion.endswith("Correct the schedule parameters.")
    assert "workflow_online" in assert_sequence(details["completed_stages"])
    workflow_code = assert_mapping(
        assert_mapping(details["known_resources"])["workflow"]
    )["code"]
    assert isinstance(workflow_code, int)
    assert workflow_code > 0
    assert workflow_harness.schedule_adapter.schedules == []


def test_edit_readback_failure_keeps_updated_workflow_identity(
    workflow_harness: _WorkflowServiceHarness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workflow_harness.workflow_adapter.offline(project_code=7, workflow_code=101)
    operations = workflow_harness.install()
    original_detail = operations.detail

    def detail(scope: WorkflowScope, *, action: str) -> WorkflowPayloadRecord:
        if workflow_harness.workflow_adapter.update_calls:
            message = "readback unavailable"
            raise ApiTransportError(message)
        return original_detail(scope, action=action)

    monkeypatch.setattr(operations, "detail", detail)
    path = tmp_path / "edit.yaml"
    path.write_text("patch:\n  workflow:\n    set:\n      description: revised\n")
    with pytest.raises(ApiTransportError) as caught:
        service.edit_workflow_result("daily-sync", project="etl-prod", patch=path)
    assert caught.value.details.get("mutation_applied") is True, str(caught.value)
    assert caught.value.details["completed_stages"] == ["workflow_update"]
    assert caught.value.details["failed_stage"] == "workflow_readback"


@pytest.mark.parametrize(
    "failed_stage", ["workflow_online", "schedule_online", "workflow_readback"]
)
def test_late_create_failures_retain_completed_stages_and_known_resources(
    workflow_harness: _WorkflowServiceHarness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failed_stage: str,
) -> None:
    operations = workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter
    )
    if failed_stage == "workflow_online":

        def fail_release(_scope: WorkflowScope, *, state: str) -> None:
            del state
            message = "release unavailable"
            raise UserInputError(message)

        monkeypatch.setattr(operations, "release", fail_release)
    elif failed_stage == "schedule_online":

        def fail_online(*, schedule_id: int) -> None:
            del schedule_id
            message = "schedule activation unavailable"
            raise UserInputError(message)

        monkeypatch.setattr(workflow_harness.schedule_adapter, "online", fail_online)
    else:

        def fail_detail(_scope: WorkflowScope, *, action: str) -> WorkflowPayloadRecord:
            del action
            message = "final read unavailable"
            raise ApiTransportError(message)

        monkeypatch.setattr(operations, "detail", fail_detail)
    with pytest.raises((UserInputError, ApiTransportError)) as caught:
        service.create_workflow_result(file=_create_file(tmp_path, schedule=True))
    details = caught.value.details
    assert details["mutation_applied"] is True
    assert details["failed_stage"] == failed_stage
    assert caught.value.suggestion is not None
    assert caught.value.suggestion.count("Inspect known_resources") == 1
    known = assert_mapping(details["known_resources"])
    assert "code" in assert_mapping(known["workflow"])
    if failed_stage != "workflow_online":
        assert "id" in assert_mapping(known["schedule"])
        assert "schedule_create" in assert_sequence(details["completed_stages"])


def test_uncertain_first_stage_does_not_claim_no_mutation() -> None:
    progress = WorkflowMutationProgress({"workflow": {"name": "unknown"}})
    message = "dispatch timeout"
    error = ApiTransportError(message, details={"mutation_may_have_applied": True})
    progress.annotate(error, failed_stage="workflow_create")
    assert error.details["mutation_may_have_applied"] is True
    assert "mutation_applied" not in error.details
    assert error.details["completed_stages"] == []
    assert error.details["unconfirmed_stages"] == ["workflow_create"]
