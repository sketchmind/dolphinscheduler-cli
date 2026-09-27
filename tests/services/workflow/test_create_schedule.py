"""Independent schedule planning and application during workflow creation."""

import json
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    FakeScheduleAdapter,
    FakeWorkflowAdapter,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ConfirmationRequiredError,
    ConflictError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services.task_authoring_catalog import (
    get_task_authoring_catalog,
)


def _high_frequency_workflow_file(tmp_path: Path, name: str) -> Path:
    path = tmp_path / "scheduled-workflow.yaml"
    path.write_text(
        f"""
workflow:
  name: {name}
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 */5 * * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )
    return path


def _worker_preference(worker_group: str) -> FakeProjectPreference:
    return FakeProjectPreference(
        id=8,
        code=8,
        project_code_value=7,
        state=1,
        preferences_value=json.dumps({"workerGroup": worker_group}),
    )


def test_workflow_schedule_preflights_positive_project_preference_before_write(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    workflow_harness.install(
        schedule_adapter=fake_schedule_adapter,
        profile=make_profile(ds_version="3.2.1"),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=8,
                    code=8,
                    project_code_value=7,
                    state=1,
                    preferences_value='{"environmentCode":99}',
                )
            ]
        ),
    )
    spec_path = tmp_path / "scheduled-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: preference-scheduled
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )
    existing_workflows = list(fake_workflow_adapter.workflows)

    with pytest.raises(UnsupportedFeatureError):
        workflow_service.create_workflow_result(file=spec_path)

    assert fake_workflow_adapter.workflows == existing_workflows
    assert fake_schedule_adapter.schedules == []


def test_workflow_schedule_rechecks_changed_preference_before_schedule_write(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    preference_adapter = FakeProjectPreferenceAdapter(project_preferences=[])
    positive_preference = FakeProjectPreference(
        id=8,
        code=8,
        project_code_value=7,
        state=1,
        preferences_value='{"environmentCode":99}',
    )
    preference_reads = 0

    def changing_preference(*, project_code: int) -> FakeProjectPreference | None:
        nonlocal preference_reads
        assert project_code == 7
        preference_reads += 1
        return None if preference_reads == 1 else positive_preference

    monkeypatch.setattr(preference_adapter, "get", changing_preference)
    workflow_harness.install(
        schedule_adapter=fake_schedule_adapter,
        profile=make_profile(ds_version="3.2.1"),
        project_preference_adapter=preference_adapter,
    )
    spec_path = tmp_path / "scheduled-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: preference-changed
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UnsupportedFeatureError) as caught:
        workflow_service.create_workflow_result(file=spec_path)

    assert preference_reads >= 2
    assert any(
        workflow.name == "preference-changed"
        for workflow in fake_workflow_adapter.workflows
    )
    assert fake_schedule_adapter.schedules == []
    assert caught.value.details["failed_stage"] == "schedule_create"
    assert caught.value.details["mutation_applied"] is True
    assert "workflow" in _mapping(caught.value.details["known_resources"])


def test_workflow_schedule_confirmation_rejects_changed_effective_preference(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2026-01-01 00:00:00",
        "2026-01-01 00:05:00",
    ]
    preferences = FakeProjectPreferenceAdapter(
        project_preferences=[_worker_preference("worker-a")]
    )
    workflow_harness.install(
        schedule_adapter=fake_schedule_adapter,
        project_preference_adapter=preferences,
    )
    spec_path = _high_frequency_workflow_file(tmp_path, "preference-confirmation")
    existing_workflows = list(fake_workflow_adapter.workflows)

    with pytest.raises(ConfirmationRequiredError) as caught:
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    confirmation = _mapping(caught.value.details)["confirmation_token"]
    assert isinstance(confirmation, str)

    dry_run = workflow_service.create_workflow_result(
        file=spec_path, dry_run=True, confirm_risk=confirmation
    )
    requests = _sequence(_mapping(dry_run.data)["requests"])
    schedule_form = _mapping(_mapping(requests[2])["form"])
    assert schedule_form["workerGroup"] == "worker-a"
    dry_run_confirmation = _mapping(_mapping(dry_run.data)["schedule_confirmation"])
    assert dry_run_confirmation["token"] == confirmation

    preferences.project_preferences[0] = _worker_preference("worker-b")
    with pytest.raises(ConfirmationRequiredError) as changed:
        workflow_service.create_workflow_result(
            file=spec_path, confirm_risk=confirmation
        )
    assert _mapping(changed.value.details)["confirmation_token"] != confirmation
    assert fake_workflow_adapter.workflows == existing_workflows
    assert fake_schedule_adapter.schedules == []


def test_workflow_schedule_stops_when_preference_changes_after_workflow_write(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2026-01-01 00:00:00",
        "2026-01-01 00:05:00",
    ]
    preferences = FakeProjectPreferenceAdapter(
        project_preferences=[_worker_preference("worker-a")]
    )
    workflow_harness.install(
        schedule_adapter=fake_schedule_adapter,
        project_preference_adapter=preferences,
    )
    spec_path = _high_frequency_workflow_file(tmp_path, "preference-race")
    with pytest.raises(ConfirmationRequiredError) as caught:
        workflow_service.create_workflow_result(file=spec_path)
    confirmation = _mapping(caught.value.details)["confirmation_token"]
    assert isinstance(confirmation, str)

    reads = 0

    def changing_preference(*, project_code: int) -> FakeProjectPreference:
        nonlocal reads
        assert project_code == 7
        reads += 1
        return _worker_preference("worker-a" if reads == 1 else "worker-b")

    monkeypatch.setattr(preferences, "get", changing_preference)
    with pytest.raises(ConflictError) as changed:
        workflow_service.create_workflow_result(
            file=spec_path, confirm_risk=confirmation
        )

    assert reads >= 2
    assert changed.value.details["changed_fields"] == ["workerGroup"]
    assert changed.value.details["failed_stage"] == "schedule_create"
    assert changed.value.details["mutation_applied"] is True
    assert "workflow" in _mapping(changed.value.details["known_resources"])
    assert any(
        workflow.name == "preference-race"
        for workflow in fake_workflow_adapter.workflows
    )
    assert fake_schedule_adapter.schedules == []


def test_139_create_workflow_schedule_dry_run_uses_name_and_id_native_requests(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-scheduled-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-scheduled
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  release_state: ONLINE
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    requests = [
        _mapping(value) for value in _sequence(_mapping(result.data)["requests"])
    ]
    assert [request["path"] for request in requests] == [
        "/projects/etl-prod/process/save",
        "/projects/etl-prod/process/release",
        "/projects/etl-prod/schedule/create",
        "/projects/etl-prod/schedule/online",
    ]
    schedule_form = _mapping(requests[2]["form"])
    assert schedule_form["processDefinitionId"] == (
        "<legacy-scheduled:created_workflow_id>"
    )
    assert json.loads(str(schedule_form["schedule"])) == {
        "crontab": "0 0 0 * * ?",
        "endTime": "2026-12-31 23:59:59",
        "startTime": "2026-01-01 00:00:00",
    }
    assert schedule_form["failureStrategy"] == "CONTINUE"
    assert schedule_form["warningType"] == "NONE"
    assert schedule_form["processInstancePriority"] == "MEDIUM"
    assert schedule_form["workerGroup"] == "default"
    assert _mapping(requests[3]["form"]) == {
        "id": "<legacy-scheduled:created_schedule_id>"
    }


def test_139_create_workflow_rejects_explicit_unrepresentable_schedule_timezone(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-scheduled-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-scheduled
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: Asia/Shanghai
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="server-local timezone"):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert fake_workflow_adapter.create_calls == []


def test_139_workflow_schedule_create_export_edit_roundtrip_preserves_server_local(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-scheduled-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-scheduled
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  release_state: ONLINE
""".strip(),
        encoding="utf-8",
    )

    created = workflow_service.create_workflow_result(file=spec_path)
    created_schedule = _mapping(_mapping(created.data)["schedule"])

    assert created_schedule["timezoneId"] is None
    assert created_schedule["releaseState"] == "ONLINE"
    exported = workflow_service.export_workflow_yaml_result(
        "legacy-scheduled",
        project="etl-prod",
    )
    exported_yaml = str(_mapping(exported.data)["yaml"])
    exported_document = _mapping(yaml.safe_load(exported_yaml))
    exported_schedule = _mapping(exported_document["schedule"])
    assert "timezone" not in exported_schedule
    assert exported_schedule["release_state"] == "ONLINE"

    roundtrip_path = tmp_path / "legacy-scheduled-roundtrip.yaml"
    roundtrip_path.write_text(exported_yaml, encoding="utf-8")
    edited = workflow_service.edit_workflow_result(
        "legacy-scheduled",
        project="etl-prod",
        file=roundtrip_path,
        dry_run=True,
    )

    assert _mapping(edited.data)["requests"] == []
    assert _mapping(edited.data)["no_change"] is True


def test_create_workflow_result_rejects_offline_workflow_schedule_block(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match=r"workflow\.release_state=ONLINE"):
        workflow_service.create_workflow_result(file=spec_path)


def test_create_workflow_result_rejects_five_field_schedule_cron(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 2 * * *"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="Quartz cron expression with 6 or 7 fields",
    ) as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        "Run `dsctl template workflow` to inspect the stable YAML surface, "
        "then run `dsctl lint workflow PATH` before retrying create."
    )


def test_create_workflow_result_can_dry_run_schedule_plan(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  failure_strategy: END
  priority: HIGH
  enabled: true
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    data = _mapping(result.data)
    requests = _sequence(data["requests"])

    assert len(requests) == 4
    schedule_request = _mapping(requests[2])
    schedule_form = _mapping(schedule_request["form"])
    assert schedule_request["path"] == "/projects/7/schedules"
    assert schedule_form["workflowDefinitionCode"] == (
        "<nightly-sync:created_workflow_code>"
    )
    assert "environmentCode" not in schedule_form
    assert schedule_form["failureStrategy"] == "END"
    assert schedule_form["workflowInstancePriority"] == "HIGH"
    assert json.loads(str(schedule_form["schedule"])) == {
        "crontab": "0 0 0 * * ?",
        "endTime": "2026-12-31 23:59:59",
        "startTime": "2026-01-01 00:00:00",
        "timezoneId": "UTC",
    }
    assert _mapping(requests[3])["path"] == (
        "/projects/7/schedules/<nightly-sync:created_schedule_id>/online"
    )
    schedule_preview = _mapping(data["schedule_preview"])
    schedule_confirmation = _mapping(data["schedule_confirmation"])

    assert schedule_preview["count"] == 5
    assert _mapping(schedule_preview["analysis"])["risk_level"] == "none"
    assert schedule_confirmation["required"] is False
    assert schedule_confirmation["token"] is None
    assert result.warnings == [
        "dry run: no mutation was sent; lookup and verification reads may occur"
    ]


def test_create_workflow_result_dry_run_returns_confirmed_schedule_metadata(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 */5 * * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_service.create_workflow_result(file=spec_path)

    confirmation = _mapping(captured.value.details)["confirmation_token"]
    assert isinstance(confirmation, str)

    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
        "2024-01-01 00:25:00",
    ]

    result = workflow_service.create_workflow_result(
        file=spec_path,
        dry_run=True,
        confirm_risk=confirmation,
    )
    data = _mapping(result.data)
    schedule_confirmation = _mapping(data["schedule_confirmation"])

    assert schedule_confirmation["required"] is True
    assert schedule_confirmation["token"] == confirmation
    assert schedule_confirmation["confirmFlag"] == f"--confirm-risk {confirmation}"


@pytest.mark.parametrize(
    ("schedule_enabled_yaml", "expected_schedule_state"),
    [("false", "OFFLINE"), ("true", "ONLINE")],
)
def test_create_workflow_result_returns_final_created_schedule(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
    schedule_enabled_yaml: str,
    expected_schedule_state: str,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        f"""
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  failure_strategy: CONTINUE
  priority: MEDIUM
  enabled: {schedule_enabled_yaml}
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path)
    data = _mapping(result.data)
    schedule = _mapping(data["schedule"])

    assert fake_workflow_adapter.get(project_code=7, code=103).schedule is None
    assert _mapping(result.resolved["workflow"])["source"] == "file"
    assert fake_workflow_adapter.release_calls[-1] == (103, "ONLINE")
    assert len(fake_schedule_adapter.schedules) == 1
    assert fake_schedule_adapter.schedules[0].workflowDefinitionCode == 103
    assert fake_schedule_adapter.schedules[0].releaseState is not None
    assert (
        fake_schedule_adapter.schedules[0].releaseState.value == expected_schedule_state
    )
    assert data["scheduleReleaseState"] == expected_schedule_state
    assert fake_schedule_adapter.list_calls == []
    assert schedule == {
        "id": 1,
        "startTime": "2026-01-01 00:00:00",
        "endTime": "2026-12-31 23:59:59",
        "timezoneId": "UTC",
        "crontab": "0 0 0 * * ?",
        "failureStrategy": "CONTINUE",
        "workflowInstancePriority": "MEDIUM",
        "releaseState": expected_schedule_state,
    }
