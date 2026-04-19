import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeUser,
    FakeUserAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_schedule_service(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            FakeSchedule(
                id=1,
                workflow_definition_code_value=101,
                workflow_definition_name_value="daily-sync",
                project_name_value="etl-prod",
                start_time_value="2024-01-01 00:00:00",
                end_time_value="2025-01-01 00:00:00",
                timezone_id_value="Asia/Shanghai",
                crontab_value="0 0 2 * * ?",
                project_code_value=7,
            )
        ]
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(
                project="etl-prod",
                workflow="daily-sync",
            ),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
            user_adapter=FakeUserAdapter(
                users=[
                    FakeUser(
                        id=11,
                        user_name_value="alice",
                        email="alice@example.com",
                        tenant_id_value=7,
                        tenant_code_value="tenant-current-user",
                    )
                ]
            ),
        ),
    )


def test_schedule_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["schedule", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.list"
    assert payload["resolved"]["project"]["source"] == "context"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["workflowDefinitionName"] == "daily-sync"


def test_schedule_list_help_points_to_project_and_workflow_discovery() -> None:
    result = runner.invoke(app, ["schedule", "list", "--help"])

    assert result.exit_code == 0
    assert "project list" in result.stdout
    assert "workflow list" in result.stdout


def test_schedule_list_command_rejects_workflow_and_search_together() -> None:
    result = runner.invoke(
        app,
        [
            "schedule",
            "list",
            "--workflow",
            "daily-sync",
            "--search",
            "daily",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Use `--workflow` to scope to one workflow, or drop it and use "
        "`--search` across schedules in the selected project."
    )


def test_schedule_get_command_returns_schedule_payload() -> None:
    result = runner.invoke(app, ["schedule", "get", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.get"
    assert payload["data"]["id"] == 1
    assert payload["data"]["crontab"] == "0 0 2 * * ?"


def test_schedule_get_help_points_to_schedule_list() -> None:
    result = runner.invoke(app, ["schedule", "get", "--help"])

    assert result.exit_code == 0
    assert "schedule list" in result.stdout


def test_schedule_preview_command_returns_times_and_analysis() -> None:
    result = runner.invoke(app, ["schedule", "preview", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.preview"
    assert payload["data"]["count"] == 5
    assert payload["data"]["analysis"]["risk_level"] == "none"


def test_schedule_preview_command_rejects_mixing_id_and_schedule_fields() -> None:
    result = runner.invoke(
        app,
        [
            "schedule",
            "preview",
            "1",
            "--cron",
            "0 0 2 * * ?",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.preview"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass only the schedule id to preview an existing schedule, or omit "
        "the id and pass `--project`, `--cron`, `--start`, `--end`, and "
        "`--timezone` for an ad hoc preview."
    )


def test_schedule_create_help_points_to_related_discovery_commands() -> None:
    result = runner.invoke(app, ["schedule", "create", "--help"])

    assert result.exit_code == 0
    assert "workflow list" in result.stdout
    assert "project list" in result.stdout
    assert "alert-group" in result.stdout
    assert "worker-group" in result.stdout
    assert "tenant list" in result.stdout
    assert "environment list" in result.stdout


def test_schedule_explain_command_returns_create_explanation() -> None:
    result = runner.invoke(
        app,
        [
            "schedule",
            "explain",
            "--cron",
            "0 0 4 * * ?",
            "--start",
            "2024-01-01 00:00:00",
            "--end",
            "2025-01-01 00:00:00",
            "--timezone",
            "Asia/Shanghai",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.explain"
    assert payload["data"]["mutationAction"] == "schedule.create"
    assert payload["data"]["proposedSchedule"]["crontab"] == "0 0 4 * * ?"
    assert payload["data"]["proposedSchedule"]["tenantCode"] == "tenant-current-user"
    assert payload["resolved"]["tenant"]["source"] == "current_user"
    assert payload["data"]["confirmation"]["required"] is False


def test_schedule_explain_command_returns_update_confirmation_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            FakeSchedule(
                id=1,
                workflow_definition_code_value=101,
                workflow_definition_name_value="daily-sync",
                project_name_value="etl-prod",
                start_time_value="2024-01-01 00:00:00",
                end_time_value="2025-01-01 00:00:00",
                timezone_id_value="Asia/Shanghai",
                crontab_value="0 0 2 * * ?",
                project_code_value=7,
            )
        ],
        preview_times_value=[
            "2024-01-01 00:00:00",
            "2024-01-01 00:05:00",
            "2024-01-01 00:10:00",
            "2024-01-01 00:15:00",
            "2024-01-01 00:20:00",
        ],
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )

    result = runner.invoke(
        app,
        ["schedule", "explain", "1", "--cron", "0 */5 * * * ?"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.explain"
    assert payload["data"]["mutationAction"] == "schedule.update"
    assert payload["data"]["currentSchedule"]["crontab"] == "0 0 2 * * ?"
    assert payload["data"]["proposedSchedule"]["crontab"] == "0 */5 * * * ?"
    assert payload["data"]["requestedFields"] == ["crontab"]
    assert payload["data"]["changedFields"] == ["crontab"]
    assert payload["resolved"]["schedule"]["projectCode"] == 7
    assert payload["data"]["confirmation"]["required"] is True
    assert payload["data"]["confirmation"]["token"].startswith("risk_")
    assert payload["data"]["confirmation"]["confirmFlag"].startswith("--confirm-risk ")


def test_schedule_create_command_uses_context_workflow() -> None:
    result = runner.invoke(
        app,
        [
            "schedule",
            "create",
            "--cron",
            "0 0 4 * * ?",
            "--start",
            "2024-01-01 00:00:00",
            "--end",
            "2025-01-01 00:00:00",
            "--timezone",
            "Asia/Shanghai",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.create"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["resolved"]["tenant"]["source"] == "current_user"
    assert payload["data"]["id"] == 2
    assert payload["data"]["tenantCode"] == "tenant-current-user"


def test_schedule_create_command_requires_confirmation_for_high_frequency_risk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[],
        preview_times_value=[
            "2024-01-01 00:00:00",
            "2024-01-01 00:05:00",
            "2024-01-01 00:10:00",
            "2024-01-01 00:15:00",
            "2024-01-01 00:20:00",
        ],
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )

    result = runner.invoke(
        app,
        [
            "schedule",
            "create",
            "--cron",
            "0 */5 * * * ?",
            "--start",
            "2024-01-01 00:00:00",
            "--end",
            "2025-01-01 00:00:00",
            "--timezone",
            "Asia/Shanghai",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "confirmation_required"
    assert payload["error"]["details"]["risk_type"] == "high_frequency_schedule"
    assert payload["error"]["details"]["confirm_flag"].startswith("--confirm-risk ")
    assert payload["error"]["suggestion"].startswith(
        "Retry the same command with --confirm-risk "
    )


def test_schedule_create_command_rejects_five_field_cron() -> None:
    result = runner.invoke(
        app,
        [
            "schedule",
            "create",
            "--cron",
            "0 4 * * *",
            "--start",
            "2024-01-01 00:00:00",
            "--end",
            "2025-01-01 00:00:00",
            "--timezone",
            "Asia/Shanghai",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["details"] == {
        "field": "cron",
        "format": "quartz",
        "expected_field_counts": [6, 7],
        "field_count": 5,
    }
    assert payload["error"]["suggestion"] == (
        "Use a DolphinScheduler Quartz cron such as `0 0 2 * * ?` instead of "
        "a five-field Unix cron such as `0 2 * * *`."
    )


def test_schedule_update_command_requires_a_change() -> None:
    result = runner.invoke(app, ["schedule", "update", "1"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["action"] == "schedule.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --cron, --start, or --timezone."
    )


def test_schedule_update_command_reports_offline_retry_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            FakeSchedule(
                id=1,
                workflow_definition_code_value=101,
                workflow_definition_name_value="daily-sync",
                project_name_value="etl-prod",
                start_time_value="2024-01-01 00:00:00",
                end_time_value="2025-01-01 00:00:00",
                timezone_id_value="Asia/Shanghai",
                crontab_value="0 0 2 * * ?",
                project_code_value=7,
            )
        ]
    )

    def fail_update(**_: object) -> FakeSchedule:
        raise ApiResultError(
            result_code=10023,
            result_message="online status does not allow update operations",
        )

    monkeypatch.setattr(schedule_adapter, "update", fail_update)
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )

    result = runner.invoke(
        app,
        ["schedule", "update", "1", "--cron", "0 0 6 * * ?"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.update"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schedule offline 1`, then retry `schedule update`."
    )


def test_schedule_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["schedule", "delete", "1"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["action"] == "schedule.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_schedule_delete_command_reports_offline_retry_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            FakeSchedule(
                id=1,
                workflow_definition_code_value=101,
                workflow_definition_name_value="daily-sync",
                project_name_value="etl-prod",
                start_time_value="2024-01-01 00:00:00",
                end_time_value="2025-01-01 00:00:00",
                timezone_id_value="Asia/Shanghai",
                crontab_value="0 0 2 * * ?",
                project_code_value=7,
            )
        ]
    )

    def fail_delete(*, schedule_id: int) -> bool:
        raise ApiResultError(
            result_code=50023,
            result_message=f"the status of schedule {schedule_id} is already online",
        )

    monkeypatch.setattr(schedule_adapter, "delete", fail_delete)
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )

    result = runner.invoke(app, ["schedule", "delete", "1", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.delete"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schedule offline 1`, then retry `schedule delete`."
    )


def test_schedule_create_command_reports_conflict_with_remote_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
        dags={},
    )
    schedule_adapter = FakeScheduleAdapter(schedules=[])

    def fail_create(**_: object) -> FakeSchedule:
        raise ApiResultError(
            result_code=10204,
            result_message=(
                "workflow 101 schedule 1 already exist, please update or delete it"
            ),
        )

    monkeypatch.setattr(schedule_adapter, "create", fail_create)
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )

    result = runner.invoke(
        app,
        [
            "schedule",
            "create",
            "--cron",
            "0 0 4 * * ?",
            "--start",
            "2024-01-01 00:00:00",
            "--end",
            "2025-01-01 00:00:00",
            "--timezone",
            "Asia/Shanghai",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.create"
    assert payload["error"]["type"] == "conflict"
    assert payload["error"]["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10204,
        "result_message": (
            "workflow 101 schedule 1 already exist, please update or delete it"
        ),
    }


def test_schedule_online_command_returns_refreshed_schedule() -> None:
    result = runner.invoke(app, ["schedule", "online", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schedule.online"
    assert payload["data"]["releaseState"] == "ONLINE"
