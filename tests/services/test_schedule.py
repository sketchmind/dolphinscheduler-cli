from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeUser,
    FakeUserAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.config import ClusterProfile
from dsctl.context import SessionContext
from dsctl.errors import (
    ApiResultError,
    ConfirmationRequiredError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
)
from dsctl.services import runtime as runtime_service
from dsctl.services import schedule as schedule_service


def _install_schedule_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    schedule_adapter: FakeScheduleAdapter,
    user_adapter: FakeUserAdapter | None = None,
    context: SessionContext | None = None,
    profile: ClusterProfile | None = None,
    project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile() if profile is None else profile,
            context=context,
            workflow_adapter=workflow_adapter,
            schedule_adapter=schedule_adapter,
            user_adapter=user_adapter,
            project_preference_adapter=project_preference_adapter,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_workflow_adapter() -> FakeWorkflowAdapter:
    return FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=102,
                name="adhoc-backfill",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
        dags={},
    )


@pytest.fixture
def fake_schedule_adapter() -> FakeScheduleAdapter:
    return FakeScheduleAdapter(
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
                release_state_value=None,
                project_code_value=7,
            ),
            FakeSchedule(
                id=2,
                workflow_definition_code_value=102,
                workflow_definition_name_value="adhoc-backfill",
                project_name_value="etl-prod",
                start_time_value="2024-01-01 00:00:00",
                end_time_value="2025-01-01 00:00:00",
                timezone_id_value="Asia/Shanghai",
                crontab_value="0 0 3 * * ?",
                project_code_value=7,
            ),
        ]
    )


def test_list_schedules_result_returns_project_page(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = schedule_service.list_schedules_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert data["total"] == 2
    assert data["pageSize"] == 1
    assert _mapping(items[0])["workflowDefinitionName"] == "daily-sync"


def test_list_schedules_result_can_filter_by_workflow(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = schedule_service.list_schedules_result(workflow="daily-sync")
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert _mapping(result.resolved["workflow"])["code"] == 101
    assert len(items) == 1
    assert _mapping(items[0])["id"] == 1


def test_get_schedule_result_returns_one_schedule(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    result = schedule_service.get_schedule_result(1)
    data = _mapping(result.data)

    assert result.resolved == {"schedule": {"id": 1}}
    assert data["workflowDefinitionCode"] == 101
    assert data["crontab"] == "0 0 2 * * ?"


def test_preview_schedule_result_returns_times_and_analysis(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = schedule_service.preview_schedule_result(
        cron="0 0 2 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    data = _mapping(result.data)
    analysis = _mapping(data["analysis"])
    times = _sequence(data["times"])

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert data["count"] == 5
    assert len(times) == 5
    assert analysis["risk_level"] == "none"


def test_preview_schedule_result_rejects_five_field_cron(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Quartz cron expression") as exc_info:
        schedule_service.preview_schedule_result(
            cron="0 2 * * *",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )

    assert exc_info.value.details == {
        "field": "cron",
        "format": "quartz",
        "expected_field_counts": [6, 7],
        "field_count": 5,
    }
    assert exc_info.value.suggestion == (
        "Use a DolphinScheduler Quartz cron such as `0 0 2 * * ?` instead of "
        "a five-field Unix cron such as `0 2 * * *`."
    )


def test_list_schedules_result_rejects_workflow_and_search_together(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="--workflow and --search cannot be used together",
    ) as exc_info:
        schedule_service.list_schedules_result(
            workflow="daily-sync",
            search="daily",
        )

    assert exc_info.value.suggestion == (
        "Use `--workflow` to scope to one workflow, or drop it and use "
        "`--search` across schedules in the selected project."
    )


def test_preview_schedule_result_rejects_mixing_id_and_schedule_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="schedule id preview does not accept project or schedule fields",
    ) as exc_info:
        schedule_service.preview_schedule_result(
            schedule_id=1,
            cron="0 0 2 * * ?",
        )

    assert exc_info.value.suggestion == (
        "Pass only the schedule id to preview an existing schedule, or omit "
        "the id and pass `--project`, `--cron`, `--start`, `--end`, and "
        "`--timezone` for an ad hoc preview."
    )


def test_explain_schedule_result_describes_safe_create_mutation(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = schedule_service.explain_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    data = _mapping(result.data)
    proposed = _mapping(data["proposedSchedule"])
    confirmation = _mapping(data["confirmation"])

    assert data["mutationAction"] == "schedule.create"
    assert proposed["crontab"] == "0 0 4 * * ?"
    assert proposed["timezoneId"] == "Asia/Shanghai"
    assert confirmation["required"] is False
    assert confirmation["nextAction"] == "apply"
    assert confirmation["token"] is None
    assert _mapping(result.resolved["workflow"])["source"] == "context"
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }


def test_explain_schedule_result_uses_current_user_tenant_for_create_mutation(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = schedule_service.explain_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    proposed = _mapping(_mapping(result.data)["proposedSchedule"])

    assert proposed["tenantCode"] == "tenant-current-user"
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-current-user",
        "source": "current_user",
    }


def test_explain_schedule_result_uses_enabled_project_preference_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=8,
                    code=8,
                    project_code_value=7,
                    state=1,
                    preferences_value=(
                        "{"
                        '"taskPriority":"HIGH",'
                        '"warningType":"ALL",'
                        '"workerGroup":"pref-group",'
                        '"tenant":"tenant-pref",'
                        '"environmentCode":99,'
                        '"alertGroups":7'
                        "}"
                    ),
                )
            ]
        ),
    )

    result = schedule_service.explain_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    proposed = _mapping(_mapping(result.data)["proposedSchedule"])

    assert proposed["warningType"] == "ALL"
    assert proposed["warningGroupId"] == 7
    assert proposed["workflowInstancePriority"] == "HIGH"
    assert proposed["workerGroup"] == "pref-group"
    assert proposed["tenantCode"] == "tenant-pref"
    assert proposed["environmentCode"] == 99
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-pref",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["project_preference"]) == {
        "used_fields": [
            "warningType",
            "warningGroupId",
            "workflowInstancePriority",
            "workerGroup",
            "environmentCode",
            "tenantCode",
        ]
    }


def test_create_schedule_result_requires_confirmation_for_high_frequency_preview(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 */5 * * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )

    details = _mapping(captured.value.details)
    preview = _mapping(details["preview"])
    analysis = _mapping(preview["analysis"])
    assert details["risk_type"] == "high_frequency_schedule"
    assert analysis["min_interval_seconds"] == 300


def test_explain_schedule_result_matches_create_confirmation_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    explain_result = schedule_service.explain_schedule_result(
        workflow=None,
        cron="0 */5 * * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    explain_token = _mapping(_mapping(explain_result.data)["confirmation"])["token"]

    with pytest.raises(ConfirmationRequiredError) as captured:
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 */5 * * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )

    create_token = _mapping(captured.value.details)["confirmation_token"]
    assert explain_token == create_token


def test_create_schedule_result_accepts_matching_confirmation_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 */5 * * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )

    confirmation = _mapping(captured.value.details)["confirmation_token"]
    assert isinstance(confirmation, str)

    result = schedule_service.create_schedule_result(
        workflow=None,
        cron="0 */5 * * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
        confirm_risk=confirmation,
    )

    assert result.warnings
    assert "confirmed high-frequency schedule risk" in result.warnings[0]
    assert result.warning_details == [
        {
            "code": "confirmed_high_frequency_schedule",
            "message": result.warnings[0],
            "risk_type": "high_frequency_schedule",
            "min_interval_seconds": 300,
            "threshold_seconds": 600,
        }
    ]


def test_explain_schedule_result_matches_update_confirmation_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    explain_result = schedule_service.explain_schedule_result(
        schedule_id=1,
        cron="0 */5 * * * ?",
    )
    explain_data = _mapping(explain_result.data)
    current = _mapping(explain_data["currentSchedule"])
    proposed = _mapping(explain_data["proposedSchedule"])
    explain_token = _mapping(explain_data["confirmation"])["token"]

    assert explain_data["mutationAction"] == "schedule.update"
    assert current["crontab"] == "0 0 2 * * ?"
    assert proposed["crontab"] == "0 */5 * * * ?"
    assert proposed["timezoneId"] == "Asia/Shanghai"
    assert explain_data["requestedFields"] == ["crontab"]
    assert explain_data["changedFields"] == ["crontab"]
    assert explain_data["unchangedRequestedFields"] == []
    assert explain_data["inheritedFields"] == [
        "startTime",
        "endTime",
        "timezoneId",
        "failureStrategy",
        "warningType",
        "warningGroupId",
        "workflowInstancePriority",
        "workerGroup",
        "tenantCode",
        "environmentCode",
    ]
    assert _mapping(explain_result.resolved["schedule"]) == {
        "id": 1,
        "workflowDefinitionCode": 101,
        "workflowDefinitionName": "daily-sync",
        "projectName": "etl-prod",
        "projectCode": 7,
    }

    with pytest.raises(ConfirmationRequiredError) as captured:
        schedule_service.update_schedule_result(
            1,
            cron="0 */5 * * * ?",
        )

    update_token = _mapping(captured.value.details)["confirmation_token"]
    assert explain_token == update_token


def test_explain_schedule_result_translates_missing_bound_workflow(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    class MissingBoundWorkflowAdapter(FakeWorkflowAdapter):
        def get(self, *, code: int) -> FakeWorkflow:
            raise ApiResultError(
                result_code=50003,
                result_message=f"workflow code {code} not found",
            )

    fake_schedule_adapter.schedules = [
        FakeSchedule(
            id=1,
            workflow_definition_code_value=404,
            workflow_definition_name_value="missing-workflow",
            project_name_value="etl-prod",
            start_time_value="2024-01-01 00:00:00",
            end_time_value="2025-01-01 00:00:00",
            timezone_id_value="Asia/Shanghai",
            crontab_value="0 0 2 * * ?",
            project_code_value=7,
        )
    ]
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=MissingBoundWorkflowAdapter(workflows=[], dags={}),
        schedule_adapter=fake_schedule_adapter,
    )

    with pytest.raises(NotFoundError) as exc_info:
        schedule_service.explain_schedule_result(
            schedule_id=1,
            cron="0 */5 * * * ?",
        )

    assert exc_info.value.message == (
        "The workflow bound to this schedule does not exist."
    )
    assert exc_info.value.details == {
        "resource": "schedule",
        "operation": "explain",
        "schedule_id": 1,
        "workflow_code": 404,
    }


def test_create_schedule_result_uses_workflow_context(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = schedule_service.create_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    data = _mapping(result.data)

    assert _mapping(result.resolved["workflow"])["source"] == "context"
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }
    assert data["id"] == 3
    assert data["workflowDefinitionCode"] == 101
    assert data["releaseState"] == "OFFLINE"


def test_create_schedule_result_prefers_explicit_tenant_over_current_user(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
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
    )

    result = schedule_service.create_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )
    data = _mapping(result.data)

    assert data["tenantCode"] == "tenant-current-user"
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-current-user",
        "source": "current_user",
    }

    explicit = schedule_service.create_schedule_result(
        workflow=None,
        cron="0 0 5 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
        tenant_code="tenant-flag",
    )

    assert _mapping(explicit.data)["tenantCode"] == "tenant-flag"
    assert _mapping(explicit.resolved["tenant"]) == {
        "value": "tenant-flag",
        "source": "flag",
    }


def test_create_schedule_result_uses_current_user_tenant_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
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
    )

    result = schedule_service.create_schedule_result(
        workflow=None,
        cron="0 0 4 * * ?",
        start="2024-01-01 00:00:00",
        end="2025-01-01 00:00:00",
        timezone="Asia/Shanghai",
    )

    assert _mapping(result.data)["tenantCode"] == "tenant-current-user"
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-current-user",
        "source": "current_user",
    }


def test_update_schedule_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    result = schedule_service.update_schedule_result(
        1,
        cron="0 0 6 * * ?",
    )
    data = _mapping(result.data)

    assert data["id"] == 1
    assert data["crontab"] == "0 0 6 * * ?"
    assert data["timezoneId"] == "Asia/Shanghai"


def test_delete_schedule_result_requires_force(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    with pytest.raises(UserInputError, match="requires --force"):
        schedule_service.delete_schedule_result(1, force=False)


def test_online_schedule_result_returns_refreshed_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    result = schedule_service.online_schedule_result(1)

    assert _mapping(result.data)["releaseState"] == "ONLINE"


def test_list_schedules_result_requires_project_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(),
    )

    with pytest.raises(UserInputError, match="Project is required") as exc_info:
        schedule_service.list_schedules_result()
    assert exc_info.value.suggestion == (
        "Pass --project NAME or run `dsctl use project NAME`."
    )


def test_create_schedule_result_requires_workflow_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Workflow is required") as exc_info:
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 0 4 * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )
    assert exc_info.value.suggestion == (
        "Pass --workflow NAME or run `dsctl use workflow NAME`."
    )


def test_get_schedule_result_reports_missing_schedules(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    with pytest.raises(NotFoundError, match="Schedule 999 does not exist"):
        schedule_service.get_schedule_result(999)


def test_create_schedule_result_maps_duplicate_schedule_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    def fail_create(**_: object) -> FakeSchedule:
        raise ApiResultError(
            result_code=10204,
            result_message=(
                "workflow 101 schedule 1 already exist, please update or delete it"
            ),
        )

    monkeypatch.setattr(fake_schedule_adapter, "create", fail_create)

    with pytest.raises(ConflictError, match="already has a schedule"):
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 0 4 * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )


def test_create_schedule_result_maps_offline_workflow_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    def fail_create(**_: object) -> FakeSchedule:
        raise ApiResultError(
            result_code=50004,
            result_message=(
                "workflow definition daily-sync workflow version 1 not online"
            ),
        )

    monkeypatch.setattr(fake_schedule_adapter, "create", fail_create)

    with pytest.raises(InvalidStateError, match="workflow must be online") as exc_info:
        schedule_service.create_schedule_result(
            workflow=None,
            cron="0 0 4 * * ?",
            start="2024-01-01 00:00:00",
            end="2025-01-01 00:00:00",
            timezone="Asia/Shanghai",
        )

    assert exc_info.value.suggestion == (
        "Bring the owning workflow online, then retry the schedule operation."
    )


def test_update_schedule_result_maps_online_constraint_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    def fail_update(**_: object) -> FakeSchedule:
        raise ApiResultError(
            result_code=10023,
            result_message="online status does not allow update operations",
        )

    monkeypatch.setattr(fake_schedule_adapter, "update", fail_update)

    with pytest.raises(InvalidStateError, match="cannot be updated") as exc_info:
        schedule_service.update_schedule_result(1, cron="0 0 6 * * ?")

    assert exc_info.value.suggestion == (
        "Run `dsctl schedule offline 1`, then retry `schedule update`."
    )


def test_update_schedule_result_rejects_five_field_cron(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    with pytest.raises(UserInputError, match="Quartz cron expression") as exc_info:
        schedule_service.update_schedule_result(1, cron="0 6 * * *")

    assert exc_info.value.details == {
        "field": "cron",
        "format": "quartz",
        "expected_field_counts": [6, 7],
        "field_count": 5,
    }


def test_delete_schedule_result_maps_online_constraint_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_schedule_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        schedule_adapter=fake_schedule_adapter,
    )

    def fail_delete(*, schedule_id: int) -> bool:
        raise ApiResultError(
            result_code=50023,
            result_message=f"the status of schedule {schedule_id} is already online",
        )

    monkeypatch.setattr(fake_schedule_adapter, "delete", fail_delete)

    with pytest.raises(InvalidStateError, match="cannot be deleted") as exc_info:
        schedule_service.delete_schedule_result(1, force=True)

    assert exc_info.value.suggestion == (
        "Run `dsctl schedule offline 1`, then retry `schedule delete`."
    )
