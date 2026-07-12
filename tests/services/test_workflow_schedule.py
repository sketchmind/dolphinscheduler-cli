from __future__ import annotations

from dataclasses import replace

import pytest
from tests.fakes import FakeEnumValue, FakeSchedule, FakeScheduleAdapter

from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    DsctlError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services._workflow_schedule import load_attached_schedule


def _schedule(
    *, schedule_id: int | None = 23, workflow_code: int = 101
) -> FakeSchedule:
    return FakeSchedule(
        id=schedule_id,
        workflow_definition_code_value=workflow_code,
        project_code_value=7,
        start_time_value="2026-01-01 00:00:00",
        end_time_value="2026-12-31 23:59:59",
        timezone_id_value="UTC",
        crontab_value="0 0 0 * * ?",
        release_state_value=FakeEnumValue("OFFLINE"),
    )


def test_load_attached_schedule_distinguishes_absent_and_present() -> None:
    empty_adapter = FakeScheduleAdapter(schedules=[])
    populated_adapter = FakeScheduleAdapter(schedules=[_schedule()])

    absent = load_attached_schedule(
        adapter=empty_adapter,
        project_code=7,
        workflow_code=101,
        workflow_name="daily-sync",
        action="workflow.get",
        phase="read",
    )
    present = load_attached_schedule(
        adapter=populated_adapter,
        project_code=7,
        workflow_code=101,
        workflow_name="daily-sync",
        action="workflow.get",
        phase="read",
    )

    assert absent is None
    assert present is not None
    assert present.id == 23
    assert populated_adapter.list_calls == [
        {
            "project_code": 7,
            "page_no": 1,
            "page_size": 2,
            "workflow_code": 101,
            "search": None,
        }
    ]


@pytest.mark.parametrize(
    ("schedules", "invalid_field"),
    [
        ([_schedule(schedule_id=None)], "id"),
        ([_schedule(schedule_id=0)], "id"),
        ([_schedule(schedule_id=True)], "id"),
        ([_schedule(workflow_code=102)], "workflowDefinitionCode"),
        ([replace(_schedule(), crontab_value="")], "crontab"),
        ([replace(_schedule(), timezone_id_value=None)], "timezoneId"),
        ([replace(_schedule(), start_time_value=None)], "startTime"),
        ([replace(_schedule(), end_time_value=None)], "endTime"),
        ([replace(_schedule(), release_state_value=None)], "releaseState"),
        (
            [replace(_schedule(), release_state_value=FakeEnumValue("PAUSED"))],
            "releaseState",
        ),
        ([_schedule(schedule_id=23), _schedule(schedule_id=24)], ""),
    ],
)
def test_load_attached_schedule_rejects_inconsistent_results(
    schedules: list[FakeSchedule],
    invalid_field: str,
) -> None:
    adapter = FakeScheduleAdapter(
        schedules=schedules,
        ignore_workflow_filter=invalid_field == "workflowDefinitionCode",
    )

    with pytest.raises(
        ApiTransportError,
        match="inconsistent attached-schedule",
    ) as captured:
        load_attached_schedule(
            adapter=adapter,
            project_code=7,
            workflow_code=101,
            workflow_name="daily-sync",
            action="workflow.describe",
            phase="read",
        )

    error = captured.value
    assert error.details["dependency_resource"] == "schedule"
    assert error.details["operation"] == "workflow.describe"
    assert error.details["phase"] == "read"
    if invalid_field:
        assert error.details["invalid_fields"] == [invalid_field]


@pytest.mark.parametrize("reported_total", [None, 0, 2, True])
def test_load_attached_schedule_rejects_total_and_rows_mismatch(
    reported_total: int | None,
) -> None:
    adapter = FakeScheduleAdapter(
        schedules=[_schedule()],
        list_totals_by_call={1: reported_total},
    )

    with pytest.raises(ApiTransportError) as captured:
        load_attached_schedule(
            adapter=adapter,
            project_code=7,
            workflow_code=101,
            workflow_name="daily-sync",
            action="workflow.export",
            phase="read",
        )

    assert captured.value.details["reported_total"] is reported_total
    assert captured.value.details["returned_count"] == 1


@pytest.mark.parametrize(
    ("result_code", "error_type"),
    [
        (10018, NotFoundError),
        (50003, NotFoundError),
        (30001, PermissionDeniedError),
        (99999, ApiTransportError),
    ],
)
def test_load_attached_schedule_translates_lookup_errors(
    result_code: int,
    error_type: type[DsctlError],
) -> None:
    adapter = FakeScheduleAdapter(
        schedules=[],
        list_error=ApiResultError(
            result_code=result_code,
            result_message="lookup failed",
        ),
    )

    with pytest.raises(error_type) as captured:
        load_attached_schedule(
            adapter=adapter,
            project_code=7,
            workflow_code=101,
            workflow_name="daily-sync",
            action="workflow.offline",
            phase="post_mutation_refresh",
        )

    error = captured.value
    assert not isinstance(error, ApiResultError)
    assert error.details["mutation_applied"] is True
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion


@pytest.mark.parametrize(
    ("upstream_error", "error_type"),
    [
        (ApiTransportError("connection reset"), ApiTransportError),
        (
            ApiHttpError(
                "gateway unavailable",
                status_code=503,
                body={"message": "unavailable"},
            ),
            ApiHttpError,
        ),
    ],
)
def test_load_attached_schedule_marks_post_mutation_transport_errors(
    upstream_error: Exception,
    error_type: type[Exception],
) -> None:
    adapter = FakeScheduleAdapter(schedules=[], list_error=upstream_error)

    with pytest.raises(error_type) as captured:
        load_attached_schedule(
            adapter=adapter,
            project_code=7,
            workflow_code=101,
            workflow_name="daily-sync",
            action="workflow.offline",
            phase="post_mutation_refresh",
        )

    error = captured.value
    assert isinstance(error, (ApiHttpError, ApiTransportError))
    assert error.details["mutation_applied"] is True
    assert error.details["upstream_error_type"] in {
        "api_http_error",
        "api_transport_error",
    }
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion
