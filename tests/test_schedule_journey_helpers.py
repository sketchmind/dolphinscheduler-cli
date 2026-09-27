from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import TYPE_CHECKING, cast

import pytest

from tests.live import workflow_support
from tests.live.schedule_journey import (
    _capture_failed_task_diagnostics,
    _confirmed_schedule_call,
    _expect_online_update_invalid_state,
    _require_selected_tenant,
    _schedule_fields,
    _schedule_window,
    _wait_for_scheduled_instance,
)
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from tests.live.journey_support import Journey


class _RowsJourney:
    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self._pages = iter(pages)

    def rows(self, argv: list[str]) -> list[dict[str, object]]:
        assert argv == ["workflow-instance", "list"]
        return next(self._pages)


class _TokenRolloverJourney:
    def __init__(
        self,
        *,
        action: str = "schedule.update",
        first_error: str = "confirmation_required",
        final_error: str | None = "invalid_state",
        rolled_value: str = "risk_new",
    ) -> None:
        self.action = action
        self.first_error = first_error
        self.final_error = final_error
        self.rolled_value = rolled_value
        self.data_calls = 0
        self.call_argv: list[list[str]] = []

    def data(self, argv: list[str]) -> dict[str, object]:
        assert argv == ["schedule", "explain", "7"]
        self.data_calls += 1
        token = "risk_old" if self.data_calls == 1 else self.rolled_value
        return {"confirmation": {"required": True, "token": token}}

    def call(self, argv: list[str]) -> DsctlCommandResult:
        self.call_argv.append(argv)
        if len(self.call_argv) == 1:
            payload: dict[str, object] = {
                "ok": False,
                "action": self.action,
                "data": {},
                "error": {
                    "type": self.first_error,
                    "details": {
                        "risk_type": "high_frequency_schedule",
                        "confirmation_token": self.rolled_value,
                    },
                },
            }
        else:
            payload = {
                "ok": False,
                "action": self.action,
                "data": {},
                "error": {"type": self.final_error},
            }
        if len(self.call_argv) > 1 and self.final_error is None:
            payload = {"ok": True, "action": self.action, "data": {"id": 7}}
        return DsctlCommandResult(
            tuple(argv), 0 if payload["ok"] else 1, "", "", payload
        )


class _DiagnosticJourney:
    project = "owned-project"

    def __init__(
        self,
        *,
        list_ok: bool = True,
        log_supported: bool = True,
        workspace: Path | None = None,
        call_error: Exception | None = None,
    ) -> None:
        self.list_ok = list_ok
        self.log_supported = log_supported
        self.workspace = workspace
        self.call_error = call_error
        self.call_argv: list[list[str]] = []

    def capability(self, action: str) -> bool:
        assert action in {"task-instance.list", "task-instance.log"}
        return action == "task-instance.list" or self.log_supported

    def call(self, argv: list[str]) -> DsctlCommandResult:
        self.call_argv.append(argv)
        if self.call_error is not None:
            raise self.call_error
        action = ".".join(argv[:2])
        if action == "task-instance.list":
            if not self.list_ok:
                payload: dict[str, object] = {
                    "ok": False,
                    "action": action,
                    "error": {"type": "api_transport_error"},
                }
                return DsctlCommandResult(tuple(argv), 1, "", "", payload)
            payload = {
                "ok": True,
                "action": action,
                "data": {
                    "totalList": [
                        {"id": 14, "workflowInstanceId": 7, "state": "FAILURE"},
                        {"id": 11, "workflowInstanceId": 7, "state": "FAILURE"},
                        {"id": 12, "workflowInstanceId": 7, "state": "FAILURE"},
                        {"id": 10, "workflowInstanceId": 7, "state": "SUCCESS"},
                        {"id": 9, "workflowInstanceId": 8, "state": "FAILURE"},
                    ]
                },
            }
            return DsctlCommandResult(tuple(argv), 0, "", "", payload)
        payload = {
            "ok": False,
            "action": action,
            "error": {"type": "task_not_dispatched"},
        }
        return DsctlCommandResult(tuple(argv), 1, "", "", payload)


def test_legacy_scheduler_command_type_is_canonical_string() -> None:
    versions = ["1.3.9", *(f"2.0.{patch}" for patch in range(10))]

    for version in versions:
        module = import_module(
            "dsctl.generated.versions."
            f"ds_{version.replace('.', '_')}.common.enums.command_type"
        )
        scheduler = module.CommandType.SCHEDULER
        assert scheduler.value == "SCHEDULER"
        assert scheduler.code == 6


def test_schedule_fields_follow_exact_schema_options() -> None:
    legacy = _schedule_fields(
        option_flags=set(),
        start="2026-09-20 12:00:00",
        end="2026-09-20 12:15:00",
    )
    modern = _schedule_fields(
        option_flags={
            "--timezone",
            "--tenant-code",
            "--environment-code",
            "--missed-fire-policy",
        },
        start="2026-09-20 12:00:00",
        end="2026-09-20 12:15:00",
    )
    preview = _schedule_fields(
        option_flags={
            "--timezone",
            "--tenant-code",
            "--environment-code",
            "--missed-fire-policy",
        },
        start="2026-09-20 12:00:00",
        end="2026-09-20 12:15:00",
        include_create_options=False,
    )

    assert "--timezone" not in legacy
    assert "--environment-code" not in legacy
    assert modern[-8:] == [
        "--timezone",
        "Asia/Shanghai",
        "--tenant-code",
        "default",
        "--environment-code",
        "0",
        "--missed-fire-policy",
        "SKIP_MISSED",
    ]
    assert "--timezone" in preview
    assert "--tenant-code" not in preview
    assert "--environment-code" not in preview
    assert "--missed-fire-policy" not in preview


def test_selected_tenant_checks_persisted_and_optional_instance() -> None:
    _require_selected_tenant(
        {"tenantCode": "default"},
        selected_tenant="default",
        label="stored schedule",
        required=True,
    )
    _require_selected_tenant(
        {},
        selected_tenant="default",
        label="scheduled workflow instance",
    )

    with pytest.raises(AssertionError, match="expected 'default', got 'fixture'"):
        _require_selected_tenant(
            {"tenantCode": "fixture"},
            selected_tenant="default",
            label="scheduled workflow instance",
        )
    with pytest.raises(AssertionError, match="expected 'default', got None"):
        _require_selected_tenant(
            {},
            selected_tenant="default",
            label="stored schedule",
            required=True,
        )


def test_failed_task_diagnostics_are_scoped_bounded_and_non_masking() -> None:
    journey = _DiagnosticJourney()

    _capture_failed_task_diagnostics(cast("Journey", journey), instance_id=7)

    assert journey.call_argv == [
        [
            "task-instance",
            "list",
            "--project",
            "owned-project",
            "--workflow-instance",
            "7",
            "--page-size",
            "20",
        ],
        ["task-instance", "log", "11", "--tail", "100"],
        ["task-instance", "log", "12", "--tail", "100"],
    ]

    failed_read = _DiagnosticJourney(list_ok=False)
    _capture_failed_task_diagnostics(cast("Journey", failed_read), instance_id=7)
    assert len(failed_read.call_argv) == 1

    unsupported_log = _DiagnosticJourney(log_supported=False)
    _capture_failed_task_diagnostics(cast("Journey", unsupported_log), instance_id=7)
    assert len(unsupported_log.call_argv) == 1


def test_failed_task_diagnostic_timeout_preserves_original_failure(
    tmp_path: Path,
) -> None:
    journey = _DiagnosticJourney(
        workspace=tmp_path,
        call_error=subprocess.TimeoutExpired(["dsctl"], 60),
    )

    _capture_failed_task_diagnostics(cast("Journey", journey), instance_id=7)

    assert (tmp_path / "failed-task-diagnostic-error.json").read_text() == (
        '{"error_type": "TimeoutExpired"}\n'
    )
    mode = (tmp_path / "failed-task-diagnostic-error.json").stat().st_mode
    assert mode & 0o777 == 0o600


def test_schedule_window_is_future_with_wait_budget_and_legacy_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedDatetime:
        @classmethod
        def now(cls, *, tz: ZoneInfo) -> datetime:
            return datetime(2026, 9, 21, 0, 7, 59, tzinfo=tz)

    monkeypatch.setattr("tests.live.schedule_journey.datetime", _FixedDatetime)

    plain_start, plain_end = _schedule_window()
    offset_start, offset_end = _schedule_window(include_offset=True)

    assert (plain_start, plain_end) == (
        "2026-09-21 00:09:00",
        "2026-09-21 00:25:00",
    )
    assert (offset_start, offset_end) == (
        "2026-09-21T00:09:00+08:00",
        "2026-09-21T00:25:00+08:00",
    )
    assert datetime.fromisoformat(offset_end) - datetime.fromisoformat(
        offset_start
    ) == timedelta(minutes=16)


def test_online_update_refreshes_one_rolled_preflight_token() -> None:
    journey = _TokenRolloverJourney()

    _expect_online_update_invalid_state(
        cast("Journey", journey),
        explain_argv=["schedule", "explain", "7"],
        update_argv=["schedule", "update", "7"],
    )

    assert journey.data_calls == 2
    assert journey.call_argv == [
        ["schedule", "update", "7", "--confirm-risk", "risk_old"],
        ["schedule", "update", "7", "--confirm-risk", "risk_new"],
    ]


def test_wait_requires_new_scheduler_instance_with_schedule_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journey = _RowsJourney(
        [
            [
                {
                    "id": 8,
                    "commandType": "START_PROCESS",
                    "scheduleTime": None,
                }
            ],
            [
                {
                    "id": 8,
                    "commandType": "START_PROCESS",
                    "scheduleTime": None,
                },
                {
                    "id": 9,
                    "commandType": "SCHEDULER",
                    "scheduleTime": "2026-09-20 12:01:00",
                },
            ],
        ]
    )
    monkeypatch.setattr("tests.live.schedule_journey.time.sleep", lambda _: None)

    row = _wait_for_scheduled_instance(
        cast("Journey", journey),
        instance_argv=["workflow-instance", "list"],
        baseline_ids={8},
        timeout_seconds=1,
    )

    assert row["id"] == 9
    assert row["commandType"] == "SCHEDULER"


@pytest.mark.parametrize("operation", ["create", "update"])
def test_confirmed_schedule_mutation_refreshes_once(operation: str) -> None:
    journey = _TokenRolloverJourney(action=f"schedule.{operation}", final_error=None)
    result = _confirmed_schedule_call(
        cast("Journey", journey),
        explain_argv=["schedule", "explain", "7"],
        mutation_argv=["schedule", operation, "7"],
    )
    assert result.payload["ok"] is True
    assert len(journey.call_argv) == journey.data_calls == 2


@pytest.mark.parametrize("error_type", ["api_transport_error", "api_result_error"])
def test_confirmed_schedule_does_not_retry_uncertain_mutation(error_type: str) -> None:
    journey = _TokenRolloverJourney(first_error=error_type)
    result = _confirmed_schedule_call(
        cast("Journey", journey),
        explain_argv=["schedule", "explain", "7"],
        mutation_argv=["schedule", "update", "7"],
    )
    assert result.exit_code == 1
    assert len(journey.call_argv) == journey.data_calls == 1


def test_confirmed_schedule_does_not_retry_twice() -> None:
    journey = _TokenRolloverJourney(final_error="confirmation_required")
    result = _confirmed_schedule_call(
        cast("Journey", journey),
        explain_argv=["schedule", "explain", "7"],
        mutation_argv=["schedule", "update", "7"],
    )
    assert result.exit_code == 1
    assert len(journey.call_argv) == journey.data_calls == 2


def test_confirmed_schedule_rejects_unchanged_token() -> None:
    journey = _TokenRolloverJourney(rolled_value="risk_old")
    with pytest.raises(AssertionError, match="changed confirmation token"):
        _confirmed_schedule_call(
            cast("Journey", journey),
            explain_argv=["schedule", "explain", "7"],
            mutation_argv=["schedule", "update", "7"],
        )
    assert len(journey.call_argv) == journey.data_calls == 1


@pytest.mark.parametrize("second", [0, 59])
def test_activation_refresh_uses_current_time_and_a_new_confirmation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, second: int
) -> None:
    class _FixedDatetime:
        @classmethod
        def now(cls, *, tz: ZoneInfo) -> datetime:
            return datetime(2026, 9, 24, 12, 7, second, tzinfo=tz)

    monkeypatch.setattr(workflow_support, "datetime", _FixedDatetime)
    calls: list[list[str]] = []

    def run(root: Path, argv: list[str], *, env_file: Path) -> DsctlCommandResult:
        assert root == tmp_path
        assert env_file == tmp_path / "etl.env"
        calls.append(argv)
        data: dict[str, object] = (
            {"confirmation": {"required": True, "token": "fresh-window-token"}}
            if argv[1] == "explain"
            else {"id": 7, "warningType": "SUCCESS", "environmentCode": 8}
        )
        payload: dict[str, object] = {
            "ok": True,
            "action": f"schedule.{argv[1]}",
            "data": data,
        }
        return DsctlCommandResult(tuple(argv), 0, "", "", payload)

    monkeypatch.setattr(workflow_support, "run_dsctl", run)
    result = workflow_support.refresh_schedule_window(
        tmp_path, tmp_path / "etl.env", schedule_id=7, project="owned-project"
    )
    fields = [
        "7",
        "--project",
        "owned-project",
        "--start",
        "2026-09-24 12:10:00",
        "--end",
        "2026-09-24 12:25:00",
    ]
    assert calls == [
        ["schedule", "explain", *fields],
        ["schedule", "update", *fields, "--confirm-risk", "fresh-window-token"],
    ]
    assert result == {"id": 7, "warningType": "SUCCESS", "environmentCode": 8}
    lead = datetime(2026, 9, 24, 12, 10, tzinfo=UTC) - datetime(
        2026, 9, 24, 12, 7, second, tzinfo=UTC
    )
    assert timedelta(minutes=2) < lead <= timedelta(minutes=3)


@pytest.mark.parametrize("token", [None, ""])
def test_activation_refresh_does_not_update_without_a_fresh_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, token: str | None
) -> None:
    calls: list[list[str]] = []

    def run(root: Path, argv: list[str], *, env_file: Path) -> DsctlCommandResult:
        del root, env_file
        calls.append(argv)
        payload: dict[str, object] = {
            "ok": True,
            "action": "schedule.explain",
            "data": {"confirmation": {"required": True, "token": token}},
        }
        return DsctlCommandResult(tuple(argv), 0, "", "", payload)

    monkeypatch.setattr(workflow_support, "run_dsctl", run)
    with pytest.raises(TypeError, match="window risk token"):
        workflow_support.refresh_schedule_window(
            tmp_path, tmp_path / "etl.env", schedule_id=7, project="owned-project"
        )
    assert len(calls) == 1
    assert calls[0][:2] == ["schedule", "explain"]
