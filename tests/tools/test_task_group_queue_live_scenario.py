from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests.live import test_runtime_control_surfaces as controls
from tests.live.support import DsctlCommandResult
from tests.live.test_runtime_control_surfaces import (
    _assert_force_started_task_overlap,
    _owned_waiting_queue_row,
)

if TYPE_CHECKING:
    from pathlib import Path


def _queue_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 71,
        "groupId": 12,
        "projectName": "owned-project",
        "workflowInstanceId": 34,
        "taskName": "slot-two",
        "status": "WAIT_QUEUE",
        "priority": 2,
    }
    row.update(changes)
    return row


def _owned_row(
    rows: list[object], *, queue_id: int | None = None, priority: int | None = None
) -> dict[str, object] | None:
    return _owned_waiting_queue_row(
        rows,
        task_group_id=12,
        project="owned-project",
        workflow_instance_id=34,
        task_names=frozenset({"slot-one", "slot-two"}),
        queue_id=queue_id,
        priority=priority,
    )


def test_waiting_queue_selection_requires_owned_workflow_and_group() -> None:
    owned = _queue_row()
    rows: list[object] = [
        _queue_row(id=70, workflowInstanceId=99),
        _queue_row(id=72, groupId=99),
        _queue_row(id=73, projectName="other-project"),
        _queue_row(id=74, taskName="other-task"),
        _queue_row(id=75, status="ACQUIRE_SUCCESS"),
        owned,
    ]
    assert _owned_row(rows) == owned
    assert _owned_row(rows, queue_id=72) is None
    assert _owned_row([owned, _queue_row(id=76)]) is None


def test_priority_readback_requires_same_waiting_queue_row() -> None:
    original = _queue_row()
    assert _owned_row([original], queue_id=71, priority=5) is None
    assert _owned_row([_queue_row(priority=5)], queue_id=71, priority=5) == (
        _queue_row(priority=5)
    )
    assert _owned_row([_queue_row(priority=5, status="RELEASE")], priority=5) is None


def _task_row(name: str, *, start: str, end: str, task_id: int) -> dict[str, object]:
    return {
        "id": task_id,
        "name": name,
        "workflowInstanceId": 34,
        "state": "SUCCESS",
        "startTime": start,
        "endTime": end,
    }


def test_force_start_proof_requires_actual_execution_overlap() -> None:
    holder = _task_row(
        "slot-one", start="2026-09-25 10:00:00", end="2026-09-25 10:02:00", task_id=1
    )
    forced = _task_row(
        "slot-two", start="2026-09-25 10:01:00", end="2026-09-25 10:03:00", task_id=2
    )
    _assert_force_started_task_overlap(
        [holder, forced],
        workflow_instance_id=34,
        queued_task_name="slot-two",
        holder_task_name="slot-one",
    )

    forced["startTime"] = "2026-09-25 10:02:01"
    with pytest.raises(AssertionError, match="did not begin before"):
        _assert_force_started_task_overlap(
            [holder, forced],
            workflow_instance_id=34,
            queued_task_name="slot-two",
            holder_task_name="slot-one",
        )


def _result(
    action: str, *, data: dict[str, object], ok: bool = True
) -> DsctlCommandResult:
    return DsctlCommandResult(
        argv=(action,),
        exit_code=0 if ok else 1,
        stdout="",
        stderr="",
        payload={
            "ok": ok,
            "action": action,
            "data": data if ok else None,
            "error": None if ok else {"type": "not_found"},
        },
    )


def test_cleanup_refuses_foreign_group_before_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def execute(
        _repo_root: Path, argv: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        calls.append(argv)
        return _result(
            "task-group.get",
            data={
                "id": 71,
                "name": "owned-group",
                "projectCode": 999,
                "description": "live task-group queue control group",
                "groupSize": 1,
            },
        )

    monkeypatch.setattr(controls, "run_dsctl", execute)
    with pytest.raises(AssertionError):
        controls._close_owned_queue_task_group(
            tmp_path,
            tmp_path / "profile.env",
            task_group="owned-group",
            project_code=34,
            expected_id=71,
        )
    assert calls == [["task-group", "get", "owned-group"]]


def test_uncertain_group_create_receipt_never_closes_by_global_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def execute(
        _repo_root: Path, argv: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        calls.append(argv)
        return _result(
            "task-group.get",
            data={
                "id": 71,
                "name": "owned-group",
                "projectCode": 34,
                "description": "live task-group queue control group",
                "groupSize": 1,
            },
        )

    monkeypatch.setattr(controls, "run_dsctl", execute)
    controls._close_owned_queue_task_group(
        tmp_path,
        tmp_path / "profile.env",
        task_group="owned-group",
        project_code=34,
        expected_id=None,
    )
    assert calls == [["task-group", "get", "owned-group"]]


def test_project_cascade_refuses_an_unrelated_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def execute(
        _repo_root: Path, argv: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        assert argv == [
            "task-group",
            "list",
            "--project",
            "owned-project",
            "--all",
        ]
        return _result(
            "task-group.list",
            data={
                "total": 1,
                "totalList": [
                    {
                        "id": 72,
                        "name": "other-group",
                        "projectCode": 34,
                        "description": "foreign fixture",
                        "groupSize": 1,
                    }
                ],
                "coverage": {"scope_complete": True, "totals_changed": False},
            },
        )

    monkeypatch.setattr(controls, "run_dsctl", execute)
    with pytest.raises(AssertionError):
        controls._require_only_owned_project_task_group(
            tmp_path,
            tmp_path / "profile.env",
            project="owned-project",
            project_code=34,
            task_group="owned-group",
            expected_id=71,
            create_attempted=True,
        )


def test_queue_cleanup_wait_covers_two_serial_sleep_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[tuple[list[str], float]] = []

    def execute(
        _repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
        timeout_seconds: float,
    ) -> DsctlCommandResult:
        requests.append((argv, timeout_seconds))
        return _result("workflow-instance.watch", data={"id": 55, "state": "SUCCESS"})

    monkeypatch.setattr(controls, "run_dsctl", execute)
    controls._wait_for_cleanup_terminal(
        tmp_path,
        tmp_path / "profile.env",
        project="owned-project",
        workflow_instance_id=55,
        timeout_seconds=420,
    )
    argv, subprocess_timeout = requests[0]
    assert argv[-2:] == ["--timeout-seconds", "420"]
    assert subprocess_timeout == 430.0
