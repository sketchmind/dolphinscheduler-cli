"""Hermetic checks for live runtime probes consuming execution receipts."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests.live import execution_support as runtime_probe
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def _query_result(data: dict[str, object]) -> DsctlCommandResult:
    return DsctlCommandResult(
        argv=(),
        exit_code=0,
        stdout="",
        stderr="",
        payload={"ok": True, "action": "workflow-instance.list", "data": data},
    )


def _pending_receipt() -> dict[str, object]:
    return {
        "accepted": True,
        "workflowInstanceIds": [],
        "instanceResolution": "pending",
        "triggerCode": 70001,
    }


@pytest.mark.parametrize("stays_pending", [False, True])
def test_live_probe_resolves_pending_trigger_through_project_scoped_list(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, stays_pending: bool
) -> None:
    calls: list[list[str]] = []

    def poll(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
        accept: Callable[[DsctlCommandResult], bool],
        timeout_seconds: float,
        interval_seconds: float,
        command_timeout_seconds: float,
    ) -> DsctlCommandResult:
        assert repo_root == tmp_path
        assert env_file == tmp_path / "unused.env"
        assert (timeout_seconds, interval_seconds, command_timeout_seconds) == (
            60.0,
            2.0,
            30.0,
        )
        calls.append(argv)
        pending = _query_result(
            {
                "triggerCode": 70001,
                "instanceResolution": "pending",
                "totalList": [],
            }
        )
        assert accept(pending) is False
        if stays_pending:
            return pending
        resolved = _query_result(
            {
                "triggerCode": 70001,
                "instanceResolution": "resolved",
                "totalList": [{"id": 42}],
            }
        )
        assert accept(resolved) is True
        return resolved

    monkeypatch.setattr(runtime_probe, "wait_for_result", poll)
    if stays_pending:
        with pytest.raises(AssertionError, match="trigger 70001 stayed pending"):
            runtime_probe._execution_instance_id(
                _pending_receipt(),
                repo_root=tmp_path,
                env_file=tmp_path / "unused.env",
                project="project with spaces",
                fresh_workflow="fresh workflow",
                label="workflow run",
            )
    else:
        assert (
            runtime_probe._execution_instance_id(
                _pending_receipt(),
                repo_root=tmp_path,
                env_file=tmp_path / "unused.env",
                project="project with spaces",
                fresh_workflow="fresh workflow",
                label="workflow run",
            )
            == 42
        )
    assert calls == [
        [
            "workflow-instance",
            "list",
            "--project",
            "project with spaces",
            "--trigger-code",
            "70001",
        ]
    ]


@pytest.mark.parametrize(
    ("resolution", "identities", "accepted", "error"),
    [
        ("resolved", [42], True, None),
        ("resolved", [], True, "exactly one"),
        ("resolved", [42, 43], True, "exactly one"),
        ("resolved", [True], True, "positive integer"),
        ("resolved", [0], True, "positive integer"),
        ("unknown", [], True, "unavailable instance identity"),
        ("pending", [70001], True, "mixed unresolved"),
        ("resolved", [42], False, "was not accepted"),
    ],
)
def test_live_probe_validates_resolved_and_invalid_receipts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    resolution: str,
    identities: list[object],
    *,
    accepted: bool,
    error: str | None,
) -> None:
    def unexpected_poll(*args: object, **kwargs: object) -> None:
        pytest.fail("a resolved or invalid receipt must not issue a trigger query")

    monkeypatch.setattr(runtime_probe, "wait_for_result", unexpected_poll)
    data: dict[str, object] = {
        "accepted": accepted,
        "instanceResolution": resolution,
        "workflowInstanceIds": identities,
    }
    if error is not None:
        with pytest.raises(AssertionError, match=error):
            runtime_probe._execution_instance_id(
                data,
                repo_root=tmp_path,
                env_file=tmp_path / "unused.env",
                project="project",
                fresh_workflow="fresh workflow",
                label="workflow run",
            )
    else:
        assert (
            runtime_probe._execution_instance_id(
                data,
                repo_root=tmp_path,
                env_file=tmp_path / "unused.env",
                project="project",
                fresh_workflow="fresh workflow",
                label="workflow run",
            )
            == 42
        )


@pytest.mark.parametrize(
    ("data", "error"),
    [
        (
            {"triggerCode": 999, "instanceResolution": "resolved", "totalList": []},
            "changed identity",
        ),
        (
            {
                "triggerCode": 70001,
                "instanceResolution": "pending",
                "totalList": [{"id": 42}],
            },
            "pending trigger query returned instance rows",
        ),
        (
            {
                "triggerCode": 70001,
                "instanceResolution": "resolved",
                "totalList": [{"id": 42}, {"id": 43}],
            },
            "exactly one",
        ),
    ],
)
def test_live_trigger_query_rejects_inconsistent_identity(
    data: dict[str, object], error: str
) -> None:
    with pytest.raises(AssertionError, match=error):
        runtime_probe._trigger_instance_id(_query_result(data), trigger_code=70001)


def test_live_trigger_query_does_not_hide_a_read_error_as_pending() -> None:
    failed = DsctlCommandResult(
        argv=(),
        exit_code=1,
        stdout="",
        stderr="permission denied",
        payload={
            "ok": False,
            "action": "workflow-instance.list",
            "error": {"type": "permission_denied"},
        },
    )
    with pytest.raises(AssertionError):
        runtime_probe._trigger_instance_id(failed, trigger_code=70001)
