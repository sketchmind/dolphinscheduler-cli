from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

import pytest
from tests.live import test_runtime_surfaces as runtime_surfaces
from tests.live.support import DsctlCommandResult, cleanup_live_resources

if TYPE_CHECKING:
    from pathlib import Path


def test_cleanup_attempts_every_task_and_preserves_each_error() -> None:
    calls: list[str] = []
    first_error = RuntimeError("first delete failed")
    second_error = AssertionError("second delete not confirmed")

    def cleanup(name: str, failure: Exception | None) -> None:
        calls.append(name)
        if failure is not None:
            raise failure

    with pytest.raises(ExceptionGroup) as caught:
        cleanup_live_resources(
            [
                partial(cleanup, "first", first_error),
                partial(cleanup, "second", second_error),
                partial(cleanup, "third", None),
                partial(cleanup, "verify", None),
            ]
        )

    assert calls == ["first", "second", "third", "verify"]
    assert caught.value.exceptions == (first_error, second_error)


def test_resource_success_is_registered_before_content_assertions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        calls.append(args)
        data: dict[str, object]
        if args[1] == "create":
            data = {"fullName": "/wrong-response-name.sql"}
        elif args[1] == "delete":
            data = {"deleted": True}
        else:
            data = {"totalList": [], "total": 0}
        if args[1] == "list":
            data["coverage"] = {"scope_complete": True, "totals_changed": False}
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={
                "ok": True,
                "action": f"resource.{args[1]}",
                "resolved": {"directory": "/"},
                "data": data,
            },
        )

    monkeypatch.setattr(runtime_surfaces, "run_dsctl", run_dsctl)

    with pytest.raises(AssertionError):
        runtime_surfaces.test_etl_resource_lifecycle_round_trips(
            tmp_path, tmp_path / "etl.env", lambda stem: stem, tmp_path
        )

    assert calls[-2:] == [
        ["resource", "delete", "/resource.sql", "--force"],
        ["resource", "list", "--all"],
    ]


def test_resource_cleanup_verifies_absence_after_delete_acknowledgement(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        calls.append(args)
        data: dict[str, object]
        data = (
            {"deleted": True}
            if args[1] == "delete"
            else {"total": 1, "totalList": [{"fullName": "file:///owned-dir/"}]}
        )
        if args[1] == "list":
            data["coverage"] = {"scope_complete": True, "totals_changed": False}
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={"ok": True, "action": f"resource.{args[1]}", "data": data},
        )

    monkeypatch.setattr(runtime_surfaces, "run_dsctl", run_dsctl)

    with pytest.raises(ExceptionGroup) as caught:
        runtime_surfaces._cleanup_resources(
            tmp_path,
            tmp_path / "etl.env",
            registered_paths=["file:/owned-dir/"],
            expected_paths={"file:/owned-dir"},
        )

    assert calls[-1] == ["resource", "list", "--all"]
    assert len(caught.value.exceptions) == 1
    assert "Test resources remain" in str(caught.value.exceptions[0])


def test_resource_inventory_uses_full_name_when_file_names_are_paths() -> None:
    first = {
        "fullName": "file:///storage/tenant/resources/first/shared.sql",
        "fileName": "first/shared.sql",
        "isDirectory": False,
    }
    second = {
        "fullName": "file:///storage/tenant/resources/second/shared.sql/",
        "fileName": "second/shared.sql",
        "isDirectory": False,
    }

    indexed = runtime_surfaces._resource_rows_by_full_name([first, second])

    first_identity = runtime_surfaces._resource_full_name_identity(
        "file:/storage/tenant/resources/first/shared.sql"
    )
    second_identity = runtime_surfaces._resource_full_name_identity(
        "file:/storage/tenant/resources/second/shared.sql"
    )
    assert indexed[first_identity] is first
    assert indexed[second_identity] is second
    assert first_identity != second_identity
