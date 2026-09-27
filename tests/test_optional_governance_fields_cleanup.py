from __future__ import annotations

from pathlib import Path

import pytest

from tests.live import test_optional_governance_fields as fields
from tests.live.support import DsctlCommandResult


def _result(
    action: str, *, data: object = None, error: str | None = None
) -> DsctlCommandResult:
    payload: dict[str, object] = {"ok": error is None, "action": action}
    if error is None:
        payload["data"] = data
    else:
        payload["error"] = {"type": error}
    return DsctlCommandResult(
        argv=(),
        exit_code=0 if error is None else 1,
        stdout="",
        stderr="",
        payload=payload,
    )


def test_cleanup_refuses_recreated_name_with_different_native_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def execute(_root: Path, argv: list[str], *, env_file: Path) -> DsctlCommandResult:
        del env_file
        calls.append(argv)
        return _result("worker-group.get", data={"id": 72, "name": "owned-name"})

    monkeypatch.setattr(fields, "run_dsctl", execute)
    with pytest.raises(AssertionError, match="id ownership"):
        fields._cleanup_owned(
            Path("/unused"),
            Path("/unused.env"),
            command="worker-group",
            selector="owned-name",
            identity={"id": 41, "name": "owned-name"},
        )
    assert calls == [["worker-group", "get", "owned-name"]]


def test_cleanup_does_not_treat_permission_error_as_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def execute(_root: Path, argv: list[str], *, env_file: Path) -> DsctlCommandResult:
        del env_file
        calls.append(argv)
        return _result("user.get", error="permission_denied")

    monkeypatch.setattr(fields, "run_dsctl", execute)
    with pytest.raises(AssertionError, match="expected 'not_found'"):
        fields._cleanup_owned(
            Path("/unused"),
            Path("/unused.env"),
            command="user",
            selector="owned-user",
            identity={"id": 41, "userName": "owned-user"},
        )
    assert calls == [["user", "get", "owned-user"]]


def test_cleanup_fails_if_deleted_resource_remains_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def execute(_root: Path, argv: list[str], *, env_file: Path) -> DsctlCommandResult:
        del env_file
        calls.append(argv)
        if argv[1] == "delete":
            return _result("tenant.delete", data={"deleted": True})
        return _result("tenant.get", data={"id": 41, "tenantCode": "owned-tenant"})

    monkeypatch.setattr(fields, "run_dsctl", execute)
    with pytest.raises(AssertionError, match="unexpectedly succeeded"):
        fields._cleanup_owned(
            Path("/unused"),
            Path("/unused.env"),
            command="tenant",
            selector="owned-tenant",
            identity={"id": 41, "tenantCode": "owned-tenant"},
        )
    assert calls == [
        ["tenant", "get", "owned-tenant"],
        ["tenant", "delete", "owned-tenant", "--force"],
        ["tenant", "get", "owned-tenant"],
    ]
