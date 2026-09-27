from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests.live import test_governance_optional_surfaces as optional_surfaces
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from pathlib import Path


def _result(
    action: str,
    *,
    data: object | None = None,
    error_type: str | None = None,
) -> DsctlCommandResult:
    payload: dict[str, object] = {
        "ok": error_type is None,
        "action": action,
        "resolved": {},
        "data": data,
    }
    if error_type is not None:
        payload = {
            "ok": False,
            "action": action,
            "error": {"type": error_type},
        }
    return DsctlCommandResult(
        argv=(),
        exit_code=0 if error_type is None else 1,
        stdout="",
        stderr="",
        payload=payload,
    )


def test_alert_cleanup_uses_native_ids_and_attempts_every_owned_resource(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    responses = {
        ("alert-plugin", "definition", "list"): _result(
            "alert-plugin.definition.list",
            data={"definitions": [{"pluginName": "Script"}]},
        ),
        ("alert-plugin", "schema", "Script"): _result(
            "alert-plugin.schema",
            data={"pluginName": "Script"},
        ),
        (
            "alert-plugin",
            "create",
            "--name",
            "alert-plugin",
            "--plugin",
            "Script",
            "--params-json",
            "[]",
        ): _result(
            "alert-plugin.create",
            data={"id": 11, "instanceName": "alert-plugin"},
        ),
        ("alert-plugin", "get", "alert-plugin"): _result(
            "alert-plugin.get",
            data={"id": 11},
        ),
        (
            "alert-plugin",
            "list",
            "--search",
            "alert-plugin",
            "--page-size",
            "20",
        ): _result("alert-plugin.list", data={"totalList": [{"id": 11}]}),
        (
            "alert-plugin",
            "update",
            "alert-plugin",
            "--name",
            "alert-plugin-updated",
            "--params-json",
            "[]",
        ): _result(
            "alert-plugin.update",
            data={"instanceName": "alert-plugin-updated"},
        ),
        (
            "alert-group",
            "create",
            "--name",
            "alert-group",
            "--description",
            "live alert-group create path",
            "--instance-id",
            "11",
        ): _result(
            "alert-group.create",
            data={"id": 22, "groupName": "alert-group"},
        ),
        ("alert-group", "get", "alert-group"): _result(
            "alert-group.get",
            data={"groupName": "alert-group"},
        ),
        (
            "alert-group",
            "list",
            "--search",
            "alert-group",
            "--page-size",
            "20",
        ): _result(
            "alert-group.list",
            data={"totalList": [{"groupName": "alert-group"}]},
        ),
        (
            "alert-group",
            "update",
            "alert-group",
            "--name",
            "alert-group-updated",
            "--description",
            "live alert-group update path",
            "--clear-instance-ids",
        ): _result("alert-group.update", error_type="conflict"),
        ("alert-group", "delete", "22", "--force"): _result(
            "alert-group.delete",
            error_type="conflict",
        ),
        ("alert-plugin", "delete", "11", "--force"): _result(
            "alert-plugin.delete",
            data={"deleted": True},
        ),
        ("alert-plugin", "get", "11"): _result(
            "alert-plugin.get",
            error_type="not_found",
        ),
    }

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(argv)
        return responses[tuple(argv)]

    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)

    with pytest.raises(ExceptionGroup) as caught:
        optional_surfaces.test_admin_alert_plugin_and_group_lifecycle_round_trip(
            tmp_path,
            tmp_path / "admin.env",
            lambda stem: stem,
        )

    assert calls[-3:] == [
        ["alert-group", "delete", "22", "--force"],
        ["alert-plugin", "delete", "11", "--force"],
        ["alert-plugin", "get", "11"],
    ]
    assert len(caught.value.exceptions) == 1
    assert isinstance(caught.value.exceptions[0], AssertionError)
    assert isinstance(caught.value.__context__, AssertionError)
