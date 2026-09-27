from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import doctor as doctor_service

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.monkeypatch import MonkeyPatch

runner = CliRunner()


@dataclass
class FakeDoctorClient:
    payload: dict[str, object] = field(default_factory=lambda: {"status": "UP"})

    def __enter__(self) -> FakeDoctorClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def healthcheck(self) -> dict[str, object]:
        return dict(self.payload)


def test_doctor_command_returns_structured_diagnostics(
    monkeypatch: MonkeyPatch,
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\nDS_API_TOKEN=secret-token\nDS_VERSION=3.4.1\n",
        encoding="utf-8",
    )
    (isolated_cwd / ".dsctl-context.yaml").write_text(
        "project: etl-prod\nworkflow: daily-etl\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(
            payload={
                "status": "UP",
                "components": {"db": {"status": "UP"}},
            }
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )
    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "doctor"],
        env={"XDG_CONFIG_HOME": str(isolated_cwd / "xdg")},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "doctor"
    assert "warnings" not in payload
    assert "warning_details" not in payload
    assert payload["data"]["status"] == "ok"
    assert payload["data"]["summary"] == {"ok": 5, "warning": 0, "error": 0}
    assert [check["name"] for check in payload["data"]["checks"]] == [
        "profile",
        "context",
        "adapter",
        "api",
        "current_user",
    ]

    context_check = payload["data"]["checks"][1]
    assert context_check["details"] == {
        "source": "flag",
        "context": None,
        "env_file": str((isolated_cwd / "cluster.env").resolve()),
        "api_url": "http://example.test/dolphinscheduler",
        "project": None,
    }
    assert "secret-token" not in result.stdout


def test_doctor_failed_readiness_returns_all_checks_and_nonzero(
    monkeypatch: MonkeyPatch,
    isolated_cwd: Path,
) -> None:
    profile = isolated_cwd / "cluster.env"
    profile.write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\nDS_API_TOKEN=secret-token\nDS_VERSION=3.4.1\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(payload={"status": "DOWN"}),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {"userName": "alice"},
    )
    result = runner.invoke(app, ["--env-file", str(profile), "doctor"])
    assert result.exit_code == 1
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "check_failed"
    assert payload["data"]["status"] == "error"
    assert len(payload["data"]["checks"]) == 5
    assert "secret-token" not in result.stderr
