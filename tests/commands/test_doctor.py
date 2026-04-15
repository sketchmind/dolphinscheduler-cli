from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import doctor as doctor_service

if TYPE_CHECKING:
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
) -> None:
    with runner.isolated_filesystem():
        Path("cluster.env").write_text(
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=secret-token\n",
            encoding="utf-8",
        )
        Path(".dsctl-context.yaml").write_text(
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
            env={"XDG_CONFIG_HOME": str(Path("xdg").resolve())},
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "doctor"
    assert payload["warnings"] == []
    assert payload["warning_details"] == []
    assert payload["data"]["status"] == "ok"
    assert payload["data"]["summary"] == {"ok": 5, "warning": 0, "error": 0}
    assert [check["name"] for check in payload["data"]["checks"]] == [
        "profile",
        "context",
        "adapter",
        "api",
        "current_user",
    ]
