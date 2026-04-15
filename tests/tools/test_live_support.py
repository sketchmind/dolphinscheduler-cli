from __future__ import annotations

import os
import subprocess
from typing import TYPE_CHECKING

from tests.live.support import (
    LiveProfileConfig,
    load_live_settings,
    run_dsctl,
    write_profile_env,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_write_profile_env_materializes_only_runtime_profile_keys(
    tmp_path: Path,
) -> None:
    env_file = write_profile_env(
        tmp_path / "etl.env",
        LiveProfileConfig(
            api_url="http://example.test/dolphinscheduler",
            api_token="etl-token",
            tenant_code="tenant-for-harness",
        ),
    )

    lines = env_file.read_text(encoding="utf-8").splitlines()

    assert lines == [
        "DS_API_URL=http://example.test/dolphinscheduler",
        "DS_API_TOKEN=etl-token",
    ]


def test_load_live_settings_accepts_harness_metadata_in_admin_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin_env_file = tmp_path / "admin.env"
    admin_env_file.write_text(
        "\n".join(
            [
                "DS_API_URL=http://example.test/dolphinscheduler",
                "DS_API_TOKEN=admin-token",
                "DS_LIVE_TENANT_CODE=tenant-for-harness",
                "DS_WEB_UI=http://example.test/dolphinscheduler/ui",
                "DS_DEPLOY_VERSION=3.4.1",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_LIVE_ADMIN_ENV_FILE", str(admin_env_file))

    settings = load_live_settings()

    assert settings.admin is not None
    assert settings.admin.api_url == "http://example.test/dolphinscheduler"
    assert settings.admin.api_token == "admin-token"
    assert settings.admin.tenant_code == "tenant-for-harness"


def test_run_dsctl_sanitizes_live_and_profile_environment_when_env_file_is_used(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_env: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        capture_output: bool,
        check: bool,
        cwd: Path,
        env: dict[str, str],
        text: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert check is False
        assert cwd == tmp_path
        assert text is True
        assert timeout == 60.0
        captured_env.update(env)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"ok": true, "action": "version", "data": {}}',
            stderr="",
        )

    profile_file = tmp_path / "profile.env"
    profile_file.write_text(
        "DS_API_URL=http://file.test/dolphinscheduler\nDS_API_TOKEN=file-token\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DSCTL_RUN_LIVE_TESTS", "1")
    monkeypatch.setenv("DSCTL_RUN_LIVE_ADMIN_TESTS", "1")
    monkeypatch.setenv("DS_LIVE_ADMIN_ENV_FILE", str(tmp_path / "admin.env"))
    monkeypatch.setenv("DS_LIVE_TENANT_CODE", "tenant-for-harness")
    monkeypatch.setenv("DS_API_URL", "http://ambient.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "ambient-token")
    monkeypatch.setattr("tests.live.support.subprocess.run", fake_run)

    result = run_dsctl(tmp_path, ["version"], env_file=profile_file)

    assert result.exit_code == 0
    assert "DSCTL_RUN_LIVE_TESTS" not in captured_env
    assert "DSCTL_RUN_LIVE_ADMIN_TESTS" not in captured_env
    assert "DS_LIVE_ADMIN_ENV_FILE" not in captured_env
    assert "DS_LIVE_TENANT_CODE" not in captured_env
    assert "DS_API_URL" not in captured_env
    assert "DS_API_TOKEN" not in captured_env
    assert captured_env["PYTHONPATH"].split(os.pathsep)[0] == str(tmp_path / "src")
