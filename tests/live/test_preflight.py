from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import LiveBootstrapState, require_mapping, run_dsctl

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.live]


@pytest.mark.live_admin
def test_admin_doctor_preflight(
    live_repo_root: Path,
    live_admin_env_file: Path,
) -> None:
    result = run_dsctl(live_repo_root, ["doctor"], env_file=live_admin_env_file)

    assert result.exit_code == 0
    assert result.payload["ok"] is True
    assert result.payload["action"] == "doctor"
    data = require_mapping(result.payload["data"], label="doctor data")
    checks = data.get("checks")
    assert isinstance(checks, list)
    assert checks


@pytest.mark.live_admin
def test_admin_monitor_health_preflight(
    live_repo_root: Path,
    live_admin_env_file: Path,
) -> None:
    result = run_dsctl(
        live_repo_root,
        ["monitor", "health"],
        env_file=live_admin_env_file,
    )

    assert result.exit_code == 0
    assert result.payload["ok"] is True
    assert result.payload["action"] == "monitor.health"
    data = require_mapping(result.payload["data"], label="monitor health data")
    assert "status" in data


@pytest.mark.live_developer
@pytest.mark.destructive
def test_bootstrapped_etl_token_can_call_project_list(
    live_repo_root: Path,
    live_bootstrap_state: LiveBootstrapState,
) -> None:
    result = run_dsctl(
        live_repo_root,
        ["project", "list"],
        env_file=live_bootstrap_state.etl_env_file,
    )

    assert result.exit_code == 0
    assert result.payload["ok"] is True
    assert result.payload["action"] == "project.list"


@pytest.mark.live_developer
@pytest.mark.destructive
def test_bootstrapped_etl_token_is_not_admin(
    live_repo_root: Path,
    live_bootstrap_state: LiveBootstrapState,
) -> None:
    result = run_dsctl(
        live_repo_root,
        ["user", "list"],
        env_file=live_bootstrap_state.etl_env_file,
    )

    assert result.exit_code == 1
    assert result.payload["ok"] is False
    assert result.payload["action"] == "user.list"
    error = require_mapping(result.payload["error"], label="user list error")
    assert error["type"] == "permission_denied"
