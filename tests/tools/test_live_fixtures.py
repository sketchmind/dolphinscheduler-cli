from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from _pytest.pytester import Pytester

pytest_plugins = ["pytester"]


@pytest.mark.parametrize(
    ("existing_etl", "fixture", "passes"),
    [
        (True, "live_bootstrap_state", True),
        (False, "live_bootstrap_state", False),
        (True, "live_admin_env_file", False),
    ],
)
def test_live_fixture_admin_dependency_is_only_required_when_used(
    pytester: Pytester,
    monkeypatch: pytest.MonkeyPatch,
    *,
    existing_etl: bool,
    fixture: str,
    passes: bool,
) -> None:
    repo = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(repo), str(repo / "src")]))
    for key in (
        "DS_LIVE_API_URL",
        "DS_LIVE_ADMIN_TOKEN",
        "DS_LIVE_ADMIN_ENV_FILE",
        "DS_LIVE_ETL_ENV_FILE",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DSCTL_RUN_LIVE_TESTS", "1")
    if existing_etl:
        env_file = pytester.path / "existing.env"
        env_file.write_text(
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=etl-token\nDS_VERSION=3.2.0\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("DS_LIVE_ETL_ENV_FILE", str(env_file))
    pytester.makeconftest('pytest_plugins = ["tests.live.conftest"]')
    pytester.makepyfile(
        f"""
        def test_identity({fixture}):
            state = {fixture}
            assert state.used_existing_etl_profile is True
            assert state.admin_env_file is None
            assert state.etl_env_file.is_file()
        """
    )

    result = pytester.runpytest_subprocess("-q", "-rs")

    result.assert_outcomes(passed=int(passes), skipped=int(not passes))
    if not passes:
        result.stdout.fnmatch_lines(
            ["*Configure DS_LIVE_API_URL and DS_LIVE_ADMIN_TOKEN*"]
        )
