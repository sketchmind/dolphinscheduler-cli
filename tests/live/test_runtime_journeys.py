from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from tests.live.runtime_journey import run_runtime_journey
from tests.live.schedule_journey import run_schedule_journey
from tests.live.support import create_live_run_prefix, require_mapping

if TYPE_CHECKING:
    from collections.abc import Mapping


_EXECUTABLE_ENV: Final = "DSCTL_JOURNEY_EXECUTABLE"
_ENV_FILE_ENV: Final = "DSCTL_JOURNEY_ENV_FILE"
_VERSION_ENV: Final = "DSCTL_JOURNEY_VERSION"
_REQUIRED_ENV: Final = (_EXECUTABLE_ENV, _ENV_FILE_ENV, _VERSION_ENV)
_EXACT_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\Z")

pytestmark = [
    pytest.mark.live,
    pytest.mark.live_developer,
    pytest.mark.live_runtime_journey,
    pytest.mark.destructive,
    pytest.mark.slow,
]


@dataclass(frozen=True)
class RuntimeJourneyConfig:
    executable: Path
    env_file: Path
    ds_version: str


def _configured_value(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name, "").strip()
    return value or None


@pytest.fixture(scope="module")
def runtime_journey_config() -> RuntimeJourneyConfig:
    values = {name: _configured_value(os.environ, name) for name in _REQUIRED_ENV}
    if not any(values.values()):
        pytest.skip(
            "Set DSCTL_JOURNEY_EXECUTABLE, DSCTL_JOURNEY_ENV_FILE, and "
            "DSCTL_JOURNEY_VERSION to run installed-wheel runtime journeys."
        )
    missing = [name for name, value in values.items() if value is None]
    if missing:
        pytest.fail(
            "Installed-wheel runtime journey configuration is incomplete; missing: "
            + ", ".join(missing)
        )

    executable_text = values[_EXECUTABLE_ENV]
    env_file_text = values[_ENV_FILE_ENV]
    ds_version = values[_VERSION_ENV]
    assert executable_text is not None
    assert env_file_text is not None
    assert ds_version is not None

    executable = Path(executable_text).expanduser()
    env_file = Path(env_file_text).expanduser()
    if not executable.is_file() or not os.access(executable, os.X_OK):
        pytest.fail(f"{_EXECUTABLE_ENV} must name an executable file: {executable}")
    wheel_python = executable.parent / "python"
    if not wheel_python.is_file() or not os.access(wheel_python, os.X_OK):
        pytest.fail(
            f"{_EXECUTABLE_ENV} must share its bin directory with an executable "
            f"python interpreter: {wheel_python}"
        )
    if not env_file.is_file():
        pytest.fail(f"{_ENV_FILE_ENV} must name an existing file: {env_file}")
    if _EXACT_VERSION.fullmatch(ds_version) is None:
        pytest.fail(
            f"{_VERSION_ENV} must be an explicit x.y.z DolphinScheduler version"
        )
    return RuntimeJourneyConfig(
        executable=executable.resolve(),
        env_file=env_file.resolve(),
        ds_version=ds_version,
    )


def test_runtime_journey_installed_wheel(
    live_repo_root: Path,
    runtime_journey_config: RuntimeJourneyConfig,
    tmp_path: Path,
) -> None:
    config = runtime_journey_config
    result = run_runtime_journey(
        repo_root=live_repo_root,
        executable=config.executable,
        env_file=config.env_file,
        ds_version=config.ds_version,
        workspace=tmp_path / "runtime",
        prefix=f"{create_live_run_prefix()}-runtime",
    )
    assert result["status"] == "passed"
    cleanup = require_mapping(result["cleanup"], label="runtime journey cleanup")
    assert cleanup["projects_remaining"] == 0


def test_schedule_journey_installed_wheel(
    live_repo_root: Path,
    runtime_journey_config: RuntimeJourneyConfig,
    tmp_path: Path,
) -> None:
    config = runtime_journey_config
    result = run_schedule_journey(
        repo_root=live_repo_root,
        executable=config.executable,
        env_file=config.env_file,
        ds_version=config.ds_version,
        workspace=tmp_path / "schedule",
        prefix=f"{create_live_run_prefix()}-schedule",
    )
    assert result["kind"] == "schedule_journey"
    cleanup = require_mapping(result["cleanup"], label="schedule journey cleanup")
    assert cleanup["projects_remaining"] == 0
