from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    import pytest


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.installed_wheel")


def test_install_wheel_uses_an_isolated_private_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _load_module()
    wheel = tmp_path / "runtime.whl"
    wheel.touch()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    captured: dict[str, object] = {}

    def create_venv(_builder: object, venv_dir: Path) -> None:
        for name in ("python", "dsctl"):
            binary = harness.venv_binary(venv_dir, name)
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.touch()

    def run_install(command: list[str], **kwargs: object) -> None:
        captured["command"] = command
        captured.update(kwargs)

    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "source"))
    monkeypatch.setenv("DS_API_TOKEN", "ambient-token")
    monkeypatch.setenv("DS_API_FUTURE_SECRET", "ambient-future-secret")
    monkeypatch.setattr(harness.venv.EnvBuilder, "create", create_venv)
    monkeypatch.setattr(harness.subprocess, "run", run_install)

    installed = harness.install_wheel(
        wheel,
        workspace,
        missing_executable_message="missing dsctl",
    )

    assert installed.python == harness.venv_binary(workspace / "venv", "python")
    assert installed.executable == harness.venv_binary(workspace / "venv", "dsctl")
    command = captured["command"]
    assert isinstance(command, list)
    assert command[-1] == str(wheel)
    assert captured["check"] is True
    assert captured["cwd"] == workspace
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert "PYTHONPATH" not in environment
    assert "DS_API_TOKEN" not in environment
    assert "DS_API_FUTURE_SECRET" not in environment


def test_isolated_environment_removes_all_dsctl_target_and_policy_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _load_module()
    ambient = {
        "DSCTL_CONTEXT": "production",
        "DSCTL_ENV_FILE": "/private/production.env",
        "DSCTL_FUTURE_POLICY": "ambient-policy",
        "DS_FUTURE_TARGET": "ambient-target",
        "DS_LIVE_FUTURE_CONTROL": "ambient-control",
        "PYTHONHOME": "/ambient/python",
        "PYTHONPATH": "/ambient/source",
    }
    for key, value in ambient.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("UNRELATED_VALUE", "kept")

    environment = harness.isolated_environment()

    assert ambient.keys().isdisjoint(environment)
    assert environment["UNRELATED_VALUE"] == "kept"


def test_venv_binary_matches_the_host_platform(tmp_path: Path) -> None:
    harness = _load_module()

    binary = harness.venv_binary(tmp_path / "venv", "dsctl")

    expected_directory = "Scripts" if os.name == "nt" else "bin"
    expected_name = "dsctl.exe" if os.name == "nt" else "dsctl"
    assert binary == tmp_path / "venv" / expected_directory / expected_name
