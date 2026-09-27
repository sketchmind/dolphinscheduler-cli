"""Local safety checks for the dedicated installed-wheel stale-plan runner."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.upstream.task_definitions import TaskDefinitions, TaskSelector
from tests.live import stale_plan_runner
from tests.live import test_task_stale_plan as harness

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("failure", ["nonzero_exit", "timeout"])
def test_runner_retains_private_failure_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    wheel = tmp_path / "candidate.whl"
    wheel.write_bytes(b"wheel")
    config = SimpleNamespace(
        policy=SimpleNamespace(ds_version="3.4.2"),
        fixture=SimpleNamespace(
            exclusive=True,
            task_type="SHELL",
            workflow_release_state="OFFLINE",
            project_name="owned",
            workflow_name="flow",
            task_code=7001,
            task_name="shell",
        ),
        python=tmp_path / "venv/bin/python",
        env_file=tmp_path / "owned.env",
        wheel=wheel,
    )
    monkeypatch.setenv("DS_LIVE_EXACT_ENV_FILE", str(config.env_file))
    monkeypatch.setattr(harness, "load_exact_profile_gate_config", lambda _env: config)
    monkeypatch.setattr(harness, "inspect_installed_exact_profile", lambda _c: None)
    monkeypatch.setattr(harness, "create_live_run_prefix", lambda: "owned-marker")

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if failure == "timeout":
            raise subprocess.TimeoutExpired(
                cmd="installed-python",
                timeout=120,
                output=b"prepared\n",
                stderr=b"waited\n",
            )
        return subprocess.CompletedProcess(
            args="installed-python",
            returncode=7,
            stdout="prepared\n",
            stderr="failed\n",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(AssertionError, match="see"):
        harness.test_task_update_stale_plan_same_prepared_object(tmp_path)
    artifact = (
        tmp_path / "build/live-evidence/task-stale-plan/"
        "live-3.4.2-owned-marker.failure.json"
    )
    retained = json.loads(artifact.read_text(encoding="utf-8"))
    assert retained["kind"] == failure
    assert retained["stdout"] == "prepared\n"
    assert retained["stderr"] == ("waited\n" if failure == "timeout" else "failed\n")
    assert artifact.stat().st_mode & 0o777 == 0o600


def test_cleanup_guard_rejects_unrelated_task_change() -> None:
    baseline = {
        "code": 7001,
        "name": "shell",
        "taskType": "SHELL",
        "version": 1,
        "taskParams": {"rawScript": "echo original"},
        "workerGroup": "default",
    }
    current = {
        **baseline,
        "version": 2,
        "taskParams": {"rawScript": "echo interference"},
        "workerGroup": "another-actor",
    }

    class FakeDefinitions:
        prepare_calls = 0
        apply_calls = 0

        def get(self, _selector: object) -> SimpleNamespace:
            return SimpleNamespace(view=SimpleNamespace(to_data=lambda: current))

        def prepare_update(self, _intent: object) -> None:
            self.prepare_calls += 1

        def apply(self, _prepared: object) -> None:
            self.apply_calls += 1

    definitions = FakeDefinitions()
    with pytest.raises(AssertionError, match="Refusing cleanup"):
        stale_plan_runner._restore_after_interference(
            cast("TaskDefinitions", definitions),
            TaskSelector("owned", "flow", "7001"),
            task_code=7001,
            task_name="shell",
            original="echo original",
            interfering="echo interference",
            original_version=1,
            marker_version=2,
            baseline_non_command_state=stale_plan_runner._non_command_state(baseline),
        )
    assert definitions.prepare_calls == 0
    assert definitions.apply_calls == 0
