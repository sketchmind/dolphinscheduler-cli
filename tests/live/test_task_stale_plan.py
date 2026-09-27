"""Owned-fixture installed-wheel acceptance for the 3.4.2 stale-plan negative."""

from __future__ import annotations

import json
import os
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest

from tests.live.exact_profile_gate import (
    ExactProfileGateConfig,
    inspect_installed_exact_profile,
    load_exact_profile_gate_config,
)
from tests.live.support import create_live_run_prefix


def _fail(message: str) -> None:
    raise AssertionError(message)


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
        stream.write("\n")


def _output_text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def _record_subprocess_failure(
    output: Path,
    *,
    kind: str,
    returncode: int | None,
    stdout: str | bytes | None,
    stderr: str | bytes | None,
) -> Path:
    failure = output.with_suffix(".failure.json")
    _write_private_json(
        failure,
        {
            "status": "failed",
            "kind": kind,
            "returncode": returncode,
            "stdout": _output_text(stdout),
            "stderr": _output_text(stderr),
            "scope": "installed-wheel exact 3.4.2 stale-plan runner",
        },
    )
    return failure


pytestmark = [
    pytest.mark.live,
    pytest.mark.live_developer,
    pytest.mark.live_exact_profile,
    pytest.mark.destructive,
]


def test_task_update_stale_plan_same_prepared_object(live_repo_root: Path) -> None:
    if not os.environ.get("DS_LIVE_EXACT_ENV_FILE", "").strip():
        pytest.skip("Set DS_LIVE_EXACT_* to run the owned exact-profile fixture")
    config: ExactProfileGateConfig = load_exact_profile_gate_config(os.environ)
    if config.policy.ds_version != "3.4.2":
        pytest.skip("This focused stale-plan runner covers exact DS 3.4.2")
    fixture = config.fixture
    if (
        not fixture.exclusive
        or fixture.task_type != "SHELL"
        or fixture.workflow_release_state != "OFFLINE"
    ):
        _fail("Stale-plan runner requires an exclusive offline SHELL fixture")
    inspect_installed_exact_profile(config)

    marker = create_live_run_prefix()
    output = (
        live_repo_root
        / "build/live-evidence/task-stale-plan"
        / f"live-3.4.2-{marker}.json"
    )
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    runner = Path(__file__).with_name("stale_plan_runner.py")
    command = [
        str(config.python),
        "-I",
        str(runner),
        "--env-file",
        str(config.env_file),
        "--project",
        fixture.project_name,
        "--workflow",
        fixture.workflow_name,
        "--task-code",
        str(fixture.task_code),
        "--task-name",
        fixture.task_name,
        "--marker",
        marker,
        "--output",
        str(output),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=config.python.parent,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired as error:
        failure = _record_subprocess_failure(
            output,
            kind="timeout",
            returncode=None,
            stdout=error.stdout,
            stderr=error.stderr,
        )
        _fail(f"Installed-wheel stale-plan runner timed out; see {failure}")
    except OSError as error:
        failure = _record_subprocess_failure(
            output,
            kind="process_start",
            returncode=None,
            stdout=None,
            stderr=str(error),
        )
        _fail(f"Installed-wheel stale-plan runner did not start; see {failure}")
    if completed.returncode:
        failure = _record_subprocess_failure(
            output,
            kind="nonzero_exit",
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        _fail(f"Installed-wheel stale-plan runner failed; see {failure}")
    evidence = json.loads(output.read_text(encoding="utf-8"))
    assert evidence["prepare_object_reused"] is True
    assert evidence["interference_applied"] is True
    assert evidence["stale_apply_wire_calls"] == 0
    assert evidence["stale_conflict"] is True
    assert evidence["wrong_write_absent"] is True
    assert evidence["restored"] is True
    evidence.update(
        {
            "evidence_kind": "supplemental_live_stale_plan_negative",
            "wheel_sha256": sha256(config.wheel.read_bytes()).hexdigest(),
            "cluster_image_digest": config.cluster.image_digest,
            "fixture_manifest_sha256": fixture.manifest_sha256,
            "release_gate_receipt": False,
        }
    )
    _write_private_json(output, evidence)
