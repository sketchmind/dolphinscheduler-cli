from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from types import ModuleType

    from live_gate.installed_wheel import InstalledWheel


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.exact_profile_runner")


def test_parse_args_resolves_exact_gate_inputs(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = gate._parse_args(
        [
            "--version",
            "3.4.2",
            "--wheel",
            str(tmp_path / "runtime.whl"),
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--attestation-key-file",
            str(tmp_path / "attestation.key"),
            "--cluster-manifest",
            str(tmp_path / "cluster.json"),
            "--fixture-manifest",
            str(tmp_path / "fixture.json"),
            "--evidence",
            str(tmp_path / "evidence.json"),
        ]
    )

    assert inputs.wheel == (tmp_path / "runtime.whl").resolve()
    assert inputs.evidence == (tmp_path / "evidence.json").resolve()


def test_validate_inputs_requires_private_profile_and_new_evidence(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path)
    inputs.env_file.chmod(0o644)

    with pytest.raises(PermissionError, match="group or others"):
        gate._validate_inputs(inputs)

    inputs.env_file.chmod(0o600)
    inputs.evidence.touch()
    with pytest.raises(FileExistsError, match="overwrite"):
        gate._validate_inputs(inputs)


def test_read_env_file_only_removes_matching_outer_quotes(tmp_path: Path) -> None:
    gate = _load_module()
    profile = tmp_path / "profile.env"
    profile.write_text(
        "DS_API_URL='http://cluster.test/dolphinscheduler\"\n"
        'DS_API_TOKEN="quoted-token"\n',
        encoding="utf-8",
    )

    values = gate._read_env_file(profile)

    assert values["DS_API_URL"] == "'http://cluster.test/dolphinscheduler\""
    assert values["DS_API_TOKEN"] == "quoted-token"


def test_isolated_environment_removes_live_and_pythonpath(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "source"))
    monkeypatch.setenv("DS_LIVE_EXACT_ENV_FILE", str(tmp_path / "cluster.env"))
    monkeypatch.setenv("DSCTL_RUN_LIVE_TESTS", "1")
    monkeypatch.setenv("DS_API_URL", "https://wrong-cluster.example")
    monkeypatch.setenv("DS_API_TOKEN", "wrong-token")
    monkeypatch.setenv("DS_API_RETRY_ATTEMPTS", "9")
    monkeypatch.setenv("DS_API_RETRY_BACKOFF_MS", "999")
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("UNRELATED_VALUE", "kept")

    environment = gate._isolated_environment()

    assert "PYTHONPATH" not in environment
    assert "DS_LIVE_EXACT_ENV_FILE" not in environment
    assert "DSCTL_RUN_LIVE_TESTS" not in environment
    assert not gate._PROFILE_ENV_NAMES.intersection(environment)
    assert environment["UNRELATED_VALUE"] == "kept"


def test_snapshot_inputs_are_private_stable_copies(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path / "source")
    inputs.wheel.write_bytes(b"wheel-v1")
    inputs.env_file.write_text("profile-v1", encoding="utf-8")
    inputs.attestation_key_file.write_text("k" * 32, encoding="utf-8")
    inputs.cluster_manifest.write_text("cluster-v1", encoding="utf-8")
    inputs.fixture_manifest.write_text("fixture-v1", encoding="utf-8")
    workspace = tmp_path / "workspace"

    snapshot = gate._snapshot_inputs(inputs, workspace)
    inputs.wheel.write_bytes(b"wheel-v2")
    inputs.env_file.write_text("profile-v2", encoding="utf-8")

    assert snapshot.wheel.read_bytes() == b"wheel-v1"
    assert snapshot.env_file.read_text(encoding="utf-8") == "profile-v1"
    assert snapshot.attestation_key_file.read_text(encoding="utf-8") == "k" * 32
    assert snapshot.cluster_manifest.read_text(encoding="utf-8") == "cluster-v1"
    assert snapshot.fixture_manifest.read_text(encoding="utf-8") == "fixture-v1"
    assert snapshot.evidence == inputs.evidence
    assert snapshot.env_file.stat().st_mode & 0o077 == 0


def test_exact_gate_environment_is_developer_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path)
    monkeypatch.setenv("NO_PROXY", "localhost")

    environment = gate._exact_gate_environment(
        inputs,
        executable=tmp_path / "bin" / "dsctl",
        python=tmp_path / "bin" / "python",
        candidate_evidence=tmp_path / "candidate.json",
    )

    assert environment["DSCTL_RUN_LIVE_TESTS"] == "1"
    assert "DSCTL_RUN_LIVE_ADMIN_TESTS" not in environment
    assert environment["DS_LIVE_EXACT_WHEEL"] == str(inputs.wheel)
    assert environment["NO_PROXY"] == "localhost,192.0.2.10"
    assert environment["no_proxy"] == "localhost,192.0.2.10"


def test_publish_evidence_requires_passed_candidate_and_never_overwrites(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    candidate = tmp_path / "candidate.json"
    destination = tmp_path / "evidence" / "receipt.json"
    candidate.write_text(json.dumps({"status": "failed"}), encoding="utf-8")

    with pytest.raises(ValueError, match="passing receipt"):
        gate._publish_evidence(candidate, destination)

    candidate.write_text(json.dumps({"status": "passed"}), encoding="utf-8")
    gate._publish_evidence(candidate, destination)
    assert json.loads(destination.read_text(encoding="utf-8"))["status"] == "passed"

    with pytest.raises(FileExistsError):
        gate._publish_evidence(candidate, destination)


def _inputs(tmp_path: Path) -> Any:
    gate = _load_module()
    tmp_path.mkdir(parents=True, exist_ok=True)
    wheel = tmp_path / "runtime.whl"
    env_file = tmp_path / "cluster.env"
    attestation_key_file = tmp_path / "attestation.key"
    cluster_manifest = tmp_path / "cluster.json"
    fixture_manifest = tmp_path / "fixture.json"
    for path in (
        wheel,
        env_file,
        attestation_key_file,
        cluster_manifest,
        fixture_manifest,
    ):
        path.touch()
    env_file.write_text(
        "DS_API_URL=http://192.0.2.10:11342/dolphinscheduler\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    attestation_key_file.chmod(0o600)
    return gate.GateInputs(
        wheel=wheel,
        env_file=env_file,
        attestation_key_file=attestation_key_file,
        cluster_manifest=cluster_manifest,
        fixture_manifest=fixture_manifest,
        evidence=tmp_path / "evidence.json",
        ds_version="3.4.2",
    )


@pytest.mark.parametrize("returncode", [0, 1])
def test_runner_publishes_only_after_the_whole_scenario_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path)
    candidates: list[Path] = []

    def install_wheel(wheel: Path, workspace: Path, **kwargs: object) -> InstalledWheel:
        assert wheel.read_bytes() == inputs.wheel.read_bytes()
        assert wheel != inputs.wheel
        from live_gate.installed_wheel import InstalledWheel  # noqa: PLC0415

        return InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def run_scenario(
        argv: list[str], *, check: bool, cwd: Path, env: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        assert argv == [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "tests/live/test_exact_profile.py",
            "-m",
            "live_exact_profile",
        ]
        assert env["DS_LIVE_EXACT_VERSION"] == "3.4.2"
        assert Path(env["DS_LIVE_EXACT_WHEEL"]) != inputs.wheel
        assert not inputs.evidence.exists()
        candidate = Path(env["DS_LIVE_EXACT_EVIDENCE"])
        candidate.write_text('{"status":"passed"}', encoding="utf-8")
        candidates.append(candidate)
        return subprocess.CompletedProcess(argv, returncode)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", install_wheel)
    monkeypatch.setattr(gate.subprocess, "run", run_scenario)

    assert gate.run_gate(inputs) == returncode
    assert inputs.evidence.exists() is (returncode == 0)
    assert len(candidates) == 1
    assert not candidates[0].exists()
