from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("run_exact_profile_read_gate")


def test_parse_args_resolves_version_bound_read_gate_inputs(tmp_path: Path) -> None:
    gate = _load_module()

    inputs = gate._parse_args(
        [
            "--ds-version",
            "3.2.2",
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

    assert inputs.ds_version == "3.2.2"
    assert inputs.wheel == (tmp_path / "runtime.whl").resolve()
    assert inputs.evidence == (tmp_path / "evidence.json").resolve()


def test_run_gate_rejects_profile_for_another_ds_version(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    inputs.env_file.write_text(
        "DS_API_URL=http://cluster.test/dolphinscheduler\n"
        "DS_API_TOKEN=secret-token\n"
        "DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"profile DS_VERSION 3\.4\.2.*3\.2\.2"):
        gate.run_gate(inputs)


def test_read_env_file_only_removes_matching_outer_quotes(tmp_path: Path) -> None:
    gate = _load_module()
    profile = tmp_path / "profile.env"
    profile.write_text(
        'DS_VERSION=\'3.2.2"\nDS_API_TOKEN="quoted-token"\n',
        encoding="utf-8",
    )

    values = gate._read_env_file(profile)

    assert values["DS_VERSION"] == "'3.2.2\""
    assert values["DS_API_TOKEN"] == "quoted-token"


def test_isolated_environment_removes_ambient_profile_and_live_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "source"))
    monkeypatch.setenv("DS_LIVE_EXACT_READ_ENV_FILE", "wrong.env")
    monkeypatch.setenv("DSCTL_RUN_LIVE_TESTS", "1")
    monkeypatch.setenv("DS_API_URL", "https://wrong.example")
    monkeypatch.setenv("DS_API_TOKEN", "wrong-token")
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("UNRELATED_VALUE", "kept")

    environment = gate._isolated_environment()

    assert "PYTHONPATH" not in environment
    assert "DS_LIVE_EXACT_READ_ENV_FILE" not in environment
    assert "DSCTL_RUN_LIVE_TESTS" not in environment
    assert not gate._PROFILE_ENV_NAMES.intersection(environment)
    assert environment["UNRELATED_VALUE"] == "kept"


def test_snapshot_inputs_are_stable_private_copies(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path / "source", gate=gate)
    inputs.wheel.write_bytes(b"wheel-v1")
    inputs.env_file.write_text(
        "DS_API_URL=http://matrix.test/dolphinscheduler\nDS_VERSION=3.2.2\n",
        encoding="utf-8",
    )
    inputs.attestation_key_file.write_text("k" * 32, encoding="utf-8")
    inputs.cluster_manifest.write_text("cluster-v1", encoding="utf-8")
    inputs.fixture_manifest.write_text("fixture-v1", encoding="utf-8")

    snapshot = gate._snapshot_inputs(inputs, tmp_path / "workspace")
    inputs.wheel.write_bytes(b"wheel-v2")

    assert snapshot.ds_version == "3.2.2"
    assert snapshot.wheel.read_bytes() == b"wheel-v1"
    assert snapshot.env_file.stat().st_mode & 0o077 == 0
    assert snapshot.evidence == inputs.evidence


def test_exact_read_environment_binds_version_and_direct_api_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    inputs.env_file.write_text(
        "DS_API_URL=http://192.0.2.10:11322/dolphinscheduler\nDS_VERSION=3.2.2\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("NO_PROXY", "localhost")

    environment = gate._exact_read_environment(
        inputs,
        executable=tmp_path / "bin" / "dsctl",
        python=tmp_path / "bin" / "python",
        candidate_evidence=tmp_path / "candidate.json",
    )

    assert environment["DSCTL_RUN_LIVE_TESTS"] == "1"
    assert "DSCTL_RUN_LIVE_ADMIN_TESTS" not in environment
    assert environment["DS_LIVE_EXACT_READ_VERSION"] == "3.2.2"
    assert environment["DS_LIVE_EXACT_READ_WHEEL"] == str(inputs.wheel)
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


def _inputs(tmp_path: Path, *, gate: Any) -> Any:
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
    env_file.chmod(0o600)
    attestation_key_file.chmod(0o600)
    return gate.ReadGateInputs(
        ds_version="3.2.2",
        wheel=wheel,
        env_file=env_file,
        attestation_key_file=attestation_key_file,
        cluster_manifest=cluster_manifest,
        fixture_manifest=fixture_manifest,
        evidence=tmp_path / "evidence.json",
    )
