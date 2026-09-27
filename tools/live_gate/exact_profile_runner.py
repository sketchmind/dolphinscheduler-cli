from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from live_gate import installed_wheel as gate_harness
from live_gate.exact_profile_policy import (
    EXACT_PROFILE_GATE_POLICIES,
    exact_profile_gate_policy,
)

ROOT = Path(__file__).resolve().parents[2]
_PROFILE_ENV_NAMES = gate_harness.PROFILE_ENV_NAMES
_isolated_environment = gate_harness.isolated_environment
_read_env_file = gate_harness.read_env_file
_venv_binary = gate_harness.venv_binary


@dataclass(frozen=True)
class GateInputs(gate_harness.GateFiles):
    """Immutable inputs for the reviewed exact-profile live gate."""

    ds_version: str


def run_gate(inputs: GateInputs) -> int:
    """Install one wheel and run its selected exact DolphinScheduler live gate."""
    _validate_inputs(inputs)
    with gate_harness.private_workspace(prefix="dsctl-exact-profile-") as workspace:
        snapshot = _snapshot_inputs(inputs, workspace / "inputs")
        expected_dsctl = _venv_binary(workspace / "venv", "dsctl")
        installed = gate_harness.install_wheel(
            snapshot.wheel,
            workspace,
            missing_executable_message=(
                f"Installed wheel did not create {expected_dsctl}"
            ),
        )

        candidate_evidence = workspace / "candidate-evidence.json"
        environment = _exact_gate_environment(
            snapshot,
            executable=installed.executable,
            python=installed.python,
            candidate_evidence=candidate_evidence,
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/live/test_exact_profile.py",
                "-m",
                "live_exact_profile",
            ],
            check=False,
            cwd=ROOT,
            env=environment,
        )
        if completed.returncode != 0:
            candidate_evidence.unlink(missing_ok=True)
            return completed.returncode
        if not candidate_evidence.is_file():
            message = "Passing exact profile test did not produce candidate evidence"
            raise RuntimeError(message)
        _publish_evidence(candidate_evidence, inputs.evidence)
        return 0


def _snapshot_inputs(inputs: GateInputs, destination: Path) -> GateInputs:
    """Copy every mutable gate input once into a private stable workspace."""
    snapshot = gate_harness.snapshot_gate_files(
        inputs,
        destination,
        changed_template="Exact profile gate input changed while snapshotting: {path}",
    )
    return GateInputs(
        ds_version=inputs.ds_version,
        wheel=snapshot.wheel,
        env_file=snapshot.env_file,
        attestation_key_file=snapshot.attestation_key_file,
        cluster_manifest=snapshot.cluster_manifest,
        fixture_manifest=snapshot.fixture_manifest,
        evidence=snapshot.evidence,
    )


def _exact_gate_environment(
    inputs: GateInputs,
    *,
    executable: Path,
    python: Path,
    candidate_evidence: Path,
) -> dict[str, str]:
    """Return a developer-scoped environment bound to snapshotted inputs."""
    environment = _isolated_environment()
    environment.update(
        {
            "DSCTL_RUN_LIVE_TESTS": "1",
            "DS_LIVE_EXACT_VERSION": inputs.ds_version,
            "DS_LIVE_EXACT_ENV_FILE": str(inputs.env_file),
            "DS_LIVE_EXACT_ATTESTATION_KEY_FILE": str(inputs.attestation_key_file),
            "DS_LIVE_EXACT_DSCTL": str(executable),
            "DS_LIVE_EXACT_PYTHON": str(python),
            "DS_LIVE_EXACT_WHEEL": str(inputs.wheel),
            "DS_LIVE_EXACT_CLUSTER_MANIFEST": str(inputs.cluster_manifest),
            "DS_LIVE_EXACT_FIXTURE_MANIFEST": str(inputs.fixture_manifest),
            "DS_LIVE_EXACT_EVIDENCE": str(candidate_evidence),
        }
    )
    _bind_direct_api_target(environment, inputs.env_file)
    return environment


def _bind_direct_api_target(environment: dict[str, str], env_file: Path) -> None:
    """Keep one signed exact-cluster gate off ambient desktop proxies."""
    gate_harness.bind_direct_api_target(
        environment,
        env_file,
        invalid_url_message=("Exact profile profile must contain a valid DS_API_URL"),
    )


def _profile_api_host(env_file: Path) -> str:
    return gate_harness.profile_api_host(
        env_file,
        invalid_url_message="Exact profile profile must contain a valid DS_API_URL",
    )


def _validate_inputs(inputs: GateInputs) -> None:
    exact_profile_gate_policy(inputs.ds_version)
    gate_harness.validate_gate_files(
        inputs,
        messages=gate_harness.GateValidationMessages(
            missing_template="Exact profile {label} does not exist: {path}",
            wheel="Exact profile gate requires a wheel artifact",
            profile_private=(
                "Exact profile profile must not be accessible by group or others"
            ),
            attestation_private=(
                "Exact profile attestation key must not be accessible "
                "by group or others"
            ),
            overwrite_template="Refusing to overwrite existing live evidence: {path}",
        ),
    )


def _publish_evidence(candidate: Path, destination: Path) -> None:
    """Publish one passing candidate only after the whole pytest process passes."""
    gate_harness.publish_passing_evidence(
        candidate,
        destination,
        invalid_candidate_message=(
            "Exact profile candidate evidence is not a passing receipt"
        ),
    )


def _parse_args(argv: list[str] | None = None) -> GateInputs:
    parser = argparse.ArgumentParser(
        description="Run the installed-wheel exact DolphinScheduler live gate.",
    )
    parser.add_argument(
        "--version", required=True, choices=tuple(EXACT_PROFILE_GATE_POLICIES)
    )
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--attestation-key-file", type=Path, required=True)
    parser.add_argument("--cluster-manifest", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(argv)
    return GateInputs(
        ds_version=args.version,
        wheel=args.wheel.expanduser().resolve(),
        env_file=args.env_file.expanduser().resolve(),
        attestation_key_file=args.attestation_key_file.expanduser().resolve(),
        cluster_manifest=args.cluster_manifest.expanduser().resolve(),
        fixture_manifest=args.fixture_manifest.expanduser().resolve(),
        evidence=args.evidence.expanduser().resolve(),
    )


def main(argv: list[str] | None = None) -> int:
    return run_gate(_parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
