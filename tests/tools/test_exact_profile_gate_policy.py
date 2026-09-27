"""Version selection must never infer a scenario from an action superset."""

from __future__ import annotations

import importlib
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from tests.tools.test_exact_profile_live_gate import _gate_environment
from tests.tools.test_run_exact_profile_live_gate import _inputs, _load_module

from live_gate.exact_profile_policy import exact_profile_gate_policy


@pytest.mark.parametrize("version", ["3.4.1", "3.3.2", "3.4", "3.4.3", ""])
def test_unreviewed_exact_policy_is_rejected(version: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No reviewed exact-profile gate policy"):
        exact_profile_gate_policy(version)

    runner = _load_module()
    inputs = replace(_inputs(tmp_path), ds_version=version)
    with pytest.raises(ValueError, match="No reviewed exact-profile gate policy"):
        runner.run_gate(inputs)
    assert not inputs.evidence.exists()

    fixture = importlib.import_module("live_gate.exact_profile_fixture")
    with pytest.raises(ValueError, match="No reviewed exact-profile gate policy"):
        fixture.project_exact_profile_matrix_fixture(
            ds_version=version,
            cluster_manifest=tmp_path / "missing-cluster",
            fixture_manifest=tmp_path / "missing-fixture",
            state_file=tmp_path / "missing-state",
            image_inspection=tmp_path / "missing-image",
            cluster_output=tmp_path / "cluster-output",
            fixture_output=tmp_path / "fixture-output",
        )
    assert not (tmp_path / "cluster-output").exists()

    promotion = importlib.import_module("live_gate.exact_profile_promotion_evidence")
    with pytest.raises(ValueError, match="No reviewed exact-profile gate policy"):
        promotion.check_exact_profile_promotion_evidence(
            tmp_path / "missing-receipts",
            source_root=tmp_path / "missing-source",
            ds_version=version,
        )


def test_live_configuration_requires_reviewed_selected_version(tmp_path: Path) -> None:
    live = importlib.import_module("tests.live.exact_profile_gate")
    environment, _paths = _gate_environment(tmp_path)
    environment["DS_LIVE_EXACT_VERSION"] = "3.4.1"
    with pytest.raises(ValueError, match="No reviewed exact-profile gate policy"):
        live.load_exact_profile_gate_config(environment)


def test_external_shell_policy_preserves_current_and_historical_boundaries() -> None:
    policy = exact_profile_gate_policy("3.4.2")
    assert policy.scenario == "external-shell/v1"
    assert policy.gate_id == "exact-profile-3.4.2"
    assert policy.current_schema_version == 7
    assert policy.supported_schema_versions == {3, 4, 5, 6, 7}
    assert policy.support_level == "experimental"
    assert policy.tested is False
    assert policy.actions == (
        "doctor",
        "project.create",
        "project.delete",
        "project.get",
        "project.list",
        "project.update",
        "schedule.list",
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
        "workflow.get",
        "workflow.list",
        "task.list",
        "task.get",
        "task.update",
    )


@pytest.mark.parametrize(
    "script",
    [
        "check_exact_profile_promotion_evidence.py",
        "project_exact_profile_matrix_fixture.py",
        "run_exact_profile_live_gate.py",
    ],
)
def test_public_entrypoints_reject_unreviewed_versions(script: str) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(  # noqa: S603 - fixed tool paths and invalid version
        [sys.executable, str(root / "tools" / script), "--version", "3.4.1"],
        check=False,
        capture_output=True,
        text=True,
        cwd=root,
    )
    assert completed.returncode == 2
    assert "invalid choice: '3.4.1'" in completed.stderr
