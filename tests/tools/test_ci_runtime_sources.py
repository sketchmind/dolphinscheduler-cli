from __future__ import annotations

from pathlib import Path

import yaml


def test_only_development_gate_leg_prepares_runtime_bundle_sources() -> None:
    workflow_path = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["test"]["steps"]

    source_steps = [
        step for step in steps if step["name"] == "Prepare Runtime Bundle Sources"
    ]

    assert source_steps == [
        {
            "name": "Prepare Runtime Bundle Sources",
            "if": "matrix.python-version == '3.11'",
            "run": (
                "python tools/prepare_ds_runtime_sources.py --selection runtime-slice"
            ),
        }
    ]
    development_step = next(
        step for step in steps if step["name"] == "Development Quality Gate"
    )
    assert development_step["if"] == "matrix.python-version == '3.11'"
    assert development_step["run"] == (
        "python tools/check_quality_gate.py --mode development"
    )
    step_names = [step["name"] for step in steps]
    assert step_names.index("Prepare Runtime Bundle Sources") < step_names.index(
        "Development Quality Gate"
    )


def test_installed_wheel_smoke_loads_reviewed_and_exact_generated_artifacts() -> None:
    workflow_path = Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    package_steps = workflow["jobs"]["package"]["steps"]
    smoke = next(
        step for step in package_steps if step["name"] == "Install wheel smoke test"
    )["run"]

    assert "validate_compiled_wire_installation()" in smoke
    assert "for ds_version in TARGET_DS_VERSIONS" in smoke
    assert 'WORKFLOW_PROGRAMS.profile(ds_version).program("task_log")' in smoke
    assert "RESOURCE_PROGRAMS.profile(ds_version)" in smoke
    assert 'resource.program("upload").codec.file_fields == ("file",)' in smoke
    assert 'resource.program("download").codec.response_transport == "binary"' in smoke
    assert 'TaskTypeAdapter.for_version("3.4.2")' in smoke
    assert "ExactGeneratedPackage" not in smoke
