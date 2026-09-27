from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dsctl.generated.version_profiles import PROFILE_DATA

if TYPE_CHECKING:
    from types import ModuleType


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("analyze_ds_conformance_bundles")


def test_analyzer_emits_the_current_static_assessment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    analyzer = _load_module()

    exit_code = analyzer.main([])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["target_versions"]) == 37
    assert payload["claim"] == "static-action-closure-only"
    assert payload["catalog_digest"].startswith("sha256:")
    assert payload["summary"] == {
        "bundle_count": 2,
        "coordinate_count": 74,
        "ready_coordinate_count": 74,
        "blocked_coordinate_count": 0,
    }


def test_analyzer_rejects_stale_generated_conformance_data(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analyzer = _load_module()
    monkeypatch.setattr(
        analyzer,
        "_load_generated_assessment",
        lambda: {"kind": "stale"},
        raising=False,
    )

    exit_code = analyzer.main(["--check-generated"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "generated conformance bundle data has diverged" in captured.err


def test_require_assessed_accepts_complete_ready_matrix_without_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analyzer = _load_module()
    output = tmp_path / "assessment.json"

    exit_code = analyzer.main(
        ["--check-generated", "--require-assessed", "--output", str(output)]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == ""
    assert captured.err == ""
    assert json.loads(output.read_text(encoding="utf-8"))["summary"] == {
        "bundle_count": 2,
        "coordinate_count": 74,
        "ready_coordinate_count": 74,
        "blocked_coordinate_count": 0,
    }


@pytest.mark.parametrize(
    "corruption",
    ["missing_new_version", "duplicate", "unexpected", "legacy_summary", "subset"],
)
def test_require_assessed_rejects_incomplete_or_inconsistent_real_matrix(
    corruption: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    analyzer = _load_module()
    report = analyzer.assess_static_conformance_bundles(PROFILE_DATA)
    incomplete = copy.deepcopy(report)
    versions = incomplete["bundles"][1]["versions"]
    if corruption == "missing_new_version":
        versions[:] = [row for row in versions if row["version"] != "3.4.3"]
    elif corruption == "duplicate":
        versions[-1] = copy.deepcopy(versions[0])
    elif corruption == "unexpected":
        versions[-1]["version"] = "9.9.9"
    elif corruption == "legacy_summary":
        incomplete["summary"]["coordinate_count"] = 30
    else:
        incomplete["target_versions"].remove("3.4.3")
        for bundle in incomplete["bundles"]:
            bundle["versions"] = [
                row for row in bundle["versions"] if row["version"] != "3.4.3"
            ]
        incomplete["summary"]["coordinate_count"] = 72
        incomplete["summary"]["ready_coordinate_count"] = 72
    monkeypatch.setattr(
        analyzer,
        "assess_static_conformance_bundles",
        lambda _profile_data: incomplete,
    )

    exit_code = analyzer.main(["--require-assessed"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "74 terminal coordinates" in captured.err
    assert {
        "missing_new_version": "full_core/v1 does not cover every exact version",
        "duplicate": "full_core/v1 versions are duplicated",
        "unexpected": "full_core/v1 exact version set differs",
        "legacy_summary": "coordinate summary is inconsistent",
        "subset": "exact target versions differ",
    }[corruption] in captured.err
