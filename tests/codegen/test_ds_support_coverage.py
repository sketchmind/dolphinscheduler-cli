from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_coverage_requires_a_terminal_decision_for_every_coordinate() -> None:
    analyzer = _module("ds_codegen.support_coverage")
    report = analyzer.analyze_support_coverage(
        _profiles(
            first={
                "project.list": _supported(),
                "workflow.run": _pending(),
            },
            second={
                "project.list": _absent(),
                "workflow.run": _limited(),
            },
        )
    )

    assert report["complete"] is False
    assert report["coordinate_count"] == 4
    assert report["terminal_coordinate_count"] == 3
    assert report["counts"] == {
        "supported": 1,
        "upstream_limited": 1,
        "upstream_absent": 1,
        "pending": 1,
    }
    assert report["pending"] == [
        {
            "version": "1.0.0",
            "action": "workflow.run",
            "availability": "unsupported",
            "reason": "compatibility_gate_not_passed",
        }
    ]
    assert {item["domain"]: item["complete"] for item in report["domains"]} == {
        "project": True,
        "workflow": False,
    }


def test_coverage_rejects_absence_claim_without_constraint() -> None:
    analyzer = _module("ds_codegen.support_coverage")
    capability = _absent()
    capability.pop("constraint")

    with pytest.raises(ValueError, match="requires a constraint"):
        analyzer.analyze_support_coverage(
            _profiles(
                first={"project.list": capability},
                second={"project.list": _supported()},
            )
        )


def test_coverage_rejects_absence_claim_without_source_evidence() -> None:
    analyzer = _module("ds_codegen.support_coverage")
    capability = _absent()
    capability.pop("evidence_sources")

    with pytest.raises(ValueError, match="requires evidence_sources"):
        analyzer.analyze_support_coverage(
            _profiles(
                first={"project.list": capability},
                second={"project.list": _supported()},
            )
        )


def test_wire_present_runtime_limitation_is_terminal_only_with_evidence() -> None:
    analyzer = _module("ds_codegen.support_coverage")
    limitation = _limited()
    limitation["reason"] = "upstream_runtime_limited"
    report = analyzer.analyze_support_coverage(
        _profiles(
            first={"workflow.run": limitation},
            second={"workflow.run": _supported()},
        )
    )
    assert report["complete"] is True
    assert report["counts"]["upstream_limited"] == 1

    limitation.pop("evidence_sources")
    with pytest.raises(ValueError, match="requires evidence_sources"):
        analyzer.analyze_support_coverage(
            _profiles(
                first={"workflow.run": limitation},
                second={"workflow.run": _supported()},
            )
        )


def test_current_profiles_expose_a_complete_terminal_baseline() -> None:
    analyzer = _module("ds_codegen.support_coverage")
    profiles = _module("dsctl.generated.version_profiles")

    report = analyzer.analyze_support_coverage(profiles.PROFILE_DATA)

    assert report["target_version_count"] == 37
    assert report["stable_action_count"] == 181
    assert report["coordinate_count"] == 6697
    assert report["complete"] is True
    assert report["counts"]["pending"] == 0
    assert report["terminal_coordinate_count"] == 6697


def _profiles(
    *,
    first: dict[str, dict[str, object]],
    second: dict[str, dict[str, object]],
) -> dict[str, object]:
    actions = list(first)
    assert set(actions) == set(second)
    return {
        "target_versions": ["1.0.0", "2.0.0"],
        "stable_actions": actions,
        "profiles": {
            "1.0.0": {"actions": first},
            "2.0.0": {"actions": second},
        },
    }


def _supported() -> dict[str, object]:
    return {
        "availability": "supported",
        "execution_mode": "generated_adapter",
        "verification": "contract_tested",
    }


def _pending() -> dict[str, object]:
    return {
        "availability": "unsupported",
        "execution_mode": "not_executable",
        "verification": "static",
        "constraint": "Compatibility review is incomplete.",
        "reason": "compatibility_gate_not_passed",
    }


def _absent() -> dict[str, object]:
    return {
        "availability": "unsupported",
        "execution_mode": "not_executable",
        "verification": "static",
        "constraint": "The exact upstream release does not provide this capability.",
        "reason": "upstream_capability_absent",
        "evidence_sources": ["controller:1.0.0:ProjectController"],
    }


def _limited() -> dict[str, object]:
    return {
        "availability": "limited",
        "execution_mode": "not_executable",
        "verification": "static",
        "constraint": "The upstream release exposes only a lossy subset.",
        "reason": "upstream_capability_limited",
        "evidence_sources": ["ui:2.0.0:projects/actions.js"],
    }
