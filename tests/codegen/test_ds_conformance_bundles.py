from __future__ import annotations

import copy
import hashlib
import importlib
import json
import runpy
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dsctl.generated.version_profiles import PROFILE_DATA

if TYPE_CHECKING:
    from types import ModuleType


LEGACY_CORE_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "workflow.get",
    "workflow.list",
)
FULL_CORE_ADDITIONAL_ACTIONS = (
    "workflow.create",
    "workflow.delete",
    "workflow.describe",
    "workflow.digest",
    "workflow.edit",
    "workflow.export",
    "task.get",
    "task.list",
    "task.update",
)
CANONICAL_FULL_CORE_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "task.get",
    "task.list",
    "task.update",
    "workflow.create",
    "workflow.delete",
    "workflow.describe",
    "workflow.digest",
    "workflow.edit",
    "workflow.export",
    "workflow.get",
    "workflow.list",
)


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.conformance_bundles")


def _recompute_assessment_digest(assessment: dict[str, object]) -> None:
    payload = {
        key: copy.deepcopy(value)
        for key, value in assessment.items()
        if key != "assessment_digest"
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assessment["assessment_digest"] = f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def test_catalog_declares_the_two_fixed_support_tier_action_bundles() -> None:
    conformance = _load_module()

    catalog = conformance.load_conformance_bundle_catalog()

    assert catalog == {
        "schema_version": 1,
        "kind": "dsctl-conformance-bundle-catalog",
        "bundles": [
            {
                "name": "legacy_core/v1",
                "extends": [],
                "actions": list(LEGACY_CORE_ACTIONS),
            },
            {
                "name": "full_core/v1",
                "extends": ["legacy_core/v1"],
                "actions": [
                    *LEGACY_CORE_ACTIONS,
                    *FULL_CORE_ADDITIONAL_ACTIONS,
                ],
            },
        ],
    }


def test_static_assessment_reports_all_thirty_ready_coordinates() -> None:
    conformance = _load_module()

    report = conformance.assess_static_conformance_bundles(PROFILE_DATA)

    assert {
        key: report[key]
        for key in (
            "schema_version",
            "kind",
            "claim",
            "promotion_claimed",
            "support_level_changes",
            "tested_changes",
            "unassessed_axes",
            "target_versions",
            "summary",
        )
    } == {
        "schema_version": 1,
        "kind": "dsctl-conformance-bundle-static-assessment",
        "claim": "static-action-closure-only",
        "promotion_claimed": False,
        "support_level_changes": False,
        "tested_changes": False,
        "unassessed_axes": [
            "facets",
            "scenarios",
            "freshness",
            "live_evidence",
        ],
        "target_versions": list(PROFILE_DATA["target_versions"]),
        "summary": {
            "bundle_count": 2,
            "coordinate_count": 74,
            "ready_coordinate_count": 74,
            "blocked_coordinate_count": 0,
        },
    }


def test_static_assessment_closes_the_full_core_action_surface() -> None:
    conformance = _load_module()

    report = conformance.assess_static_conformance_bundles(PROFILE_DATA)

    bundles = {bundle["name"]: bundle for bundle in report["bundles"]}
    legacy = bundles["legacy_core/v1"]
    assert legacy["required_actions"] == list(LEGACY_CORE_ACTIONS)
    assert legacy["status"] == "ready"
    assert legacy["ready_coordinate_count"] == 37
    assert legacy["blocked_coordinate_count"] == 0

    full = bundles["full_core/v1"]
    assert full["required_actions"] == list(CANONICAL_FULL_CORE_ACTIONS)
    assert full["status"] == "ready"
    assert full["ready_coordinate_count"] == 37
    assert full["blocked_coordinate_count"] == 0
    versions = {version["version"]: version for version in full["versions"]}
    assert versions["1.3.9"]["blockers"] == []
    assert versions["2.0.0"]["blockers"] == []
    assert versions["3.4.2"] == {
        "version": "3.4.2",
        "status": "ready",
        "support_level": "experimental",
        "tested": False,
        "blockers": [],
    }
    assert versions["3.4.3"] == {
        "version": "3.4.3",
        "status": "ready",
        "support_level": "experimental",
        "tested": False,
        "blockers": [],
    }
    assert all(
        set(blocker)
        == {
            "action",
            "availability",
            "reason",
            "constraint",
            "execution_mode",
            "verification",
        }
        for version in full["versions"]
        for blocker in version["blockers"]
    )


def test_catalog_loader_rejects_unknown_inheritance(tmp_path: Path) -> None:
    conformance = _load_module()
    path = tmp_path / "catalog.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dsctl-conformance-bundle-catalog",
                "bundles": [
                    {
                        "name": "full_core/v1",
                        "extends": ["missing/v1"],
                        "actions": ["doctor"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown conformance bundle parent"):
        conformance.load_conformance_bundle_catalog(path)


def test_static_assessment_rejects_an_incomplete_supported_capability() -> None:
    conformance = _load_module()
    profile_data = copy.deepcopy(PROFILE_DATA)
    del profile_data["profiles"]["3.4.2"]["actions"]["doctor"]["execution_mode"]

    with pytest.raises(TypeError, match=r"doctor\.execution_mode"):
        conformance.assess_static_conformance_bundles(profile_data)


def test_generated_conformance_data_round_trips_deterministically(
    tmp_path: Path,
) -> None:
    conformance = _load_module()
    expected = conformance.assess_static_conformance_bundles(PROFILE_DATA)

    first = conformance.render_conformance_bundle_data(expected)
    second = conformance.render_conformance_bundle_data(expected)
    output = conformance.write_conformance_bundle_data(tmp_path, PROFILE_DATA)

    assert first == second
    assert output == tmp_path / "generated" / "conformance_bundles.py"
    assert output.read_text(encoding="utf-8") == first
    assert runpy.run_path(str(output))["CONFORMANCE_BUNDLE_DATA"] == expected


def test_canonical_digests_ignore_catalog_format_and_order_but_bind_actions(
    tmp_path: Path,
) -> None:
    conformance = _load_module()
    canonical = conformance.load_conformance_bundle_catalog()
    reordered = copy.deepcopy(canonical)
    reordered["bundles"].reverse()
    for bundle in reordered["bundles"]:
        bundle["actions"].reverse()
        bundle["extends"].reverse()
    canonical_path = tmp_path / "canonical.json"
    reordered_path = tmp_path / "reordered.json"
    canonical_path.write_text(json.dumps(canonical, indent=2), encoding="utf-8")
    reordered_path.write_text(
        json.dumps(reordered, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )

    first = conformance.assess_static_conformance_bundles(
        PROFILE_DATA,
        catalog=conformance.load_conformance_bundle_catalog(canonical_path),
    )
    second = conformance.assess_static_conformance_bundles(
        PROFILE_DATA,
        catalog=conformance.load_conformance_bundle_catalog(reordered_path),
    )
    tampered = copy.deepcopy(canonical)
    tampered["bundles"][1]["actions"].remove("task.update")
    changed = conformance.assess_static_conformance_bundles(
        PROFILE_DATA,
        catalog=tampered,
    )

    assert first["catalog_digest"].startswith("sha256:")
    assert first["catalog_digest"] == second["catalog_digest"]
    assert first["assessment_digest"] == second["assessment_digest"]
    assert first == second
    assert first["catalog_digest"] != changed["catalog_digest"]
    first_bundle_digests = {
        bundle["name"]: bundle["bundle_digest"] for bundle in first["bundles"]
    }
    second_bundle_digests = {
        bundle["name"]: bundle["bundle_digest"] for bundle in second["bundles"]
    }
    changed_bundle_digests = {
        bundle["name"]: bundle["bundle_digest"] for bundle in changed["bundles"]
    }
    assert first_bundle_digests == second_bundle_digests
    assert (
        first_bundle_digests["legacy_core/v1"]
        == (changed_bundle_digests["legacy_core/v1"])
    )
    assert (
        first_bundle_digests["full_core/v1"] != (changed_bundle_digests["full_core/v1"])
    )


def test_static_assessment_covers_the_explicit_profile_selection() -> None:
    conformance = _load_module()
    profile_data = copy.deepcopy(PROFILE_DATA)
    profile_data["target_versions"].remove("3.4.2")
    del profile_data["profiles"]["3.4.2"]

    assessment = conformance.assess_static_conformance_bundles(profile_data)
    assert assessment["target_versions"] == profile_data["target_versions"]
    assert assessment["summary"]["coordinate_count"] == 72


def test_static_assessment_requires_each_exact_profile_action_domain() -> None:
    conformance = _load_module()
    profile_data = copy.deepcopy(PROFILE_DATA)
    profile_data["profiles"]["3.4.2"]["actions"]["unexpected.action"] = {
        "availability": "supported",
        "execution_mode": "local",
        "verification": "static",
    }

    with pytest.raises(ValueError, match="actions do not match the stable action set"):
        conformance.assess_static_conformance_bundles(profile_data)


def test_catalog_loader_rejects_unknown_schema_fields(tmp_path: Path) -> None:
    conformance = _load_module()
    catalog = conformance.load_conformance_bundle_catalog()
    catalog["promotion"] = "full"
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog), encoding="utf-8")

    with pytest.raises(ValueError, match="catalog fields must be exactly"):
        conformance.load_conformance_bundle_catalog(path)


def test_renderer_rejects_actions_that_do_not_match_their_bundle_digest() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    assessment["bundles"][1]["required_actions"].remove("task.update")

    with pytest.raises(ValueError, match="bundle_digest"):
        conformance.render_conformance_bundle_data(assessment)


def test_renderer_rejects_a_stale_coordinate_summary() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    assessment["summary"]["ready_coordinate_count"] = 29

    with pytest.raises(ValueError, match="coordinate summary"):
        conformance.render_conformance_bundle_data(assessment)


def test_renderer_rejects_a_nonterminal_coordinate() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    coordinate = assessment["bundles"][0]["versions"][0]
    coordinate["status"] = "blocked"

    with pytest.raises(ValueError, match="must be ready exactly when"):
        conformance.render_conformance_bundle_data(assessment)


def test_assessment_digest_binds_the_full_static_result() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)

    assert assessment["assessment_digest"].startswith("sha256:")
    coordinate = assessment["bundles"][1]["versions"][0]
    coordinate["support_level"] = "full"
    coordinate["tested"] = True
    with pytest.raises(ValueError, match="assessment_digest"):
        conformance.render_conformance_bundle_data(assessment)


def test_renderer_rejects_noncanonical_exact_version_order() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    for bundle in assessment["bundles"]:
        bundle["versions"].reverse()
    _recompute_assessment_digest(assessment)

    with pytest.raises(ValueError, match="version order is not canonical"):
        conformance.render_conformance_bundle_data(
            assessment,
            profile_data=PROFILE_DATA,
        )


def test_renderer_binds_support_metadata_to_the_exact_profiles() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    for bundle in assessment["bundles"]:
        coordinate = next(
            row for row in bundle["versions"] if row["version"] == "3.4.2"
        )
        coordinate["support_level"] = "full"
        coordinate["tested"] = True
    _recompute_assessment_digest(assessment)

    with pytest.raises(ValueError, match="does not match profile"):
        conformance.render_conformance_bundle_data(assessment)


@pytest.mark.parametrize("subset_assessment", [False, True])
def test_renderer_rejects_different_assessment_and_profile_membership(
    *,
    subset_assessment: bool,
) -> None:
    conformance = _load_module()
    selected = copy.deepcopy(PROFILE_DATA)
    selected["target_versions"] = ["3.4.1"]
    selected["profiles"] = {"3.4.1": selected["profiles"]["3.4.1"]}
    assessed_profiles = selected if subset_assessment else PROFILE_DATA
    validated_profiles = PROFILE_DATA if subset_assessment else selected
    assessment = conformance.assess_static_conformance_bundles(assessed_profiles)

    with pytest.raises(ValueError, match="target_versions do not match profile data"):
        conformance.render_conformance_bundle_data(
            assessment, profile_data=validated_profiles
        )


def test_renderer_preserves_matching_selected_profile_membership(
    tmp_path: Path,
) -> None:
    conformance = _load_module()
    selected = copy.deepcopy(PROFILE_DATA)
    selected["target_versions"] = ["3.4.1"]
    selected["profiles"] = {"3.4.1": selected["profiles"]["3.4.1"]}
    assessment = conformance.assess_static_conformance_bundles(selected)

    rendered = conformance.render_conformance_bundle_data(
        assessment, profile_data=selected
    )
    output = tmp_path / "conformance_bundles.py"
    output.write_text(rendered, encoding="utf-8")
    loaded = runpy.run_path(str(output))["CONFORMANCE_BUNDLE_DATA"]

    assert loaded == assessment
    assert loaded["target_versions"] == ["3.4.1"]
    assert loaded["summary"]["coordinate_count"] == 2
    assert all(
        [coordinate["version"] for coordinate in bundle["versions"]] == ["3.4.1"]
        for bundle in loaded["bundles"]
    )


def test_renderer_rejects_semantically_inconsistent_blocker_metadata() -> None:
    conformance = _load_module()
    assessment = conformance.assess_static_conformance_bundles(PROFILE_DATA)
    full_bundle = next(
        bundle for bundle in assessment["bundles"] if bundle["name"] == "full_core/v1"
    )
    coordinate = next(
        row for row in full_bundle["versions"] if row["version"] == "1.3.9"
    )
    coordinate["status"] = "blocked"
    coordinate["blockers"] = [
        {
            "action": "task.get",
            "availability": "limited",
            "reason": "upstream_capability_absent",
            "constraint": "synthetic invalid blocker",
            "execution_mode": "wire_program",
            "verification": "live_full",
        }
    ]
    _recompute_assessment_digest(assessment)

    with pytest.raises(ValueError, match="blocker metadata is inconsistent"):
        conformance.render_conformance_bundle_data(assessment)


def test_catalog_rejects_duplicate_actions_and_non_superset_children(
    tmp_path: Path,
) -> None:
    conformance = _load_module()
    canonical = conformance.load_conformance_bundle_catalog()
    invalid_catalogs = []
    duplicate = copy.deepcopy(canonical)
    duplicate["bundles"][0]["actions"].append("doctor")
    invalid_catalogs.append((duplicate, "must not contain duplicates"))
    non_superset = copy.deepcopy(canonical)
    non_superset["bundles"][1]["actions"].remove("doctor")
    invalid_catalogs.append((non_superset, "is not a superset"))

    for index, (catalog, message) in enumerate(invalid_catalogs):
        path = tmp_path / f"invalid-{index}.json"
        path.write_text(json.dumps(catalog), encoding="utf-8")
        with pytest.raises(ValueError, match=message):
            conformance.load_conformance_bundle_catalog(path)


def test_static_assessment_rejects_catalog_actions_outside_the_stable_surface() -> None:
    conformance = _load_module()
    catalog = conformance.load_conformance_bundle_catalog()
    catalog["bundles"][1]["actions"].append("unknown.action")

    with pytest.raises(ValueError, match="unknown stable actions"):
        conformance.assess_static_conformance_bundles(
            PROFILE_DATA,
            catalog=catalog,
        )
