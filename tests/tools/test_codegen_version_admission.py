"""Acceptance boundaries between source inventory, review, and runtime packaging."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from ds_codegen import (
    datasource_profiles,
    runtime_bundles,
    source_matrix,
    task_profiles,
    version_profiles,
)
from ds_codegen.profile_ledger import reviewed_profile_versions
from dsctl.cli_surface import stable_leaf_actions
from dsctl.upstream.registry import SUPPORTED_VERSIONS

if TYPE_CHECKING:
    from ds_codegen.runtime_bundles import RuntimeBundle

ROOT = Path(__file__).resolve().parents[2]
ADMITTED_RELEASES = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)


def test_candidate_source_inventory_does_not_admit_a_runtime_profile(
    tmp_path: Path,
) -> None:
    inventory = tmp_path / "source-inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dolphinscheduler-exact-source-matrix",
                "targets": [
                    {"version": version, "source_root": f"sources/ds-{version}"}
                    for version in (*ADMITTED_RELEASES, "9.9.9")
                ],
            }
        ),
        encoding="utf-8",
    )

    specs = source_matrix.load_exact_source_specs(tmp_path, inventory)

    assert {spec.version for spec in specs} == {*ADMITTED_RELEASES, "9.9.9"}
    assert "9.9.9" not in reviewed_profile_versions()
    with pytest.raises(ValueError, match=r"9\.9\.9"):
        version_profiles.compile_version_profile_data(
            stable_actions=stable_leaf_actions(), versions=("9.9.9",)
        )


def test_unreviewed_runtime_manifest_is_rejected_before_source_loading(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "runtime-bundles.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "9.9.9",
                        "source_root": "sources/ds-9.9.9",
                        "selection": "runtime-slice",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"version profile ledger.*9\.9\.9"):
        runtime_bundles.load_prepared_runtime_bundles(
            tmp_path, manifest, snapshot_mode="require"
        )


def test_current_inventory_review_and_packaging_sets_match_admitted_releases() -> None:
    assert (
        tuple(spec.version for spec in source_matrix.load_exact_source_specs(ROOT))
        == ADMITTED_RELEASES
    )
    assert reviewed_profile_versions() == ADMITTED_RELEASES
    assert (
        tuple(spec.version for spec in runtime_bundles.load_runtime_bundle_specs(ROOT))
        == ADMITTED_RELEASES
    )
    assert tuple(SUPPORTED_VERSIONS) == ADMITTED_RELEASES
    data = version_profiles.compile_version_profile_data(
        stable_actions=stable_leaf_actions()
    )
    assert tuple(data["profiles"]) == ADMITTED_RELEASES
    for version, profile in data["profiles"].items():
        assert profile["server_version"] == profile["contract_version"] == version
        assert profile["support_level"] == (
            "full" if version == "3.4.1" else "experimental"
        )
        assert profile["tested"] is (version == "3.4.1")


def test_selected_profile_does_not_include_other_reviewed_releases() -> None:
    data = version_profiles.compile_version_profile_data(
        stable_actions=stable_leaf_actions(), versions=("3.4.1",)
    )

    assert data["target_versions"] == ["3.4.1"]
    assert tuple(data["profiles"]) == ("3.4.1",)
    assert data["profiles"]["3.4.1"]["support_level"] == "full"
    assert data["profiles"]["3.4.1"]["tested"] is True


@pytest.mark.parametrize("missing_document", ["facts", "reviews"])
def test_selected_task_profile_requires_both_facts_and_review(
    missing_document: str,
) -> None:
    facts = task_profiles.load_task_profile_document(
        task_profiles.DEFAULT_TASK_PROFILE_FACTS
    )
    reviews = task_profiles.load_task_profile_document(
        task_profiles.DEFAULT_TASK_PROFILE_REVIEWS
    )
    document = facts if missing_document == "facts" else reviews
    del document["profiles"]["3.4.1"]

    with pytest.raises(ValueError, match=r"3\.4\.1"):
        task_profiles.compile_task_profile_data(facts, reviews, versions=("3.4.1",))


@pytest.mark.source_contract
def test_packaged_version_matches_every_generated_profile_membership(
    tmp_path: Path,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    selected = tuple(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    assert len(selected) == 1

    runtime_bundles.render_runtime_bundles(selected, tmp_path)

    generated = tmp_path / "generated"
    profile_constants = {
        "version_profiles.py": "TARGET_DS_VERSIONS",
        "task_profiles.py": "TARGET_DS_VERSIONS",
        "datasource_profiles.py": "TARGET_DATASOURCE_VERSIONS",
        "workflow_profiles.py": "TARGET_WORKFLOW_VERSIONS",
        "task_definition_profiles.py": "TARGET_TASK_DEFINITION_VERSIONS",
        "runtime_instance_profiles.py": "TARGET_RUNTIME_INSTANCE_VERSIONS",
    }
    for filename, constant in profile_constants.items():
        namespace = runpy.run_path(str(generated / filename))
        assert namespace[constant] == ("3.4.1",), filename
    assert sorted(
        path.name for path in (generated / "versions").iterdir() if path.is_dir()
    ) == ["ds_3_4_1"]


@pytest.mark.source_contract
def test_packaging_rejects_missing_datasource_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    selected = tuple(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    reviews = datasource_profiles.load_datasource_profile_reviews()
    del reviews["3.4.1"]
    # Replace only the review-file input; keep the compiler and its checks real.
    monkeypatch.setattr(
        runtime_bundles, "load_datasource_profile_reviews", lambda: reviews
    )

    with pytest.raises(ValueError, match=r"3\.4\.1"):
        runtime_bundles.render_runtime_bundles(selected, tmp_path)
