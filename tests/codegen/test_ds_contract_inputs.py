from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest


def _api() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.api")


def test_contract_inputs_share_natural_version_order_and_duplicate_validation(
    tmp_path: Path,
) -> None:
    api = _api()

    inputs = api.parse_contract_inputs(
        snapshot_values=[f"3.10.0={tmp_path / 'new.json'}"],
        source_values=[f"3.9.0={tmp_path / 'old'}"],
        minimum_count=2,
        require_version_labels=True,
    )

    ordered = sorted(
        inputs,
        key=lambda item: api.natural_version_key(item.label),
    )
    assert [item.label for item in ordered] == [
        "3.9.0",
        "3.10.0",
    ]
    assert [(item.kind, item.path) for item in inputs] == [
        ("snapshot", tmp_path / "new.json"),
        ("ds-source", tmp_path / "old"),
    ]

    with pytest.raises(ValueError, match=r"duplicate input labels: 3\.4\.1"):
        api.parse_contract_inputs(
            snapshot_values=[f"3.4.1={tmp_path / 'one.json'}"],
            source_values=[f"3.4.1={tmp_path / 'source'}"],
            minimum_count=1,
            require_version_labels=True,
        )


def test_contract_input_parser_can_keep_non_version_diff_labels(tmp_path: Path) -> None:
    api = _api()

    inputs = api.parse_contract_inputs(
        snapshot_values=[f"baseline={tmp_path / 'base.json'}"],
        source_values=[f"candidate={tmp_path / 'candidate'}"],
        minimum_count=2,
        require_version_labels=False,
    )

    assert [item.label for item in inputs] == ["baseline", "candidate"]


def test_git_source_provenance_survives_a_cached_snapshot(tmp_path: Path) -> None:
    api = _api()
    source_root = _tagged_source_tree(tmp_path, version="9.9.9")
    snapshot = _snapshot("9.9.9")

    source_provenance = api.build_source_provenance(
        source_root,
        label="9.9.9",
        snapshot=snapshot,
    )

    assert source_provenance["exact"] is True
    assert source_provenance["input_kind"] == "ds-source"
    assert source_provenance["content_digest"] == (
        f"git-tree:{_git(source_root, 'rev-parse', 'HEAD^{tree}')}"
    )
    assert source_provenance["origin"]["git"] == {
        "commit": _git(source_root, "rev-parse", "HEAD^{commit}"),
        "tree": _git(source_root, "rev-parse", "HEAD^{tree}"),
        "tag": "9.9.9",
        "ref": "refs/tags/9.9.9",
        "dirty": False,
    }

    cached = tmp_path / "cached.json"
    api.write_contract_snapshot(snapshot, cached, provenance=source_provenance)
    loaded = api.load_contract_input(
        api.ContractInput("9.9.9", cached, "snapshot"),
        require_exact=True,
    )

    assert loaded.snapshot == snapshot
    assert loaded.provenance["exact"] is True
    assert loaded.provenance["input_kind"] == "snapshot"
    assert loaded.provenance["content_digest"] == api.contract_snapshot_digest(snapshot)
    assert loaded.provenance["origin"] == source_provenance["origin"]


def test_cached_snapshot_rejects_missing_or_stale_exact_provenance(
    tmp_path: Path,
) -> None:
    api = _api()
    snapshot = _snapshot("3.4.1")
    legacy = tmp_path / "legacy.json"
    api.write_contract_snapshot(snapshot, legacy)

    with pytest.raises(api.ExactProvenanceError, match="has no embedded provenance"):
        api.load_contract_input(
            api.ContractInput("3.4.1", legacy, "snapshot"),
            require_exact=True,
        )

    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    provenance = api.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    api.write_contract_snapshot(snapshot, legacy, provenance=provenance)
    payload = json.loads(legacy.read_text(encoding="utf-8"))
    payload["operation_count"] = 1
    legacy.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="contract digest does not match"):
        api.load_contract_input(
            api.ContractInput("3.4.1", legacy, "snapshot"),
            require_exact=True,
        )


def test_dirty_or_mismatched_git_source_is_not_exact(tmp_path: Path) -> None:
    api = _api()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    snapshot = _snapshot("3.4.1")
    (source_root / "pom.xml").write_text("dirty", encoding="utf-8")

    dirty = api.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    _git(source_root, "restore", "pom.xml")
    wrong_label = api.build_source_provenance(
        source_root,
        label="3.4.2",
        snapshot=replace(snapshot, ds_version="3.4.2"),
    )

    assert dirty["exact"] is False
    assert dirty["origin"]["git"]["dirty"] is True
    assert wrong_label["exact"] is False
    assert wrong_label["origin"]["git"]["dirty"] is False
    assert "tag" not in wrong_label["origin"]["git"]
    with pytest.raises(api.ExactProvenanceError, match="not a clean exact Git tag"):
        api.require_exact_provenance(dirty, label="3.4.1")


def test_inventory_orchestration_persists_provenance_and_keeps_diagnostics(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _tagged_source_tree(tmp_path, version="9.9.9")
    legacy = tmp_path / "legacy.json"
    api.write_contract_snapshot(_snapshot("8.8.8"), legacy)
    snapshot_dir = tmp_path / "snapshots"

    report = api.generate_contract_inventory(
        [
            api.ContractInput("8.8.8", legacy, "snapshot"),
            api.ContractInput("9.9.9", source_root, "ds-source"),
        ],
        snapshot_dir=snapshot_dir,
    )

    assert report["complete"] is False
    assert [target["ds_version"] for target in report["targets"]] == ["9.9.9"]
    assert report["targets"][0]["provenance"]["exact"] is True
    assert report["diagnostics"][0]["error_type"] == "ExactProvenanceError"
    assert report["diagnostics"][0]["provenance"]["exact"] is False

    persisted = snapshot_dir / "ds-9.9.9-contract.json"
    payload = json.loads(persisted.read_text(encoding="utf-8"))
    assert (
        payload["provenance"]["contract_digest"]
        == report["targets"][0]["provenance"]["contract_digest"]
    )


def _snapshot(version: str) -> Any:
    api = _api()
    return api.ContractSnapshot(
        ds_version=version,
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )


def _tagged_source_tree(tmp_path: Path, *, version: str) -> Path:
    source_root = tmp_path / f"source-{version}"
    source_root.mkdir()
    (source_root / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion>"
        f"<groupId>test</groupId><artifactId>ds</artifactId><version>{version}</version>"
        "</project>",
        encoding="utf-8",
    )
    _git(source_root, "init")
    _git(source_root, "config", "user.email", "test@example.com")
    _git(source_root, "config", "user.name", "Test")
    _git(source_root, "add", "pom.xml")
    _git(source_root, "commit", "-m", "source")
    _git(source_root, "tag", version)
    return source_root


def _git(cwd: Path, *args: str) -> str:
    executable = shutil.which("git")
    assert executable is not None
    completed = subprocess.run(  # noqa: S603
        [executable, *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
