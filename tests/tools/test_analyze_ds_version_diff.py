from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_version_diff_entrypoint_delegates_shared_snapshot_loading(
    tmp_path: Path,
) -> None:
    tool = _load_module("analyze_ds_version_diff")
    api = _load_module("ds_codegen.api")
    base = tmp_path / "base.json"
    target = tmp_path / "target.json"
    api.write_contract_snapshot(_snapshot(api, "3.9.0"), base)
    api.write_contract_snapshot(_snapshot(api, "3.10.0"), target)
    output = tmp_path / "diff.json"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.9.0={base}",
            "--snapshot",
            f"3.10.0={target}",
            "--base",
            "3.9.0",
            "--format",
            "json",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert report["base"]["label"] == "3.9.0"
    assert report["target"]["label"] == "3.10.0"


def _snapshot(api: Any, version: str) -> Any:
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


def test_cli_scope_rejects_snapshots_without_exact_evidence(
    tmp_path: Path, capsys: Any
) -> None:
    tool = _load_module("analyze_ds_version_diff")
    api = _load_module("ds_codegen.api")
    snapshots = [tmp_path / f"{version}.json" for version in ("3.4.1", "3.4.2")]
    for path in snapshots:
        api.write_contract_snapshot(_snapshot(api, path.stem), path)
    output = tmp_path / "diff.json"

    result = tool.main(
        [
            "--scope",
            "cli",
            "--base",
            "3.4.1",
            "--snapshot",
            f"3.4.1={snapshots[0]}",
            "--snapshot",
            f"3.4.2={snapshots[1]}",
            "--output",
            str(output),
        ]
    )

    assert result == 2
    assert not output.exists()
    assert "has no embedded provenance" in capsys.readouterr().err


def test_source_manifest_does_not_load_unselected_candidates(tmp_path: Path) -> None:
    tool = _load_module("analyze_ds_version_diff")
    api = _load_module("ds_codegen.api")
    snapshot = tmp_path / "base.json"
    api.write_contract_snapshot(_snapshot(api, "3.4.1"), snapshot)
    manifest = tmp_path / "candidates.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dolphinscheduler-exact-source-matrix",
                "targets": [
                    {"version": "3.4.3", "source_root": "not-downloaded/ds-3.4.3"}
                ],
            }
        )
    )
    output = tmp_path / "diff.json"

    result = tool.main(
        [
            "--repo-root",
            str(tmp_path),
            "--source-manifest",
            str(manifest),
            "--snapshot",
            f"3.4.1={snapshot}",
            "--base",
            "3.4.1",
            "--target",
            "3.4.1",
            "--format",
            "json",
            "--output",
            str(output),
        ]
    )

    assert result == 0
    report = json.loads(output.read_text())
    assert report["base"]["ds_version"] == report["target"]["ds_version"] == "3.4.1"
    assert not any(
        count for counts in report["summary"].values() for count in counts.values()
    )
