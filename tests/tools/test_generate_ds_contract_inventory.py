from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any


def _load_module() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("generate_ds_contract_inventory")


def test_inventory_tool_keeps_successful_targets_and_reports_each_failure(
    tmp_path: Path,
) -> None:
    tool = _load_module()
    valid = tmp_path / "3.4.1.json"
    _write_exact_snapshot(valid, "3.4.1")
    output = tmp_path / "inventory.json"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.2={tmp_path / 'missing.json'}",
            "--snapshot",
            f"3.4.1={valid}",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    missing_message = (
        f"[Errno 2] No such file or directory: '{tmp_path / 'missing.json'}'"
    )
    assert exit_code == 1
    assert report["complete"] is False
    assert [target["ds_version"] for target in report["targets"]] == ["3.4.1"]
    assert report["diagnostics"] == [
        {
            "label": "3.4.2",
            "input_kind": "snapshot",
            "error_type": "FileNotFoundError",
            "message": missing_message,
        }
    ]


def test_inventory_tool_rejects_duplicate_labels(tmp_path: Path) -> None:
    tool = _load_module()
    snapshot = tmp_path / "snapshot.json"
    _write_exact_snapshot(snapshot, "3.4.1")

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={snapshot}",
            "--ds-source",
            f"3.4.1={tmp_path}",
        ]
    )

    assert exit_code == 2


def test_inventory_tool_persists_reusable_exact_snapshots(tmp_path: Path) -> None:
    tool = _load_module()
    source_snapshot = tmp_path / "source.json"
    _write_exact_snapshot(source_snapshot, "3.4.1")
    snapshot_dir = tmp_path / "snapshots"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={source_snapshot}",
            "--snapshot-dir",
            str(snapshot_dir),
            "--output",
            str(tmp_path / "inventory.json"),
        ]
    )

    persisted = snapshot_dir / "ds-3.4.1-contract.json"
    assert exit_code == 0
    payload = json.loads(persisted.read_text(encoding="utf-8"))
    provenance = payload.pop("provenance")
    assert payload == _snapshot_payload("3.4.1")
    assert provenance["exact"] is True
    assert provenance["input_kind"] == "snapshot"


def test_inventory_tool_does_not_claim_legacy_snapshot_as_exact(
    tmp_path: Path,
) -> None:
    tool = _load_module()
    legacy = tmp_path / "legacy.json"
    legacy.write_text(json.dumps(_snapshot_payload("3.4.1")), encoding="utf-8")
    output = tmp_path / "inventory.json"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={legacy}",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["targets"] == []
    assert report["diagnostics"][0]["error_type"] == "ExactProvenanceError"
    assert report["diagnostics"][0]["provenance"]["exact"] is False


def _snapshot_payload(version: str) -> dict[str, object]:
    return {
        "ds_version": version,
        "operation_count": 0,
        "enum_count": 0,
        "dto_count": 0,
        "model_count": 0,
        "operations": [],
        "enums": [],
        "dtos": [],
        "models": [],
    }


def _write_exact_snapshot(path: Path, version: str) -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    api = importlib.import_module("ds_codegen.api")
    snapshot = api.ContractSnapshot(**_snapshot_payload(version))
    tree = "b" * 40
    provenance = {
        "schema_version": 1,
        "exact": True,
        "input_kind": "ds-source",
        "content_digest": f"git-tree:{tree}",
        "contract_digest": api.contract_snapshot_digest(snapshot),
        "origin": {
            "kind": "ds-source",
            "content_digest": f"git-tree:{tree}",
            "git": {
                "commit": "a" * 40,
                "tree": tree,
                "tag": version,
                "ref": f"refs/tags/{version}",
                "dirty": False,
            },
        },
    }
    api.write_contract_snapshot(snapshot, path, provenance=provenance)
