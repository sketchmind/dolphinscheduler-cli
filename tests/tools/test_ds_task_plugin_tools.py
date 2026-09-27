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


def test_task_plugin_diff_entrypoint_writes_machine_report(tmp_path: Path) -> None:
    api = _load_module("ds_codegen.api")
    tool = _load_module("analyze_ds_task_plugin_diff")
    base = tmp_path / "base.json"
    target = tmp_path / "target.json"
    api.write_task_plugin_snapshot(_empty_snapshot(api, "3.4.1"), base)
    api.write_task_plugin_snapshot(_empty_snapshot(api, "3.4.2"), target)
    raw_report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=api.load_task_plugin_snapshot(base),
        target_label="3.4.2",
        target=api.load_task_plugin_snapshot(target),
    )
    review = tmp_path / "review.yaml"
    review.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dolphinscheduler-task-plugin-impact-review",
                "base": {
                    "label": "3.4.1",
                    "contract_fingerprint": raw_report["base"]["contract_fingerprint"],
                },
                "target": {
                    "label": "3.4.2",
                    "contract_fingerprint": raw_report["target"][
                        "contract_fingerprint"
                    ],
                },
                "decisions": [],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "diff.json"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={base}",
            "--snapshot",
            f"3.4.2={target}",
            "--base",
            "3.4.1",
            "--format",
            "json",
            "--review",
            str(review),
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert report["base"]["label"] == "3.4.1"
    assert report["target"]["label"] == "3.4.2"
    assert report["review_complete"] is True
    assert report["review"]["mapped_change_ids"] == []


def test_task_plugin_inventory_entrypoint_rejects_non_exact_snapshot(
    tmp_path: Path,
) -> None:
    api = _load_module("ds_codegen.api")
    tool = _load_module("generate_ds_task_plugin_inventory")
    snapshot = tmp_path / "snapshot.json"
    api.write_task_plugin_snapshot(_empty_snapshot(api, "3.4.1"), snapshot)
    output = tmp_path / "inventory.json"

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={snapshot}",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["complete"] is False
    assert report["targets"] == []
    assert report["diagnostics"][0]["error_type"] == "ExactProvenanceError"


def test_task_plugin_diff_review_returns_one_when_source_is_incomplete(
    tmp_path: Path,
) -> None:
    api = _load_module("ds_codegen.api")
    tool = _load_module("analyze_ds_task_plugin_diff")
    base = tmp_path / "base.json"
    target = tmp_path / "target.json"
    api.write_task_plugin_snapshot(_empty_snapshot(api, "3.4.1"), base)
    api.write_task_plugin_snapshot(
        _empty_snapshot(api, "3.4.2", source_complete=False),
        target,
    )
    raw_report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=api.load_task_plugin_snapshot(base),
        target_label="3.4.2",
        target=api.load_task_plugin_snapshot(target),
    )
    review = tmp_path / "review.json"
    review.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dolphinscheduler-task-plugin-impact-review",
                "base": raw_report["base"],
                "target": raw_report["target"],
                "decisions": [],
            }
        ),
        encoding="utf-8",
    )

    exit_code = tool.main(
        [
            "--snapshot",
            f"3.4.1={base}",
            "--snapshot",
            f"3.4.2={target}",
            "--base",
            "3.4.1",
            "--review",
            str(review),
            "--format",
            "json",
        ]
    )

    assert exit_code == 1


def _empty_snapshot(
    api: Any,
    version: str,
    *,
    source_complete: bool = True,
) -> Any:
    return api.TaskPluginSnapshot(
        schema_version=1,
        ds_version=version,
        source_complete=source_complete,
        plugins=[],
        models=[],
        enums=[],
        diagnostics=[],
    )
