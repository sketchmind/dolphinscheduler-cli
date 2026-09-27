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
    return importlib.import_module("analyze_ds_compatibility_impact")


def test_impact_tool_scopes_report_without_reextracting_sources(tmp_path: Path) -> None:
    tool = _load_module()
    inventory_path = tmp_path / "inventory.json"
    output_path = tmp_path / "impact.json"
    inventory_path.write_text(json.dumps(_inventory()), encoding="utf-8")

    exit_code = tool.main(
        [
            "--inventory",
            str(inventory_path),
            "--version",
            "3.4.1",
            "--version",
            "3.4.2",
            "--semantic-operation",
            "project.page",
            "--output",
            str(output_path),
        ]
    )

    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert list(report["semantic_operations"]) == ["project.page"]
    groups = report["semantic_operations"]["project.page"]["contract_groups"]
    assert len(groups) == 1
    assert groups[0]["versions"] == ["3.4.1", "3.4.2"]
    assert groups[0]["closure_fingerprint"].startswith("sha256:")


def _inventory() -> dict[str, object]:
    return {
        "complete": True,
        "targets": [
            _target("3.4.1"),
            _target("3.4.2"),
        ],
    }


def _target(version: str) -> dict[str, object]:
    return {
        "ds_version": version,
        "surfaces": {
            "dtos": [],
            "enums": [],
            "models": [
                {
                    "key": key,
                    "fingerprint": f"same:{key}",
                }
                for key in (
                    "org.apache.dolphinscheduler.api.utils.PageInfo",
                    "org.apache.dolphinscheduler.api.utils.Result",
                    "org.apache.dolphinscheduler.dao.entity.Project",
                )
            ],
            "operations": [
                {
                    "key": "ProjectController.queryProjectListPaging",
                    "fingerprint": "same-project-page",
                    "http_method": "GET",
                    "path": "projects",
                }
            ],
        },
    }
