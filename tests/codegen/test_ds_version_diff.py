from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module(name: str) -> Any:
    _ensure_tools_on_path()
    return importlib.import_module(name)


def test_version_diff_reports_operation_dto_and_enum_drift() -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    base = _snapshot("3.4.1")
    target = _snapshot("3.4.0", changed=True)

    report = version_diff.compare_contract_snapshots(
        base_label="3.4.1",
        base=base,
        target_label="3.4.0",
        target=target,
    )

    assert report["summary"]["operations"] == {
        "added": 1,
        "removed": 1,
        "changed": 1,
    }
    operation_change = report["operations"]["changed"][0]
    assert operation_change["key"] == "WorkflowController.query"
    assert operation_change["changes"] == [
        {
            "field": "path",
            "before": "projects/{projectCode}/workflow-definition",
            "after": "projects/{projectCode}/workflow-definition/list",
        }
    ]
    assert operation_change["parameters"]["added"][0]["key"] == (
        "request_param:searchVal"
    )
    assert operation_change["parameters"]["changed"][0]["changes"] == [
        {"field": "java_type", "before": "Long", "after": "long"},
        {"field": "schema_type", "before": "Long", "after": "long"},
    ]
    assert report["summary"]["dtos"] == {"added": 0, "removed": 0, "changed": 1}
    assert report["dtos"]["changed"][0]["fields"]["added"][0]["key"] == "description"
    assert report["dtos"]["changed"][0]["fields"]["changed"][0]["changes"] == [
        {"field": "java_type", "before": "String", "after": "Integer"}
    ]
    assert report["summary"]["enums"] == {"added": 0, "removed": 0, "changed": 1}
    assert report["enums"]["changed"][0]["values"]["added"][0]["key"] == "NEW"
    assert report["enums"]["changed"][0]["values"]["removed"][0]["key"] == "OLD"


def test_version_diff_markdown_summarizes_changed_sections() -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    report = version_diff.compare_contract_snapshots(
        base_label="3.4.1",
        base=_snapshot("3.4.1"),
        target_label="3.4.0",
        target=_snapshot("3.4.0", changed=True),
    )

    markdown = version_diff.render_markdown_report(report, max_items=10)

    assert "# DolphinScheduler Contract Diff: 3.4.1 -> 3.4.0" in markdown
    assert "| operations | 1 | 1 | 1 |" in markdown
    assert "- `WorkflowController.query`" in markdown
    assert "parameters: +1 -0 ~1" in markdown


def test_version_diff_loads_snapshot_json(tmp_path: Path) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(asdict(_snapshot("3.4.1"))), encoding="utf-8")

    loaded = version_diff.load_snapshot(snapshot_path)

    assert loaded.ds_version == "3.4.1"
    assert loaded.operations[0].operation_id == "WorkflowController.query"
    assert loaded.dtos[0].fields[0].wire_name == "name"


def test_ds_source_version_reads_direct_project_version(tmp_path: Path) -> None:
    source = _load_module("ds_codegen.source")
    ds_root = tmp_path / "dolphinscheduler"
    ds_root.mkdir()
    (ds_root / "pom.xml").write_text(
        """<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <parent>
    <groupId>org.apache</groupId>
    <artifactId>apache</artifactId>
    <version>25</version>
  </parent>
  <artifactId>dolphinscheduler</artifactId>
  <version>3.3.2</version>
</project>
""",
        encoding="utf-8",
    )

    assert source.read_ds_source_version(ds_root) == "3.3.2"


def _snapshot(version: str, *, changed: bool = False) -> Any:
    ir = _load_module("ds_codegen.ir")
    common_operation = ir.OperationSpec(
        operation_id="WorkflowController.query",
        controller="WorkflowController",
        method_name="query",
        api_group="v1",
        http_method="GET",
        path=(
            "projects/{projectCode}/workflow-definition/list"
            if changed
            else "projects/{projectCode}/workflow-definition"
        ),
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Result",
        inferred_return_type="WorkflowDefinition",
        logical_return_type="WorkflowDefinition",
        response_projection="single_data",
        parameters=[
            _parameter(ir, java_type="long" if changed else "Long"),
            *(
                [
                    _parameter(
                        ir,
                        name="search",
                        java_type="String",
                        binding="request_param",
                        wire_name="searchVal",
                    )
                ]
                if changed
                else []
            ),
        ],
    )
    operations = [
        common_operation,
        ir.OperationSpec(
            operation_id=(
                "WorkflowController.added" if changed else "WorkflowController.removed"
            ),
            controller="WorkflowController",
            method_name="added" if changed else "removed",
            api_group="v1",
            http_method="POST",
            path="projects/{projectCode}/workflow-definition/extra",
            summary=None,
            description=None,
            documentation=None,
            parameter_docs={},
            returns_doc=None,
            consumes=[],
            return_type="Result",
            inferred_return_type="long",
            logical_return_type="long",
            response_projection="single_data",
            parameters=[],
        ),
    ]
    dtos = [
        ir.DtoSpec(
            name="WorkflowDefinitionDto",
            import_path="org.apache.WorkflowDefinitionDto",
            documentation=None,
            extends=None,
            fields=[
                _field(ir, java_type="Integer" if changed else "String"),
                *(
                    [_field(ir, name="description", java_type="String")]
                    if changed
                    else []
                ),
            ],
        )
    ]
    enums = [
        ir.EnumSpec(
            name="ReleaseState",
            import_path="org.apache.ReleaseState",
            documentation=None,
            fields=[],
            json_value_field=None,
            values=[
                ir.EnumValueSpec(
                    name="ONLINE",
                    arguments=["1"] if changed else ["0"],
                    documentation=None,
                ),
                ir.EnumValueSpec(
                    name="NEW" if changed else "OLD",
                    arguments=[],
                    documentation=None,
                ),
            ],
        )
    ]
    return ir.ContractSnapshot(
        ds_version=version,
        operation_count=len(operations),
        enum_count=len(enums),
        dto_count=len(dtos),
        model_count=0,
        operations=operations,
        enums=enums,
        dtos=dtos,
        models=[],
    )


def _parameter(
    ir: Any,
    *,
    name: str = "projectCode",
    java_type: str = "Long",
    binding: str = "path_variable",
    wire_name: str = "projectCode",
) -> Any:
    return ir.ParameterSpec(
        name=name,
        java_type=java_type,
        binding=binding,
        wire_name=wire_name,
        required=True,
        default_value=None,
        hidden=False,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=java_type,
    )


def _field(
    ir: Any,
    *,
    name: str = "name",
    java_type: str = "String",
) -> Any:
    return ir.DtoFieldSpec(
        name=name,
        java_type=java_type,
        wire_name=name,
        required=None,
        default_value=None,
        nullable=True,
        default_factory=None,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )
