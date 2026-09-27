from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest


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


def test_version_diff_keys_same_named_types_by_exact_import_path() -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    ir = _load_module("ds_codegen.ir")
    base = replace(
        _snapshot("3.4.1"),
        model_count=2,
        models=[
            _model(ir, "example.first.Thing", java_type="String"),
            _model(ir, "example.second.Thing", java_type="String"),
        ],
    )
    changed_models = [
        _model(ir, "example.first.Thing", java_type="Long"),
        _model(ir, "example.second.Thing", java_type="String"),
    ]

    reports = [
        version_diff.compare_contract_snapshots(
            base_label="base",
            base=base,
            target_label="target",
            target=replace(base, models=models),
        )
        for models in (changed_models, list(reversed(changed_models)))
    ]

    assert reports[0]["models"] == reports[1]["models"]
    assert reports[0]["summary"]["models"] == {
        "added": 0,
        "removed": 0,
        "changed": 1,
    }
    assert reports[0]["models"]["changed"][0]["key"] == "example.first.Thing"


def test_version_diff_reports_qualified_reference_identity_drift() -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    ir = _load_module("ds_codegen.ir")
    base_snapshot = _snapshot("3.4.1")
    operation = replace(
        base_snapshot.operations[0],
        inferred_return_type="example.first.Thing",
        logical_return_type="example.first.Thing",
    )
    models = [
        _model(ir, "example.first.Thing", java_type="String"),
        _model(ir, "example.second.Thing", java_type="String"),
    ]
    base = replace(
        base_snapshot,
        operations=[operation, *base_snapshot.operations[1:]],
        model_count=2,
        models=models,
    )
    target = replace(
        base,
        operations=[
            replace(
                operation,
                inferred_return_type="example.second.Thing",
                logical_return_type="example.second.Thing",
            ),
            *base_snapshot.operations[1:],
        ],
    )

    report = version_diff.compare_contract_snapshots(
        base_label="base",
        base=base,
        target_label="target",
        target=target,
    )

    assert report["summary"]["operations"] == {
        "added": 0,
        "removed": 0,
        "changed": 1,
    }
    assert report["operations"]["changed"] == [
        {
            "key": "WorkflowController.query",
            "changes": [
                {
                    "field": "inferred_return_type",
                    "before": "example.first.Thing",
                    "after": "example.second.Thing",
                },
                {
                    "field": "logical_return_type",
                    "before": "example.first.Thing",
                    "after": "example.second.Thing",
                },
            ],
        }
    ]
    markdown = version_diff.render_markdown_report(report)
    assert "| operations | 0 | 0 | 1 |" in markdown
    assert "example.first.Thing" in markdown
    assert "example.second.Thing" in markdown


def test_version_diff_loads_snapshot_json(tmp_path: Path) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps(asdict(_snapshot("3.4.1"))), encoding="utf-8")

    loaded = version_diff.load_snapshot(snapshot_path)

    assert loaded.ds_version == "3.4.1"
    assert loaded.operations[0].operation_id == "WorkflowController.query"
    assert loaded.dtos[0].fields[0].wire_name == "name"


def test_version_diff_orchestration_loads_shared_contract_inputs(
    tmp_path: Path,
) -> None:
    api = _load_module("ds_codegen.api")
    base_path = tmp_path / "3.9.0.json"
    target_path = tmp_path / "3.10.0.json"
    api.write_contract_snapshot(_snapshot("3.9.0"), base_path)
    api.write_contract_snapshot(_snapshot("3.10.0", changed=True), target_path)
    inputs = api.parse_contract_inputs(
        snapshot_values=[
            f"3.10.0={target_path}",
            f"3.9.0={base_path}",
        ],
        source_values=[],
        minimum_count=2,
        require_version_labels=False,
    )

    reports = api.analyze_contract_versions(
        inputs,
        base_label="3.9.0",
        target_labels=[],
    )
    rendered = api.render_version_diff_reports(
        reports,
        output_format="json",
        max_items=10,
    )

    assert [report["target"]["label"] for report in reports] == ["3.10.0"]
    assert json.loads(rendered)["summary"]["operations"] == {
        "added": 1,
        "removed": 1,
        "changed": 1,
    }


def test_version_diff_does_not_load_unselected_inventory_entries(
    tmp_path: Path,
) -> None:
    api = _load_module("ds_codegen.api")
    paths = [tmp_path / f"{version}.json" for version in ("3.9.0", "3.10.0")]
    for path in paths:
        api.write_contract_snapshot(_snapshot(path.stem), path)
    inputs = [api.ContractInput(path.stem, path, "snapshot") for path in paths]
    inputs.append(api.ContractInput("3.11.0", tmp_path / "not-downloaded", "ds-source"))

    reports = api.analyze_contract_versions(
        inputs, base_label="3.9.0", target_labels=["3.10.0"]
    )

    assert [report["target"]["label"] for report in reports] == ["3.10.0"]


@pytest.mark.parametrize(
    ("change", "assessment"),
    [
        ("unconsumed", "equal"),
        ("route", "different"),
        ("missing", "incomplete_evidence"),
        ("unresolved_target", "incomplete_evidence"),
        ("invalid_base", "error"),
    ],
)
def test_cli_scope_keeps_candidate_identity_and_distinguishes_missing_evidence(
    tmp_path: Path, monkeypatch: Any, change: str, assessment: str
) -> None:
    api = _load_module("ds_codegen.api")
    analysis = _load_module("ds_codegen.contract_analysis")
    compatibility = _load_module("ds_codegen.compatibility_impact")
    base = _snapshot("3.9.0")
    operation = replace(
        base.operations[0],
        parameters=[],
        logical_return_type="String",
        inferred_return_type="String",
    )
    base = replace(base, operations=[operation, base.operations[1]])
    candidate = replace(base, ds_version="3.10.0")
    if change == "route":
        candidate = replace(
            candidate, operations=[replace(operation, path="other/path")]
        )
    elif change == "missing":
        candidate = replace(candidate, operations=[base.operations[1]])
    else:
        candidate = replace(candidate, operations=[operation])
    candidate = replace(candidate, operation_count=len(candidate.operations))
    binding = replace(
        analysis.runtime_operation_bindings("3.4.1")["identity.current"],
        source_operations=(operation.operation_id,),
        type_closure=(compatibility.WireTypeRef("enums", base.enums[0].import_path),),
    )
    monkeypatch.setattr(
        analysis,
        "runtime_operation_bindings",
        lambda version: {"identity.current": binding},
    )
    monkeypatch.setattr(analysis, "runtime_auxiliary_operation_bindings", lambda _: {})
    if change in {"unresolved_target", "invalid_base"}:
        normalize = analysis.effective_response_candidate
        invalid_version = (
            base.ds_version if change == "invalid_base" else candidate.ds_version
        )

        def normalize_response(operation: Any, snapshot: Any, **kwargs: Any) -> Any:
            if snapshot.ds_version == invalid_version:
                message = "source type unresolved"
                raise ValueError(message)
            return normalize(operation, snapshot, **kwargs)

        monkeypatch.setattr(
            analysis, "effective_response_candidate", normalize_response
        )
    inputs = [
        _write_exact_snapshot(api, tmp_path, snapshot) for snapshot in (base, candidate)
    ]
    before = candidate.to_json_dict()

    if change == "invalid_base":
        with pytest.raises(ValueError, match="source type unresolved"):
            api.analyze_contract_versions(
                inputs, base_label=base.ds_version, target_labels=[], scope="cli"
            )
        return

    [report] = api.analyze_contract_versions(
        inputs, base_label=base.ds_version, target_labels=[], scope="cli"
    )

    assert report["assessment"] == assessment
    assert report["target"]["version"] == "3.10.0"
    assert report["semantic_support"] == "not_assessed"
    assert candidate.to_json_dict() == before
    if change == "missing":
        assert report["closure_diff"] is None
        assert report["missing_operations"] == [operation.operation_id]
    else:
        assert report["closure_diff"]["target"]["ds_version"] == "3.10.0"
    markdown = api.render_version_diff_reports(
        [report], output_format="markdown", max_items=2
    )
    assert f"`{assessment}`" in markdown
    assert "semantic support are not assessed" in markdown


def test_cli_scope_rejects_snapshot_version_different_from_exact_identity(
    tmp_path: Path,
) -> None:
    api = _load_module("ds_codegen.api")
    inputs = [
        _write_exact_snapshot(api, tmp_path, _snapshot(v)) for v in ("3.9.0", "3.10.0")
    ]
    path = inputs[0].path
    payload = json.loads(path.read_text())
    # A valid digest and exact tag still cannot authorize a different body version.
    payload["ds_version"] = "3.10.0"
    payload["provenance"]["contract_digest"] = api.contract_snapshot_digest(
        _snapshot("3.10.0")
    )
    path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="contains a different DS version"):
        api.analyze_contract_versions(
            inputs, base_label="3.9.0", target_labels=[], scope="cli"
        )


def test_cli_scope_rejects_stale_cache_before_comparing(tmp_path: Path) -> None:
    api = _load_module("ds_codegen.api")
    inputs = [
        _write_exact_snapshot(api, tmp_path, _snapshot(v)) for v in ("3.9.0", "3.10.0")
    ]
    payload = json.loads(inputs[1].path.read_text())
    payload["provenance"]["extractor_fingerprint"] = "sha256:" + "0" * 64
    inputs[1].path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="different extractor; regenerate"):
        api.analyze_contract_versions(
            inputs, base_label="3.9.0", target_labels=[], scope="cli"
        )


def _write_exact_snapshot(api: Any, root: Path, snapshot: Any) -> Any:
    version = snapshot.ds_version
    tree = "a" * 40
    provenance = {
        "schema_version": 1,
        "exact": True,
        "input_kind": "ds-source",
        "content_digest": f"git-tree:{tree}",
        "contract_digest": api.contract_snapshot_digest(snapshot),
        "extractor_fingerprint": api.contract_extractor_fingerprint(),
        "origin": {
            "kind": "ds-source",
            "content_digest": f"git-tree:{tree}",
            "git": {
                "tag": version,
                "ref": f"refs/tags/{version}",
                "commit": "b" * 40,
                "tree": tree,
                "dirty": False,
            },
        },
    }
    path = root / f"{version}.json"
    api.write_contract_snapshot(snapshot, path, provenance=provenance)
    return api.ContractInput(version, path, "snapshot")


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
        inferred_return_type="example.WorkflowDefinition",
        logical_return_type="example.WorkflowDefinition",
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


def _model(ir: Any, import_path: str, *, java_type: str) -> Any:
    return ir.ModelSpec(
        name="Thing",
        import_path=import_path,
        kind="other_class",
        documentation=None,
        extends=None,
        fields=[_field(ir, java_type=java_type)],
    )
