from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_adapter_wire_report_normalizes_only_mechanical_candidates(
    tmp_path: Path,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    matrix = _module("ds_codegen.compatibility_impact")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    inputs = []
    for version in matrix.REVIEWED_DS_VERSIONS:
        if version == "2.0.9":
            operations = []
        else:
            operations = [
                _operation(
                    ir,
                    path="projects/list" if version == "1.3.9" else "projects",
                    logical_return_type="Long" if version == "2.0.0" else "Integer",
                    normalized_variant=version != "3.4.1",
                )
            ]
        snapshot = _snapshot(ir, version, operations)
        snapshot_path = tmp_path / f"ds-{version}-contract.json"
        api.write_contract_snapshot(
            snapshot,
            snapshot_path,
            provenance=_exact_provenance(api, snapshot, version),
        )
        inputs.append(api.ContractInput(version, snapshot_path, "snapshot"))

    report = analyzer.analyze_adapter_wire_candidates(
        adapter_source=adapter,
        generated_client_source=client,
        generated_operations_root=operations_root,
        inputs=inputs,
    )

    assert report["complete"] is True
    assert report["kind"] == "dolphinscheduler-adapter-wire-family-candidates"
    assert report["claim"] == "wire-family-candidates-only"
    assert report["automatic_support"] is False
    assert report["comparison_unit"] == "unique-generated-client-operation"
    assert report["baseline"] == {
        "version": "3.4.1",
        "adapter_call_count": 1,
        "generated_operation_count": 1,
    }
    operation = report["operations"][0]
    assert operation["client_operation"] == "project.query_project_list_paging"
    assert operation["source_operation"] == ("ProjectController.queryProjectListPaging")
    assert operation["call_count"] == 1
    by_version = {item["version"]: item for item in operation["versions"]}
    assert by_version["3.0.0"] == {
        "version": "3.0.0",
        "same_source_id": True,
        "same_route": True,
        "same_effective_request_candidate": True,
        "same_effective_response_candidate": True,
        "same_effective_request_and_response_candidate": True,
    }
    assert by_version["1.3.9"]["same_source_id"] is True
    assert by_version["1.3.9"]["same_route"] is False
    assert by_version["1.3.9"]["same_effective_request_candidate"] is False
    assert by_version["1.3.9"]["same_effective_response_candidate"] is True
    assert by_version["2.0.0"]["same_effective_request_candidate"] is True
    assert by_version["2.0.0"]["same_effective_response_candidate"] is False
    assert by_version["2.0.9"] == {
        "version": "2.0.9",
        "same_source_id": False,
        "same_route": False,
        "same_effective_request_candidate": False,
        "same_effective_response_candidate": False,
        "same_effective_request_and_response_candidate": False,
    }
    summary = {item["version"]: item for item in report["summary"]}
    assert summary["3.4.1"]["same_source_id"] == 1
    assert summary["1.3.9"]["same_effective_request_candidate"] == 0
    assert summary["2.0.0"]["same_effective_response_candidate"] == 0
    assert summary["2.0.9"]["same_source_id"] == 0
    rendered = analyzer.render_adapter_wire_candidate_report(report)
    assert rendered.endswith("\n")
    reversed_report = analyzer.analyze_adapter_wire_candidates(
        adapter_source=adapter,
        generated_client_source=client,
        generated_operations_root=operations_root,
        inputs=list(reversed(inputs)),
    )
    assert analyzer.render_adapter_wire_candidate_report(reversed_report) == rendered


def test_adapter_wire_report_detects_response_identity_edge_drift(
    tmp_path: Path,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    matrix = _module("ds_codegen.compatibility_impact")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    inputs = []
    models = [
        _response_model(ir, "example.first.Thing"),
        _response_model(ir, "example.second.Thing"),
    ]
    for version in matrix.REVIEWED_DS_VERSIONS:
        target = "example.second.Thing" if version == "3.4.2" else "example.first.Thing"
        logical_type = f"Map<{target}, Map<example.first.Thing, example.second.Thing>>"
        operation = _operation(
            ir,
            path="projects",
            logical_return_type=logical_type,
            normalized_variant=True,
        )
        snapshot = _snapshot(
            ir,
            version,
            [operation],
            models=models,
        )
        snapshot_path = tmp_path / f"identity-{version}.json"
        api.write_contract_snapshot(
            snapshot,
            snapshot_path,
            provenance=_exact_provenance(api, snapshot, version),
        )
        inputs.append(api.ContractInput(version, snapshot_path, "snapshot"))

    report = analyzer.analyze_adapter_wire_candidates(
        adapter_source=adapter,
        generated_client_source=client,
        generated_operations_root=operations_root,
        inputs=inputs,
    )

    assert report["schema_version"] == 2
    row = next(
        item
        for item in report["operations"][0]["versions"]
        if item["version"] == "3.4.2"
    )
    assert row["same_effective_response_candidate"] is False
    assert row["same_effective_request_and_response_candidate"] is False


def test_adapter_wire_analysis_fails_closed_for_unknown_generated_call(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    adapter.write_text(
        """
class BoundProjectOperations:
    def list(self):
        return self.client.project.unknown_operation()
""",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"ProjectOperations\.unknown_operation does not exist",
    ):
        analyzer.discover_adapter_generated_operations(
            adapter_source=adapter,
            generated_client_source=client,
            generated_operations_root=operations_root,
        )


def test_adapter_call_discovery_follows_typed_clients_and_local_group_aliases(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    adapter.write_text(
        """
def helper(client: DS341Client):
    return client.project.query_project_list_paging()

class BoundProjectOperations:
    def list(self):
        api = self.client.project
        return api.query_project_list_paging()
""",
        encoding="utf-8",
    )

    operations = analyzer.discover_adapter_generated_operations(
        adapter_source=adapter,
        generated_client_source=client,
        generated_operations_root=operations_root,
    )

    assert len(operations) == 1
    assert operations[0].client_operation == "project.query_project_list_paging"
    assert operations[0].call_count == 2


def test_effective_request_candidate_covers_nested_body_type_changes(
    tmp_path: Path,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    matrix = _module("ds_codegen.compatibility_impact")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    (operations_root / "project.py").write_text(
        '''
class ProjectOperations:
    def query_project_list_paging(self):
        """
        DS operation: ProjectController.queryProjectListPaging | POST /projects
        """
''',
        encoding="utf-8",
    )
    inputs = []
    for version in matrix.REVIEWED_DS_VERSIONS:
        operation = _body_operation(ir)
        dto = _request_dto(
            ir,
            field_type="Long" if version == "3.4.2" else "Integer",
        )
        snapshot = _snapshot(ir, version, [operation], dtos=[dto])
        snapshot_path = tmp_path / f"body-{version}.json"
        api.write_contract_snapshot(
            snapshot,
            snapshot_path,
            provenance=_exact_provenance(api, snapshot, version),
        )
        inputs.append(api.ContractInput(version, snapshot_path, "snapshot"))

    report = analyzer.analyze_adapter_wire_candidates(
        adapter_source=adapter,
        generated_client_source=client,
        generated_operations_root=operations_root,
        inputs=inputs,
    )

    target = next(
        item
        for item in report["operations"][0]["versions"]
        if item["version"] == "3.4.2"
    )
    assert target["version"] == "3.4.2"
    assert target["same_source_id"] is True
    assert target["same_route"] is True
    assert target["same_effective_request_candidate"] is False


def test_adapter_wire_analysis_fails_closed_when_baseline_operation_is_missing(
    tmp_path: Path,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    matrix = _module("ds_codegen.compatibility_impact")
    adapter, client, operations_root = _generated_fixture(tmp_path)
    inputs = []
    for version in matrix.REVIEWED_DS_VERSIONS:
        snapshot = _snapshot(ir, version, [])
        snapshot_path = tmp_path / f"empty-{version}.json"
        api.write_contract_snapshot(
            snapshot,
            snapshot_path,
            provenance=_exact_provenance(api, snapshot, version),
        )
        inputs.append(api.ContractInput(version, snapshot_path, "snapshot"))

    with pytest.raises(
        ValueError,
        match=r"baseline contains 0 matching operations",
    ):
        analyzer.analyze_adapter_wire_candidates(
            adapter_source=adapter,
            generated_client_source=client,
            generated_operations_root=operations_root,
            inputs=inputs,
        )


def test_operation_anchor_accepts_exact_overload_source_id() -> None:
    ir = _module("ds_codegen.ir")
    analyzer = _module("ds_codegen.adapter_wire_candidates")
    operation = replace(
        _operation(
            ir,
            path="projects",
            logical_return_type="Integer",
            normalized_variant=True,
        ),
        operation_id=("ProjectController.queryProjectListPaging__get_projects"),
    )
    other_overload = replace(
        operation,
        operation_id=("ProjectController.queryProjectListPaging__get_projects_alt"),
    )
    snapshot = _snapshot(ir, "3.4.1", [operation, other_overload])

    resolved = analyzer.resolve_operation_anchor(
        snapshot,
        source_operation=operation.operation_id,
        http_method="GET",
        path="projects",
    )

    assert resolved is operation


def _generated_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    adapter = tmp_path / "adapter.py"
    adapter.write_text(
        """
class BoundProjectOperations:
    def list(self):
        return self.client.project.query_project_list_paging()
""",
        encoding="utf-8",
    )
    package = tmp_path / "generated"
    operations_root = package / "api" / "operations"
    operations_root.mkdir(parents=True)
    client = package / "client.py"
    client.write_text(
        """
from .api.operations.project import ProjectOperations

class DS341Client:
    def __init__(self):
        self.project = ProjectOperations()
""",
        encoding="utf-8",
    )
    (operations_root / "project.py").write_text(
        '''
class ProjectOperations:
    def query_project_list_paging(self):
        """
        DS operation: ProjectController.queryProjectListPaging | GET /projects
        """
''',
        encoding="utf-8",
    )
    return adapter, client, operations_root


def _operation(
    api: Any,
    *,
    path: str,
    logical_return_type: str,
    normalized_variant: bool,
) -> Any:
    request_attribute = api.ParameterSpec(
        name="loginUser",
        java_type="User",
        binding="request_attribute",
        wire_name="Constants.SESSION_USER",
        required=None,
        default_value=None,
        hidden=normalized_variant,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=None,
    )
    page_number = api.ParameterSpec(
        name="pageNo",
        java_type="Integer" if normalized_variant else "int",
        binding="request_param",
        wire_name="pageNo",
        required=True if normalized_variant else None,
        default_value=None,
        hidden=False,
        description=None,
        example=None,
        allowable_values=None,
        schema_type="int",
    )
    search = api.ParameterSpec(
        name="searchVal",
        java_type="String",
        binding="request_param",
        wire_name="searchVal",
        required=False,
        default_value=None,
        hidden=False,
        description=None,
        example=None,
        allowable_values=None,
        schema_type="String",
    )
    parameters = (
        [search, page_number, request_attribute]
        if normalized_variant
        else [request_attribute, page_number, search]
    )
    return api.OperationSpec(
        operation_id="ProjectController.queryProjectListPaging",
        controller="ProjectController",
        method_name="queryProjectListPaging",
        api_group="v1",
        http_method="GET",
        path=path,
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Result",
        inferred_return_type=logical_return_type,
        logical_return_type=logical_return_type,
        response_projection="direct",
        parameters=parameters,
    )


def _body_operation(ir: Any) -> Any:
    return ir.OperationSpec(
        operation_id="ProjectController.queryProjectListPaging",
        controller="ProjectController",
        method_name="queryProjectListPaging",
        api_group="v1",
        http_method="POST",
        path="projects",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=["application/json"],
        return_type="Result<Void>",
        inferred_return_type=None,
        logical_return_type="Void",
        response_projection="direct",
        parameters=[
            ir.ParameterSpec(
                name="request",
                java_type="WidgetRequest",
                binding="request_body",
                wire_name="request",
                required=True,
                default_value=None,
                hidden=False,
                description=None,
                example=None,
                allowable_values=None,
                schema_type=None,
            )
        ],
    )


def _request_dto(ir: Any, *, field_type: str) -> Any:
    return ir.DtoSpec(
        name="WidgetRequest",
        import_path="example.WidgetRequest",
        documentation=None,
        extends=None,
        fields=[
            ir.DtoFieldSpec(
                name="amount",
                java_type=field_type,
                wire_name="amount",
                required=True,
                default_value=None,
                nullable=False,
                default_factory=None,
                description=None,
                example=None,
                allowable_values=None,
                documentation=None,
            )
        ],
    )


def _response_model(ir: Any, import_path: str) -> Any:
    return ir.ModelSpec(
        name="Thing",
        import_path=import_path,
        kind="class",
        documentation=None,
        extends=None,
        fields=[],
    )


def _snapshot(
    ir: Any,
    version: str,
    operations: list[Any],
    *,
    dtos: list[Any] | None = None,
    models: list[Any] | None = None,
) -> Any:
    dto_items = dtos or []
    model_items = models or []
    return ir.ContractSnapshot(
        ds_version=version,
        operation_count=len(operations),
        enum_count=0,
        dto_count=len(dto_items),
        model_count=len(model_items),
        operations=operations,
        enums=[],
        dtos=dto_items,
        models=model_items,
    )


def _exact_provenance(api: Any, snapshot: Any, version: str) -> dict[str, object]:
    tree = f"{int(version.replace('.', '')):040x}"
    content_digest = f"git-tree:{tree}"
    return {
        "schema_version": 1,
        "exact": True,
        "input_kind": "ds-source",
        "content_digest": content_digest,
        "contract_digest": api.contract_snapshot_digest(snapshot),
        "origin": {
            "kind": "ds-source",
            "content_digest": content_digest,
            "git": {
                "commit": "a" * 40,
                "tree": tree,
                "ref": f"refs/tags/{version}",
                "tag": version,
                "dirty": False,
            },
        },
    }
