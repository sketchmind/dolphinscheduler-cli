"""Full source audits retain standalone exact wrappers; runtime slices share support."""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


@pytest.mark.source_rebuild
def test_full_exact_source_audit_keeps_typed_direct_wrappers(
    tmp_path: Path,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    package = _module("ds_codegen.render.package")
    snapshot = exact_contract_corpus.snapshot("3.4.1")
    package.write_generated_package(snapshot, tmp_path)
    package_root = tmp_path / "generated" / "versions" / "ds_3_4_1"
    source = (package_root / "api" / "operations" / "project.py").read_text()
    tree = ast.parse(source)
    controller = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ProjectOperations"
    )
    methods = {
        node.name: node for node in controller.body if isinstance(node, ast.FunctionDef)
    }
    assert "class QueryProjectListPagingParams(BaseParamsModel):" in source
    assert "TypeAdapter(PageInfoProject)" in source
    assert "TypeAdapter(Project)" in source
    for name in (
        "create_project",
        "query_project_by_code",
        "query_project_list_paging",
    ):
        assert "self._request(" in ast.unparse(methods[name])
    return_annotation = methods["query_project_by_code"].returns
    assert return_annotation is not None
    assert ast.unparse(return_annotation) == "Project"
    assert "Query project details by code" in (
        ast.get_docstring(methods["query_project_by_code"]) or ""
    )
    assert (
        "wire_runtime"
        not in (package_root / "api" / "operations" / "_base.py").read_text()
    )
    assert "wire_runtime" not in (package_root / "_models.py").read_text()
    assert (package_root / "client.py").is_file()
    assert "DS341Client" in (package_root / "__init__.py").read_text()
    assert "_exchange" not in source


def test_runtime_slice_omits_empty_wrappers_and_preserves_enums(
    tmp_path: Path,
) -> None:
    ir = _module("ds_codegen.ir")
    package = _module("ds_codegen.render.package")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operations=[],
        operation_count=0,
        models=[],
        model_count=0,
        dtos=[],
        dto_count=0,
        enums=[
            ir.EnumSpec(
                name="SampleMode",
                import_path="org.apache.dolphinscheduler.common.enums.SampleMode",
                documentation=None,
                fields=[],
                json_value_field=None,
                values=[
                    ir.EnumValueSpec(name="FAST", arguments=[], documentation=None),
                    ir.EnumValueSpec(name="SAFE", arguments=[], documentation=None),
                ],
            )
        ],
        enum_count=1,
    )
    package.write_generated_package(snapshot, tmp_path, shared_runtime=True)
    package_root = tmp_path / "generated" / "versions" / "ds_9_9_9"
    assert (package_root / "__init__.py").read_text() == ""
    assert not (package_root / "client.py").exists()
    assert not (package_root / "_models.py").exists()
    assert not (package_root / "api" / "operations").exists()
    enum_source = (package_root / "common" / "enums" / "sample_mode.py").read_text()
    assert "class SampleMode(StrEnum):" in enum_source
    assert "FAST = 'FAST'" in enum_source
    assert "SAFE = 'SAFE'" in enum_source
