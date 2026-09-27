from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType

    from tests.codegen.exact_contract_corpus import ExactContractCorpus


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_type_lookup_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("ds_codegen.extract.type_lookup")


@pytest.mark.source_contract
def test_resolve_service_impl_import_path_maps_service_interface(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    type_lookup = _load_type_lookup_module()
    with exact_contract_corpus.codegen_repo_root("3.4.1") as repo_root:
        resolved = type_lookup._resolve_service_impl_import_path(
            repo_root,
            "org.apache.dolphinscheduler.api.service.ExecutorService",
        )

    assert (
        resolved == "org.apache.dolphinscheduler.api.service.impl.ExecutorServiceImpl"
    )


@pytest.mark.source_contract
def test_infer_imported_type_method_return_type_reads_accessor_fields(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    type_lookup = _load_type_lookup_module()
    with exact_contract_corpus.codegen_repo_root("3.4.1") as repo_root:
        inferred = type_lookup._infer_imported_type_method_return_type(
            repo_root=repo_root,
            type_name="EnvironmentDto",
            method_name="getCode",
            method_arity=0,
            argument_types=[],
            import_map={
                "EnvironmentDto": ("org.apache.dolphinscheduler.api.dto.EnvironmentDto")
            },
            package_name=None,
        )

    assert inferred == "Long"


@pytest.mark.source_contract
def test_infer_instance_method_return_type_applies_receiver_generics(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    type_lookup = _load_type_lookup_module()
    with exact_contract_corpus.codegen_repo_root("3.4.1") as repo_root:
        inferred = type_lookup._infer_instance_method_return_type(
            repo_root=repo_root,
            instance_java_type="Result<Project>",
            method_name="getData",
            method_arity=0,
            argument_types=[],
            import_map={"Result": "org.apache.dolphinscheduler.api.utils.Result"},
            package_name=None,
        )

    assert inferred == "Project"
