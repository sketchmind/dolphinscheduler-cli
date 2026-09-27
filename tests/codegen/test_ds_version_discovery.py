from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from ds_codegen.ir import ContractSnapshot
from ds_codegen.version_discovery import (
    compile_discovery_profile,
    write_version_discovery,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    from tests.codegen.exact_contract_corpus import ExactContractCorpus


INTERMEDIATE_VERSIONS = (
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
)


@pytest.mark.source_contract
def test_discovery_memberships_come_from_all_exact_sources(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    profiles = [
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    ]
    assert len(profiles) == 37
    assert [profile.version for profile in profiles if profile.product_path] == [
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ]
    assert [profile.version for profile in profiles if profile.openapi] == [
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ]
    assert {
        p.version: p.route_miss_code for p in profiles if p.route_miss_code is not None
    } == {
        "2.0.0": 110003,
        **dict.fromkeys(INTERMEDIATE_VERSIONS, 110003),
        "2.0.9": 110003,
        "3.0.0": 110003,
        "3.0.6": 110003,
        "3.1.0": 110003,
        "3.1.9": 110003,
        "3.2.0": 110003,
        "3.2.1": 110003,
        "3.2.2": 110003,
    }
    assert {
        p.version: p.database_product_version
        for p in profiles
        if p.database_product_version not in {None, p.version}
    } == {"2.0.8": "2.0.7", "3.0.3": "3.0.2", "3.2.2": "3.3.0"}
    for profile in profiles:
        if profile.product_path:
            assert profile.product_path == "ui-plugins/query-product-info"
            assert profile.product_version_field == "version"
            assert profile.evidence


@pytest.mark.source_contract
@pytest.mark.parametrize("change", ["method", "return", "field"])
def test_product_probe_rejects_changed_source_contract(
    exact_contract_corpus: ExactContractCorpus, change: str
) -> None:
    snapshot = deepcopy(exact_contract_corpus.snapshot("3.4.2"))
    if change == "field":
        model = next(
            model for model in snapshot.models if model.name == "ProductInfoDto"
        )
        model.fields[0] = replace(model.fields[0], java_type="Integer")
    else:
        index = next(
            i
            for i, operation in enumerate(snapshot.operations)
            if operation.operation_id == "UiPluginController.queryProductInfo"
        )
        operation = snapshot.operations[index]
        snapshot.operations[index] = (
            replace(operation, http_method="POST")
            if change == "method"
            else replace(operation, return_type="String")
        )
    with pytest.raises(ValueError, match="product-info"):
        compile_discovery_profile(snapshot, exact_contract_corpus.source_root("3.4.2"))


@pytest.mark.source_contract
def test_openapi_api_label_is_not_a_product_version_fact(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    path = (
        tmp_path
        / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api"
        / "configuration/SwaggerConfiguration.java"
    )
    path.parent.mkdir(parents=True)
    path.write_text('new OpenAPI().info(new Info().version("V1"));')
    with pytest.raises(ValueError, match="source seam changed"):
        compile_discovery_profile(exact_contract_corpus.snapshot("3.2.0"), tmp_path)


@pytest.mark.source_contract
def test_intermediate_sources_remain_explicit_version_only_after_admission(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    profiles = tuple(
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    )
    intermediate = tuple(
        profile for profile in profiles if profile.version in INTERMEDIATE_VERSIONS
    )
    assert len(intermediate) == 21
    for profile in intermediate:
        assert profile.product_path is None
        assert profile.product_version_field is None
        assert not profile.openapi
        assert profile.route_miss_code == 110003
        assert profile.evidence

    write_version_discovery(tmp_path, profiles)
    generated = _load_generated_discovery(tmp_path)
    assert set(INTERMEDIATE_VERSIONS) <= generated["SOURCE_EVIDENCE"].keys()
    for probe in generated["PROBES"]:
        assert set(probe.exact_versions).isdisjoint(INTERMEDIATE_VERSIONS)

    # Matching database version text without a trusted endpoint is insufficient.
    write_version_discovery(tmp_path, intermediate)
    isolated = _load_generated_discovery(tmp_path)
    assert isolated["PROBES"] == ()
    assert set(isolated["SOURCE_EVIDENCE"]) == set(INTERMEDIATE_VERSIONS)


@pytest.mark.source_contract
def test_public_api_documents_retain_all_exact_memberships(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    profiles = tuple(
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    )
    for profile in profiles:
        assert profile.documents
        assert profile.public_operations
        if profile.version.startswith("3.1."):
            assert [(doc.path, doc.api_group) for doc in profile.documents] == [
                ("v3/api-docs?group=v1(current)", "v1"),
                ("v3/api-docs?group=v2", "v2"),
            ]
        elif profile.openapi:
            assert [doc.path for doc in profile.documents] == ["v3/api-docs"]
        else:
            assert [doc.path for doc in profile.documents] == ["v2/api-docs"]
    legacy = profiles[0]
    assert legacy.version == "1.3.9"
    assert len(legacy.public_operations) == 133
    operations = {
        operation.operation_id: operation for operation in legacy.public_operations
    }
    assert "AccessTokenController.createToken" not in operations
    assert "AccessTokenController.queryAccessTokenList" in operations
    write_version_discovery(tmp_path, profiles)
    generated = _load_generated_discovery(tmp_path)
    assert tuple(generated["CONTRACT_PROFILES"]) == exact_contract_corpus.versions
    assert len(generated["DISCOVERY_CONTRACT_DIGEST"]) == 64
    assert generated["DOCUMENT_PROBES"][0].source == "swagger2"
    assert generated["DOCUMENT_PROBES"][0].exact_versions[0] == "1.3.9"
    assert len(generated["OPERATION_CONTRACTS"]) < sum(
        len(profile.public_operations) for profile in profiles
    )
    before = generated["DISCOVERY_CONTRACT_DIGEST"]
    write_version_discovery(tmp_path, profiles[:-1])
    changed = _load_generated_discovery(tmp_path)
    assert changed["DISCOVERY_CONTRACT_DIGEST"] != before


@pytest.mark.source_contract
def test_springfox_documentation_quirks_do_not_change_wire_contracts(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    profile = compile_discovery_profile(
        exact_contract_corpus.snapshot("1.3.9"),
        exact_contract_corpus.source_root("1.3.9"),
    )
    operations = {
        operation.operation_id: operation for operation in profile.public_operations
    }
    paging = operations["ProjectController.queryProjectListPaging"]
    assert paging.ignored_document_parameters == ("projectId",)
    assert {parameter.name for parameter in paging.parameters} == {
        "pageNo",
        "pageSize",
        "searchVal",
    }
    user_list = operations["UsersController.listAll"]
    assert not user_list.parameters
    assert {"tenantName", "userName", "userPassword", "userType"} <= set(
        user_list.ignored_document_parameters
    )
    email = next(
        parameter
        for parameter in operations["UsersController.createUser"].parameters
        if parameter.name == "email"
    )
    assert email.schema_type == "string"
    assert email.document_schema_types == ("integer", "string")
    assert email.required
    assert email.document_required == (False, True)
    enum = next(
        parameter
        for parameter in operations["AlertGroupController.createAlertgroup"].parameters
        if parameter.name == "groupType"
    )
    assert enum.schema_type == "string"
    assert enum.enum_values == ("EMAIL", "SMS")


def _load_generated_discovery(output_root: Path) -> dict[str, Any]:
    # Load scratch output as its own package so its relative data-module imports
    # cannot silently resolve the installed generated contracts.
    package_name = f"discovery_test_{uuid4().hex}"
    package = ModuleType(package_name)
    package.__path__ = [str(output_root / "generated")]
    sys.modules[package_name] = package
    name = f"{package_name}.version_discovery"
    spec = importlib.util.spec_from_file_location(
        name, output_root / "generated/version_discovery.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return vars(module)
    finally:
        for key in tuple(sys.modules):
            if key == package_name or key.startswith(package_name + "."):
                del sys.modules[key]


def test_discovery_rejects_missing_document_configuration(tmp_path: Path) -> None:
    snapshot = ContractSnapshot(
        ds_version="3.4.1",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )
    with pytest.raises(ValueError, match="API documentation configuration missing"):
        compile_discovery_profile(snapshot, tmp_path)
