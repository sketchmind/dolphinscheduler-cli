from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from dsctl.cli_surface import stable_leaf_actions

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resource_contract() -> Any:
    tools_dir = _REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.resource_contract")


def _version_profiles() -> Any:
    _resource_contract()
    return importlib.import_module("ds_codegen.version_profiles")


@pytest.mark.parametrize(
    "ds_version",
    [
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    ],
)
@pytest.mark.source_contract
def test_resource_contract_closure_exists_in_exact_snapshot(
    ds_version: str,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _resource_contract()
    snapshot = exact_contract_corpus.snapshot(ds_version)
    operation_ids = {
        item.operation_id
        for item in snapshot.operations
        if item.operation_id is not None
    }
    model_keys = {item.import_path for item in snapshot.models}

    sources = contract.semantic_operation_sources(ds_version)
    roots = contract.semantic_operation_type_roots(ds_version)
    assert set(sources) == set(contract.RESOURCE_SEMANTIC_OPERATIONS)
    assert set(roots) == set(contract.RESOURCE_SEMANTIC_OPERATIONS)
    assert all(set(operations) <= operation_ids for operations in sources.values())
    assert all(set(type_roots) <= model_keys for type_roots in roots.values())


def test_resource_wire_epochs_are_explicit_at_reviewed_boundaries() -> None:
    contract = _resource_contract()
    legacy = contract.resource_contract("1.3.9").resource
    rest_id = contract.resource_contract("3.1.9").resource
    storage = contract.resource_contract("3.2.0").resource
    storage_322 = contract.resource_contract("3.2.2").resource
    modern = contract.resource_contract("3.3.1").resource

    assert legacy.upload_route == "legacy-create"
    assert rest_id.identity_wire == "id"
    assert storage.identity_wire == "full-name"
    assert storage.tenant_code_on_reads is True
    assert storage_322.create_operation.endswith(".createResourceFile")
    assert modern.parent_directory_wire == "absolute-path"
    assert modern.view_wire == "binary-window"


@pytest.mark.parametrize(
    "ds_version",
    ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"],
)
def test_modern_resource_view_uses_binary_download_evidence(ds_version: str) -> None:
    contract = _resource_contract()
    assert contract.semantic_operation_sources(ds_version)["resource.view"][-1] == (
        "ResourcesController.downloadResource"
    )
    evidence = contract.semantic_operation_evidence(ds_version)["resource.view"]
    assert any("misroutes limit" in item.conclusion for item in evidence)


@pytest.mark.parametrize(
    "ds_version",
    [
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    ],
)
@pytest.mark.source_contract
def test_resource_view_projection_keeps_alias_and_content(
    ds_version: str,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    snapshot = exact_contract_corpus.snapshot(ds_version)
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ResourcesController.viewResource"
    )
    if ds_version in {"2.0.1", "2.0.2", "2.0.3", "2.0.4", "2.0.5"}:
        assert operation.return_type == "Result"
        assert operation.inferred_return_type == "Map<String, String>"
        assert operation.logical_return_type == "Map<String, String>"
        assert not any(
            item.import_path == "generated.view.ResourcesServiceImpl_readResource_map"
            for item in snapshot.models
        )
        return

    model = next(
        item
        for item in snapshot.models
        if item.import_path == "generated.view.ResourcesServiceImpl_readResource_map"
    )
    assert {field.wire_name for field in model.fields} == {
        "alias",
        "content",
    }


def test_unknown_resource_contract_fails_closed() -> None:
    contract = _resource_contract()
    with pytest.raises(ValueError, match="no reviewed resource-domain contract"):
        contract.resource_contract("9.9.9")


def test_resource_ledger_promotes_every_exact_profile() -> None:
    contract = _resource_contract()
    profiles = _version_profiles().compile_version_profile_data(
        stable_actions=stable_leaf_actions()
    )["profiles"]
    stable_action_by_operation = {
        "resource.create": "resource.create",
        "resource.delete": "resource.delete",
        "resource.download": "resource.download",
        "resource.mkdir": "resource.mkdir",
        "resource.page": "resource.list",
        "resource.upload": "resource.upload",
        "resource.view": "resource.view",
    }

    for ds_version, profile in profiles.items():
        for semantic_operation, stable_action in stable_action_by_operation.items():
            assert profile["actions"][stable_action] == {
                "availability": "supported",
                "execution_mode": "generated_adapter",
                "verification": "contract_tested",
            }
            decision = profile["build_decisions"][semantic_operation]
            assert decision["build_status"] == "accepted"
            assert decision["source_operations"] == list(
                contract.semantic_operation_sources(ds_version)[semantic_operation]
            )


def test_fixed_native_resource_view_has_an_independent_exact_epoch() -> None:
    contract = _resource_contract()
    fixed = contract.resource_contract("3.4.3").resource
    assert fixed.view_wire == "generated-path"
    assert fixed.view_operation == "ResourcesController.viewResource"
    assert fixed.view_evidence_operation is None
    assert fixed.download_operation == "ResourcesController.downloadResource"
    assert contract.semantic_operation_sources("3.4.3")["resource.view"] == (
        "ResourcesController.viewResource",
    )
    for version in ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"]:
        assert contract.resource_contract(version).resource.view_wire == "binary-window"
