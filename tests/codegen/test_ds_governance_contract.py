from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.ir import ContractSnapshot

REPO_ROOT = Path(__file__).resolve().parents[2]


def _governance_contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.governance_contract")


def _operation_ids(snapshot: ContractSnapshot) -> set[str]:
    return {operation.operation_id for operation in snapshot.operations}


def _structured_type_keys(snapshot: ContractSnapshot) -> set[str]:
    return (
        {model.import_path for model in snapshot.models}
        | {dto.import_path for dto in snapshot.dtos}
        | {enum.import_path for enum in snapshot.enums}
    )


def test_governance_contract_covers_every_exact_target_without_inference() -> None:
    contract = _governance_contract()

    assert tuple(contract.GOVERNANCE_CONTRACTS) == contract.TARGET_GOVERNANCE_VERSIONS
    for version in contract.TARGET_GOVERNANCE_VERSIONS:
        assert contract.governance_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed governance-domain contract"):
        contract.governance_contract("3.4.4")


@pytest.mark.source_contract
def test_every_declared_governance_source_and_model_exists_in_snapshot_matrix(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _governance_contract()

    for version in contract.TARGET_GOVERNANCE_VERSIONS:
        snapshot = exact_contract_corpus.snapshot(version)
        operation_ids = _operation_ids(snapshot)
        type_keys = _structured_type_keys(snapshot)
        sources = contract.semantic_operation_sources(version)
        roots = contract.semantic_operation_type_roots(version)

        assert roots.keys() == sources.keys()
        for action, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, action)
            assert set(roots[action]) <= type_keys, (version, action)


def test_namespace_absence_is_terminal_only_before_its_upstream_introduction() -> None:
    contract = _governance_contract()

    for version in (
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
    ):
        sources = contract.semantic_operation_sources(version)
        support = contract.action_support(version)
        absences = contract.terminal_absences(version)

        assert all(not action.startswith("namespace.") for action in sources)
        assert set(absences) == set(contract.NAMESPACE_SEMANTIC_OPERATIONS)
        assert all(
            item.reason == "upstream_capability_absent" for item in absences.values()
        )
        assert all(item.introduced_in == "3.0.0" for item in absences.values())
        assert all(support[action] == "absent" for action in absences)

    for version in contract.TARGET_GOVERNANCE_VERSIONS[
        contract.TARGET_GOVERNANCE_VERSIONS.index("3.0.0") :
    ]:
        assert contract.terminal_absences(version) == {}
        sources = contract.semantic_operation_sources(version)
        assert sources["namespace.get"] == sources["namespace.page"]
        assert all(
            contract.action_support(version)[action] == "supported"
            for action in contract.NAMESPACE_SEMANTIC_OPERATIONS
        )


def test_datasource_wire_and_result_boundaries_are_exact_decisions() -> None:
    contract = _governance_contract()

    assert (
        contract.governance_contract("1.3.9").datasource.payload_wire == "legacy-form"
    )
    assert contract.governance_contract("2.0.9").datasource.payload_wire == "typed-body"
    assert (
        contract.governance_contract("3.1.0").datasource.payload_wire == "json-string"
    )
    assert contract.governance_contract("3.0.6").datasource.delete_operation.endswith(
        ".delete"
    )
    assert contract.governance_contract("3.1.0").datasource.delete_operation.endswith(
        ".deleteDataSource"
    )

    three_two_zero = contract.governance_contract("3.2.0").datasource
    three_two_one = contract.governance_contract("3.2.1").datasource
    assert (three_two_zero.create_result, three_two_zero.delete_result) == (
        "none",
        "none",
    )
    assert (three_two_one.create_result, three_two_one.delete_result) == (
        "entity",
        "boolean",
    )
    assert three_two_zero.password_visibility == "clear"
    assert three_two_one.password_visibility == "masked"
    assert contract.governance_contract("1.3.9").datasource.password_update == (
        "explicit-value"
    )
    assert contract.governance_contract("2.0.0").datasource.password_update == (
        "blank-preserves"
    )
    assert (
        contract.governance_contract("3.4.1").datasource.update_body_required is False
    )
    assert contract.governance_contract("3.4.2").datasource.update_body_required is True


@pytest.mark.source_contract
def test_ds_200_typed_datasource_body_retains_its_jackson_discriminator(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    snapshot = exact_contract_corpus.snapshot("2.0.0")
    base_dto = next(
        model
        for model in snapshot.models
        if model.import_path
        == "org.apache.dolphinscheduler.common.datasource.BaseDataSourceParamDTO"
    )

    assert "type" in {field.name for field in base_dto.fields}


def test_namespace_selector_quota_result_and_delete_boundaries_are_exact() -> None:
    contract = _governance_contract()

    three_zero = contract.governance_contract("3.0.0").namespace
    three_one = contract.governance_contract("3.1.0").namespace
    three_two_zero = contract.governance_contract("3.2.0").namespace
    three_two_one = contract.governance_contract("3.2.1").namespace
    latest = contract.governance_contract("3.4.2").namespace

    assert three_zero.page_operation == "K8sNamespaceController.queryProjectListPaging"
    assert three_zero.selector == "k8s"
    assert three_one.selector == "cluster-code"
    assert three_one.deletes_kubernetes_namespace is True
    assert three_two_zero.deletes_kubernetes_namespace is False
    assert three_two_zero.quotas_supported is True
    assert three_two_one.quotas_supported is False
    assert three_two_one.create_result == "none"
    assert contract.governance_contract("3.2.2").namespace.create_result == "entity"
    assert latest.create_result == "entity"
    for version in contract.TARGET_GOVERNANCE_VERSIONS[
        contract.TARGET_GOVERNANCE_VERSIONS.index("3.0.0") :
    ]:
        assert (
            contract.governance_contract(
                version
            ).namespace.creates_kubernetes_namespace_if_absent
            is True
        )


def test_supported_actions_have_exact_controller_and_ui_evidence() -> None:
    contract = _governance_contract()

    for version in contract.TARGET_GOVERNANCE_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        evidence = contract.semantic_operation_evidence(version)
        assert evidence.keys() == sources.keys()
        for action, action_evidence in evidence.items():
            assert {item.kind for item in action_evidence} == {"controller", "ui"}
            assert all(item.version == version for item in action_evidence)
            controller = next(
                item for item in action_evidence if item.kind == "controller"
            )
            assert set(controller.symbol.split(";")) == {
                operation.partition(".")[2] for operation in sources[action]
            }


def test_action_facets_exist_exactly_for_supported_sources() -> None:
    contract = _governance_contract()

    for version in contract.TARGET_GOVERNANCE_VERSIONS:
        assert contract.semantic_operation_facets(version).keys() == (
            contract.semantic_operation_sources(version).keys()
        )


def test_mutation_and_get_recipes_include_their_stable_resolver_dependencies() -> None:
    contract = _governance_contract()

    for version in contract.TARGET_GOVERNANCE_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        assert sources["datasource.get"][:1] == (
            "DataSourceController.queryDataSourceListPaging",
        )
        for action in (
            "datasource.create",
            "datasource.update",
            "datasource.delete",
            "datasource.saved-test",
        ):
            assert sources[action][:2] == (
                "DataSourceController.queryDataSourceListPaging",
                "DataSourceController.queryDataSource",
            )

        if version not in {
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
        }:
            assert sources["namespace.get"] == sources["namespace.page"]
            assert sources["namespace.delete"][:1] == sources["namespace.page"]
