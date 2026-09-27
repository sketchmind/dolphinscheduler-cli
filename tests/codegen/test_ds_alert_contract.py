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


def _alert_contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.alert_contract")


def _operation_ids(snapshot: ContractSnapshot) -> set[str]:
    return {operation.operation_id for operation in snapshot.operations}


def _structured_type_keys(snapshot: ContractSnapshot) -> set[str]:
    return (
        {model.import_path for model in snapshot.models}
        | {dto.import_path for dto in snapshot.dtos}
        | {enum.import_path for enum in snapshot.enums}
    )


def test_alert_contract_covers_every_exact_target_without_version_inference() -> None:
    contract = _alert_contract()

    assert tuple(contract.ALERT_CONTRACTS) == contract.TARGET_ALERT_VERSIONS
    assert "3.4.3" in contract.TARGET_ALERT_VERSIONS
    for version in contract.TARGET_ALERT_VERSIONS:
        assert contract.alert_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed alert-domain contract"):
        contract.alert_contract("3.4.4")


@pytest.mark.source_contract
def test_every_executable_alert_source_type_and_enum_exists_in_snapshots(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _alert_contract()

    for version in contract.TARGET_ALERT_VERSIONS:
        snapshot = exact_contract_corpus.snapshot(version)
        operation_ids = _operation_ids(snapshot)
        type_keys = _structured_type_keys(snapshot)
        sources = contract.semantic_operation_sources(version)
        type_roots = contract.semantic_operation_type_roots(version)
        enum_roots = contract.semantic_operation_enum_roots(version)

        assert type_roots.keys() == sources.keys()
        assert enum_roots.keys() == sources.keys()
        for action, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, action)
            assert set(type_roots[action]) <= type_keys, (version, action)
            assert set(enum_roots[action]) <= type_keys, (version, action)


def test_alert_terminal_boundaries_are_explicit_and_zero_request_eligible() -> None:
    contract = _alert_contract()

    legacy = contract.terminal_decisions("1.3.9")
    assert set(legacy) == set(contract.ALERT_PLUGIN_SEMANTIC_OPERATIONS)
    assert legacy["alert-plugin.page"].reason == "upstream_capability_absent"

    for version in (
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
    ):
        decisions = contract.terminal_decisions(version)
        assert set(decisions) == {"alert-plugin.test"}
        assert decisions["alert-plugin.test"].reason == "upstream_capability_absent"

    for version in contract.TARGET_ALERT_VERSIONS[
        contract.TARGET_ALERT_VERSIONS.index("3.2.1") :
    ]:
        assert contract.terminal_decisions(version) == {}


def test_alert_wire_recipe_transitions_match_reviewed_upstream_boundaries() -> None:
    contract = _alert_contract()

    one_three = contract.alert_contract("1.3.9")
    two_zero = contract.alert_contract("2.0.0")
    two_zero_nine = contract.alert_contract("2.0.9")
    three_two_zero = contract.alert_contract("3.2.0")
    three_two_one = contract.alert_contract("3.2.1")
    three_three = contract.alert_contract("3.3.1")

    assert one_three.group.association == "legacy-alert-type"
    assert one_three.group.direct_get is False
    assert contract.action_support("1.3.9")["alert-group.create"] == "supported"
    assert contract.action_support("1.3.9")["alert-group.update"] == "supported"
    assert contract.semantic_operation_enum_roots("1.3.9")["alert-group.create"] == (
        "org.apache.dolphinscheduler.common.enums.AlertType",
    )
    assert two_zero.group.page_model.endswith("AlertGroupVo")
    assert two_zero.group.direct_get is True
    assert two_zero.plugin.searchable_page is False
    assert two_zero_nine.plugin.searchable_page is True
    assert (three_two_zero.plugin.update_result, three_two_zero.plugin.test_send) == (
        "none",
        False,
    )
    assert (
        three_two_one.plugin.update_result,
        three_two_one.plugin.test_send,
        three_two_one.plugin.transient_delivery_fields,
    ) == ("entity", True, True)
    assert three_three.plugin.transient_delivery_fields is False


@pytest.mark.source_contract
def test_alert_facets_and_evidence_are_closed_over_executable_actions(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _alert_contract()

    for version in contract.TARGET_ALERT_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        facets = contract.semantic_operation_facets(version)
        evidence = contract.semantic_operation_evidence(version)
        assert facets.keys() == sources.keys()
        assert evidence.keys() == sources.keys()
        for action_evidence in evidence.values():
            assert {item.kind for item in action_evidence} == {"controller", "ui"}
            for item in action_evidence:
                assert item.version == version
                assert exact_contract_corpus.source_file(
                    version,
                    item.source,
                ).is_file()
