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


def _contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.observability_contract")


def _operation_ids(snapshot: ContractSnapshot) -> set[str]:
    return {operation.operation_id for operation in snapshot.operations}


def _model_and_dto_keys(snapshot: ContractSnapshot) -> set[str]:
    return {model.import_path for model in snapshot.models} | {
        dto.import_path for dto in snapshot.dtos
    }


def _enum_keys(snapshot: ContractSnapshot) -> set[str]:
    return {item.import_path for item in snapshot.enums}


def test_contract_covers_every_exact_target_without_version_inference() -> None:
    contract = _contract()

    assert tuple(contract.OBSERVABILITY_CONTRACTS) == (
        contract.TARGET_OBSERVABILITY_VERSIONS
    )
    for version in contract.TARGET_OBSERVABILITY_VERSIONS:
        assert contract.observability_contract(version).version == version

    with pytest.raises(ValueError, match="no reviewed observability-domain contract"):
        contract.observability_contract("3.4.4")


def test_343_audit_visibility_is_source_bound_without_actor_wire_parameter() -> None:
    contract = _contract()
    latest = contract.semantic_operation_facets("3.4.3")["audit.list"]
    previous = contract.semantic_operation_facets("3.4.2")["audit.list"]
    assert latest == {**previous, "visibility": "admin-all-or-self"}
    assert contract.semantic_operation_sources("3.4.3")["audit.list"] == (
        "AuditLogController.queryAuditLogListPaging",
    )
    assert any(
        item.kind == "service"
        and item.source.endswith("AuditServiceImpl.java")
        and "loginUser.id" in item.conclusion
        for item in contract.semantic_operation_evidence("3.4.3")["audit.list"]
    )


@pytest.mark.source_contract
def test_declared_generated_sources_and_types_exist_in_exact_snapshots(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_OBSERVABILITY_VERSIONS:
        snapshot = exact_contract_corpus.snapshot(version)
        operation_ids = _operation_ids(snapshot)
        model_keys = _model_and_dto_keys(snapshot)
        enum_keys = _enum_keys(snapshot)
        sources = contract.semantic_operation_sources(version)
        roots = contract.semantic_operation_type_roots(version)
        enum_roots = contract.semantic_operation_enum_roots(version)

        assert roots.keys() == sources.keys()
        assert enum_roots.keys() == sources.keys()
        for action, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, action)
            missing_types = set(roots[action]) - model_keys
            assert not missing_types, (version, action, missing_types)
            assert set(enum_roots[action]) <= enum_keys, (version, action)


def test_monitor_route_and_payload_boundaries_are_exact() -> None:
    contract = _contract()

    one_three = contract.observability_contract("1.3.9").monitor
    three_two_zero = contract.observability_contract("3.2.0").monitor
    three_two_one = contract.observability_contract("3.2.1").monitor
    three_two_two = contract.observability_contract("3.2.2").monitor

    assert one_three.database_operation == "MonitorController.queryDatabaseState"
    assert one_three.server_wire == "fixed-routes"
    assert three_two_zero.database_model.endswith("MonitorRecord")
    assert three_two_one.database_model.endswith("DatabaseMetrics")
    assert three_two_one.server_operations == (
        "MonitorController.listMaster",
        "MonitorController.listWorker",
    )
    assert three_two_two.server_operations == ("MonitorController.listServer",)
    assert three_two_two.server_wire == "node-type-route"
    assert three_two_two.server_field_wire == "legacy"
    assert contract.observability_contract("3.3.1").monitor.server_field_wire == (
        "canonical"
    )
    assert three_two_two.node_types == ("MASTER", "WORKER", "ALERT_SERVER")
    assert "ALERT_SERVER" not in three_two_one.node_types


def test_audit_introduction_filter_and_discovery_boundaries_are_exact() -> None:
    contract = _contract()

    assert contract.observability_contract("2.0.9").audit.support == "absent"
    three_zero = contract.observability_contract("3.0.0").audit
    three_two_one = contract.observability_contract("3.2.1").audit
    three_two_two = contract.observability_contract("3.2.2").audit

    assert three_zero.filter_wire == "singular-enums"
    assert three_zero.record_wire == "legacy-resource"
    assert three_zero.model_type_values == ("USER_MODULE", "PROJECT_MODULE")
    assert three_zero.operation_type_values == (
        "CREATE",
        "READ",
        "UPDATE",
        "DELETE",
    )
    assert three_zero.metadata_support == "absent"
    assert three_two_one.model_name_filter is False
    assert three_two_two.filter_wire == "csv-text"
    assert three_two_two.record_wire == "canonical-model"
    assert three_two_two.metadata_support == "supported"
    assert three_two_two.model_name_filter is True


def test_terminal_absences_stop_at_their_exact_upstream_introductions() -> None:
    contract = _contract()

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
        absences = contract.terminal_absences(version)
        assert set(absences) == {
            "monitor.health",
            "audit.list",
            "audit.model-types",
            "audit.operation-types",
        }
        assert absences["monitor.health"].introduced_in == "3.0.0"
        assert absences["audit.list"].introduced_in == "3.0.0"
        assert absences["audit.model-types"].introduced_in == "3.2.2"
        assert absences["audit.operation-types"].introduced_in == "3.2.2"

    for version in (
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
    ):
        absences = contract.terminal_absences(version)
        assert set(absences) == {"audit.model-types", "audit.operation-types"}
        assert all(item.introduced_in == "3.2.2" for item in absences.values())

    for version in contract.TARGET_OBSERVABILITY_VERSIONS[
        contract.TARGET_OBSERVABILITY_VERSIONS.index("3.2.2") :
    ]:
        assert contract.terminal_absences(version) == {}


def test_generated_action_facets_and_evidence_cover_the_same_closure() -> None:
    contract = _contract()

    for version in contract.TARGET_OBSERVABILITY_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        assert contract.semantic_operation_facets(version).keys() == sources.keys()
        assert contract.semantic_operation_evidence(version).keys() == sources.keys()


def test_runtime_bindings_compile_and_validate_exact_observability_closure() -> None:
    contract = _contract()
    impact = importlib.import_module("ds_codegen.compatibility_impact")
    selected: dict[str, dict[str, object]] = {}

    for version in contract.TARGET_OBSERVABILITY_VERSIONS:
        expected = contract.semantic_operation_sources(version)
        bindings = {
            action: impact.RUNTIME_OPERATION_BINDINGS[version][action]
            for action in expected
        }
        selected[version] = bindings
        assert {
            action: binding.source_operations for action, binding in bindings.items()
        } == expected

    impact.validate_reviewed_bindings(selected)
    assert "audit.list" not in selected["2.0.9"]
    assert set(selected["3.2.1"]) == {
        "monitor.database",
        "monitor.server",
        "audit.list",
    }
    assert set(selected["3.2.2"]) == {
        "monitor.database",
        "monitor.server",
        "audit.list",
        "audit.model-types",
        "audit.operation-types",
    }
