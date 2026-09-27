from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

from dsctl.cli_surface import stable_leaf_actions


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_every_exact_version_has_the_complete_tenant_recipe() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    expected_types = {
        ("models", "org.apache.dolphinscheduler.api.utils.PageInfo"),
        ("models", "org.apache.dolphinscheduler.api.utils.Result"),
        ("models", "org.apache.dolphinscheduler.dao.entity.Queue"),
        ("models", "org.apache.dolphinscheduler.dao.entity.Tenant"),
    }

    for version in impact.REVIEWED_DS_VERSIONS:
        bindings = runtime_contract.runtime_operation_bindings(version)
        impact.validate_reviewed_bindings({version: bindings})

        tenant_page_operation = (
            "TenantController.queryTenantlistPaging"
            if version
            in {
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
            }
            else "TenantController.queryTenantListPaging"
        )
        assert bindings["tenant.page"].source_operations == (tenant_page_operation,)
        assert bindings["tenant.get"].source_operations == (tenant_page_operation,)
        assert bindings["tenant.create"].source_operations == (
            "QueueController.queryQueueListPaging",
            "TenantController.createTenant",
            tenant_page_operation,
        )
        assert bindings["tenant.update"].source_operations == (
            tenant_page_operation,
            "QueueController.queryQueueListPaging",
            "TenantController.updateTenant",
        )
        assert bindings["tenant.delete"].source_operations == (
            tenant_page_operation,
            "TenantController.deleteTenantById",
        )
        for operation in (
            "tenant.create",
            "tenant.delete",
            "tenant.get",
            "tenant.page",
            "tenant.update",
        ):
            binding = bindings[operation]
            assert {
                (item.surface, item.key) for item in binding.type_closure
            } == expected_types
            assert {source.kind for source in binding.evidence_sources} == {
                "controller",
                "ui",
            }


def test_tenant_actions_are_contract_tested_in_every_exact_profile() -> None:
    version_profiles = _load_module("ds_codegen.version_profiles")
    data = version_profiles.compile_version_profile_data(
        stable_actions=stable_leaf_actions()
    )

    for version, profile in data["profiles"].items():
        for action in (
            "tenant.create",
            "tenant.delete",
            "tenant.get",
            "tenant.list",
            "tenant.update",
        ):
            capability = profile["actions"][action]
            assert capability["availability"] == "supported", version
            assert capability["execution_mode"] == "generated_adapter", version
            assert capability["verification"] in {
                "contract_tested",
                "live_smoke",
            }


def test_tenant_update_ledger_treats_tenant_code_as_immutable_identity() -> None:
    version_profiles = _load_module("ds_codegen.version_profiles")
    ledger = version_profiles.load_version_profile_ledger()
    contract = ledger["operation_contracts"]["tenant.update"]

    assert contract["wire_policy"]["tenant_code"] == (
        "preserve-current-identity-on-all-versions"
    )
    assert contract["preservation"]["owned_paths"] == [
        "queueId",
        "description",
    ]
