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


def test_every_exact_version_compiles_the_reviewed_current_user_closure() -> None:
    impact = _load_module("ds_codegen.compatibility_impact")
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    expected_types = {
        ("enums", "org.apache.dolphinscheduler.common.enums.UserType"),
        ("models", "org.apache.dolphinscheduler.api.utils.Result"),
        ("models", "org.apache.dolphinscheduler.dao.entity.User"),
    }

    assert tuple(runtime_contract.RUNTIME_OPERATION_BINDINGS) == (
        *impact.REVIEWED_DS_VERSIONS,
    )
    for version in impact.REVIEWED_DS_VERSIONS:
        operations = runtime_contract.runtime_semantic_operations(version)
        bindings = runtime_contract.runtime_operation_bindings(version)
        auxiliary = runtime_contract.runtime_auxiliary_operation_bindings(version)
        identity = bindings["identity.current"]

        assert set(operations) == {*bindings, *auxiliary}
        assert "identity.current" in operations
        assert identity.source_operations == ("UsersController.getUserInfo",)
        assert {
            (item.surface, item.key) for item in identity.type_closure
        } == expected_types
        assert identity.selector_semantics == ()
        assert any(
            source.kind == "controller"
            and source.reference.endswith("UsersController.java#getUserInfo")
            for source in identity.evidence_sources
        )


def test_identity_build_decision_tracks_reviewed_runtime_capabilities() -> None:
    version_profiles = _load_module("ds_codegen.version_profiles")
    ledger = version_profiles.load_version_profile_ledger()
    data = version_profiles.compile_version_profile_data(
        stable_actions=stable_leaf_actions(),
        ledger=ledger,
    )

    assert "blocked_source_closure_versions" not in ledger
    for profile in data["profiles"].values():
        identity = profile["build_decisions"]["identity.current"]
        assert identity["stable_action"] == "doctor"
        assert identity["source_operations"] == ["UsersController.getUserInfo"]
        assert identity["build_status"] == (
            "accepted"
            if profile["actions"]["doctor"]["availability"] == "supported"
            else "pending"
        )

    assert data["profiles"]["3.2.2"]["actions"]["doctor"] == {
        "availability": "supported",
        "execution_mode": "diagnostic_recipe",
        "verification": "static",
    }
    assert (
        data["profiles"]["3.4.2"]["actions"]["doctor"]["verification"] == "live_smoke"
    )
