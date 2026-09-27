from __future__ import annotations

import copy
import importlib
import runpy
import sys
from pathlib import Path
from typing import Any

import pytest

from dsctl.cli_surface import stable_leaf_actions
from dsctl.upstream.registry import SUPPORTED_VERSIONS, get_version_support

HISTORICAL_EXACT_READ_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
FULL_CORE_EXACT_READ_VERSIONS = (
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
    "3.4.3",
)

GENERIC_EXACT_READ_ACTIONS = (
    "project.get",
    "project.list",
    "workflow.get",
    "workflow.list",
)
EXACT_342_GATE_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "workflow.describe",
    "workflow.digest",
    "workflow.export",
    "workflow.get",
    "workflow.list",
    "task.list",
    "task.get",
    "task.update",
)


def _load_module() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.version_profiles")


def test_ledger_declares_its_own_reviewed_version_membership() -> None:
    profiles = _load_module()
    ledger = profiles.load_version_profile_ledger()
    for field in ("sources", "profile_metadata", "runtime_profiles"):
        ledger[field] = {"3.4.1": ledger[field]["3.4.1"]}

    data = profiles.compile_version_profile_data(
        stable_actions=stable_leaf_actions(), ledger=ledger
    )

    assert data["target_versions"] == ["3.4.1"]
    assert tuple(data["profiles"]) == ("3.4.1",)


@pytest.fixture(scope="module")
def version_profile_ledger() -> dict[str, Any]:
    ledger: dict[str, Any] = _load_module().load_version_profile_ledger()
    return ledger


@pytest.fixture(scope="module")
def compiled_version_profiles(
    version_profile_ledger: dict[str, Any],
) -> dict[str, Any]:
    data: dict[str, Any] = _load_module().compile_version_profile_data(
        stable_actions=stable_leaf_actions(),
        ledger=version_profile_ledger,
    )
    return data


def test_queue_page_build_decision_records_exact_paging_projection(
    compiled_version_profiles: dict[str, Any],
) -> None:
    contract = importlib.import_module("ds_codegen.task_group_contract")
    profiles = compiled_version_profiles["profiles"]
    for version in compiled_version_profiles["target_versions"]:
        decisions = profiles[version]["build_decisions"]
        queue = decisions["task-group.queue.page"]
        reviewed = contract.task_group_contract(version)
        if reviewed.task_group.support == "absent":
            assert "paging_projection" not in queue
            continue
        projection = reviewed.queue_page_projection
        assert projection is not None
        assert queue["paging_projection"] == {
            "taskId": projection.task_id_projected,
            "inQueue": projection.in_queue_projected,
        }
        assert {source["kind"] for source in queue["evidence_sources"]} == {
            "controller",
            "ui",
            "mapper",
        }
        for operation, decision in decisions.items():
            if operation != "task-group.queue.page":
                assert "paging_projection" not in decision, (version, operation)
    early = profiles["3.2.0"]["build_decisions"]["task-group.queue.page"]
    late = profiles["3.2.1"]["build_decisions"]["task-group.queue.page"]
    assert early["paging_projection"]["inQueue"] is False
    assert late["paging_projection"]["inQueue"] is True
    assert (
        early["fingerprints"]["effective_wire"]
        != late["fingerprints"]["effective_wire"]
    )


def test_compiler_materializes_every_version_and_stable_action(
    compiled_version_profiles: dict[str, Any],
    version_profile_ledger: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    actions = stable_leaf_actions()

    data = compiled_version_profiles
    expected_build_decisions = tuple(
        sorted(version_profile_ledger["operation_contracts"])
    )

    assert (
        tuple(data["target_versions"]) == version_profiles.reviewed_profile_versions()
    )
    assert tuple(data["stable_actions"]) == tuple(sorted(actions))
    assert tuple(data["profiles"]) == version_profiles.reviewed_profile_versions()
    for version, profile in data["profiles"].items():
        assert profile["server_version"] == version
        assert profile["contract_version"] == version
        assert isinstance(profile["family"], str)
        assert profile["family"]
        assert profile["support_level"] in {
            "full",
            "legacy_core",
            "experimental",
        }
        assert isinstance(profile["tested"], bool)
        assert set(profile["actions"]) == actions
        assert set(profile["fingerprints"]) == {
            "source",
            "effective_wire",
            "consumed_projection",
            "preservation",
        }
        assert all(
            fingerprint.startswith("sha256:")
            for fingerprint in profile["fingerprints"].values()
        )
        assert tuple(sorted(profile["build_decisions"])) == expected_build_decisions
        for action, capability in profile["actions"].items():
            assert capability["availability"] in {
                "supported",
                "limited",
                "unsupported",
            }, action
            assert capability["execution_mode"] in {
                "not_executable",
                "local",
                "diagnostic_recipe",
                "legacy_adapter",
                "generated_adapter",
                "wire_program",
            }, action
            if capability["availability"] == "unsupported":
                assert capability["execution_mode"] == "not_executable"
                assert capability["reason"]


def test_universal_local_actions_are_supported_in_all_exact_profiles(
    compiled_version_profiles: dict[str, Any],
) -> None:
    data = compiled_version_profiles

    for profile in data["profiles"].values():
        for action in (
            "capabilities",
            "context",
            "schema",
            "context.list",
            "context.get",
            "context.create",
            "context.update",
            "context.delete",
            "config.get",
            "config.set",
            "config.unset",
            "version",
        ):
            assert profile["actions"][action] == {
                "availability": "supported",
                "execution_mode": "local",
                "verification": "static",
            }


def test_322_project_worker_group_clear_is_limited_without_narrowing_list_or_set(
    compiled_version_profiles: dict[str, Any],
) -> None:
    profiles = compiled_version_profiles["profiles"]
    profile = profiles["3.2.2"]

    assert profile["actions"]["project-worker-group.clear"] == {
        "availability": "limited",
        "execution_mode": "not_executable",
        "verification": "static",
        "constraint": (
            "DolphinScheduler 3.2.2 supports reading and assigning nonempty "
            "project worker-group sets, but cannot clear them because an empty "
            "workerGroups list returns native 1402003 "
            "(WORKER_GROUP_TO_PROJECT_IS_EMPTY)."
        ),
        "reason": "upstream_capability_limited",
        "evidence_sources": [
            (
                "apache/dolphinscheduler@3.2.2:dolphinscheduler-api/src/main/java/"
                "org/apache/dolphinscheduler/api/service/impl/"
                "ProjectWorkerGroupRelationServiceImpl.java#"
                "assignWorkerGroupsToProject rejects an empty workerGroups list "
                "with WORKER_GROUP_TO_PROJECT_IS_EMPTY"
            ),
            (
                "apache/dolphinscheduler@3.2.2:dolphinscheduler-api/src/test/java/"
                "org/apache/dolphinscheduler/api/service/"
                "ProjectWorkerGroupRelationServiceTest.java#"
                "testAssignWorkerGroupsToProject asserts the empty-list rejection"
            ),
        ],
    }
    for action in ("project-worker-group.list", "project-worker-group.set"):
        assert profile["actions"][action] == {
            "availability": "supported",
            "execution_mode": "generated_adapter",
            "verification": "contract_tested",
        }

    clear_decision = profile["build_decisions"]["project-worker-group.clear"]
    assert clear_decision["build_status"] == "blocked"
    assert clear_decision["status_reason"] == "upstream_capability_limited"
    assert clear_decision["source_operations"] == []
    assert clear_decision["type_closure"] == []
    assert {source["kind"] for source in clear_decision["evidence_sources"]} == {
        "upstream_limitation"
    }
    for operation in ("project-worker-group.page", "project-worker-group.set"):
        assert profile["build_decisions"][operation]["build_status"] == "accepted"

    for version in ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"):
        assert profiles[version]["actions"]["project-worker-group.clear"] == {
            "availability": "supported",
            "execution_mode": "generated_adapter",
            "verification": "contract_tested",
        }


def test_execute_task_master_gap_blocks_runtime_but_preserves_exact_wire(
    compiled_version_profiles: dict[str, Any],
    version_profile_ledger: dict[str, Any],
) -> None:
    profiles = compiled_version_profiles["profiles"]
    action = "workflow-instance.execute-task"
    blocked_versions = ("3.3.1", "3.3.2", "3.4.0", "3.4.1")
    for version in blocked_versions:
        capability = profiles[version]["actions"][action]
        decision = profiles[version]["build_decisions"][action]
        assert capability["availability"] == "limited"
        assert capability["execution_mode"] == "not_executable"
        assert capability["reason"] == "upstream_runtime_limited"
        assert "no handler" in capability["constraint"]
        assert capability["evidence_sources"]
        assert decision["build_status"] == "blocked"
        assert decision["status_reason"] == "upstream_runtime_limited"
        assert decision["source_operations"]
        assert decision["type_closure"]
        assert all(
            value.startswith("sha256:") for value in decision["fingerprints"].values()
        )

        # A runtime limitation must not rewrite the source or wire evidence.
        supported_ledger = copy.deepcopy(version_profile_ledger)
        layer = next(
            item
            for item in supported_ledger["runtime_profiles"][version]
            if item["selector"] == "runtime_instance_execute_task"
        )
        layer["policy"] = "generated_contract"
        supported = _load_module().compile_version_profile_data(
            stable_actions=stable_leaf_actions(),
            ledger=supported_ledger,
            versions=(version,),
        )["profiles"][version]["build_decisions"][action]
        assert decision["source_operations"] == supported["source_operations"]
        assert decision["fingerprints"] == supported["fingerprints"]

    for version in ("3.2.0", "3.2.1", "3.2.2", "3.4.2", "3.4.3"):
        assert profiles[version]["actions"][action]["availability"] == "supported"
        assert (
            profiles[version]["build_decisions"][action]["build_status"] == "accepted"
        )


def test_wire_present_runtime_limitation_requires_binding_and_limited_state(
    version_profile_ledger: dict[str, Any],
) -> None:
    profiles = _load_module()
    policy_name = "runtime_instance_execute_task_no_master_handler_3_3_1_to_3_4_1"
    ledger = copy.deepcopy(version_profile_ledger)
    policy = ledger["runtime_policies"][policy_name]
    policy["availability"] = "unsupported"
    with pytest.raises(ValueError, match="must be limited"):
        profiles.compile_version_profile_data(
            stable_actions=stable_leaf_actions(), ledger=ledger, versions=("3.4.1",)
        )

    for missing in (True, False):
        ledger = copy.deepcopy(version_profile_ledger)
        policy = ledger["runtime_policies"][policy_name]
        if missing:
            del policy["evidence_sources"]
        else:
            policy["evidence_sources"] = []
        with pytest.raises(ValueError, match="requires evidence_sources"):
            profiles.compile_version_profile_data(
                stable_actions=stable_leaf_actions(),
                ledger=ledger,
                versions=("3.4.1",),
            )

    ledger = copy.deepcopy(version_profile_ledger)
    layer = next(
        item
        for item in ledger["runtime_profiles"]["3.1.9"]
        if item["selector"] == "runtime_instance_execute_task"
    )
    layer["policy"] = policy_name
    with pytest.raises(ValueError, match="has no reviewed binding"):
        profiles.compile_version_profile_data(
            stable_actions=stable_leaf_actions(), ledger=ledger, versions=("3.1.9",)
        )


@pytest.mark.parametrize("campaign", ["historical_sweep", "full_core_22"])
def test_exact_read_promotions_change_only_their_four_verified_actions(
    compiled_version_profiles: dict[str, Any],
    version_profile_ledger: dict[str, Any],
    campaign: str,
) -> None:
    version_profiles = _load_module()
    actions = stable_leaf_actions()
    assert set(HISTORICAL_EXACT_READ_VERSIONS).isdisjoint(FULL_CORE_EXACT_READ_VERSIONS)
    assert set(HISTORICAL_EXACT_READ_VERSIONS) | set(
        FULL_CORE_EXACT_READ_VERSIONS
    ) == set(version_profiles.reviewed_profile_versions())

    after_ledger = copy.deepcopy(version_profile_ledger)
    if campaign == "historical_sweep":
        # Reconstruct the historical stage independently of later promotions.
        for version in FULL_CORE_EXACT_READ_VERSIONS:
            read_layer = next(
                layer
                for layer in after_ledger["runtime_profiles"][version]
                if layer["selector"] == "read_tracer"
            )
            read_layer["policy"] = "generated_contract"
        after = version_profiles.compile_version_profile_data(
            stable_actions=actions, ledger=after_ledger
        )
        promoted_versions = tuple(
            version for version in HISTORICAL_EXACT_READ_VERSIONS if version != "3.2.2"
        )
        live_versions = HISTORICAL_EXACT_READ_VERSIONS
    else:
        after = compiled_version_profiles
        promoted_versions = FULL_CORE_EXACT_READ_VERSIONS
        live_versions = version_profiles.reviewed_profile_versions()

    before_sweep = copy.deepcopy(after_ledger)
    for version in promoted_versions:
        layers = before_sweep["runtime_profiles"][version]
        read_layer = next(
            layer for layer in layers if layer["selector"] == "read_tracer"
        )
        read_layer["policy"] = "generated_contract"

    before = version_profiles.compile_version_profile_data(
        stable_actions=actions,
        ledger=before_sweep,
    )

    expected_differences = {
        (version, action, "verification"): ("contract_tested", "live_smoke")
        for version in promoted_versions
        for action in GENERIC_EXACT_READ_ACTIONS
    }
    actual_differences: dict[tuple[str, str, str], tuple[object, object]] = {}
    for version in version_profiles.reviewed_profile_versions():
        before_profile = before["profiles"][version]
        after_profile = after["profiles"][version]
        assert {
            key: value for key, value in after_profile.items() if key != "actions"
        } == {key: value for key, value in before_profile.items() if key != "actions"}

        for action in sorted(actions):
            before_capability = before_profile["actions"][action]
            after_capability = after_profile["actions"][action]
            for field in sorted(set(before_capability) | set(after_capability)):
                before_value = before_capability.get(field)
                after_value = after_capability.get(field)
                if before_value != after_value:
                    actual_differences[(version, action, field)] = (
                        before_value,
                        after_value,
                    )

        expected_support = "full" if version == "3.4.1" else "experimental"
        assert after_profile["support_level"] == expected_support
        assert after_profile["tested"] is (version == "3.4.1")

    assert actual_differences == expected_differences

    for version, profile in after["profiles"].items():
        expected_verification = (
            "live_smoke" if version in live_versions else "contract_tested"
        )
        for action in GENERIC_EXACT_READ_ACTIONS:
            assert profile["actions"][action] == {
                "availability": "supported",
                "execution_mode": "generated_adapter",
                "verification": expected_verification,
            }


def test_exact_342_mutating_gate_promotes_only_its_eleven_verified_actions(
    compiled_version_profiles: dict[str, Any],
    version_profile_ledger: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    before_gate = copy.deepcopy(version_profile_ledger)
    old_policies = {
        "diagnostic": "diagnostic_static",
        "generated_3_4_2": "generated_contract",
        "schedule_list": "generated_contract",
        "task_wire": "wire_contract",
    }
    for layer in before_gate["runtime_profiles"]["3.4.2"]:
        selector = layer["selector"]
        if selector in old_policies:
            layer["policy"] = old_policies[selector]

    actions = stable_leaf_actions()
    before = version_profiles.compile_version_profile_data(
        stable_actions=actions,
        ledger=before_gate,
    )
    after = compiled_version_profiles

    expected_promotions = set(EXACT_342_GATE_ACTIONS) - set(GENERIC_EXACT_READ_ACTIONS)
    actual_differences: dict[tuple[str, str, str], tuple[object, object]] = {}
    for version in version_profiles.reviewed_profile_versions():
        before_profile = before["profiles"][version]
        after_profile = after["profiles"][version]
        assert {
            key: value for key, value in after_profile.items() if key != "actions"
        } == {key: value for key, value in before_profile.items() if key != "actions"}
        for action in sorted(actions):
            before_capability = before_profile["actions"][action]
            after_capability = after_profile["actions"][action]
            for field in sorted(set(before_capability) | set(after_capability)):
                before_value = before_capability.get(field)
                after_value = after_capability.get(field)
                if before_value != after_value:
                    actual_differences[(version, action, field)] = (
                        before_value,
                        after_value,
                    )

    assert actual_differences == {
        ("3.4.2", action, "verification"): (
            "static" if action == "doctor" else "contract_tested",
            "live_smoke",
        )
        for action in expected_promotions
    }
    assert {
        action
        for action, capability in after["profiles"]["3.4.2"]["actions"].items()
        if capability["verification"] == "live_smoke"
    }.issuperset(EXACT_342_GATE_ACTIONS)
    for version, profile in after["profiles"].items():
        expected_support = "full" if version == "3.4.1" else "experimental"
        assert profile["support_level"] == expected_support
        assert profile["tested"] is (version == "3.4.1")


def test_workflow_actions_are_closed_in_every_exact_profile(
    compiled_version_profiles: dict[str, Any],
    version_profile_ledger: dict[str, Any],
) -> None:
    ledger = version_profile_ledger
    workflow_contracts = {
        operation: contract
        for operation, contract in ledger["operation_contracts"].items()
        if operation.startswith("workflow.")
    }
    data = compiled_version_profiles

    for version, profile in data["profiles"].items():
        for operation, contract in workflow_contracts.items():
            action = contract["stable_action"]
            capability = profile["actions"][action]
            decision = profile["build_decisions"][operation]
            assert capability.get("reason") != "compatibility_gate_not_passed", (
                version,
                action,
            )
            assert decision["build_status"] != "pending", (version, operation)
            if capability["availability"] == "supported":
                assert capability["execution_mode"] == "generated_adapter"
                assert decision["build_status"] == "accepted"
            else:
                assert capability["reason"] in {
                    "upstream_capability_absent",
                    "upstream_capability_limited",
                }
                assert capability["execution_mode"] == "not_executable"
                assert decision["build_status"] == "blocked"

    profile_341 = data["profiles"]["3.4.1"]
    assert {
        profile_341["actions"][action]["verification"]
        for action in ("workflow.get", "workflow.list")
    } == {"live_smoke"}
    assert {
        profile_341["actions"][contract["stable_action"]]["verification"]
        for contract in workflow_contracts.values()
        if contract["stable_action"] not in {"workflow.get", "workflow.list"}
    } == {"contract_tested"}

    profile_342 = data["profiles"]["3.4.2"]
    for action in (
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
        "schedule.list",
    ):
        assert profile_342["actions"][action]["verification"] == "live_smoke"


def test_runtime_cells_match_every_existing_registry_capability(
    compiled_version_profiles: dict[str, Any],
) -> None:
    data = compiled_version_profiles

    for version in SUPPORTED_VERSIONS:
        profile = data["profiles"][version]
        catalog = get_version_support(version).catalog
        for action, expected in catalog.entries.items():
            actual = profile["actions"][action]
            assert actual["availability"] == expected.availability.value
            assert actual["verification"] == expected.verification.value
            assert (actual.get("constraint")) == expected.constraint


def test_reviewed_generated_project_slice_is_promoted_without_cross_version_reuse(
    compiled_version_profiles: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    data = compiled_version_profiles
    profiles = data["profiles"]

    for operation in (
        "project.get",
        "project.page",
        "workflow.get",
        "workflow.page",
    ):
        decisions = [
            profiles[version]["build_decisions"][operation]
            for version in ("3.3.1", "3.3.2", "3.4.0", "3.4.1")
        ]
        assert len({decision["fingerprints"]["source"] for decision in decisions}) == 1
        assert (
            len({decision["fingerprints"]["effective_wire"] for decision in decisions})
            == 1
        )
        assert all(decision["build_status"] == "accepted" for decision in decisions)

    for version in version_profiles.reviewed_profile_versions():
        profile = profiles[version]
        for action in (
            "project.create",
            "project.delete",
            "project.get",
            "project.list",
            "project.update",
            "workflow.get",
            "workflow.list",
        ):
            capability = profile["actions"][action]
            assert capability["availability"] == "supported"
            if version != "3.4.1":
                assert capability["execution_mode"] == "generated_adapter"

    legacy = profiles["1.3.9"]
    assert legacy["actions"]["doctor"]["availability"] == "supported"
    assert legacy["actions"]["project.get"]["availability"] == "supported"
    assert legacy["actions"]["project.get"]["execution_mode"] == ("generated_adapter")
    assert legacy["build_decisions"]["project.get"]["build_status"] == "accepted"
    assert legacy["actions"]["project.create"]["availability"] == "supported"
    assert legacy["build_decisions"]["project.create"]["build_status"] == "accepted"


def test_four_fingerprints_change_independently() -> None:
    version_profiles = _load_module()
    inputs: dict[str, dict[str, object]] = {
        "source": {"contract": "source-v1"},
        "effective_wire": {"program": "wire-v1"},
        "consumed_projection": {"fields": ["code", "name"]},
        "preservation": {"policy": "read-only"},
    }
    baseline = version_profiles.compute_fingerprints(**inputs)

    for axis in inputs:
        changed_inputs = copy.deepcopy(inputs)
        changed_inputs[axis]["revision"] = 2
        changed = version_profiles.compute_fingerprints(**changed_inputs)

        assert changed[axis] != baseline[axis]
        assert {key: value for key, value in changed.items() if key != axis} == {
            key: value for key, value in baseline.items() if key != axis
        }


def test_renderer_is_deterministic_and_importable(
    tmp_path: Path,
    compiled_version_profiles: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    actions = stable_leaf_actions()
    data = compiled_version_profiles

    first = version_profiles.render_version_profiles(data)
    second = version_profiles.render_version_profiles(
        version_profiles.compile_version_profile_data(
            stable_actions=sorted(actions, reverse=True)
        )
    )
    assert first == second
    assert '\n  "build_records": {' in first
    assert '"source": "sha256:' in first
    assert "base64" not in first

    output_root = tmp_path / "dsctl"
    output_path = version_profiles.write_version_profiles(
        output_root,
        stable_actions=actions,
    )
    rendered = runpy.run_path(str(output_path))
    assert rendered["PROFILE_DATA"] == data
    assert (
        rendered["TARGET_DS_VERSIONS"] == version_profiles.reviewed_profile_versions()
    )
    assert rendered["STABLE_ACTIONS"] == tuple(sorted(actions))
    assert (
        tuple(rendered["VERSION_PROFILES"])
        == version_profiles.reviewed_profile_versions()
    )


def test_writer_rejects_precompiled_profile_action_domain_drift(
    tmp_path: Path,
    compiled_version_profiles: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    actions = tuple(sorted(stable_leaf_actions()))
    data = compiled_version_profiles

    with pytest.raises(ValueError, match="stable_actions do not match"):
        version_profiles.write_version_profiles(
            tmp_path / "stable-action-drift",
            stable_actions=actions[:-1],
            profile_data=data,
        )

    domain_drift = copy.deepcopy(data)
    del domain_drift["profiles"]["3.4.2"]["actions"][actions[0]]
    with pytest.raises(ValueError, match="action domain does not match"):
        version_profiles.write_version_profiles(
            tmp_path / "profile-domain-drift",
            stable_actions=actions,
            profile_data=domain_drift,
        )


def test_named_records_share_storage_but_keep_exact_profile_values_independent(
    tmp_path: Path,
    compiled_version_profiles: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    output = tmp_path / "profiles.py"
    output.write_text(
        version_profiles.render_version_profiles(compiled_version_profiles),
        encoding="utf-8",
    )
    loaded = runpy.run_path(str(output))
    shared = loaded["_SHARED_DATA"]
    profiles = loaded["VERSION_PROFILES"]
    decision_count = sum(
        len(profile["build_decisions"]) for profile in profiles.values()
    )
    assert len(shared["build_records"]) < decision_count
    assert len(shared["capability_records"]) < len(shared["stable_actions"])
    assert all("@" in name for name in shared["build_records"])
    assert all(isinstance(record, dict) for record in shared["build_records"].values())

    first = profiles["3.4.1"]
    second = profiles["3.4.2"]
    assert first["actions"]["project.get"] == second["actions"]["project.get"]
    first["actions"]["project.get"]["availability"] = "changed"
    assert second["actions"]["project.get"]["availability"] == "supported"
    first["build_decisions"]["project.get"]["type_closure"].clear()
    assert (
        second["build_decisions"]["project.get"]["type_closure"]
        == (
            compiled_version_profiles["profiles"]["3.4.2"]["build_decisions"][
                "project.get"
            ]["type_closure"]
        )
    )


def test_ledger_rejects_pending_as_a_runtime_availability(
    version_profile_ledger: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    ledger = copy.deepcopy(version_profile_ledger)
    ledger["runtime_policies"]["unsupported"]["availability"] = "pending"

    with pytest.raises(ValueError, match="runtime availability"):
        version_profiles.compile_version_profile_data(
            stable_actions=stable_leaf_actions(),
            ledger=ledger,
        )


def test_source_anchor_guard_rejects_a_different_exact_source(
    version_profile_ledger: dict[str, Any],
) -> None:
    version_profiles = _load_module()
    ledger = version_profile_ledger
    observed = {"3.4.1": dict(ledger["sources"]["3.4.1"])}

    version_profiles.require_version_profile_source_anchors(
        observed,
        ledger=ledger,
    )
    observed["3.4.1"]["tree"] = "f" * 40

    with pytest.raises(ValueError, match="source anchor changed"):
        version_profiles.require_version_profile_source_anchors(
            observed,
            ledger=ledger,
        )
