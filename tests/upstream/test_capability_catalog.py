from __future__ import annotations

import json
from typing import cast

import pytest

from dsctl.cli_surface import stable_leaf_actions
from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.capability_catalog import (
    ActionCapability,
    Availability,
    CapabilityCatalog,
    Verification,
)
from dsctl.upstream.registry import get_version_support


def _supported_capability() -> ActionCapability:
    return ActionCapability(
        availability=Availability.SUPPORTED,
        verification=Verification.STATIC,
    )


def _catalog_with(
    action: str,
    capability: ActionCapability,
    *,
    server_version: str = "1.3.9",
) -> CapabilityCatalog:
    entries = {
        stable_action: _supported_capability()
        for stable_action in stable_leaf_actions()
    }
    entries[action] = capability
    return CapabilityCatalog(server_version=server_version, entries=entries)


def test_catalog_requires_exact_stable_leaf_action_coverage() -> None:
    stable_actions = stable_leaf_actions()
    entries = {action: _supported_capability() for action in stable_actions}
    missing_action = min(stable_actions)
    del entries[missing_action]

    with pytest.raises(ValueError, match=f"missing actions: {missing_action}"):
        CapabilityCatalog(server_version="3.4.2", entries=entries)


def test_catalog_rejects_actions_outside_the_stable_leaf_surface() -> None:
    entries = {action: _supported_capability() for action in stable_leaf_actions()}
    entries["workflow.future-action"] = _supported_capability()

    with pytest.raises(
        ValueError,
        match=r"unknown actions: workflow\.future-action",
    ):
        CapabilityCatalog(server_version="3.4.2", entries=entries)


def test_capability_vocabulary_is_closed_and_has_no_unknown_state() -> None:
    assert (
        tuple(value.value for value in Availability),
        tuple(value.value for value in Verification),
    ) == (
        ("supported", "limited", "unsupported"),
        ("static", "contract_tested", "live_smoke", "live_full"),
    )


def test_capability_rejects_untyped_unknown_state() -> None:
    with pytest.raises(TypeError, match="availability must be an Availability value"):
        ActionCapability(
            availability=cast("Availability", "unknown"),
            verification=Verification.STATIC,
        )


def test_limited_capability_requires_an_explicit_constraint() -> None:
    with pytest.raises(
        ValueError,
        match="limited capability requires an explicit constraint",
    ):
        ActionCapability(
            availability=Availability.LIMITED,
            verification=Verification.STATIC,
        )


def test_supported_capability_rejects_contradictory_constraint() -> None:
    with pytest.raises(
        ValueError,
        match="supported capability cannot define a constraint",
    ):
        ActionCapability(
            availability=Availability.SUPPORTED,
            verification=Verification.STATIC,
            constraint="This would make the action limited.",
        )


def test_unsupported_action_preflight_fails_before_transport() -> None:
    catalog = _catalog_with(
        "workflow.create",
        ActionCapability(
            availability=Availability.UNSUPPORTED,
            verification=Verification.STATIC,
            constraint="Workflow authoring is not available in this profile.",
        ),
    )
    transport_calls: list[str] = []

    def call_guarded_transport() -> None:
        catalog.preflight("workflow.create")
        transport_calls.append("POST /projects/example/process-definition")

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        call_guarded_transport()

    assert (transport_calls, exc_info.value.to_payload()) == (
        [],
        {
            "type": "unsupported_feature",
            "message": ("workflow.create is unsupported on DolphinScheduler 1.3.9."),
            "details": {
                "action": "workflow.create",
                "selected_version": "1.3.9",
                "availability": "unsupported",
                "constraint": ("Workflow authoring is not available in this profile."),
            },
            "suggestion": (
                "Run `dsctl capabilities --action workflow.create` to inspect the "
                "constraint. This operation requires a server version that "
                "supports it; the selected profile must match that server."
            ),
        },
    )


def test_limited_action_stays_fail_closed_until_intent_gating_exists() -> None:
    catalog = _catalog_with(
        "schedule.create",
        ActionCapability(
            availability=Availability.LIMITED,
            verification=Verification.CONTRACT_TESTED,
            constraint="Schedules with an environment cannot be represented.",
        ),
    )
    transport_calls: list[str] = []

    def call_guarded_transport() -> None:
        catalog.preflight("schedule.create")
        transport_calls.append("POST /projects/example/schedules")

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        call_guarded_transport()

    assert (transport_calls, exc_info.value.to_payload()) == (
        [],
        {
            "type": "unsupported_feature",
            "message": "schedule.create is limited on DolphinScheduler 1.3.9.",
            "details": {
                "action": "schedule.create",
                "selected_version": "1.3.9",
                "availability": "limited",
                "constraint": ("Schedules with an environment cannot be represented."),
            },
            "suggestion": (
                "Run `dsctl capabilities --action schedule.create` to inspect the "
                "constraint. This operation requires a server version that "
                "supports it; the selected profile must match that server."
            ),
        },
    )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1"])
def test_execute_task_master_gap_preflight_blocks_before_transport(
    version: str,
) -> None:
    catalog = get_version_support(version).catalog
    calls: list[str] = []

    def dispatch() -> None:
        catalog.preflight("workflow-instance.execute-task")
        calls.append("POST /projects/1/executors/execute-task")

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        dispatch()

    assert calls == []
    details = exc_info.value.details
    assert details["selected_version"] == version
    assert details["availability"] == "limited"
    constraint = details["constraint"]
    assert isinstance(constraint, str)
    assert "no handler" in constraint


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2", "3.4.2", "3.4.3"])
def test_execute_task_with_master_handler_passes_preflight(version: str) -> None:
    catalog = get_version_support(version).catalog
    assert (
        catalog.preflight("workflow-instance.execute-task").availability
        is Availability.SUPPORTED
    )


def test_supported_action_passes_preflight() -> None:
    supported = _catalog_with(
        "project.list",
        ActionCapability(
            availability=Availability.SUPPORTED,
            verification=Verification.LIVE_SMOKE,
        ),
    )
    assert supported.preflight("project.list") is supported.entries["project.list"]


def test_available_actions_excludes_non_supported_entries() -> None:
    unsupported = _catalog_with(
        "workflow.create",
        ActionCapability(
            availability=Availability.UNSUPPORTED,
            verification=Verification.STATIC,
            constraint="Not yet verified.",
        ),
    )
    limited = _catalog_with(
        "schedule.create",
        ActionCapability(
            availability=Availability.LIMITED,
            verification=Verification.CONTRACT_TESTED,
            constraint="Input-facet gating is not available yet.",
        ),
    )

    assert "project.list" in unsupported.available_actions()
    assert "workflow.create" not in unsupported.available_actions()
    assert "schedule.create" not in limited.available_actions()


def test_catalog_exposes_json_safe_bounded_summary_and_action_metadata() -> None:
    catalog = _catalog_with(
        "schedule.create",
        ActionCapability(
            availability=Availability.LIMITED,
            verification=Verification.CONTRACT_TESTED,
            constraint="Environment-bound schedules require a newer profile.",
        ),
        server_version="2.0.9",
    )

    summary = catalog.summary_metadata()
    action = catalog.action_metadata("schedule.create")
    rendered = json.dumps(
        {"summary": summary, "action": action},
        ensure_ascii=False,
        sort_keys=True,
    )

    assert (json.loads(rendered), len(rendered) < 1_024) == (
        {
            "summary": {
                "selected_version": "2.0.9",
                "action_count": len(stable_leaf_actions()),
                "availability_counts": {
                    "supported": len(stable_leaf_actions()) - 1,
                    "limited": 1,
                    "unsupported": 0,
                },
                "verification_counts": {
                    "static": len(stable_leaf_actions()) - 1,
                    "contract_tested": 1,
                    "live_smoke": 0,
                    "live_full": 0,
                },
            },
            "action": {
                "action": "schedule.create",
                "availability": "limited",
                "verification": "contract_tested",
                "constraint": ("Environment-bound schedules require a newer profile."),
            },
        },
        True,
    )


def test_action_capability_rejects_unbounded_constraint_metadata() -> None:
    with pytest.raises(ValueError, match="constraint must be at most 256 characters"):
        ActionCapability(
            availability=Availability.UNSUPPORTED,
            verification=Verification.STATIC,
            constraint="x" * 257,
        )
