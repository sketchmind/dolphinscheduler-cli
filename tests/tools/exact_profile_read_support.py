"""Shared public fixtures for exact-profile read receipt tests."""

from __future__ import annotations

from tests.live.exact_read_gate import (
    EXACT_PROFILE_READ_ACTIONS,
    EXACT_PROFILE_READ_CAPABILITY_ACTIONS,
    EXACT_PROFILE_READ_RECIPES,
    canonical_read_bundle_digest,
)


def evidence_payload(ds_version: str = "3.2.2") -> dict[str, object]:
    """Build one legacy-compatible schema-1 exact-read receipt."""
    identity_kind = "id" if ds_version == "1.3.9" else "code"
    recipes = [
        _recipe(action, operation, str(index))
        for index, (action, operation) in enumerate(
            EXACT_PROFILE_READ_RECIPES,
            start=1,
        )
    ]
    read_bundle = {
        "actions": list(EXACT_PROFILE_READ_ACTIONS),
        "recipes": recipes,
    }
    return {
        "schema_version": 1,
        "sanitization_schema_version": 1,
        "gate": "exact-profile-read",
        "status": "passed",
        "recorded_at": "2026-08-06T08:05:00Z",
        "runner": {
            "artifact": "installed-wheel-console-script",
            "cli_version": "0.4.0",
            "wheel_filename": "dolphinscheduler_cli-0.4.0-py3-none-any.whl",
            "wheel_sha256": "sha256:" + "a" * 64,
        },
        "dolphinscheduler": {
            "release": ds_version,
            "image_ref": f"apache/dolphinscheduler-api:{ds_version}",
            "image_id": "sha256:" + "b" * 64,
            "image_source": "swarm service inspection",
            "image_observed_at": "2026-08-06T08:00:00Z",
            "api_target_hmac_sha256": "hmac-sha256:" + "c" * 64,
            "principal_hmac_sha256": "hmac-sha256:" + "d" * 64,
            "persona": "etl-developer",
        },
        "profile": {
            "ds": ds_version,
            "selected_ds_version": ds_version,
            "contract_version": ds_version,
            "family": "process-definition-3.2",
            "support_level": "experimental",
            "tested": False,
            "read_adapter": (
                "GeneratedLegacyReadAdapter"
                if ds_version == "1.3.9"
                else "GeneratedReadAdapter"
            ),
            "project_domain": "project",
            "project_domain_adapter": (
                "_LegacyProjectDomainAdapter"
                if ds_version == "1.3.9"
                else "_CodeProjectDomainAdapter"
            ),
            "identity_adapter": "GeneratedIdentityAdapter",
            "full_adapter": False,
            "fingerprints": {
                "source": "sha256:" + "1" * 64,
                "effective_wire": "sha256:" + "2" * 64,
                "consumed_projection": "sha256:" + "3" * 64,
                "preservation": "sha256:" + "4" * 64,
            },
        },
        "contract": {
            "bundle_manifest_schema_version": 1,
            "ds_version": ds_version,
            "selection": "runtime-slice",
            "semantic_operations": [
                operation for _, operation in EXACT_PROFILE_READ_RECIPES
            ],
            "source_tag": ds_version,
            "source_commit": "e" * 40,
            "source_tree": "f" * 40,
            "source_contract_digest": "sha256:" + "5" * 64,
            "rendered_contract_digest": "sha256:" + "6" * 64,
            "operation_count": 4,
        },
        "read_bundle": {
            **read_bundle,
            "digest": canonical_read_bundle_digest(read_bundle),
        },
        "fixture": {
            "manifest_sha256": "sha256:" + "7" * 64,
            "provisioner": "external-version-matrix",
            "project_identity_kind": identity_kind,
            "workflow_identity_kind": identity_kind,
            "scheduled_workflow": True,
            "workflow_release_state": "ONLINE",
        },
        "operation_trace": _operation_trace(identity_kind=identity_kind),
        "effects": {"remote_mutations": 0, "fixture_mutated": False},
        "secrets_recorded": False,
    }


def semantic_evidence_payload(ds_version: str = "3.2.2") -> dict[str, object]:
    """Build one schema-2 receipt without implementation identity fields."""
    payload = evidence_payload(ds_version)
    payload["schema_version"] = 2
    contract = payload["contract"]
    assert isinstance(contract, dict)
    contract["bundle_manifest_schema_version"] = 2
    profile = payload["profile"]
    assert isinstance(profile, dict)
    for field in (
        "full_adapter",
        "identity_adapter",
        "project_domain",
        "project_domain_adapter",
        "read_adapter",
    ):
        profile.pop(field)
    read_bundle = payload["read_bundle"]
    assert isinstance(read_bundle, dict)
    read_bundle["action_verifications"] = dict.fromkeys(
        EXACT_PROFILE_READ_ACTIONS,
        "live_smoke",
    )
    read_bundle["digest"] = canonical_read_bundle_digest(
        {
            "actions": read_bundle["actions"],
            "action_verifications": read_bundle["action_verifications"],
            "recipes": read_bundle["recipes"],
        }
    )
    return payload


def _recipe(action: str, semantic_operation: str, digit: str) -> dict[str, object]:
    return {
        "action": action,
        "semantic_operation": semantic_operation,
        "build_status": "accepted",
        "fingerprints": {
            "source": "sha256:" + digit * 64,
            "effective_wire": "sha256:" + digit * 64,
            "consumed_projection": "sha256:" + digit * 64,
            "preservation": "sha256:" + digit * 64,
        },
    }


def _operation_trace(*, identity_kind: str) -> list[dict[str, object]]:
    actions = [
        "version",
        "capabilities",
        "capabilities",
        "capabilities",
        "capabilities",
        "doctor",
        "project.list",
        "project.get",
        "project.get",
        "workflow.list",
        "workflow.get",
        "workflow.get",
    ]
    selectors = {
        7: "search",
        8: "project-name",
        9: f"project-{identity_kind}",
        10: "project-name+workflow-search",
        11: "workflow-name",
        12: f"workflow-{identity_kind}",
    }
    argv_shapes = {
        1: "version",
        2: "capabilities --action ACTION",
        3: "capabilities --action ACTION",
        4: "capabilities --action ACTION",
        5: "capabilities --action ACTION",
        6: "doctor",
        7: "project list --search PROJECT_NAME --page-no 1 --page-size 20",
        8: "project get PROJECT",
        9: "project get PROJECT",
        10: (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no 1 --page-size 20"
        ),
        11: "workflow get WORKFLOW --project PROJECT",
        12: "workflow get WORKFLOW --project PROJECT",
    }
    assertions = {
        1: ["selected-contract-and-family-matched"],
        **{
            sequence: [f"{action}-supported"]
            for sequence, action in enumerate(
                EXACT_PROFILE_READ_CAPABILITY_ACTIONS,
                start=2,
            )
        },
        6: [
            "api-health-ok",
            "current-user-ok",
            "general-user-confirmed",
            "principal-hmac-matched",
        ],
        7: ["native-project-identity-matched", "pagination-search-matched"],
        8: ["native-project-identity-matched"],
        9: ["native-project-identity-matched"],
        10: ["native-workflow-identity-matched", "pagination-search-matched"],
        11: [
            "native-workflow-identity-matched",
            "schedule-hydrated",
            "release-state-matched",
        ],
        12: [
            "native-workflow-identity-matched",
            "schedule-hydrated",
            "release-state-matched",
        ],
    }
    return [
        {
            "sequence": sequence,
            "argv_shape": argv_shapes[sequence],
            "action": action,
            "exit_code": 0,
            "ok": True,
            "assertions": assertions[sequence],
            **({"selector_kind": selectors[sequence]} if sequence in selectors else {}),
        }
        for sequence, action in enumerate(actions, start=1)
    ]


__all__ = ["evidence_payload", "semantic_evidence_payload"]
