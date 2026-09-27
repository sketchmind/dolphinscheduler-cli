"""Validate receipt identity and scenario obligations against current facts."""

from __future__ import annotations

import json
from pathlib import Path

from live_gate.conformance_evidence.bindings import (
    _current_generated_profile,
    _validate_bundle,
    _validate_contract,
    _validate_profile,
)
from live_gate.conformance_evidence.provenance import (
    _validate_cluster,
    _validate_receipt_image_source,
)
from live_gate.conformance_evidence.trace import (
    _validate_operation_trace,
)
from live_gate.conformance_evidence.types import (
    ConformanceBundleEvidenceSummary,
    _Assessment,
    _CurrentTruth,
)
from live_gate.conformance_evidence.values import (
    _canonical_digest,
    _exact_keys,
    _reject_sensitive_keys,
    _sha256,
    _text_sequence,
    canonical_conformance_bundle_receipt_digest,
)
from live_gate.evidence_sanitization import reject_sensitive_text
from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_hmac_sha256 as _hmac_sha256
from live_gate.evidence_values import required_int as _integer
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text
from live_gate.evidence_values import required_utc_timestamp as _timestamp

_EVIDENCE_KEYS = frozenset(
    {
        "schema_version",
        "sanitization_schema_version",
        "gate",
        "status",
        "recorded_at",
        "runner",
        "dolphinscheduler",
        "profile",
        "contract",
        "conformance_bundle",
        "scenario",
        "fixture",
        "operation_trace",
        "effects",
        "cleanup",
        "evidence_scope",
        "secrets_recorded",
        "receipt_digest",
    }
)


_AUTHORING_MODES = frozenset({"typed", "opaque"})


_FIXTURE_PROVISIONER = "dsmatrix-conformance-fixture/v1"


_LEGACY_SCENARIO_OUTCOMES = (
    "bound-current-user",
    "capability-preflight-complete",
    "external-fixture-cross-checked",
    "gate-owned-project-round-trip",
    "negative-errors-translated",
)


_SCENARIO_OUTCOMES = {
    "legacy_core/v1": _LEGACY_SCENARIO_OUTCOMES,
    "full_core/v1": (
        *_LEGACY_SCENARIO_OUTCOMES[:-1],
        "gate-owned-workflow-round-trip",
        "workflow-dag-cross-checked",
        "task-update-round-trip",
        "workflow-edit-round-trip",
        "full-cleanup-zero",
        _LEGACY_SCENARIO_OUTCOMES[-1],
    ),
}


_SCENARIO_IDS = {
    "legacy_core/v1": "legacy-core-installed-wheel/v1",
    "full_core/v1": "full-core-installed-wheel/v1",
}


_SCENARIO_AUTHORING_MODES = {
    "legacy_core/v1": "typed",
    "full_core/v1": "opaque",
}


def _validate_conformance_bundle_evidence(
    receipt: object,
    *,
    expected_ds_version: str | None,
    expected_bundle: str | None,
    expected_wheel_filename: str | None,
    expected_wheel_sha256: str | None,
    current: _CurrentTruth,
    assessment: _Assessment,
) -> ConformanceBundleEvidenceSummary:
    evidence = _mapping(receipt, label="conformance bundle evidence")
    _exact_keys(evidence, _EVIDENCE_KEYS, label="evidence")
    _reject_sensitive_keys(evidence)
    reject_sensitive_text(evidence)
    serialized = json.dumps(evidence, ensure_ascii=False, sort_keys=True).lower()
    if "http://" in serialized or "https://" in serialized:
        message = "conformance bundle evidence must not contain URLs"
        raise ValueError(message)
    _constant(evidence.get("schema_version"), 1, label="schema_version")
    _constant(
        evidence.get("sanitization_schema_version"),
        1,
        label="sanitization_schema_version",
    )
    _constant(evidence.get("gate"), "exact-conformance-bundle", label="gate")
    _constant(evidence.get("status"), "passed", label="status")
    _constant(evidence.get("secrets_recorded"), False, label="secrets_recorded")
    recorded_at = _timestamp(evidence.get("recorded_at"), label="recorded_at")
    receipt_digest = _sha256(evidence.get("receipt_digest"), label="receipt_digest")
    _constant(
        receipt_digest,
        canonical_conformance_bundle_receipt_digest(evidence),
        label="receipt_digest",
    )
    runner = _validate_runner(
        evidence.get("runner"),
        cli_version=current.cli_version,
    )
    ds_version = _validate_cluster(
        evidence.get("dolphinscheduler"),
        recorded_at=recorded_at,
        image_contract=current.image_contract,
    )
    generated_profile = _current_generated_profile(
        ds_version,
        current=current,
    )
    profile = _validate_profile(
        evidence.get("profile"),
        ds_version=ds_version,
        generated_profile=generated_profile,
    )
    contract_operations, contract_source = _validate_contract(
        evidence.get("contract"),
        ds_version=ds_version,
        contracts=current.contracts,
    )
    if profile.source != contract_source:
        message = "profile/contract source identity differs"
        raise ValueError(message)
    cluster = _mapping(evidence.get("dolphinscheduler"), label="dolphinscheduler")
    _validate_receipt_image_source(
        cluster.get("image_provenance"), source_commit=contract_source[1]
    )
    bundle_name, required_actions = _validate_bundle(
        evidence.get("conformance_bundle"),
        contract_operations=contract_operations
        | current.runtime_operations[ds_version],
        assessment=assessment,
        ds_version=ds_version,
        profile=profile,
        generated_profile=generated_profile,
    )
    scenario_authoring_mode, required_outcomes = _validate_scenario(
        evidence.get("scenario"),
        bundle_name=bundle_name,
    )
    _validate_evidence_scope(
        evidence.get("evidence_scope"),
        authoring_mode=scenario_authoring_mode,
    )
    _validate_fixture(evidence.get("fixture"))
    _validate_effects(
        evidence.get("effects"),
        bundle_name=bundle_name,
        ds_version=ds_version,
        cleanup_full_versions=current.task_cleanup.full_core_versions,
        cleanup_pre_delete_release_versions=(
            current.task_cleanup.pre_delete_release_versions
        ),
    )
    _validate_cleanup(evidence.get("cleanup"))
    _validate_operation_trace(
        evidence.get("operation_trace"),
        bundle_name=bundle_name,
        ds_version=ds_version,
        required_actions=required_actions,
        required_outcomes=required_outcomes,
        task_cleanup=current.task_cleanup,
        dependency_update_upstream_limited_versions=(
            current.dependency_update_upstream_limited_versions
        ),
    )
    wheel_filename, wheel_sha256 = runner
    _require_expected(ds_version, expected_ds_version, label="DS version")
    _require_expected(bundle_name, expected_bundle, label="bundle")
    _require_expected(
        wheel_filename,
        expected_wheel_filename,
        label="wheel filename",
    )
    _require_expected(wheel_sha256, expected_wheel_sha256, label="wheel sha256")
    return ConformanceBundleEvidenceSummary(
        schema_version=_integer(evidence.get("schema_version"), label="schema_version"),
        ds_version=ds_version,
        bundle=bundle_name,
        wheel_filename=wheel_filename,
        wheel_sha256=wheel_sha256,
        receipt_digest=receipt_digest,
        required_actions=required_actions,
        authoring_mode=scenario_authoring_mode,
    )


def _require_expected(actual: str, expected: str | None, *, label: str) -> None:
    if expected is not None and actual != expected:
        message = f"{label} must equal expected value {expected!r}"
        raise ValueError(message)


def _validate_runner(
    value: object,
    *,
    cli_version: str,
) -> tuple[str, str]:
    runner = _mapping(value, label="runner")
    _exact_keys(
        runner,
        {"artifact", "cli_version", "wheel_filename", "wheel_sha256"},
        label="runner",
    )
    _constant(
        runner.get("artifact"),
        "installed-wheel-console-script",
        label="runner artifact",
    )
    _constant(
        runner.get("cli_version"),
        cli_version,
        label="runner cli_version",
    )
    filename = _text(runner.get("wheel_filename"), label="runner wheel_filename")
    if not filename.endswith(".whl") or Path(filename).name != filename:
        message = "runner wheel_filename must be a wheel basename"
        raise ValueError(message)
    digest = _sha256(runner.get("wheel_sha256"), label="runner wheel_sha256")
    return filename, digest


def _validate_scenario(
    value: object,
    *,
    bundle_name: str,
) -> tuple[str, tuple[str, ...]]:
    scenario = _mapping(value, label="scenario")
    _exact_keys(
        scenario,
        {
            "schema_version",
            "id",
            "bundle",
            "authoring_mode",
            "required_outcomes",
            "observed_outcomes",
            "digest",
        },
        label="scenario",
    )
    _constant(scenario.get("schema_version"), 1, label="scenario schema_version")
    scenario_id = _text(scenario.get("id"), label="scenario id")
    _constant(
        scenario_id,
        _SCENARIO_IDS[bundle_name],
        label="scenario id",
    )
    _constant(scenario.get("bundle"), bundle_name, label="scenario bundle")
    authoring_mode = _text(
        scenario.get("authoring_mode"),
        label="scenario authoring_mode",
    )
    if authoring_mode not in _AUTHORING_MODES:
        message = "scenario authoring_mode must be typed or opaque"
        raise ValueError(message)
    _constant(
        authoring_mode,
        _SCENARIO_AUTHORING_MODES[bundle_name],
        label="scenario authoring_mode",
    )
    required_outcomes = tuple(
        _text_sequence(
            scenario.get("required_outcomes"),
            label="scenario required_outcomes",
        )
    )
    _constant(
        required_outcomes,
        _SCENARIO_OUTCOMES[bundle_name],
        label="scenario required_outcomes",
    )
    observed_outcomes = tuple(
        _text_sequence(
            scenario.get("observed_outcomes"),
            label="scenario observed_outcomes",
        )
    )
    _constant(
        observed_outcomes,
        required_outcomes,
        label="scenario observed_outcomes",
    )
    definition = {
        "schema_version": 1,
        "id": scenario_id,
        "bundle": bundle_name,
        "authoring_mode": authoring_mode,
        "required_outcomes": list(required_outcomes),
    }
    _constant(
        scenario.get("digest"),
        _canonical_digest(definition),
        label="scenario digest",
    )
    return authoring_mode, required_outcomes


def _validate_evidence_scope(value: object, *, authoring_mode: str) -> None:
    scope = _mapping(value, label="evidence_scope")
    expected = {
        "claim": "named-bundle-live-scenario",
        "observed_evidence": "live_smoke",
        "authoring_mode": authoring_mode,
        "facet_claims": [],
        "promotion_claimed": False,
        "support_level_changes": False,
        "tested_changes": False,
    }
    _exact_keys(scope, set(expected), label="evidence_scope")
    for field, expected_value in expected.items():
        _constant(scope.get(field), expected_value, label=f"evidence_scope {field}")


def _validate_fixture(value: object) -> None:
    fixture = _mapping(value, label="fixture")
    _exact_keys(
        fixture,
        {
            "manifest_sha256",
            "provisioner",
            "identity_hmac_sha256",
            "before_state_hmac_sha256",
            "after_state_hmac_sha256",
            "scheduled_workflow",
        },
        label="fixture",
    )
    _sha256(fixture.get("manifest_sha256"), label="fixture manifest_sha256")
    _constant(
        fixture.get("provisioner"),
        _FIXTURE_PROVISIONER,
        label="fixture provisioner",
    )
    _hmac_sha256(
        fixture.get("identity_hmac_sha256"),
        label="fixture identity_hmac_sha256",
    )
    before_hmac = _hmac_sha256(
        fixture.get("before_state_hmac_sha256"),
        label="fixture before_state_hmac_sha256",
    )
    after_hmac = _hmac_sha256(
        fixture.get("after_state_hmac_sha256"),
        label="fixture after_state_hmac_sha256",
    )
    if before_hmac != after_hmac:
        message = "fixture before/after state HMAC values differ"
        raise ValueError(message)
    _constant(
        fixture.get("scheduled_workflow"),
        True,
        label="fixture scheduled_workflow",
    )


def _validate_effects(
    value: object,
    *,
    bundle_name: str,
    ds_version: str,
    cleanup_full_versions: frozenset[str],
    cleanup_pre_delete_release_versions: frozenset[str],
) -> None:
    effects = _mapping(value, label="effects")
    _exact_keys(
        effects,
        {"remote_mutations", "gate_owned_only", "external_fixture_mutated"},
        label="effects",
    )
    expected_mutations = (
        11
        if bundle_name == "full_core/v1"
        and ds_version in cleanup_full_versions
        and ds_version in cleanup_pre_delete_release_versions
        else 9
        if bundle_name == "full_core/v1" and ds_version in cleanup_full_versions
        else 7
        if bundle_name == "full_core/v1"
        else 3
    )
    _constant(
        effects.get("remote_mutations"),
        expected_mutations,
        label="effects remote_mutations",
    )
    _constant(
        effects.get("gate_owned_only"),
        True,
        label="effects gate_owned_only",
    )
    _constant(
        effects.get("external_fixture_mutated"),
        False,
        label="effects external_fixture_mutated",
    )


def _validate_cleanup(value: object) -> None:
    cleanup = _mapping(value, label="cleanup")
    owned_resources = (
        "gate_owned_projects",
        "gate_owned_workflows",
        "gate_owned_tasks",
    )
    _exact_keys(cleanup, {*owned_resources, "external_fixture"}, label="cleanup")
    for resource in owned_resources:
        state = _mapping(cleanup.get(resource), label=f"cleanup {resource}")
        _exact_keys(state, {"confirmed", "leftovers"}, label=f"cleanup {resource}")
        _constant(
            state.get("confirmed"),
            True,
            label=f"cleanup {resource} confirmed",
        )
        _constant(
            state.get("leftovers"),
            0,
            label=f"cleanup {resource} leftovers",
        )
    external = _mapping(
        cleanup.get("external_fixture"),
        label="cleanup external_fixture",
    )
    _exact_keys(
        external,
        {"scope", "mutated_by_gate", "state_hmac_matched"},
        label="cleanup external_fixture",
    )
    _constant(
        external.get("scope"),
        "externally-managed-read-only",
        label="cleanup external_fixture scope",
    )
    _constant(
        external.get("mutated_by_gate"),
        False,
        label="cleanup external_fixture mutated_by_gate",
    )
    _constant(
        external.get("state_hmac_matched"),
        True,
        label="cleanup external_fixture state_hmac_matched",
    )
