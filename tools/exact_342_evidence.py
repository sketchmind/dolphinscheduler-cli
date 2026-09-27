from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from live_gate.evidence_sanitization import reject_sensitive_text
from live_gate.evidence_values import (
    require_constant_equal as _require_constant,
)
from live_gate.evidence_values import required_bool as _required_bool
from live_gate.evidence_values import (
    required_hmac_sha256 as _required_hmac_sha256,
)
from live_gate.evidence_values import required_int as _required_int
from live_gate.evidence_values import required_mapping as _required_mapping
from live_gate.evidence_values import required_text as _required_text
from live_gate.evidence_values import required_timestamp as _required_timestamp

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_EXPECTED_PERSONA = "etl-developer"
_EXPECTED_CURRENT_PROVISIONER = "dsmatrix-exact-read-state-projection/v2"
_IMAGE_TAG = re.compile(r"^(?P<repository>\S+):3\.4\.2$")
_IMAGE_DIGEST = re.compile(r"^(?P<repository>\S+)@sha256:(?P<digest>[0-9a-f]{64})$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)
EXACT_342_GATE_RECIPES = (
    ("doctor", "identity.current"),
    ("project.create", "project.create"),
    ("project.delete", "project.delete"),
    ("project.get", "project.get"),
    ("project.list", "project.page"),
    ("project.update", "project.update"),
    ("schedule.list", "schedule.page"),
    ("workflow.describe", "workflow.describe"),
    ("workflow.digest", "workflow.digest"),
    ("workflow.export", "workflow.export"),
    ("workflow.get", "workflow.get"),
    ("workflow.list", "workflow.page"),
    ("task.list", "task.list"),
    ("task.get", "task.get"),
    ("task.update", "task.update"),
)
EXACT_342_GATE_ACTIONS = tuple(
    action for action, _semantic_operation in EXACT_342_GATE_RECIPES
)
CURRENT_EXACT_342_SCHEMA_VERSION = 7
SUPPORTED_EXACT_342_SCHEMA_VERSIONS = frozenset(
    {3, 4, 5, 6, CURRENT_EXACT_342_SCHEMA_VERSION}
)
_SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS = frozenset({1, 2})
_REQUIRED_LIVE_SMOKE_ACTIONS = frozenset(EXACT_342_GATE_ACTIONS[:-3])
_FINGERPRINT_KEYS = {
    "consumed_projection",
    "effective_wire",
    "preservation",
    "source",
}
_SCHEMA_3_SEMANTIC_OPERATIONS = frozenset(
    {
        "identity.current",
        "project.create",
        "project.delete",
        "project.get",
        "project.page",
        "project.update",
        "schedule.page",
        "workflow.get",
        "workflow.inspect",
        "workflow.page",
    }
)
_SCHEMA_4_REQUIRED_SEMANTIC_OPERATIONS = _SCHEMA_3_SEMANTIC_OPERATIONS | {
    "task.get",
    "task.update",
}
_SENSITIVE_KEYS = frozenset(
    {
        "api_token",
        "api_url",
        "authorization",
        "credential",
        "env_file",
        "password",
        "project_code",
        "raw_argv",
        "schedule_id",
        "secret",
        "stderr",
        "stdout",
        "task_code",
        "token",
        "url",
        "workflow_code",
    }
)
_EVIDENCE_KEYS = {
    "schema_version",
    "sanitization_schema_version",
    "gate",
    "status",
    "recorded_at",
    "runner",
    "dolphinscheduler",
    "profile",
    "contract",
    "contract_operation_test",
    "fixture",
    "operation_trace",
    "cleanup",
    "secrets_recorded",
}
_CONTRACT_KEYS = {
    "ds_version",
    "selection",
    "semantic_operations",
    "source_tag",
    "source_commit",
    "source_tree",
    "source_contract_digest",
    "rendered_contract_digest",
    "operation_count",
}


def canonical_gate_bundle_digest(value: Mapping[str, object]) -> str:
    """Return the stable digest for the exact-3.4.2 gate recipe bundle."""
    raw = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


@dataclass(frozen=True)
class _TraceEntry:
    sequence: int
    argv_shape: str
    action: str
    exit_code: int
    ok: bool
    assertions: tuple[str, ...]
    selector_kind: str | None
    error_type: str | None

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            message = "operation trace sequence must be positive"
            raise ValueError(message)
        if not self.assertions:
            message = "operation trace must retain at least one assertion"
            raise ValueError(message)
        if self.ok and self.error_type is not None:
            message = "successful operation trace cannot define error_type"
            raise ValueError(message)
        if not self.ok and not self.error_type:
            message = "failed operation trace requires error_type"
            raise ValueError(message)
        if self.ok != (self.exit_code == 0):
            message = "operation trace ok state must match its exit code"
            raise ValueError(message)


def validate_exact_342_evidence_payload(
    value: object,
    *,
    expected_cli_version: str,
    compiled_semantic_operations: frozenset[str] = frozenset(),
) -> None:
    """Validate the receipt, with independently proven current compiled roots."""
    evidence = _required_mapping(value, label="exact 3.4.2 evidence")
    schema_version = _evidence_schema_version(evidence.get("schema_version"))
    compiled_operations = (
        compiled_semantic_operations
        if schema_version == CURRENT_EXACT_342_SCHEMA_VERSION
        else frozenset()
    )
    evidence_keys = set(_EVIDENCE_KEYS)
    if schema_version >= 6:
        evidence_keys.add("gate_bundle")
    _require_exact_keys(evidence, evidence_keys, label="evidence")
    _reject_sensitive_keys(evidence)
    reject_sensitive_text(evidence)
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True).lower()
    if "http://" in raw or "https://" in raw:
        message = "Live evidence must not contain URLs"
        raise ValueError(message)
    _require_constant(
        evidence.get("sanitization_schema_version"),
        1,
        label="sanitization_schema_version",
    )
    _require_constant(evidence.get("gate"), "exact-profile-3.4.2", label="gate")
    _require_constant(evidence.get("status"), "passed", label="status")
    recorded_at = _required_timestamp(evidence.get("recorded_at"), label="recorded_at")
    if evidence.get("secrets_recorded") is not False:
        message = "Passing live evidence must declare secrets_recorded=false"
        raise ValueError(message)

    runner = _required_mapping(evidence.get("runner"), label="runner")
    _require_exact_keys(
        runner,
        {"artifact", "cli_version", "wheel_filename", "wheel_sha256"},
        label="runner",
    )
    _require_constant(
        runner.get("artifact"),
        "installed-wheel-console-script",
        label="runner artifact",
    )
    _require_constant(
        runner.get("cli_version"),
        expected_cli_version,
        label="runner cli_version for current package version",
    )
    wheel_filename = _required_text(
        runner.get("wheel_filename"),
        label="runner wheel_filename",
    )
    if (
        not wheel_filename.endswith(".whl")
        or Path(wheel_filename).name != wheel_filename
    ):
        message = "runner wheel_filename must be a wheel basename"
        raise ValueError(message)
    _required_sha256(runner.get("wheel_sha256"), label="runner wheel_sha256")

    cluster = _required_mapping(
        evidence.get("dolphinscheduler"),
        label="dolphinscheduler",
    )
    _require_exact_keys(
        cluster,
        {
            "release",
            "image_tag",
            "image_digest",
            "image_source",
            "image_observed_at",
            "api_target_hmac_sha256",
            "principal_hmac_sha256",
            "persona",
        },
        label="dolphinscheduler",
    )
    _require_constant(cluster.get("release"), "3.4.2", label="DS release")
    _validate_image_identity(
        image_tag=_required_text(cluster.get("image_tag"), label="image_tag"),
        image_digest=_required_text(cluster.get("image_digest"), label="image_digest"),
    )
    _required_text(cluster.get("image_source"), label="image_source")
    image_observed_at = _required_timestamp(
        cluster.get("image_observed_at"),
        label="image_observed_at",
    )
    _validate_observation_window(image_observed_at, reference=recorded_at)
    _required_hmac_sha256(
        cluster.get("api_target_hmac_sha256"),
        label="api_target_hmac_sha256",
    )
    _required_hmac_sha256(
        cluster.get("principal_hmac_sha256"),
        label="principal_hmac_sha256",
    )
    _require_constant(cluster.get("persona"), _EXPECTED_PERSONA, label="persona")

    _validate_profile(evidence.get("profile"), schema_version=schema_version)

    semantic_operations = _validate_contract(
        evidence.get("contract"),
        schema_version=schema_version,
        compiled_operations=compiled_operations,
    )
    if schema_version >= 6:
        _validate_gate_bundle(
            evidence.get("gate_bundle"),
            contract_operations=frozenset(semantic_operations) | compiled_operations,
        )
    _require_constant(
        evidence.get("contract_operation_test"),
        "tests/upstream/test_ds_3_4_2.py",
        label="contract_operation_test",
    )

    _validate_fixture(evidence.get("fixture"), schema_version=schema_version)

    trace_values = evidence.get("operation_trace")
    if not isinstance(trace_values, list):
        message = "operation_trace must be an array"
        raise TypeError(message)
    operation_trace = tuple(_trace_entry(item) for item in trace_values)
    _validate_trace(operation_trace, require_task_round_trip=schema_version >= 4)
    _validate_cleanup(
        evidence.get("cleanup"),
        require_task_round_trip=schema_version >= 4,
    )


def _validate_contract(
    value: object,
    *,
    schema_version: int,
    compiled_operations: frozenset[str],
) -> list[str]:
    contract = _required_mapping(value, label="contract")
    contract_keys = set(_CONTRACT_KEYS)
    if schema_version >= 6:
        contract_keys.add("bundle_manifest_schema_version")
    _require_exact_keys(contract, contract_keys, label="contract")
    if schema_version >= 6:
        bundle_manifest_schema_version = _required_int(
            contract.get("bundle_manifest_schema_version"),
            label="contract bundle manifest schema",
        )
        if (
            bundle_manifest_schema_version
            not in _SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS
        ):
            message = "contract bundle manifest schema must be 1 or 2"
            raise ValueError(message)
    _require_constant(contract.get("ds_version"), "3.4.2", label="contract DS")
    _require_constant(
        contract.get("selection"),
        "runtime-slice",
        label="contract selection",
    )
    semantic_operations = _required_text_list(
        contract.get("semantic_operations"),
        label="contract semantic_operations",
        allow_empty=schema_version == CURRENT_EXACT_342_SCHEMA_VERSION,
    )
    _validate_contract_semantics(
        semantic_operations,
        operation_count=contract.get("operation_count"),
        schema_version=schema_version,
        compiled_operations=compiled_operations,
    )
    for key in ("source_tag", "source_commit", "source_tree"):
        _required_text(contract.get(key), label=f"contract {key}")
    _required_sha256(
        contract.get("source_contract_digest"),
        label="contract source_contract_digest",
    )
    _required_sha256(
        contract.get("rendered_contract_digest"),
        label="contract rendered_contract_digest",
    )
    return semantic_operations


def _evidence_schema_version(value: object) -> int:
    schema_version = _required_int(value, label="schema_version")
    if schema_version not in SUPPORTED_EXACT_342_SCHEMA_VERSIONS:
        supported = ", ".join(
            str(version) for version in sorted(SUPPORTED_EXACT_342_SCHEMA_VERSIONS)
        )
        message = f"schema_version must equal one of {supported}"
        raise ValueError(message)
    return schema_version


def _validate_contract_semantics(
    semantic_operations: list[str],
    *,
    operation_count: object,
    schema_version: int,
    compiled_operations: frozenset[str],
) -> None:
    operation_set = frozenset(semantic_operations)
    if len(operation_set) != len(semantic_operations):
        message = "contract semantic_operations must not contain duplicates"
        raise ValueError(message)
    generated_operation_count = _required_int(
        operation_count,
        label="operation_count",
    )
    if generated_operation_count < 0:
        message = "contract operation_count must be nonnegative"
        raise ValueError(message)
    if (
        schema_version != CURRENT_EXACT_342_SCHEMA_VERSION
        and generated_operation_count == 0
    ):
        message = "contract operation_count must be positive"
        raise ValueError(message)
    if schema_version == 3 and operation_set != _SCHEMA_3_SEMANTIC_OPERATIONS:
        message = "Schema-v3 evidence is bound to the historical ten-operation contract"
        raise ValueError(message)
    if schema_version == 3 and generated_operation_count != len(semantic_operations):
        message = "Schema-v3 evidence requires its historical operation count"
        raise ValueError(message)
    if schema_version >= 4 and not (operation_set | compiled_operations).issuperset(
        _SCHEMA_4_REQUIRED_SEMANTIC_OPERATIONS
    ):
        message = "Schema-v4 evidence requires the exact task-definition operations"
        raise ValueError(message)


def _validate_profile(value: object, *, schema_version: int) -> None:
    profile = _required_mapping(value, label="profile")
    expected_profile: dict[str, object] = {
        "ds": "3.4.2",
        "selected_ds_version": "3.4.2",
        "contract_version": "3.4.2",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
    }
    if schema_version >= 7:
        expected_profile["fingerprints"] = _validate_fingerprints(
            profile.get("fingerprints"),
            label="profile fingerprints",
        )
    elif schema_version >= 5:
        # Historical receipt values are schema data, not current class names.
        expected_profile.update(
            {
                "inspection_adapter": "DS342Adapter",
                "full_adapter": False,
                "read_adapter": "GeneratedReadAdapter",
                "project_domain": "project",
                "project_domain_adapter": "_CodeProjectDomainAdapter",
                "identity_adapter": "GeneratedIdentityAdapter",
                "task_definition_adapter": "GeneratedTaskDefinitionAdapter",
            }
        )
        if schema_version >= 6:
            expected_profile["fingerprints"] = _validate_fingerprints(
                profile.get("fingerprints"),
                label="profile fingerprints",
            )
    else:
        expected_profile.update(
            {
                "inspection_adapter": "DS342Adapter",
                "full_adapter": False,
                "read_adapter": "DS342Adapter",
                "project_adapter": "DS342Adapter",
                "identity_adapter": "DS342Adapter",
            }
        )
    if schema_version == 4:
        expected_profile["task_definition_adapter"] = "DS342Adapter"
    if profile != expected_profile:
        message = "Evidence profile does not describe the exact 3.4.2 semantic profile"
        raise ValueError(message)


def _validate_gate_bundle(
    value: object,
    *,
    contract_operations: frozenset[str],
) -> None:
    gate_bundle = _required_mapping(value, label="gate_bundle")
    _require_exact_keys(
        gate_bundle,
        {"actions", "action_verifications", "recipes", "digest"},
        label="gate_bundle",
    )
    actions = _required_text_list(gate_bundle.get("actions"), label="gate actions")
    if tuple(actions) != EXACT_342_GATE_ACTIONS:
        message = "gate bundle must bind the fixed 15 exact 3.4.2 actions"
        raise ValueError(message)
    action_verifications = _required_mapping(
        gate_bundle.get("action_verifications"),
        label="gate action verifications",
    )
    _require_exact_keys(
        action_verifications,
        set(EXACT_342_GATE_ACTIONS),
        label="gate action verifications",
    )
    for action in EXACT_342_GATE_ACTIONS:
        _require_constant(
            action_verifications.get(action),
            "live_smoke",
            label=f"gate action verification {action}",
        )

    raw_recipes = gate_bundle.get("recipes")
    if not isinstance(raw_recipes, list):
        message = "gate recipes must be an array"
        raise TypeError(message)
    if len(raw_recipes) != len(EXACT_342_GATE_RECIPES):
        message = "gate recipes must bind exactly 15 actions"
        raise ValueError(message)
    recipes: list[dict[str, object]] = []
    for raw_recipe, (expected_action, expected_operation) in zip(
        raw_recipes,
        EXACT_342_GATE_RECIPES,
        strict=True,
    ):
        recipe = _required_mapping(raw_recipe, label="gate recipe")
        _require_exact_keys(
            recipe,
            {"action", "semantic_operation", "build_status", "fingerprints"},
            label="gate recipe",
        )
        _require_constant(
            recipe.get("action"),
            expected_action,
            label="gate recipe action",
        )
        _require_constant(
            recipe.get("semantic_operation"),
            expected_operation,
            label="gate recipe semantic_operation",
        )
        _require_constant(
            recipe.get("build_status"),
            "accepted",
            label="gate recipe build_status",
        )
        _validate_fingerprints(
            recipe.get("fingerprints"),
            label=f"gate recipe {expected_action} fingerprints",
        )
        recipes.append(recipe)
    semantic_operations = {
        semantic_operation for _action, semantic_operation in EXACT_342_GATE_RECIPES
    }
    if not semantic_operations <= contract_operations:
        message = "gate recipes are not present in the runtime-slice manifest"
        raise ValueError(message)
    digest_payload: dict[str, object] = {
        "actions": actions,
        "action_verifications": action_verifications,
        "recipes": recipes,
    }
    _require_constant(
        gate_bundle.get("digest"),
        canonical_gate_bundle_digest(digest_payload),
        label="gate bundle digest",
    )


def _validate_fingerprints(value: object, *, label: str) -> dict[str, str]:
    fingerprints = _required_mapping(value, label=label)
    _require_exact_keys(fingerprints, _FINGERPRINT_KEYS, label=label)
    return {
        key: _required_sha256(fingerprints.get(key), label=f"{label} {key}")
        for key in sorted(_FINGERPRINT_KEYS)
    }


def _validate_fixture(value: object, *, schema_version: int) -> None:
    fixture = _required_mapping(value, label="fixture")
    fixture_keys = {"manifest_sha256", "provisioner", "scheduled_workflow"}
    if schema_version >= 4:
        fixture_keys.update({"editable_offline_shell_task", "exclusive_fixture"})
    _require_exact_keys(fixture, fixture_keys, label="fixture")
    _required_sha256(fixture.get("manifest_sha256"), label="fixture manifest_sha256")
    provisioner = _required_text(
        fixture.get("provisioner"),
        label="fixture provisioner",
    )
    if schema_version >= 6:
        _require_constant(
            provisioner,
            _EXPECTED_CURRENT_PROVISIONER,
            label="fixture provisioner",
        )
    if fixture.get("scheduled_workflow") is not True:
        message = "Exact 3.4.2 evidence requires a scheduled workflow fixture"
        raise ValueError(message)
    if schema_version >= 4 and fixture.get("editable_offline_shell_task") is not True:
        message = "Exact 3.4.2 evidence requires an editable OFFLINE SHELL task"
        raise ValueError(message)
    if schema_version >= 4 and fixture.get("exclusive_fixture") is not True:
        message = "Exact 3.4.2 evidence requires an exclusive external fixture"
        raise ValueError(message)


def _validate_cleanup(value: object, *, require_task_round_trip: bool) -> None:
    cleanup = _required_mapping(value, label="cleanup")
    _require_exact_keys(
        cleanup,
        {"gate_created_projects", "external_fixture"},
        label="cleanup",
    )
    gate_projects = _required_mapping(
        cleanup.get("gate_created_projects"),
        label="cleanup gate_created_projects",
    )
    _require_exact_keys(
        gate_projects,
        {"confirmed", "leftovers"},
        label="cleanup gate_created_projects",
    )
    if (
        _required_bool(gate_projects.get("confirmed"), label="cleanup confirmed")
        is not True
        or _required_int(gate_projects.get("leftovers"), label="cleanup leftovers") != 0
    ):
        message = "Passing cleanup attestation requires zero gate-created projects"
        raise ValueError(message)
    external_fixture = _required_mapping(
        cleanup.get("external_fixture"),
        label="cleanup external_fixture",
    )
    if not require_task_round_trip:
        _require_exact_keys(
            external_fixture,
            {"scope", "mutated_by_gate"},
            label="cleanup external_fixture",
        )
        _require_constant(
            external_fixture.get("scope"),
            "externally-managed-read-only",
            label="external fixture scope",
        )
        if external_fixture.get("mutated_by_gate") is not False:
            message = "Exact read fixture must not be mutated by the legacy gate"
            raise ValueError(message)
        return

    _require_exact_keys(
        external_fixture,
        {
            "scope",
            "mutated_by_gate",
            "restored_by_gate",
            "exact_command_restored",
            "restored_dag_version_consistent",
            "non_owned_fields_restored",
            "dag_topology_preserved",
            "workflow_release_state_preserved",
        },
        label="cleanup external_fixture",
    )
    _require_constant(
        external_fixture.get("scope"),
        "externally-managed-editable-offline-shell-task",
        label="external fixture scope",
    )
    for key in (
        "mutated_by_gate",
        "restored_by_gate",
        "exact_command_restored",
        "restored_dag_version_consistent",
        "non_owned_fields_restored",
        "dag_topology_preserved",
        "workflow_release_state_preserved",
    ):
        if (
            _required_bool(
                external_fixture.get(key),
                label=f"cleanup external_fixture {key}",
            )
            is not True
        ):
            message = f"Passing exact task cleanup requires {key}=true"
            raise ValueError(message)


def _trace_entry(value: object) -> _TraceEntry:
    data = _required_mapping(value, label="operation trace entry")
    required = {"sequence", "argv_shape", "action", "exit_code", "ok", "assertions"}
    optional = {"selector_kind", "error_type"}
    missing = sorted(required - data.keys())
    extra = data.keys() - required - optional
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if extra:
            details.append("extra: " + ", ".join(sorted(extra)))
        message = "operation trace entry has invalid fields; " + "; ".join(details)
        raise ValueError(message)
    assertion_values = data.get("assertions")
    if (
        not isinstance(assertion_values, list)
        or not assertion_values
        or not all(isinstance(item, str) and item for item in assertion_values)
    ):
        message = "operation trace assertions must be a non-empty string list"
        raise ValueError(message)
    selector = data.get("selector_kind")
    error_type = data.get("error_type")
    return _TraceEntry(
        sequence=_required_int(data.get("sequence"), label="trace sequence"),
        argv_shape=_required_text(data.get("argv_shape"), label="trace argv_shape"),
        action=_required_text(data.get("action"), label="trace action"),
        exit_code=_required_int(data.get("exit_code"), label="trace exit_code"),
        ok=_required_bool(data.get("ok"), label="trace ok"),
        assertions=tuple(assertion_values),
        selector_kind=(
            None
            if selector is None
            else _required_text(selector, label="trace selector_kind")
        ),
        error_type=(
            None
            if error_type is None
            else _required_text(error_type, label="trace error_type")
        ),
    )


def _validate_trace(
    operation_trace: Sequence[_TraceEntry],
    *,
    require_task_round_trip: bool,
) -> None:
    if [entry.sequence for entry in operation_trace] != list(
        range(1, len(operation_trace) + 1)
    ):
        message = "Operation trace sequence must be contiguous and ordered"
        raise ValueError(message)
    successful_actions = {entry.action for entry in operation_trace if entry.ok}
    required_actions = set(_REQUIRED_LIVE_SMOKE_ACTIONS)
    if require_task_round_trip:
        required_actions.update({"task.list", "task.get", "task.update"})
    missing_actions = sorted(required_actions - successful_actions)
    if missing_actions:
        message = "Live evidence is missing successful actions: " + ", ".join(
            missing_actions
        )
        raise ValueError(message)
    required_outcomes = {
        "bound-current-user": any(
            entry.action == "doctor"
            and entry.ok
            and {"principal-hmac-matched", "general-user-confirmed"}
            <= set(entry.assertions)
            for entry in operation_trace
        ),
        "duplicate-project-conflict": any(
            entry.action == "project.create"
            and not entry.ok
            and entry.error_type == "conflict"
            and {"stable-conflict", "actionable-suggestion"} <= set(entry.assertions)
            for entry in operation_trace
        ),
        "missing-workflow-not-found": any(
            entry.action == "workflow.get"
            and not entry.ok
            and entry.error_type == "not_found"
            and "stable-not-found" in entry.assertions
            for entry in operation_trace
        ),
        "post-delete-project-not-found": any(
            entry.action == "project.get"
            and not entry.ok
            and entry.error_type == "not_found"
            and "stable-not-found-after-delete" in entry.assertions
            for entry in operation_trace
        ),
        "project-cleanup-zero": any(
            entry.action == "project.list"
            and entry.ok
            and "cleanup-leftovers-zero" in entry.assertions
            for entry in operation_trace
        ),
        "workflow-dag-inspected": any(
            entry.action == "workflow.describe"
            and entry.ok
            and {
                "dag-task-set-nonempty",
                "dag-relations-valid",
                "schedule-hydrated",
            }
            <= set(entry.assertions)
            for entry in operation_trace
        ),
        "workflow-digest-cross-checked": any(
            entry.action == "workflow.digest"
            and entry.ok
            and {
                "digest-counts-match-describe",
                "digest-topology-match-describe",
            }
            <= set(entry.assertions)
            for entry in operation_trace
        ),
        "workflow-yaml-cross-checked": any(
            entry.action == "workflow.export"
            and entry.ok
            and {
                "yaml-dag-matches-describe",
                "yaml-schedule-matches-live",
            }
            <= set(entry.assertions)
            for entry in operation_trace
        ),
        "workflow-schedule-filter-cross-checked": any(
            entry.action == "schedule.list"
            and entry.ok
            and {
                "fixture-schedule-id-matched",
                "workflow-filter-matched",
            }
            <= set(entry.assertions)
            for entry in operation_trace
        ),
    }
    if require_task_round_trip:
        required_outcomes.update(
            {
                "editable-shell-task-matched": any(
                    entry.action == "task.list"
                    and entry.ok
                    and "editable-shell-task-matched" in entry.assertions
                    for entry in operation_trace
                ),
                "task-get-matched-list-version": any(
                    entry.action == "task.get"
                    and entry.ok
                    and "task-get-matched-list-version" in entry.assertions
                    for entry in operation_trace
                ),
                "exact-task-update-dry-run": any(
                    entry.action == "task.update"
                    and entry.ok
                    and {
                        "exact-update-request-compiled",
                        "dry-run-no-request-sent",
                        "dry-run-state-unchanged",
                        "dry-run-dag-unchanged",
                    }
                    <= set(entry.assertions)
                    for entry in operation_trace
                ),
                "task-update-and-readback": any(
                    entry.action == "task.update"
                    and entry.ok
                    and {"command-updated-exactly", "task-version-advanced"}
                    <= set(entry.assertions)
                    for entry in operation_trace
                )
                and any(
                    entry.action == "task.get"
                    and entry.ok
                    and "update-readback-exact" in entry.assertions
                    for entry in operation_trace
                ),
                "dag-task-version-reconciled": any(
                    entry.action == "task.list"
                    and entry.ok
                    and "dag-task-version-matched-readback" in entry.assertions
                    for entry in operation_trace
                ),
                "non-owned-fields-preserved": any(
                    entry.action == "task.get"
                    and entry.ok
                    and "non-owned-fields-preserved" in entry.assertions
                    for entry in operation_trace
                ),
                "dag-topology-preserved": any(
                    entry.action == "workflow.digest"
                    and entry.ok
                    and "task-update-topology-preserved" in entry.assertions
                    for entry in operation_trace
                ),
                "exact-task-restoration": any(
                    entry.action == "task.update"
                    and entry.ok
                    and "original-command-restored-exactly" in entry.assertions
                    for entry in operation_trace
                )
                and any(
                    entry.action == "task.get"
                    and entry.ok
                    and "original-command-readback-exact" in entry.assertions
                    for entry in operation_trace
                )
                and any(
                    entry.action == "task.list"
                    and entry.ok
                    and "restored-dag-version-consistent" in entry.assertions
                    for entry in operation_trace
                ),
                "restored-non-owned-fields": any(
                    entry.action == "task.get"
                    and entry.ok
                    and "restored-non-owned-fields" in entry.assertions
                    for entry in operation_trace
                ),
                "restored-dag-topology": any(
                    entry.action == "workflow.digest"
                    and entry.ok
                    and "restored-dag-topology-preserved" in entry.assertions
                    for entry in operation_trace
                ),
                "workflow-release-state-preserved": any(
                    entry.action == "workflow.get"
                    and entry.ok
                    and "workflow-release-state-preserved" in entry.assertions
                    for entry in operation_trace
                ),
            }
        )
    missing_outcomes = sorted(
        label for label, present in required_outcomes.items() if not present
    )
    if missing_outcomes:
        message = "Live evidence is missing required outcomes: " + ", ".join(
            missing_outcomes
        )
        raise ValueError(message)


def _validate_image_identity(*, image_tag: str, image_digest: str) -> None:
    tag_match = _IMAGE_TAG.fullmatch(image_tag)
    digest_match = _IMAGE_DIGEST.fullmatch(image_digest)
    if tag_match is None:
        message = "Cluster image tag must identify DolphinScheduler 3.4.2"
        raise ValueError(message)
    if digest_match is None:
        message = "Cluster image digest must include an immutable @sha256 identity"
        raise ValueError(message)
    if tag_match["repository"] != digest_match["repository"]:
        message = "Cluster image tag and digest must use the same repository"
        raise ValueError(message)


def _validate_observation_window(value: datetime, *, reference: datetime) -> None:
    observed_at = value.astimezone(UTC)
    normalized_reference = reference.astimezone(UTC)
    if observed_at > normalized_reference + _MAX_CLOCK_SKEW:
        message = "image_observed_at is implausibly later than the gate run"
        raise ValueError(message)
    if normalized_reference - observed_at > _MAX_OBSERVATION_AGE:
        message = "image_observed_at is too stale for exact 3.4.2 evidence"
        raise ValueError(message)


def _reject_sensitive_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in _SENSITIVE_KEYS:
                message = f"Live evidence contains forbidden field {key!r}"
                raise ValueError(message)
            _reject_sensitive_keys(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive_keys(item)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    label: str,
) -> None:
    missing = sorted(expected - value.keys())
    extra = sorted(value.keys() - expected)
    if not missing and not extra:
        return
    parts = []
    if missing:
        parts.append("missing: " + ", ".join(missing))
    if extra:
        parts.append("extra: " + ", ".join(extra))
    message = f"{label} has invalid fields; {'; '.join(parts)}"
    raise ValueError(message)


def _required_text_list(
    value: object, *, label: str, allow_empty: bool = False
) -> list[str]:
    if (
        not isinstance(value, list)
        or (not value and not allow_empty)
        or not all(isinstance(item, str) and item for item in value)
    ):
        message = f"{label} must be a non-empty string list"
        raise TypeError(message)
    if len(value) != len(set(value)):
        message = f"{label} must not contain duplicates"
        raise ValueError(message)
    return value


def _required_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        message = f"{label} must be a sha256 fingerprint"
        raise ValueError(message)
    return text
