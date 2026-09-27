"""Independent validator for generic exact-profile read receipts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from live_gate.evidence_sanitization import reject_sensitive_text
from live_gate.evidence_values import required_bool as _required_bool
from live_gate.evidence_values import (
    required_hmac_sha256 as _required_hmac_sha256,
)
from live_gate.evidence_values import required_int as _required_int
from live_gate.evidence_values import required_list as _required_sequence
from live_gate.evidence_values import required_mapping as _required_mapping
from live_gate.evidence_values import required_text as _required_text
from live_gate.evidence_values import required_utc_timestamp as _required_timestamp

if TYPE_CHECKING:
    from collections.abc import Mapping

EXACT_PROFILE_READ_RECIPES = (
    ("project.get", "project.get"),
    ("project.list", "project.page"),
    ("workflow.get", "workflow.get"),
    ("workflow.list", "workflow.page"),
)
EXACT_PROFILE_READ_ACTIONS = tuple(
    action for action, _operation in EXACT_PROFILE_READ_RECIPES
)
EXACT_PROFILE_READ_CAPABILITY_ACTIONS = (
    "project.list",
    "project.get",
    "workflow.list",
    "workflow.get",
)
CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION = 2
SUPPORTED_EXACT_PROFILE_READ_SCHEMA_VERSIONS = frozenset(
    {1, CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION}
)
_SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS = frozenset({1, 2})
_ACTION_VERIFICATIONS = frozenset(
    {"static", "contract_tested", "live_smoke", "live_full"}
)
_EXPECTED_TRACE_ACTIONS = (
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
)
_EXPECTED_TRACE_ARGV_SHAPES = (
    "version",
    "capabilities --action ACTION",
    "capabilities --action ACTION",
    "capabilities --action ACTION",
    "capabilities --action ACTION",
    "doctor",
    "project list --search PROJECT_NAME --page-no 1 --page-size 20",
    "project get PROJECT",
    "project get PROJECT",
    (
        "workflow list --project PROJECT --search WORKFLOW_NAME "
        "--page-no 1 --page-size 20"
    ),
    "workflow get WORKFLOW --project PROJECT",
    "workflow get WORKFLOW --project PROJECT",
)
_EXPECTED_TRACE_ASSERTIONS = (
    ("selected-contract-and-family-matched",),
    *((f"{action}-supported",) for action in EXACT_PROFILE_READ_CAPABILITY_ACTIONS),
    (),
    ("native-project-identity-matched", "pagination-search-matched"),
    ("native-project-identity-matched",),
    ("native-project-identity-matched",),
    ("native-workflow-identity-matched", "pagination-search-matched"),
    (
        "native-workflow-identity-matched",
        "schedule-hydrated",
        "release-state-matched",
    ),
    (
        "native-workflow-identity-matched",
        "schedule-hydrated",
        "release-state-matched",
    ),
)
_DOCTOR_ASSERTION_TAIL = (
    "current-user-ok",
    "general-user-confirmed",
    "principal-hmac-matched",
)
_EXPECTED_FINGERPRINT_KEYS = {
    "consumed_projection",
    "effective_wire",
    "preservation",
    "source",
}
_EXPECTED_PERSONA = "etl-developer"
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")
_SENSITIVE_KEYS = frozenset(
    {
        "api_token",
        "api_url",
        "authorization",
        "credential",
        "code",
        "env_file",
        "id",
        "password",
        "project_code",
        "project_id",
        "project_name",
        "raw_argv",
        "schedule_id",
        "secret",
        "stderr",
        "stdout",
        "token",
        "url",
        "value",
        "workflow_code",
        "workflow_id",
        "workflow_name",
    }
)
_EVIDENCE_KEYS = {
    "contract",
    "dolphinscheduler",
    "effects",
    "fixture",
    "gate",
    "operation_trace",
    "profile",
    "read_bundle",
    "recorded_at",
    "runner",
    "sanitization_schema_version",
    "schema_version",
    "secrets_recorded",
    "status",
}


def canonical_read_bundle_digest(value: Mapping[str, object]) -> str:
    """Return the stable digest for the receipt's bounded read recipe set."""
    raw = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def validate_exact_profile_read_evidence_payload(
    value: object,
    *,
    expected_cli_version: str,
    expected_ds_version: str,
    compiled_semantic_operations: frozenset[str] = frozenset(),
) -> None:
    """Validate a receipt; compiled roots require an independent current proof."""
    evidence = _required_mapping(value, label="exact-profile read evidence")
    _require_exact_keys(evidence, _EVIDENCE_KEYS, label="evidence")
    _reject_sensitive_keys(evidence)
    reject_sensitive_text(evidence)
    serialized = json.dumps(evidence, ensure_ascii=False, sort_keys=True).lower()
    if "http://" in serialized or "https://" in serialized:
        message = "exact-profile read evidence must not contain URLs"
        raise ValueError(message)

    schema_version = _required_int(
        evidence.get("schema_version"),
        label="schema_version",
    )
    if schema_version not in SUPPORTED_EXACT_PROFILE_READ_SCHEMA_VERSIONS:
        supported = " or ".join(
            str(version)
            for version in sorted(SUPPORTED_EXACT_PROFILE_READ_SCHEMA_VERSIONS)
        )
        message = f"schema_version must be {supported}"
        raise ValueError(message)
    _require_constant(
        evidence.get("sanitization_schema_version"),
        1,
        label="sanitization_schema_version",
    )
    _require_constant(evidence.get("gate"), "exact-profile-read", label="gate")
    _require_constant(evidence.get("status"), "passed", label="status")
    recorded_at = _required_timestamp(evidence.get("recorded_at"), label="recorded_at")
    if evidence.get("secrets_recorded") is not False:
        message = "passing read evidence must declare secrets_recorded=false"
        raise ValueError(message)

    _validate_runner(
        evidence.get("runner"),
        expected_cli_version=expected_cli_version,
    )
    _validate_cluster(
        evidence.get("dolphinscheduler"),
        expected_ds_version=expected_ds_version,
        recorded_at=recorded_at,
    )
    _validate_profile(
        evidence.get("profile"),
        expected_ds_version=expected_ds_version,
        schema_version=schema_version,
    )
    contract_operations = _validate_contract(
        evidence.get("contract"),
        expected_ds_version=expected_ds_version,
        schema_version=schema_version,
    )
    _validate_read_bundle(
        evidence.get("read_bundle"),
        contract_operations=contract_operations,
        compiled_semantic_operations=(
            compiled_semantic_operations
            if schema_version == CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION
            else frozenset()
        ),
        schema_version=schema_version,
    )
    _validate_fixture(
        evidence.get("fixture"),
        expected_ds_version=expected_ds_version,
    )
    _validate_trace(
        evidence.get("operation_trace"),
        expected_ds_version=expected_ds_version,
    )
    _validate_effects(evidence.get("effects"))


def _validate_runner(value: object, *, expected_cli_version: str) -> None:
    runner = _required_mapping(value, label="runner")
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
        label="runner cli_version",
    )
    filename = _required_text(runner.get("wheel_filename"), label="wheel_filename")
    if not filename.endswith(".whl") or Path(filename).name != filename:
        message = "runner wheel_filename must be a wheel basename"
        raise ValueError(message)
    _required_sha256(runner.get("wheel_sha256"), label="wheel_sha256")


def _validate_cluster(
    value: object,
    *,
    expected_ds_version: str,
    recorded_at: datetime,
) -> None:
    cluster = _required_mapping(value, label="dolphinscheduler")
    _require_exact_keys(
        cluster,
        {
            "api_target_hmac_sha256",
            "image_id",
            "image_observed_at",
            "image_ref",
            "image_source",
            "persona",
            "principal_hmac_sha256",
            "release",
        },
        label="dolphinscheduler",
    )
    _require_constant(cluster.get("release"), expected_ds_version, label="DS release")
    _validate_image_identity(
        image_ref=_required_text(cluster.get("image_ref"), label="image_ref"),
        image_id=_required_text(cluster.get("image_id"), label="image_id"),
        ds_version=expected_ds_version,
    )
    _required_text(cluster.get("image_source"), label="image_source")
    observed_at = _required_timestamp(
        cluster.get("image_observed_at"),
        label="image_observed_at",
    )
    _validate_observation_window(observed_at, reference=recorded_at)
    _required_hmac_sha256(
        cluster.get("api_target_hmac_sha256"),
        label="api_target_hmac_sha256",
    )
    _required_hmac_sha256(
        cluster.get("principal_hmac_sha256"),
        label="principal_hmac_sha256",
    )
    _require_constant(cluster.get("persona"), _EXPECTED_PERSONA, label="persona")


def _validate_profile(
    value: object,
    *,
    expected_ds_version: str,
    schema_version: int,
) -> None:
    profile = _required_mapping(value, label="profile")
    profile_keys = {
        "contract_version",
        "ds",
        "family",
        "fingerprints",
        "selected_ds_version",
        "support_level",
        "tested",
    }
    if schema_version == 1:
        profile_keys.update(
            {
                "full_adapter",
                "identity_adapter",
                "project_domain",
                "project_domain_adapter",
                "read_adapter",
            }
        )
    _require_exact_keys(profile, profile_keys, label="profile")
    for key in ("ds", "selected_ds_version", "contract_version"):
        _require_constant(profile.get(key), expected_ds_version, label=f"profile {key}")
    _required_text(profile.get("family"), label="profile family")
    support_level = _required_text(
        profile.get("support_level"),
        label="profile support_level",
    )
    if support_level not in {"experimental", "full", "legacy_core"}:
        message = "profile support_level is not recognized"
        raise ValueError(message)
    tested = _required_bool(profile.get("tested"), label="profile tested")
    if (support_level in {"full", "legacy_core"}) != tested:
        message = "promoted profile support and tested flag are inconsistent"
        raise ValueError(message)
    if schema_version == 1:
        # Historical receipt values are schema data, not current class names.
        _require_constant(
            profile.get("project_domain"),
            "project",
            label="project_domain",
        )
        _require_constant(
            profile.get("identity_adapter"),
            "GeneratedIdentityAdapter",
            label="identity_adapter",
        )
        expected_read_adapter = (
            "GeneratedLegacyReadAdapter"
            if expected_ds_version == "1.3.9"
            else "GeneratedReadAdapter"
        )
        _require_constant(
            profile.get("read_adapter"),
            expected_read_adapter,
            label="read_adapter",
        )
        expected_project_adapter = (
            "_LegacyProjectDomainAdapter"
            if expected_ds_version == "1.3.9"
            else "_CodeProjectDomainAdapter"
        )
        _require_constant(
            profile.get("project_domain_adapter"),
            expected_project_adapter,
            label="project_domain_adapter",
        )
        _require_constant(
            profile.get("full_adapter"),
            expected_ds_version == "3.4.1",
            label="full_adapter",
        )
    _validate_fingerprints(profile.get("fingerprints"), label="profile fingerprints")


def _validate_contract(
    value: object,
    *,
    expected_ds_version: str,
    schema_version: int,
) -> set[str]:
    contract = _required_mapping(value, label="contract")
    _require_exact_keys(
        contract,
        {
            "bundle_manifest_schema_version",
            "ds_version",
            "operation_count",
            "rendered_contract_digest",
            "selection",
            "semantic_operations",
            "source_commit",
            "source_contract_digest",
            "source_tag",
            "source_tree",
        },
        label="contract",
    )
    _required_bundle_manifest_schema_version(
        contract.get("bundle_manifest_schema_version"),
        label="bundle manifest schema",
    )
    _require_constant(
        contract.get("ds_version"),
        expected_ds_version,
        label="contract DS",
    )
    _require_constant(
        contract.get("source_tag"),
        expected_ds_version,
        label="contract source_tag",
    )
    for key in ("source_commit", "source_tree"):
        text = _required_text(contract.get(key), label=f"contract {key}")
        if _GIT_OBJECT.fullmatch(text) is None:
            message = f"contract {key} must be a full Git object id"
            raise ValueError(message)
    _required_sha256(
        contract.get("source_contract_digest"),
        label="source_contract_digest",
    )
    _required_sha256(
        contract.get("rendered_contract_digest"),
        label="rendered_contract_digest",
    )
    operation_count = _required_int(
        contract.get("operation_count"),
        label="operation_count",
    )
    operations = _required_text_list(
        contract.get("semantic_operations"),
        label="contract semantic_operations",
        allow_empty=True,
    )
    selection = _required_text(contract.get("selection"), label="contract selection")
    minimum_operation_count = (
        0
        if schema_version == CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION
        and selection == "runtime-slice"
        else 1
    )
    if operation_count < minimum_operation_count:
        message = "contract operation_count is invalid for its schema and selection"
        raise ValueError(message)
    if schema_version == 1:
        expected_selection = (
            "full" if expected_ds_version == "3.4.1" else "runtime-slice"
        )
        if selection != expected_selection:
            message = (
                f"DS {expected_ds_version} contract must use "
                f"{expected_selection} selection"
            )
            raise ValueError(message)
    elif selection not in {"full", "runtime-slice"}:
        message = "contract selection must be full or runtime-slice"
        raise ValueError(message)
    if selection == "full":
        if operations:
            message = (
                "full contract manifest must use an empty semantic operation root set"
            )
            raise ValueError(message)
    elif schema_version == 1 and not operations:
        message = (
            "runtime-slice contract manifest must include semantic operation roots"
        )
        raise ValueError(message)
    return set(operations)


def _validate_read_bundle(
    value: object,
    *,
    contract_operations: set[str],
    compiled_semantic_operations: frozenset[str],
    schema_version: int,
) -> None:
    read_bundle = _required_mapping(value, label="read_bundle")
    expected_keys = {"actions", "digest", "recipes"}
    if schema_version >= 2 or "action_verifications" in read_bundle:
        expected_keys.add("action_verifications")
    _require_exact_keys(
        read_bundle,
        expected_keys,
        label="read_bundle",
    )
    actions = _required_text_list(read_bundle.get("actions"), label="read actions")
    if tuple(actions) != EXACT_PROFILE_READ_ACTIONS:
        message = "read bundle must bind the fixed four read actions"
        raise ValueError(message)
    recipes = _required_sequence(read_bundle.get("recipes"), label="read recipes")
    normalized_recipes: list[dict[str, object]] = []
    if len(recipes) != len(EXACT_PROFILE_READ_RECIPES):
        message = "read recipes must bind exactly four actions"
        raise ValueError(message)
    for raw_recipe, (expected_action, expected_operation) in zip(
        recipes,
        EXACT_PROFILE_READ_RECIPES,
        strict=True,
    ):
        recipe = _required_mapping(raw_recipe, label="read recipe")
        _require_exact_keys(
            recipe,
            {"action", "build_status", "fingerprints", "semantic_operation"},
            label="read recipe",
        )
        _require_constant(
            recipe.get("build_status"),
            "accepted",
            label="read recipe build_status",
        )
        _require_constant(
            recipe.get("action"),
            expected_action,
            label="read recipe action",
        )
        _require_constant(
            recipe.get("semantic_operation"),
            expected_operation,
            label="read recipe exact semantic operation",
        )
        _validate_fingerprints(
            recipe.get("fingerprints"),
            label="read recipe fingerprints",
        )
        normalized_recipes.append(recipe)
    semantic_operations = {
        _required_text(
            recipe.get("semantic_operation"),
            label="read recipe semantic_operation",
        )
        for recipe in normalized_recipes
    }
    if (
        schema_version == CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION
        or contract_operations
    ) and not semantic_operations <= (
        contract_operations | compiled_semantic_operations
    ):
        message = "read recipes are not present in the runtime-slice manifest"
        raise ValueError(message)
    digest_payload: dict[str, object] = {
        "actions": actions,
        "recipes": normalized_recipes,
    }
    if "action_verifications" in read_bundle:
        digest_payload["action_verifications"] = _validate_action_verifications(
            read_bundle.get("action_verifications")
        )
    expected_digest = canonical_read_bundle_digest(digest_payload)
    _require_constant(
        read_bundle.get("digest"),
        expected_digest,
        label="read bundle digest",
    )


def _validate_action_verifications(value: object) -> dict[str, str]:
    verifications = _required_mapping(value, label="read action verifications")
    _require_exact_keys(
        verifications,
        frozenset(EXACT_PROFILE_READ_ACTIONS),
        label="read action verifications",
    )
    normalized: dict[str, str] = {}
    for action in EXACT_PROFILE_READ_ACTIONS:
        verification = _required_text(
            verifications.get(action),
            label=f"read action verification {action}",
        )
        if verification not in _ACTION_VERIFICATIONS:
            message = (
                f"read action verification {action} must be a recognized "
                "verification level"
            )
            raise ValueError(message)
        normalized[action] = verification
    return normalized


def _validate_fixture(value: object, *, expected_ds_version: str) -> None:
    fixture = _required_mapping(value, label="fixture")
    _require_exact_keys(
        fixture,
        {
            "manifest_sha256",
            "project_identity_kind",
            "provisioner",
            "scheduled_workflow",
            "workflow_identity_kind",
            "workflow_release_state",
        },
        label="fixture",
    )
    _required_sha256(fixture.get("manifest_sha256"), label="fixture manifest_sha256")
    _required_text(fixture.get("provisioner"), label="fixture provisioner")
    if fixture.get("scheduled_workflow") is not True:
        message = "read fixture must contain a scheduled workflow"
        raise ValueError(message)
    release_state = _required_text(
        fixture.get("workflow_release_state"),
        label="fixture workflow_release_state",
    )
    if release_state not in {"ONLINE", "OFFLINE"}:
        message = "fixture workflow release state must be ONLINE or OFFLINE"
        raise ValueError(message)
    expected_kind = "id" if expected_ds_version == "1.3.9" else "code"
    for key in ("project_identity_kind", "workflow_identity_kind"):
        if fixture.get(key) != expected_kind:
            message = (
                f"DS {expected_ds_version} read fixtures must use "
                f"{expected_kind} identities"
            )
            raise ValueError(message)


def _validate_trace(value: object, *, expected_ds_version: str) -> None:
    raw_entries = _required_sequence(value, label="operation_trace")
    if len(raw_entries) != len(_EXPECTED_TRACE_ACTIONS):
        message = "operation trace must contain the fixed read scenario"
        raise ValueError(message)
    identity_kind = "id" if expected_ds_version == "1.3.9" else "code"
    expected_selectors = {
        7: "search",
        8: "project-name",
        9: f"project-{identity_kind}",
        10: "project-name+workflow-search",
        11: "workflow-name",
        12: f"workflow-{identity_kind}",
    }
    for sequence, (
        raw_entry,
        expected_action,
        expected_argv,
        expected_assertions,
    ) in enumerate(
        zip(
            raw_entries,
            _EXPECTED_TRACE_ACTIONS,
            _EXPECTED_TRACE_ARGV_SHAPES,
            _EXPECTED_TRACE_ASSERTIONS,
            strict=True,
        ),
        start=1,
    ):
        entry = _required_mapping(raw_entry, label="operation trace entry")
        allowed_keys = {
            "action",
            "argv_shape",
            "assertions",
            "exit_code",
            "ok",
            "sequence",
        }
        if sequence in expected_selectors:
            allowed_keys.add("selector_kind")
        _require_exact_keys(entry, allowed_keys, label="operation trace entry")
        _require_constant(entry.get("sequence"), sequence, label="trace sequence")
        _require_constant(entry.get("action"), expected_action, label="trace action")
        _require_constant(
            entry.get("argv_shape"),
            expected_argv,
            label="trace argv_shape",
        )
        _require_constant(entry.get("exit_code"), 0, label="trace exit_code")
        _require_constant(entry.get("ok"), True, label="trace ok")
        assertions = _required_text_list(
            entry.get("assertions"),
            label="trace assertions",
        )
        if expected_action == "doctor":
            assertion_tuple = tuple(assertions)
            if (
                len(assertion_tuple) != 4
                or assertion_tuple[0]
                not in {"api-health-ok", "api-health-warning-recorded"}
                or assertion_tuple[1:] != _DOCTOR_ASSERTION_TAIL
            ):
                message = "doctor trace assertions did not bind persona readiness"
                raise ValueError(message)
        elif tuple(assertions) != expected_assertions:
            message = f"trace assertions differ for sequence {sequence}"
            raise ValueError(message)
        if sequence in expected_selectors:
            _require_constant(
                entry.get("selector_kind"),
                expected_selectors[sequence],
                label="trace selector_kind",
            )


def _validate_effects(value: object) -> None:
    effects = _required_mapping(value, label="effects")
    _require_exact_keys(
        effects,
        {"fixture_mutated", "remote_mutations"},
        label="effects",
    )
    if effects.get("remote_mutations") != 0:
        message = "read-only evidence must declare zero remote mutations"
        raise ValueError(message)
    if effects.get("fixture_mutated") is not False:
        message = "read-only evidence must declare fixture_mutated=false"
        raise ValueError(message)


def _validate_fingerprints(value: object, *, label: str) -> None:
    fingerprints = _required_mapping(value, label=label)
    _require_exact_keys(fingerprints, _EXPECTED_FINGERPRINT_KEYS, label=label)
    for name in sorted(_EXPECTED_FINGERPRINT_KEYS):
        _required_sha256(fingerprints.get(name), label=f"{label} {name}")


def _validate_image_identity(
    *,
    image_ref: str,
    image_id: str,
    ds_version: str,
) -> None:
    tagged_ref, digest_separator, published_digest = image_ref.partition("@")
    repository, separator, tag = tagged_ref.rpartition(":")
    if (
        not separator
        or not repository
        or tag != ds_version
        or any(character.isspace() for character in repository)
    ):
        message = f"cluster image reference must identify DolphinScheduler {ds_version}"
        raise ValueError(message)
    if digest_separator and _SHA256.fullmatch(published_digest) is None:
        message = "cluster image reference contains an invalid published digest"
        raise ValueError(message)
    if _SHA256.fullmatch(image_id) is None:
        message = "cluster image ID must be an immutable SHA-256 identity"
        raise ValueError(message)


def _reject_sensitive_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() in _SENSITIVE_KEYS:
                message = f"evidence contains sensitive field {key!r}"
                raise ValueError(message)
            _reject_sensitive_keys(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_sensitive_keys(nested)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str] | frozenset[str],
    *,
    label: str,
) -> None:
    actual = set(value)
    if actual != set(expected):
        message = (
            f"{label} keys differ: expected {sorted(expected)}, got {sorted(actual)}"
        )
        raise ValueError(message)


def _require_constant(value: object, expected: object, *, label: str) -> None:
    if value != expected or type(value) is not type(expected):
        message = f"{label} must be {expected!r}"
        raise ValueError(message)


def _required_bundle_manifest_schema_version(value: object, *, label: str) -> int:
    schema_version = _required_int(value, label=label)
    if schema_version not in _SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS:
        message = f"{label} must be 1 or 2"
        raise ValueError(message)
    return schema_version


def _required_text_list(
    value: object,
    *,
    label: str,
    allow_empty: bool = False,
) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        message = f"{label} must be a string list"
        raise TypeError(message)
    if not value and not allow_empty:
        message = f"{label} must not be empty"
        raise ValueError(message)
    return value


def _required_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        message = f"{label} must be a SHA-256 identity"
        raise ValueError(message)
    return text


def _validate_observation_window(observed_at: datetime, *, reference: datetime) -> None:
    if observed_at > reference + _MAX_CLOCK_SKEW:
        message = "image observation is implausibly later than the read gate"
        raise ValueError(message)
    if reference - observed_at > _MAX_OBSERVATION_AGE:
        message = "image observation is too stale for exact-profile read evidence"
        raise ValueError(message)


__all__ = [
    "EXACT_PROFILE_READ_ACTIONS",
    "EXACT_PROFILE_READ_CAPABILITY_ACTIONS",
    "EXACT_PROFILE_READ_RECIPES",
    "canonical_read_bundle_digest",
    "validate_exact_profile_read_evidence_payload",
]
