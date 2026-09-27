from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import yaml

from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles
from dsctl.generated import task_definition_profiles as task_profiles
from dsctl.generated.conformance_bundles import CONFORMANCE_BUNDLE_DATA
from tests.live.support import (
    DsctlCommandResult,
    TaskDefinitionCleanupDoNotRetryError,
    TaskDefinitionCleanupInvocation,
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
)
from tests.request_assertions import first_dry_run_request

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

IdentityKind = Literal["id", "code"]
FullTaskDefinitionGuardStrategy = Literal[
    "direct-delete",
    "workflow-cascade-proof-only",
]
_RUN_ID = re.compile(r"[a-z0-9]{16,32}\Z")
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HMAC_SHA256 = re.compile(r"hmac-sha256:[0-9a-f]{64}\Z")
_GIT_OBJECT = re.compile(r"[0-9a-f]{40}\Z")
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)
_CLUSTER_IMAGE_SOURCE = "node-local-inspection"
_FIXTURE_PROVISIONER = "dsmatrix-conformance-fixture/v1"
_TOOLS_ROOT = Path(__file__).resolve().parents[2] / "tools"
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
_RUNTIME_ENV_NAMES = {
    "version": "DS_LIVE_CONFORMANCE_VERSION",
    "bundle": "DS_LIVE_CONFORMANCE_BUNDLE",
    "env_file": "DS_LIVE_CONFORMANCE_ENV_FILE",
    "attestation_key_file": "DS_LIVE_CONFORMANCE_ATTESTATION_KEY_FILE",
    "executable": "DS_LIVE_CONFORMANCE_DSCTL",
    "python": "DS_LIVE_CONFORMANCE_PYTHON",
    "wheel": "DS_LIVE_CONFORMANCE_WHEEL",
    "cluster_manifest": "DS_LIVE_CONFORMANCE_CLUSTER_MANIFEST",
    "fixture_manifest": "DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST",
    "installed_attestation": "DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION",
    "evidence": "DS_LIVE_CONFORMANCE_EVIDENCE",
}
_RECOVERY_RUN_ID_ENV = "DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE"
_LEGACY_OUTCOMES = (
    "bound-current-user",
    "capability-preflight-complete",
    "external-fixture-cross-checked",
    "gate-owned-project-round-trip",
    "negative-errors-translated",
)
_FULL_OUTCOMES = (
    "bound-current-user",
    "capability-preflight-complete",
    "external-fixture-cross-checked",
    "gate-owned-project-round-trip",
    "gate-owned-workflow-round-trip",
    "workflow-dag-cross-checked",
    "task-update-round-trip",
    "workflow-edit-round-trip",
    "full-cleanup-zero",
    "negative-errors-translated",
)
_TASK_DEFINITION_CLEANUP_ACTION = (
    cleanup_profiles.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
)
_SCENARIO_CONTRACTS = {
    "legacy_core/v1": ("legacy-core-installed-wheel/v1", "typed", _LEGACY_OUTCOMES),
    "full_core/v1": ("full-core-installed-wheel/v1", "opaque", _FULL_OUTCOMES),
}
_TASK_DEFINITION_PROVE_ACTION = (
    _TASK_DEFINITION_CLEANUP_ACTION.removesuffix(".cleanup") + ".prove"
)
_WORKFLOW_DIGEST_KEYS = {
    "workflow",
    "taskCount",
    "relationCount",
    "taskTypeCounts",
    "globalParamNames",
    "rootTasks",
    "leafTasks",
    "isolatedTasks",
    "tasks",
}
_WORKFLOW_DIGEST_WORKFLOW_KEYS = {
    "code",
    "name",
    "version",
    "projectCode",
    "projectName",
    "description",
    "releaseState",
    "scheduleReleaseState",
    "executionType",
    "timeout",
    "schedule",
}
_LEGACY_WORKFLOW_DIGEST_WORKFLOW_KEYS = {
    "id",
    "name",
    "version",
    "projectId",
    "projectName",
    "description",
    "releaseState",
    "scheduleReleaseState",
    "timeout",
    "schedule",
}


def _generated_version_tuple(
    data: Mapping[str, object],
    *,
    key: str,
    exported: object,
) -> tuple[str, ...]:
    raw = data.get(key)
    if (
        type(raw) is not list
        or any(type(version) is not str or not version for version in raw)
        or len(set(raw)) != len(raw)
    ):
        message = f"generated task-definition {key} header drifted"
        raise AssertionError(message)
    versions = tuple(cast("list[str]", raw))
    if type(exported) is not tuple or versions != exported:
        message = f"generated task-definition {key} export drifted"
        raise AssertionError(message)
    return versions


def _validated_generated_task_definition_guard_truth() -> tuple[
    frozenset[str],
    frozenset[str],
    dict[str, FullTaskDefinitionGuardStrategy],
    frozenset[str],
]:
    data = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA
    expected_header = {
        "schema_version",
        "semantic_operation",
        "target_versions",
        "full_core_reconciliation_versions",
        "full_core_versions",
        "cross_process_recovery_versions",
        "profiles",
    }
    if (
        type(data) is not dict
        or set(data) != expected_header
        or data.get("schema_version") != 4
        or data.get("semantic_operation") != _TASK_DEFINITION_CLEANUP_ACTION
    ):
        message = "generated task-definition reconciliation header drifted"
        raise AssertionError(message)
    target = _generated_version_tuple(
        data,
        key="target_versions",
        exported=cleanup_profiles.TARGET_TASK_DEFINITION_CLEANUP_VERSIONS,
    )
    reconciliation = _generated_version_tuple(
        data,
        key="full_core_reconciliation_versions",
        exported=(cleanup_profiles.FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS),
    )
    direct_cleanup = _generated_version_tuple(
        data,
        key="full_core_versions",
        exported=cleanup_profiles.FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS,
    )
    recovery = _generated_version_tuple(
        data,
        key="cross_process_recovery_versions",
        exported=cleanup_profiles.CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS,
    )
    profiles_raw = data.get("profiles")
    if type(profiles_raw) is not dict or set(profiles_raw) != set(target):
        message = "generated task-definition reconciliation profiles drifted"
        raise AssertionError(message)
    expected_profile_keys = {
        "strategy",
        "page_params_epoch",
        "page_model",
        "row_fields",
        "execute_types",
        "workflow_binding_fields",
        "history_model",
        "delete_response",
        "pre_delete_release",
    }
    strategies: dict[str, FullTaskDefinitionGuardStrategy] = {}
    pre_delete_release_versions: set[str] = set()
    for version, profile_raw in profiles_raw.items():
        if type(version) is not str or type(profile_raw) is not dict:
            message = "generated task-definition reconciliation profile drifted"
            raise AssertionError(message)
        strategy = profile_raw.get("strategy")
        pre_delete_release = profile_raw.get("pre_delete_release")
        if (
            set(profile_raw) != expected_profile_keys
            or strategy
            not in {
                "direct-delete",
                "workflow-cascade-proof-only",
            }
            or pre_delete_release not in {"none", "offline"}
        ):
            message = "generated task-definition reconciliation strategy drifted"
            raise AssertionError(message)
        if pre_delete_release == "offline":
            if strategy != "direct-delete":
                message = "generated task-definition release policy drifted"
                raise AssertionError(message)
            pre_delete_release_versions.add(version)
        history_model = profile_raw.get("history_model")
        if strategy == "workflow-cascade-proof-only" and (
            profile_raw.get("execute_types") != ["BATCH", "STREAM"]
            or not isinstance(history_model, str)
            or not history_model.strip()
            or profile_raw.get("delete_response") != "unavailable"
        ):
            message = "generated proof-only task-definition profile drifted"
            raise AssertionError(message)
        strategies[version] = cast("FullTaskDefinitionGuardStrategy", strategy)
    target_versions = set(target)
    reconciliation_versions = set(reconciliation)
    canonical_subsets = (reconciliation, direct_cleanup, recovery)
    if (
        not reconciliation_versions.issubset(target_versions)
        or not set(direct_cleanup).issubset(reconciliation_versions)
        or not set(recovery).issubset(target_versions)
        or any(
            tuple(version for version in target if version in set(subset)) != subset
            for subset in canonical_subsets
        )
        or any(strategies[version] != "direct-delete" for version in direct_cleanup)
    ):
        message = "generated task-definition reconciliation sets drifted"
        raise AssertionError(message)
    return (
        frozenset(reconciliation),
        frozenset(recovery),
        strategies,
        frozenset(pre_delete_release_versions),
    )


(
    _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS,
    _CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS,
    _TASK_DEFINITION_GUARD_STRATEGIES,
    _TASK_DEFINITION_PRE_DELETE_RELEASE_VERSIONS,
) = _validated_generated_task_definition_guard_truth()


def _full_task_definition_guard_strategy(
    ds_version: str,
    *,
    for_recovery: bool = False,
) -> FullTaskDefinitionGuardStrategy | None:
    eligible_versions = (
        _CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS
        if for_recovery
        else _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS
    )
    if ds_version not in eligible_versions:
        return None
    try:
        return _TASK_DEFINITION_GUARD_STRATEGIES[ds_version]
    except KeyError as exc:
        message = "generated task-definition guard strategy is missing"
        raise AssertionError(message) from exc


def _validated_dependency_update_upstream_limited_versions() -> frozenset[str]:
    data = task_profiles.TASK_DEFINITION_PROFILE_DATA
    if (
        type(data) is not dict
        or set(data)
        != {"schema_version", "target_versions", "profile_pool", "profile_specs"}
        or data.get("schema_version") != 5
    ):
        message = "generated task-definition profile header drifted"
        raise AssertionError(message)
    target_versions = data.get("target_versions")
    profile_pool = data.get("profile_pool")
    profile_specs = data.get("profile_specs")
    profiles = task_profiles.TASK_DEFINITION_PROFILES
    if (
        type(target_versions) is not list
        or tuple(target_versions) != task_profiles.TARGET_TASK_DEFINITION_VERSIONS
        or type(profile_pool) is not list
        or type(profile_specs) is not dict
        or type(profiles) is not dict
        or tuple(profile_specs) != tuple(target_versions)
        or tuple(profiles) != tuple(target_versions)
    ):
        message = "generated task-definition profile version mapping drifted"
        raise AssertionError(message)
    limited_versions: set[str] = set()
    for version in target_versions:
        if type(version) is not str:
            message = "generated task-definition profile version drifted"
            raise AssertionError(message)
        pool_index = profile_specs.get(version)
        if (
            type(pool_index) is not int
            or not 0 <= pool_index < len(profile_pool)
            or type(profile_pool[pool_index]) is not dict
            or profiles.get(version) != profile_pool[pool_index]
        ):
            message = "generated task-definition profile selection drifted"
            raise AssertionError(message)
        profile = profiles[version]
        update_executable = profile.get("update_executable")
        whole_workflow_update = profile.get("whole_workflow_update")
        executable = profile.get("executable")
        dependency_update = profile.get("dependency_update")
        if (
            type(update_executable) is not bool
            or type(whole_workflow_update) is not bool
            or type(executable) is not bool
            or type(dependency_update) is not bool
        ):
            message = "generated task-definition update policy drifted"
            raise AssertionError(message)
        can_update = update_executable or (executable and whole_workflow_update)
        if dependency_update and not can_update:
            message = "generated task-definition update policy drifted"
            raise AssertionError(message)
        if can_update and not dependency_update:
            limited_versions.add(version)
    return frozenset(limited_versions)


_DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS = (
    _validated_dependency_update_upstream_limited_versions()
)


def _require_full_task_definition_callback(
    config: ConformanceBundleGateConfig,
    *,
    callback: object,
) -> None:
    if config.bundle_name != "full_core/v1" or callback is not None:
        return
    guard_strategy = _full_task_definition_guard_strategy(config.ds_version)
    if guard_strategy is None:
        return
    callback_role = "cleanup" if guard_strategy == "direct-delete" else "proof"
    message = (
        "full_core/v1 requires the installed-wheel private task-definition "
        f"{callback_role} runner for DolphinScheduler {config.ds_version}"
    )
    raise ValueError(message)


@dataclass(frozen=True)
class NativeFixtureIdentity:
    """One fixture identity in the selected release's native vocabulary."""

    kind: IdentityKind
    value: int


@dataclass(frozen=True)
class ClusterIdentity:
    """Immutable, non-secret identity of one exact DS cluster."""

    ds_version: str
    image_ref: str
    image_id: str
    image_source: str
    image_provenance: dict[str, object]
    image_observed_at: str
    api_target_hmac_sha256: str
    principal_hmac_sha256: str
    persona: str


@dataclass(frozen=True)
class ExternalFixture:
    """Externally managed scheduled workflow used only for read cross-checks."""

    project_name: str
    project_identity: NativeFixtureIdentity
    workflow_name: str
    workflow_identity: NativeFixtureIdentity
    schedule_id: int
    workflow_release_state: str
    provisioner: str
    manifest_sha256: str
    identity_hmac_sha256: str


@dataclass(frozen=True)
class BundleAssessment:
    """Current generated named-bundle identity bound to one coordinate."""

    schema_version: int
    catalog_digest: str
    assessment_digest: str
    name: str
    bundle_digest: str
    coordinate_status: str
    support_level: str
    tested: bool
    extends: tuple[str, ...]
    inheritance: tuple[dict[str, object], ...]
    direct_actions: tuple[str, ...]
    required_actions: tuple[str, ...]


@dataclass(frozen=True)
class InstalledBundleAttestation:
    """Allowlisted candidate-wheel facts attested by the outer runner."""

    distribution_version: str
    profile: dict[str, object]
    action_recipes: tuple[dict[str, object], ...]
    contract: dict[str, object]


@dataclass(frozen=True)
class ConformanceBundleGateConfig:
    """Explicit paths and exact identities for one named-bundle scenario."""

    ds_version: str
    bundle_name: str
    env_file: Path
    executable: Path
    python: Path
    wheel: Path
    evidence_path: Path
    attestation_key: bytes
    cluster: ClusterIdentity
    fixture: ExternalFixture
    bundle: BundleAssessment
    installation: InstalledBundleAttestation


@dataclass(frozen=True)
class OperationTraceEntry:
    """Sanitized black-box CLI observation for the conformance receipt."""

    sequence: int
    action: str
    argv_shape: str
    exit_code: int
    ok: bool
    assertions: tuple[str, ...]
    outcomes: tuple[str, ...] = ()
    error_type: str | None = None
    subject_action: str | None = None

    def to_data(self) -> dict[str, object]:
        data: dict[str, object] = {
            "sequence": self.sequence,
            "action": self.action,
            "argv_shape": self.argv_shape,
            "exit_code": self.exit_code,
            "ok": self.ok,
            "assertions": list(self.assertions),
            "outcomes": list(self.outcomes),
        }
        if self.error_type is not None:
            data["error_type"] = self.error_type
        if self.subject_action is not None:
            data["subject_action"] = self.subject_action
        return data


@dataclass(frozen=True)
class ScenarioEffects:
    """Bounded remote mutation claim for one passing legacy scenario."""

    remote_mutations: int
    gate_owned_only: bool = True
    external_fixture_mutated: bool = False

    def to_data(self) -> dict[str, object]:
        return {
            "remote_mutations": self.remote_mutations,
            "gate_owned_only": self.gate_owned_only,
            "external_fixture_mutated": self.external_fixture_mutated,
        }


@dataclass(frozen=True)
class ScenarioCleanup:
    """Zero-leftover and external-fixture restoration result."""

    gate_owned_projects: int
    gate_owned_workflows: int
    gate_owned_tasks: int
    external_fixture_state_hmac_matched: bool

    def to_data(self) -> dict[str, object]:
        return {
            "gate_owned_projects": {
                "confirmed": self.gate_owned_projects == 0,
                "leftovers": self.gate_owned_projects,
            },
            "gate_owned_workflows": {
                "confirmed": self.gate_owned_workflows == 0,
                "leftovers": self.gate_owned_workflows,
            },
            "gate_owned_tasks": {
                "confirmed": self.gate_owned_tasks == 0,
                "leftovers": self.gate_owned_tasks,
            },
            "external_fixture": {
                "scope": "externally-managed-read-only",
                "mutated_by_gate": False,
                "state_hmac_matched": self.external_fixture_state_hmac_matched,
            },
        }


@dataclass(frozen=True)
class ConformanceScenarioResult:
    """Passing black-box observations ready for receipt construction."""

    version_data: dict[str, object]
    operation_trace: tuple[OperationTraceEntry, ...]
    effects: ScenarioEffects
    cleanup: ScenarioCleanup
    fixture_before_hmac: str
    fixture_after_hmac: str


@dataclass(frozen=True)
class _OwnedTaskSnapshot:
    native_id: int | str
    name: str
    version: int | None
    task_type: str
    command: str
    non_owned_digest: str


@dataclass(frozen=True)
class _OwnedWorkflowSnapshot:
    code: int
    name: str
    version: int
    description: str
    release_state: str
    non_owned_digest: str
    tasks: tuple[_OwnedTaskSnapshot, ...]
    roots: tuple[int | str, ...]
    edges: tuple[tuple[int | str, int | str], ...]


@dataclass(frozen=True)
class _OwnedWorkflowCleanupProof:
    code: int
    description: str
    version: int
    tasks: tuple[_OwnedTaskSnapshot, ...]


@dataclass(frozen=True)
class _OwnedProjectScope:
    name: str
    code: int
    identity_kind: IdentityKind
    ownership: str
    created_description: str
    updated_description: str


class ConformanceScenarioCleanupError(RuntimeError):
    """A scenario failed and its proven gate-owned state could not be removed."""


def load_conformance_bundle_gate_config(
    environment: Mapping[str, str],
) -> ConformanceBundleGateConfig:
    """Load one private runner snapshot and bind it to current generated truth."""
    missing = [
        name
        for name in _RUNTIME_ENV_NAMES.values()
        if not environment.get(name, "").strip()
    ]
    if missing:
        message = "Missing conformance-bundle live-gate inputs: " + ", ".join(missing)
        raise ValueError(message)
    ds_version = environment[_RUNTIME_ENV_NAMES["version"]].strip()
    bundle_name = environment[_RUNTIME_ENV_NAMES["bundle"]].strip()
    paths = {
        key: _required_private_file(environment[name], label=name)
        for key, name in _RUNTIME_ENV_NAMES.items()
        if key not in {"version", "bundle", "evidence", "executable", "python"}
    }
    executable = _required_file(
        environment[_RUNTIME_ENV_NAMES["executable"]],
        label=_RUNTIME_ENV_NAMES["executable"],
    )
    python = _required_file(
        environment[_RUNTIME_ENV_NAMES["python"]],
        label=_RUNTIME_ENV_NAMES["python"],
    )
    if executable.parent != python.parent:
        message = "Conformance dsctl and Python must come from the same virtualenv"
        raise ValueError(message)
    wheel = paths["wheel"]
    if wheel.suffix != ".whl":
        message = "Conformance wheel input must end in .whl"
        raise ValueError(message)
    evidence_path = Path(environment[_RUNTIME_ENV_NAMES["evidence"]]).expanduser()
    if evidence_path.exists():
        message = f"Conformance candidate evidence already exists: {evidence_path}"
        raise FileExistsError(message)

    profile_values = _read_profile_values(paths["env_file"])
    if profile_values.get("DS_VERSION") != ds_version:
        message = "Conformance profile DS_VERSION differs from the requested version"
        raise ValueError(message)
    api_url = _required_text(
        profile_values.get("DS_API_URL"),
        label="profile DS_API_URL",
    ).rstrip("/")
    _required_text(profile_values.get("DS_API_TOKEN"), label="profile DS_API_TOKEN")
    attestation_key = paths["attestation_key_file"].read_bytes().strip()
    if len(attestation_key) < 32:
        message = "Conformance attestation key must contain at least 32 bytes"
        raise ValueError(message)
    cluster = _load_cluster_identity(
        paths["cluster_manifest"],
        ds_version=ds_version,
        api_url=api_url,
        attestation_key=attestation_key,
    )
    fixture = _load_external_fixture(
        paths["fixture_manifest"],
        ds_version=ds_version,
        cluster=cluster,
        attestation_key=attestation_key,
    )
    bundle = _load_current_bundle_assessment(
        ds_version=ds_version,
        bundle_name=bundle_name,
    )
    installation = _load_installed_bundle_attestation(
        paths["installed_attestation"],
        ds_version=ds_version,
        bundle=bundle,
        wheel=wheel,
    )
    return ConformanceBundleGateConfig(
        ds_version=ds_version,
        bundle_name=bundle_name,
        env_file=paths["env_file"],
        executable=executable,
        python=python,
        wheel=wheel,
        evidence_path=evidence_path,
        attestation_key=attestation_key,
        cluster=cluster,
        fixture=fixture,
        bundle=bundle,
        installation=installation,
    )


def load_conformance_recovery_run_id(
    environment: Mapping[str, str],
) -> str | None:
    """Read an optional owner-private cross-process recovery identity."""
    raw_path = environment.get(_RECOVERY_RUN_ID_ENV, "").strip()
    if not raw_path:
        return None
    manifest_io = importlib.import_module("private_manifest_io")
    payload = cast(
        "dict[str, object]",
        manifest_io.load_private_json_object(
            Path(raw_path).expanduser(),
            label="Conformance recovery run-id file",
        ),
    )
    if set(payload) != {"schema_version", "run_id"}:
        message = "Conformance recovery run-id file fields differ"
        raise ValueError(message)
    if payload.get("schema_version") != 1:
        message = "Conformance recovery run-id schema differs"
        raise ValueError(message)
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or _RUN_ID.fullmatch(run_id) is None:
        message = "Conformance recovery run_id is not 16-32 lowercase safe characters"
        raise ValueError(message)
    return run_id


def execute_conformance_bundle_scenario(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    invoke_raw: Callable[[list[str]], DsctlCommandResult] | None = None,
    invoke_task_cleanup: (
        Callable[
            [Literal["prove", "cleanup"], int, int | None, str],
            TaskDefinitionCleanupInvocation,
        ]
        | None
    ) = None,
    run_id: str,
) -> ConformanceScenarioResult:
    """Run one fixed named-bundle scenario with failure-safe owned cleanup."""
    if config.bundle_name not in _SCENARIO_CONTRACTS:
        message = f"No live scenario is implemented for {config.bundle_name!r}"
        raise NotImplementedError(message)
    if config.bundle.coordinate_status != "ready":
        message = (
            f"Bundle {config.bundle_name!r} is not static-ready for {config.ds_version}"
        )
        raise ValueError(message)
    if config.bundle_name == "full_core/v1" and invoke_raw is None:
        message = "full_core/v1 requires an explicit raw workflow-export invoker"
        raise ValueError(message)
    _require_full_task_definition_callback(
        config,
        callback=invoke_task_cleanup,
    )
    if _RUN_ID.fullmatch(run_id) is None:
        message = "Conformance scenario run_id must be 16-32 lowercase safe characters"
        raise ValueError(message)
    guard: _GateOwnedProjectGuard
    if config.bundle_name == "full_core/v1":
        guard = _GateOwnedFullGuard(
            config,
            invoke=invoke,
            invoke_task_cleanup=invoke_task_cleanup,
            run_id=run_id,
        )
    else:
        guard = _GateOwnedProjectGuard(config, invoke=invoke, run_id=run_id)
    try:
        if config.bundle_name == "legacy_core/v1":
            return _execute_legacy_bundle_scenario(
                config,
                invoke=guard.invoke,
                run_id=run_id,
            )
        assert invoke_raw is not None
        return _execute_full_bundle_scenario(
            config,
            invoke=guard.invoke,
            invoke_raw=invoke_raw,
            invoke_task_cleanup=cast("_GateOwnedFullGuard", guard).invoke_task_cleanup,
            run_id=run_id,
        )
    except Exception:
        try:
            guard.cleanup()
        except Exception as cleanup_error:
            message = "conformance scenario left unclean gate-owned state"
            raise ConformanceScenarioCleanupError(message) from cleanup_error
        raise


def recover_existing_full_conformance_state(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    invoke_task_cleanup: Callable[
        [Literal["prove", "cleanup"], int, int | None, str],
        TaskDefinitionCleanupInvocation,
    ]
    | None = None,
    run_id: str,
) -> None:
    """Fresh-read and remove one proven full-scenario residue without evidence."""
    if config.bundle_name != "full_core/v1":
        message = "Only the full conformance scenario has recoverable workflow state"
        raise ValueError(message)
    if config.ds_version not in _CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS:
        supported = ", ".join(sorted(_CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS))
        message = (
            "Cross-process full conformance recovery is only supported for "
            f"these generated DolphinScheduler coordinates: {supported}"
        )
        raise ValueError(message)
    if _RUN_ID.fullmatch(run_id) is None:
        message = "Conformance recovery run_id must be 16-32 lowercase safe characters"
        raise ValueError(message)
    if invoke_task_cleanup is None:
        strategy = _full_task_definition_guard_strategy(
            config.ds_version,
            for_recovery=True,
        )
        if strategy is None:
            message = "generated full recovery strategy is missing"
            raise AssertionError(message)
        callback_role = "cleanup" if strategy == "direct-delete" else "proof"
        message = (
            "full conformance recovery requires the private task-definition "
            f"{callback_role} runner"
        )
        raise ValueError(message)
    _GateOwnedFullGuard(
        config,
        invoke=invoke,
        invoke_task_cleanup=invoke_task_cleanup,
        run_id=run_id,
        recovery_mode=True,
    ).recover_existing()


def write_conformance_bundle_candidate(
    config: ConformanceBundleGateConfig,
    *,
    result: ConformanceScenarioResult,
    validator: Callable[..., object] | None = None,
    recorded_at: datetime | None = None,
) -> dict[str, object]:
    """Validate then create one owner-private, no-overwrite passing candidate."""
    evidence_schema = importlib.import_module("live_gate.conformance_bundle_evidence")
    canonical_receipt_digest = cast(
        "Callable[[Mapping[str, object]], str]",
        evidence_schema.canonical_conformance_bundle_receipt_digest,
    )
    default_validator = cast(
        "Callable[..., object]",
        evidence_schema.validate_conformance_bundle_evidence,
    )

    scenario_id, authoring_mode, expected_outcomes = _SCENARIO_CONTRACTS[
        config.bundle_name
    ]
    required_outcomes = list(expected_outcomes)
    scenario_definition: dict[str, object] = {
        "schema_version": 1,
        "id": scenario_id,
        "bundle": config.bundle_name,
        "authoring_mode": authoring_mode,
        "required_outcomes": required_outcomes,
    }
    wheel_sha256 = f"sha256:{_file_sha256(config.wheel)}"
    profile = config.installation.profile
    receipt: dict[str, object] = {
        "schema_version": 1,
        "sanitization_schema_version": 1,
        "gate": "exact-conformance-bundle",
        "status": "passed",
        "recorded_at": (recorded_at or datetime.now(tz=UTC)).isoformat(),
        "runner": {
            "artifact": "installed-wheel-console-script",
            "cli_version": config.installation.distribution_version,
            "wheel_filename": config.wheel.name,
            "wheel_sha256": wheel_sha256,
        },
        "dolphinscheduler": {
            "release": config.ds_version,
            "image_ref": config.cluster.image_ref,
            "image_id": config.cluster.image_id,
            "image_source": config.cluster.image_source,
            "image_provenance": config.cluster.image_provenance,
            "image_observed_at": config.cluster.image_observed_at,
            "api_target_hmac_sha256": config.cluster.api_target_hmac_sha256,
            "principal_hmac_sha256": config.cluster.principal_hmac_sha256,
            "persona": config.cluster.persona,
        },
        "profile": {
            "ds": config.ds_version,
            "selected_ds_version": config.ds_version,
            "contract_version": config.ds_version,
            "family": profile["family"],
            "support_level": profile["support_level"],
            "tested": profile["tested"],
            "source": profile["source"],
            "fingerprints": profile["fingerprints"],
        },
        "contract": dict(config.installation.contract),
        "conformance_bundle": {
            "catalog_schema_version": config.bundle.schema_version,
            "assessment_schema_version": config.bundle.schema_version,
            "catalog_digest": config.bundle.catalog_digest,
            "assessment_digest": config.bundle.assessment_digest,
            "name": config.bundle.name,
            "bundle_digest": config.bundle.bundle_digest,
            "coordinate_status": config.bundle.coordinate_status,
            "extends": list(config.bundle.extends),
            "inheritance": [dict(item) for item in config.bundle.inheritance],
            "direct_actions": list(config.bundle.direct_actions),
            "required_actions": list(config.bundle.required_actions),
            "action_recipes": [
                dict(recipe) for recipe in config.installation.action_recipes
            ],
        },
        "scenario": {
            **scenario_definition,
            "observed_outcomes": required_outcomes,
            "digest": _canonical_sha256(scenario_definition),
        },
        "fixture": {
            "manifest_sha256": f"sha256:{config.fixture.manifest_sha256}",
            "provisioner": config.fixture.provisioner,
            "identity_hmac_sha256": config.fixture.identity_hmac_sha256,
            "before_state_hmac_sha256": result.fixture_before_hmac,
            "after_state_hmac_sha256": result.fixture_after_hmac,
            "scheduled_workflow": True,
        },
        "operation_trace": [entry.to_data() for entry in result.operation_trace],
        "effects": result.effects.to_data(),
        "cleanup": result.cleanup.to_data(),
        "evidence_scope": {
            "claim": "named-bundle-live-scenario",
            "observed_evidence": "live_smoke",
            "authoring_mode": authoring_mode,
            "facet_claims": [],
            "promotion_claimed": False,
            "support_level_changes": False,
            "tested_changes": False,
        },
        "secrets_recorded": False,
    }
    receipt["receipt_digest"] = canonical_receipt_digest(receipt)
    validate = validator or default_validator
    validate(
        receipt,
        expected_ds_version=config.ds_version,
        expected_bundle=config.bundle_name,
        expected_wheel_filename=config.wheel.name,
        expected_wheel_sha256=wheel_sha256,
    )
    config.evidence_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        config.evidence_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    config.evidence_path.chmod(0o600)
    return receipt


def _execute_legacy_bundle_scenario(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    run_id: str,
) -> ConformanceScenarioResult:
    trace: list[OperationTraceEntry] = []
    version_data = _verify_version(config, invoke=invoke, trace=trace)
    _verify_capabilities(config, invoke=invoke, trace=trace)
    _verify_doctor(config, invoke=invoke, trace=trace)
    fixture_before = _read_external_fixture(
        config,
        invoke=invoke,
        trace=trace,
        observed_outcome="external-fixture-cross-checked",
    )
    fixture_before_hmac = _state_hmac(fixture_before, key=config.attestation_key)

    project_name = f"dsctl-conformance-{config.ds_version.replace('.', '-')}-{run_id}"
    ownership = f"dsctl-conformance-owner:{run_id}"
    created_description = f"{ownership};phase=created"
    updated_description = f"{ownership};phase=updated"
    identity_key = _native_identity_kind(config.ds_version)

    created = _ok_data(
        invoke(
            [
                "project",
                "create",
                "--name",
                project_name,
                "--description",
                created_description,
            ]
        ),
        expected_action="project.create",
        label="gate-owned project create",
    )
    native_value = _owned_project_identity(
        created,
        identity_key=identity_key,
        name=project_name,
        ownership=ownership,
        description=created_description,
    )
    _record_success(
        trace,
        "project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        assertions=("gate-owned-project-created", "native-identity-captured"),
    )

    conflict = invoke(
        [
            "project",
            "create",
            "--name",
            project_name,
            "--description",
            created_description,
        ]
    )
    conflict_error = require_error_payload(
        conflict,
        expected_action="project.create",
        expected_type="conflict",
        label="duplicate gate-owned project create",
    )
    if not _non_empty_text(conflict_error.get("suggestion")):
        message = "duplicate project conflict omitted an actionable suggestion"
        raise AssertionError(message)
    _record_failure(
        trace,
        conflict,
        "project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        error_type="conflict",
        assertions=("stable-conflict", "actionable-suggestion"),
    )

    listed = _project_list(
        invoke,
        project_name=project_name,
        trace=trace,
    )
    _require_owned_project(
        listed,
        identity_key=identity_key,
        identity=native_value,
        name=project_name,
        ownership=ownership,
        description=created_description,
    )
    for selector in (project_name, str(native_value)):
        data = _project_get(
            invoke,
            selector=selector,
            trace=trace,
            assertions=("gate-owned-project-matched", "native-identity-matched"),
        )
        _require_owned_project(
            [data],
            identity_key=identity_key,
            identity=native_value,
            name=project_name,
            ownership=ownership,
            description=created_description,
        )

    updated = _ok_data(
        invoke(
            [
                "project",
                "update",
                str(native_value),
                "--description",
                updated_description,
            ]
        ),
        expected_action="project.update",
        label="gate-owned project update",
    )
    _require_owned_project(
        [updated],
        identity_key=identity_key,
        identity=native_value,
        name=project_name,
        ownership=ownership,
        description=updated_description,
    )
    _record_success(
        trace,
        "project.update",
        argv_shape="project update PROJECT --description DESCRIPTION",
        assertions=("description-marker-updated",),
    )
    readback = _project_get(
        invoke,
        selector=str(native_value),
        trace=trace,
        assertions=("updated-description-read-back",),
    )
    _require_owned_project(
        [readback],
        identity_key=identity_key,
        identity=native_value,
        name=project_name,
        ownership=ownership,
        description=updated_description,
    )

    deleted = _ok_data(
        invoke(["project", "delete", str(native_value), "--force"]),
        expected_action="project.delete",
        label="gate-owned project delete",
    )
    if deleted.get("deleted") is not True:
        message = "gate-owned project delete was not confirmed"
        raise AssertionError(message)
    _record_success(
        trace,
        "project.delete",
        argv_shape="project delete PROJECT --force",
        assertions=("gate-owned-project-deleted",),
    )

    not_found = invoke(["project", "get", project_name])
    require_error_payload(
        not_found,
        expected_action="project.get",
        expected_type="not_found",
        label="deleted project lookup",
    )
    _record_failure(
        trace,
        not_found,
        "project.get",
        argv_shape="project get PROJECT",
        error_type="not_found",
        assertions=("stable-not-found",),
    )
    leftovers = _project_list(
        invoke,
        project_name=project_name,
        trace=trace,
        outcomes=("gate-owned-project-round-trip", "negative-errors-translated"),
    )
    if leftovers:
        message = "gate-owned project remained after confirmed deletion"
        raise AssertionError(message)

    fixture_after = _read_external_fixture(
        config,
        invoke=invoke,
        trace=trace,
        observed_outcome=None,
    )
    fixture_after_hmac = _state_hmac(fixture_after, key=config.attestation_key)
    if fixture_after_hmac != fixture_before_hmac:
        message = "external fixture changed during the legacy conformance scenario"
        raise AssertionError(message)
    cleanup = ScenarioCleanup(
        gate_owned_projects=0,
        gate_owned_workflows=0,
        gate_owned_tasks=0,
        external_fixture_state_hmac_matched=True,
    )
    return ConformanceScenarioResult(
        version_data=version_data,
        operation_trace=tuple(trace),
        effects=ScenarioEffects(remote_mutations=3),
        cleanup=cleanup,
        fixture_before_hmac=fixture_before_hmac,
        fixture_after_hmac=fixture_after_hmac,
    )


def _execute_full_bundle_scenario(  # noqa: C901
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    invoke_raw: Callable[[list[str]], DsctlCommandResult],
    invoke_task_cleanup: Callable[
        [Literal["prove", "cleanup"], int, int | None],
        TaskDefinitionCleanupInvocation,
    ],
    run_id: str,
) -> ConformanceScenarioResult:
    trace: list[OperationTraceEntry] = []
    version_data = _verify_version(config, invoke=invoke, trace=trace)
    task_guard_strategy = _full_task_definition_guard_strategy(config.ds_version)
    _verify_capabilities(config, invoke=invoke, trace=trace)
    _verify_doctor(config, invoke=invoke, trace=trace)
    fixture_before = _read_external_fixture(
        config,
        invoke=invoke,
        trace=trace,
        observed_outcome="external-fixture-cross-checked",
    )
    fixture_before_hmac = _state_hmac(fixture_before, key=config.attestation_key)

    project = _create_and_update_full_project(
        config,
        invoke=invoke,
        trace=trace,
        run_id=run_id,
    )
    workflow_name = f"dsctl-full-{config.ds_version.replace('.', '-')}-{run_id}"
    created_workflow_description = (
        f"{project.ownership};resource=workflow;phase=created"
    )
    updated_workflow_description = (
        f"{project.ownership};resource=workflow;phase=updated"
    )
    workflow_file, patch_file = _write_full_workflow_sources(
        config,
        run_id=run_id,
        project=project,
        workflow_name=workflow_name,
        created_description=created_workflow_description,
        updated_description=updated_workflow_description,
    )
    try:
        workflow_rows = _full_workflow_list(
            invoke,
            project_name=project.name,
            workflow_name=workflow_name,
            trace=trace,
            assertions=("pre-create-workflow-absent",),
        )
        if workflow_rows:
            message = "gate-owned workflow existed before its dry run"
            raise AssertionError(message)

        create_argv = [
            "workflow",
            "create",
            "--file",
            str(workflow_file),
            "--project",
            project.name,
        ]
        create_dry_run = invoke([*create_argv, "--dry-run", "--columns", "*"])
        _require_exact_dry_run_plan(
            create_dry_run,
            expected_action="workflow.create",
            method="POST",
            required_text=(workflow_name, "extract", "load"),
            label="gate-owned workflow create dry run",
        )
        _record_success(
            trace,
            "workflow.create",
            argv_shape=(
                "workflow create --file WORKFLOW_FILE --project PROJECT --dry-run"
            ),
            assertions=(
                "installed-exact-create-plan",
                "dry-run-no-request-sent",
            ),
        )
        after_create_dry_run = _full_workflow_list(
            invoke,
            project_name=project.name,
            workflow_name=workflow_name,
            trace=trace,
            assertions=("create-dry-run-left-workflow-absent",),
        )
        if after_create_dry_run:
            message = "workflow create dry run changed remote state"
            raise AssertionError(message)

        created = _ok_data(
            invoke(create_argv),
            expected_action="workflow.create",
            label="gate-owned workflow create",
        )
        workflow_code = _require_owned_workflow(
            created,
            project=project,
            workflow_name=workflow_name,
            description=created_workflow_description,
        )
        _record_success(
            trace,
            "workflow.create",
            argv_shape="workflow create --file WORKFLOW_FILE --project PROJECT",
            assertions=(
                "gate-owned-workflow-created",
                "native-workflow-code-captured",
                "workflow-offline",
            ),
        )
        _verify_full_workflow_identity_reads(
            invoke,
            trace=trace,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
        )
        baseline = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
        )
        duplicate_workflow = invoke(create_argv)
        require_error_payload(
            duplicate_workflow,
            expected_action="workflow.create",
            expected_type="conflict",
            label="duplicate gate-owned workflow",
        )
        _record_failure(
            trace,
            duplicate_workflow,
            "workflow.create",
            argv_shape="workflow create --file WORKFLOW_FILE --project PROJECT",
            error_type="conflict",
            assertions=("stable-conflict",),
        )
        after_duplicate = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
            describe_assertions=("duplicate-workflow-state-unchanged",),
        )
        if after_duplicate != baseline:
            message = "duplicate workflow conflict changed remote state"
            raise AssertionError(message)

        missing_workflow = invoke(
            [
                "workflow",
                "get",
                str(workflow_code + 1_000_000_000_000),
                "--project",
                project.name,
            ]
        )
        require_error_payload(
            missing_workflow,
            expected_action="workflow.get",
            expected_type="not_found",
            label="missing gate-owned workflow",
        )
        _record_failure(
            trace,
            missing_workflow,
            "workflow.get",
            argv_shape="workflow get MISSING_WORKFLOW --project PROJECT",
            error_type="not_found",
            assertions=("stable-not-found", "missing-workflow-nonmutating"),
        )

        load_task = _snapshot_task(baseline, name="load")
        task_update_selector = (
            load_task.name if config.ds_version == "1.3.9" else str(load_task.native_id)
        )
        if config.ds_version in _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS:
            dependency_result = invoke(
                [
                    "task",
                    "update",
                    task_update_selector,
                    "--project",
                    project.name,
                    "--workflow",
                    workflow_name,
                    "--set",
                    "depends_on=[]",
                    "--dry-run",
                    "--columns",
                    "*",
                ]
            )
            dependency_error = require_error_payload(
                dependency_result,
                expected_action="task.update",
                expected_type="unsupported_feature",
                label="upstream-limited dependency update dry run",
            )
            details = require_mapping(
                dependency_error.get("details"),
                label="upstream-limited dependency update details",
            )
            if (
                details.get("dependency_update") is not False
                or details.get("mutation_applied") is not False
            ):
                message = "dependency update was not rejected before I/O"
                raise AssertionError(message)
            _record_failure(
                trace,
                dependency_result,
                "task.update",
                argv_shape=(
                    "task update TASK --project PROJECT --workflow WORKFLOW "
                    "--set DEPENDS_ON --dry-run"
                ),
                error_type="unsupported_feature",
                assertions=(
                    "dependency-update-upstream-limited",
                    "pre-io-no-mutation",
                ),
            )
            dependency_readback = _inspect_full_owned_workflow(
                invoke,
                invoke_raw=invoke_raw,
                trace=trace,
                ds_version=config.ds_version,
                project=project,
                workflow_name=workflow_name,
                workflow_code=workflow_code,
                description=created_workflow_description,
            )
            if dependency_readback != baseline:
                message = "unsupported dependency dry run changed owned state"
                raise AssertionError(message)

        replacement_command = f'printf "%s\\n" "{run_id}-updated-load"\n'
        task_update_argv = [
            "task",
            "update",
            task_update_selector,
            "--project",
            project.name,
            "--workflow",
            workflow_name,
            "--set",
            f"command={replacement_command}",
        ]
        task_dry_run = invoke([*task_update_argv, "--dry-run", "--columns", "*"])
        task_dry_data = _require_exact_dry_run_plan(
            task_dry_run,
            expected_action="task.update",
            method="POST" if config.ds_version == "1.3.9" else "PUT",
            required_text=(f"{run_id}-updated-load",),
            label="gate-owned task update dry run",
        )
        expected_task_changes = [
            {
                "field": "command",
                "before": load_task.command,
                "after": replacement_command,
            }
        ]
        if (
            task_dry_data.get("changes") != expected_task_changes
            or "updated_fields" in task_dry_data
        ):
            message = "task update dry run did not isolate the command field"
            raise AssertionError(message)
        _record_success(
            trace,
            "task.update",
            argv_shape=(
                "task update TASK --project PROJECT --workflow WORKFLOW "
                "--set COMMAND --dry-run"
            ),
            assertions=(
                "installed-exact-task-update-plan",
                "dry-run-no-request-sent",
            ),
        )
        after_task_dry_run = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
        )
        if after_task_dry_run != baseline:
            message = "task update dry run changed remote state"
            raise AssertionError(message)

        updated_task_data = _ok_data(
            invoke(task_update_argv),
            expected_action="task.update",
            label="gate-owned task update",
        )
        updated_task = _task_snapshot(
            updated_task_data,
            identity_kind=project.identity_kind,
        )
        task_identity_epoch_valid = (
            updated_task.version is None and load_task.version is None
            if project.identity_kind == "id"
            else (
                updated_task.version is not None
                and load_task.version is not None
                and updated_task.version > load_task.version
            )
        )
        if (
            updated_task.native_id != load_task.native_id
            or updated_task.name != load_task.name
            or updated_task.command != replacement_command
            or not task_identity_epoch_valid
            or updated_task.non_owned_digest != load_task.non_owned_digest
        ):
            message = "task update apply did not preserve its bounded task contract"
            raise AssertionError(message)
        _record_success(
            trace,
            "task.update",
            argv_shape=(
                "task update TASK --project PROJECT --workflow WORKFLOW --set COMMAND"
            ),
            assertions=(
                "command-updated-exactly",
                (
                    "native-task-id-preserved"
                    if config.ds_version == "1.3.9"
                    else "task-version-advanced"
                ),
                "non-owned-task-state-preserved",
            ),
        )
        after_task_update = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
        )
        _require_task_update_transition(
            baseline,
            after_task_update,
            ds_version=config.ds_version,
            task_name="load",
            command=replacement_command,
        )
        if task_guard_strategy == "workflow-cascade-proof-only":
            _require_cascade_workflow_version_transition(
                baseline,
                after_task_update,
                expected=(1, 2),
                label="task update",
            )

        edit_argv = [
            "workflow",
            "edit",
            str(workflow_code),
            "--project",
            project.name,
            "--patch",
            str(patch_file),
        ]
        edit_dry_run = invoke([*edit_argv, "--dry-run", "--columns", "*"])
        edit_dry_data = _require_exact_dry_run_plan(
            edit_dry_run,
            expected_action="workflow.edit",
            method="POST" if config.ds_version == "1.3.9" else "PUT",
            required_text=(updated_workflow_description,),
            label="gate-owned workflow edit dry run",
        )
        diff = require_mapping(
            edit_dry_data.get("diff"),
            label="workflow edit dry-run diff",
        )
        expected_workflow_diff = {
            "workflow_changes": [
                {
                    "field": "description",
                    "before": created_workflow_description,
                    "after": updated_workflow_description,
                }
            ],
            "added_tasks": [],
            "task_changes": [],
            "renamed_tasks": [],
            "deleted_tasks": [],
            "added_edges": [],
            "removed_edges": [],
            "dag_valid": True,
        }
        if (
            diff != expected_workflow_diff
            or edit_dry_data.get("no_change") is not False
        ):
            message = "workflow edit dry run was not description-only"
            raise AssertionError(message)
        _record_success(
            trace,
            "workflow.edit",
            argv_shape=(
                "workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE --dry-run"
            ),
            assertions=(
                "installed-exact-workflow-edit-plan",
                "description-only-diff",
                "dry-run-no-request-sent",
            ),
        )
        after_edit_dry_run = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=created_workflow_description,
        )
        if after_edit_dry_run != after_task_update:
            message = "workflow edit dry run changed remote state"
            raise AssertionError(message)

        edited = _ok_data(
            invoke(edit_argv),
            expected_action="workflow.edit",
            label="gate-owned workflow edit",
        )
        _require_owned_workflow(
            edited,
            project=project,
            workflow_name=workflow_name,
            description=updated_workflow_description,
            expected_code=workflow_code,
        )
        _record_success(
            trace,
            "workflow.edit",
            argv_shape=("workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE"),
            assertions=("description-updated-exactly",),
        )
        after_workflow_edit = _inspect_full_owned_workflow(
            invoke,
            invoke_raw=invoke_raw,
            trace=trace,
            ds_version=config.ds_version,
            project=project,
            workflow_name=workflow_name,
            workflow_code=workflow_code,
            description=updated_workflow_description,
        )
        _require_workflow_edit_transition(
            after_task_update,
            after_workflow_edit,
            ds_version=config.ds_version,
        )
        if task_guard_strategy == "workflow-cascade-proof-only":
            _require_cascade_workflow_version_transition(
                after_task_update,
                after_workflow_edit,
                expected=(2, 3),
                label="workflow edit",
            )
        if task_guard_strategy is not None:
            proof = invoke_task_cleanup("prove", project.code, workflow_code)
            _require_task_cleanup_result(
                proof,
                operation="prove",
                ds_version=config.ds_version,
                expected_observed=2,
                expected_released=0,
                expected_deleted=0,
                expected_remaining=2,
                expected_remote_mutations=0,
            )
            if task_guard_strategy == "direct-delete":
                _record_success(
                    trace,
                    _TASK_DEFINITION_PROVE_ACTION,
                    argv_shape="release-gate task-definition prove",
                    assertions=(
                        "project-wide-task-inventory-proven",
                        "exact-two-owned-tasks",
                        "zero-remote-mutations",
                    ),
                )
            else:
                _record_success(
                    trace,
                    _TASK_DEFINITION_PROVE_ACTION,
                    argv_shape="release-gate task-definition prove",
                    assertions=(
                        "workflow-bound-task-inventory-proven",
                        "batch-and-stream-inventories-proven",
                        "task-history-lineage-proven",
                        "exact-two-owned-tasks",
                        "zero-remote-mutations",
                    ),
                )

        deleted_workflow = _ok_data(
            invoke(
                [
                    "workflow",
                    "delete",
                    str(workflow_code),
                    "--project",
                    project.name,
                    "--force",
                ]
            ),
            expected_action="workflow.delete",
            label="gate-owned workflow delete",
        )
        if deleted_workflow.get("deleted") is not True:
            message = "gate-owned workflow delete was not confirmed"
            raise AssertionError(message)
        _record_success(
            trace,
            "workflow.delete",
            argv_shape="workflow delete WORKFLOW --project PROJECT --force",
            assertions=("gate-owned-workflow-deleted",),
        )
        workflow_leftovers = _full_workflow_list(
            invoke,
            project_name=project.name,
            workflow_name=workflow_name,
            trace=trace,
            assertions=("workflow-leftovers-zero",),
            outcomes=(
                "gate-owned-workflow-round-trip",
                "workflow-dag-cross-checked",
                "task-update-round-trip",
                "workflow-edit-round-trip",
            ),
        )
        if workflow_leftovers:
            message = "gate-owned workflow remained after deletion"
            raise AssertionError(message)
        if task_guard_strategy == "direct-delete":
            cleanup_result = invoke_task_cleanup("cleanup", project.code, workflow_code)
            expected_released = (
                2
                if config.ds_version in _TASK_DEFINITION_PRE_DELETE_RELEASE_VERSIONS
                else 0
            )
            _require_task_cleanup_result(
                cleanup_result,
                operation="cleanup",
                ds_version=config.ds_version,
                expected_observed=2,
                expected_released=expected_released,
                expected_deleted=2,
                expected_remaining=0,
                expected_remote_mutations=2 + expected_released,
            )
            mutation_assertion = (
                "remote-mutations-four" if expected_released else "remote-mutations-two"
            )
            cleanup_assertions = [
                "exact-two-owned-tasks-deleted",
                "fresh-zero-reconciliation",
                mutation_assertion,
            ]
            if expected_released:
                cleanup_assertions.insert(0, "exact-two-online-tasks-released")
            _record_success(
                trace,
                _TASK_DEFINITION_CLEANUP_ACTION,
                argv_shape="release-gate task-definition cleanup",
                assertions=tuple(cleanup_assertions),
            )
        elif task_guard_strategy == "workflow-cascade-proof-only":
            cleanup_result = invoke_task_cleanup(
                "cleanup",
                project.code,
                workflow_code,
            )
            _require_task_cleanup_result(
                cleanup_result,
                operation="cleanup",
                ds_version=config.ds_version,
                expected_observed=0,
                expected_released=0,
                expected_deleted=0,
                expected_remaining=0,
                expected_remote_mutations=0,
            )
            _record_success(
                trace,
                _TASK_DEFINITION_CLEANUP_ACTION,
                argv_shape="release-gate task-definition cleanup",
                assertions=(
                    "post-cascade-zero-reconciliation",
                    "observed-zero-tasks",
                    "deleted-zero-tasks",
                    "remaining-zero-tasks",
                    "zero-remote-mutations",
                ),
            )
            _verify_full_task_absence(
                invoke,
                trace=trace,
                project_name=project.name,
                workflow_name=workflow_name,
            )
        else:
            _verify_full_task_absence(
                invoke,
                trace=trace,
                project_name=project.name,
                workflow_name=workflow_name,
            )

        deleted_project = _ok_data(
            invoke(["project", "delete", str(project.code), "--force"]),
            expected_action="project.delete",
            label="full gate-owned project delete",
        )
        if deleted_project.get("deleted") is not True:
            message = "full gate-owned project delete was not confirmed"
            raise AssertionError(message)
        _record_success(
            trace,
            "project.delete",
            argv_shape="project delete PROJECT --force",
            assertions=("gate-owned-project-deleted-after-workflow",),
        )
        deleted_project_lookup = invoke(["project", "get", project.name])
        require_error_payload(
            deleted_project_lookup,
            expected_action="project.get",
            expected_type="not_found",
            label="deleted full project lookup",
        )
        _record_failure(
            trace,
            deleted_project_lookup,
            "project.get",
            argv_shape="project get PROJECT",
            error_type="not_found",
            assertions=("stable-not-found", "deleted-project-absent"),
        )
        project_leftovers = _project_list(
            invoke,
            project_name=project.name,
            trace=trace,
            outcomes=("full-cleanup-zero", "negative-errors-translated"),
        )
        if project_leftovers:
            message = "gate-owned project remained after full cleanup"
            raise AssertionError(message)

        fixture_after = _read_external_fixture(
            config,
            invoke=invoke,
            trace=trace,
            observed_outcome=None,
        )
        fixture_after_hmac = _state_hmac(fixture_after, key=config.attestation_key)
        if fixture_after_hmac != fixture_before_hmac:
            message = "external fixture changed during the full conformance scenario"
            raise AssertionError(message)
        return ConformanceScenarioResult(
            version_data=version_data,
            operation_trace=tuple(trace),
            effects=ScenarioEffects(
                remote_mutations=(
                    9
                    + (
                        2
                        if config.ds_version
                        in _TASK_DEFINITION_PRE_DELETE_RELEASE_VERSIONS
                        else 0
                    )
                    if task_guard_strategy == "direct-delete"
                    else 7
                )
            ),
            cleanup=ScenarioCleanup(
                gate_owned_projects=0,
                gate_owned_workflows=0,
                gate_owned_tasks=0,
                external_fixture_state_hmac_matched=True,
            ),
            fixture_before_hmac=fixture_before_hmac,
            fixture_after_hmac=fixture_after_hmac,
        )
    finally:
        workflow_file.unlink(missing_ok=True)
        patch_file.unlink(missing_ok=True)


def _create_and_update_full_project(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
    run_id: str,
) -> _OwnedProjectScope:
    name = f"dsctl-conformance-{config.ds_version.replace('.', '-')}-{run_id}"
    ownership = f"dsctl-conformance-owner:{run_id}"
    created_description = f"{ownership};phase=created"
    updated_description = f"{ownership};phase=updated"
    identity_kind = _native_identity_kind(config.ds_version)
    created = _ok_data(
        invoke(
            [
                "project",
                "create",
                "--name",
                name,
                "--description",
                created_description,
            ]
        ),
        expected_action="project.create",
        label="full gate-owned project create",
    )
    code = _owned_project_identity(
        created,
        identity_key=identity_kind,
        name=name,
        ownership=ownership,
        description=created_description,
    )
    _record_success(
        trace,
        "project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        assertions=("gate-owned-project-created", "native-identity-captured"),
    )
    duplicate = invoke(
        [
            "project",
            "create",
            "--name",
            name,
            "--description",
            created_description,
        ]
    )
    duplicate_error = require_error_payload(
        duplicate,
        expected_action="project.create",
        expected_type="conflict",
        label="duplicate full gate-owned project",
    )
    if not _non_empty_text(duplicate_error.get("suggestion")):
        message = "duplicate full project conflict omitted a suggestion"
        raise AssertionError(message)
    _record_failure(
        trace,
        duplicate,
        "project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        error_type="conflict",
        assertions=("stable-conflict", "actionable-suggestion"),
    )
    listed = _project_list(invoke, project_name=name, trace=trace)
    _require_owned_project(
        listed,
        identity_key=identity_kind,
        identity=code,
        name=name,
        ownership=ownership,
        description=created_description,
    )
    for selector in (name, str(code)):
        project_data = _project_get(
            invoke,
            selector=selector,
            trace=trace,
            assertions=("gate-owned-project-matched", "native-identity-matched"),
        )
        _require_owned_project(
            [project_data],
            identity_key=identity_kind,
            identity=code,
            name=name,
            ownership=ownership,
            description=created_description,
        )
    updated = _ok_data(
        invoke(
            [
                "project",
                "update",
                str(code),
                "--description",
                updated_description,
            ]
        ),
        expected_action="project.update",
        label="full gate-owned project update",
    )
    _require_owned_project(
        [updated],
        identity_key=identity_kind,
        identity=code,
        name=name,
        ownership=ownership,
        description=updated_description,
    )
    _record_success(
        trace,
        "project.update",
        argv_shape="project update PROJECT --description DESCRIPTION",
        assertions=("description-marker-updated",),
    )
    readback = _project_get(
        invoke,
        selector=str(code),
        trace=trace,
        assertions=("updated-description-read-back",),
        outcomes=("gate-owned-project-round-trip",),
    )
    _require_owned_project(
        [readback],
        identity_key=identity_kind,
        identity=code,
        name=name,
        ownership=ownership,
        description=updated_description,
    )
    return _OwnedProjectScope(
        name=name,
        code=code,
        identity_kind=identity_kind,
        ownership=ownership,
        created_description=created_description,
        updated_description=updated_description,
    )


def _write_full_workflow_sources(
    config: ConformanceBundleGateConfig,
    *,
    run_id: str,
    project: _OwnedProjectScope,
    workflow_name: str,
    created_description: str,
    updated_description: str,
) -> tuple[Path, Path]:
    source_root = config.evidence_path.parent
    source_root.mkdir(parents=True, exist_ok=True)
    workflow_path = source_root / f".conformance-full-{run_id}.yaml"
    patch_path = source_root / f".conformance-full-{run_id}.patch.yaml"

    def task_params(command: str) -> dict[str, object]:
        return {
            "rawScript": command,
            "localParams": [],
            "resourceList": [],
        }

    task_defaults: dict[str, object] = {
        "type": "SHELL",
        "worker_group": "default",
        "priority": "MEDIUM",
        "retry": {"times": 0, "interval": 0},
        "timeout": 0,
        "delay": 0,
    }
    workflow_document = {
        "workflow": {
            "name": workflow_name,
            "project": project.name,
            "description": created_description,
            "timeout": 0,
            "execution_type": "PARALLEL",
            "release_state": "OFFLINE",
        },
        "tasks": [
            {
                "name": "extract",
                "description": f"{project.ownership};resource=task;name=extract",
                **task_defaults,
                "task_params": task_params(f'printf "%s\\n" "{run_id}-extract"\n'),
                "depends_on": [],
            },
            {
                "name": "load",
                "description": f"{project.ownership};resource=task;name=load",
                **task_defaults,
                "task_params": task_params(f'printf "%s\\n" "{run_id}-load"\n'),
                "depends_on": ["extract"],
            },
        ],
    }
    patch_document = {
        "patch": {
            "workflow": {
                "set": {"description": updated_description},
            }
        }
    }
    _write_owner_private_text(
        workflow_path,
        yaml.safe_dump(workflow_document, sort_keys=False),
    )
    try:
        _write_owner_private_text(
            patch_path,
            yaml.safe_dump(patch_document, sort_keys=False),
        )
    except Exception:
        workflow_path.unlink(missing_ok=True)
        raise
    return workflow_path, patch_path


def _write_owner_private_text(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _require_exact_dry_run_plan(
    result: DsctlCommandResult,
    *,
    expected_action: str,
    method: str,
    required_text: tuple[str, ...],
    label: str,
) -> dict[str, object]:
    data = _ok_data(result, expected_action=expected_action, label=label)
    if data.get("dry_run") is not True:
        message = f"{label} did not identify itself as a dry run"
        raise AssertionError(message)
    request = require_mapping(first_dry_run_request(data), label=f"{label} request")
    path = request.get("path")
    if (
        request.get("method") != method
        or not isinstance(path, str)
        or not path.startswith("/")
        or not isinstance(request.get("form"), dict)
    ):
        message = f"{label} did not expose one installed exact form request"
        raise AssertionError(message)
    if any(not _nested_text_contains(request, expected) for expected in required_text):
        message = f"{label} request omitted its bounded authoring intent"
        raise AssertionError(message)
    warning_items = [
        require_mapping(raw, label=f"{label} warning detail")
        for raw in require_list(
            result.payload.get("warnings"),
            label=f"{label} warning details",
        )
    ]
    if not any(
        item.get("code") == "dry_run_no_mutation_sent"
        and item.get("mutation_sent") is False
        for item in warning_items
    ):
        message = f"{label} did not attest mutation_sent=false"
        raise AssertionError(message)
    return data


def _nested_text_contains(value: object, expected: str) -> bool:
    if isinstance(value, str):
        return expected in value
    if isinstance(value, dict):
        return any(_nested_text_contains(item, expected) for item in value.values())
    if isinstance(value, list):
        return any(_nested_text_contains(item, expected) for item in value)
    return False


def _full_workflow_list(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    project_name: str,
    workflow_name: str,
    trace: list[OperationTraceEntry],
    assertions: tuple[str, ...],
    outcomes: tuple[str, ...] = (),
) -> list[dict[str, object]]:
    data = _ok_data(
        invoke(
            [
                "workflow",
                "list",
                "--project",
                project_name,
                "--search",
                workflow_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="workflow.list",
        label="gate-owned workflow list",
    )
    rows = [
        row
        for row in _page_rows(data, label="gate-owned workflow list")
        if row.get("name") == workflow_name
    ]
    _record_success(
        trace,
        "workflow.list",
        argv_shape=(
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=assertions,
        outcomes=outcomes,
    )
    return rows


def _require_task_cleanup_result(
    result: TaskDefinitionCleanupInvocation,
    *,
    operation: Literal["prove", "cleanup"],
    ds_version: str,
    expected_observed: int | None = None,
    expected_released: int | None = None,
    expected_deleted: int | None = None,
    expected_remaining: int | None = None,
    expected_remote_mutations: int | None = None,
) -> None:
    if type(result) is not TaskDefinitionCleanupInvocation:
        message = "private task-definition cleanup returned an invalid result type"
        raise AssertionError(message)
    if result.operation != operation or result.ds_version != ds_version:
        message = "private task-definition cleanup identity drifted"
        raise AssertionError(message)
    counts = (
        result.observed,
        result.released,
        result.deleted,
        result.remaining,
        result.remote_mutations,
    )
    if any(type(value) is not int or value < 0 for value in counts):
        message = "private task-definition cleanup returned invalid exact counts"
        raise AssertionError(message)
    if (
        result.observed - result.deleted != result.remaining
        or result.released > result.observed
    ):
        message = "private task-definition cleanup counts were inconsistent"
        raise AssertionError(message)
    if operation == "prove" and (
        result.released != 0 or result.deleted != 0 or result.remote_mutations != 0
    ):
        message = "private task-definition proof claimed a remote mutation"
        raise AssertionError(message)
    if (
        operation == "cleanup"
        and result.remote_mutations != result.released + result.deleted
    ):
        message = "private task-definition cleanup mutation count was inconsistent"
        raise AssertionError(message)
    expected = (
        expected_observed,
        expected_released,
        expected_deleted,
        expected_remaining,
        expected_remote_mutations,
    )
    if any(
        wanted is not None and actual != wanted
        for actual, wanted in zip(counts, expected, strict=True)
    ):
        message = "private task-definition cleanup did not match the owned scope"
        raise AssertionError(message)


def _verify_full_task_absence(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    trace: list[OperationTraceEntry],
    project_name: str,
    workflow_name: str,
) -> None:
    result = invoke(
        [
            "task",
            "list",
            "--project",
            project_name,
            "--workflow",
            workflow_name,
        ]
    )
    assertions = ("task-leftovers-zero-with-workflow-absent",)
    argv_shape = "task list --project PROJECT --workflow WORKFLOW"
    if result.payload.get("ok") is True:
        payload = require_ok_payload(
            result,
            expected_action="task.list",
            label="full cleanup task list",
        )
        if require_list(payload.get("data"), label="full cleanup task list data"):
            message = "gate-owned tasks remained after workflow deletion"
            raise AssertionError(message)
        _record_success(
            trace,
            "task.list",
            argv_shape=argv_shape,
            assertions=assertions,
        )
        return
    require_error_payload(
        result,
        expected_action="task.list",
        expected_type="not_found",
        label="full cleanup task list",
    )
    _record_failure(
        trace,
        result,
        "task.list",
        argv_shape=argv_shape,
        error_type="not_found",
        assertions=("stable-not-found", *assertions),
    )


def _require_owned_workflow(
    value: Mapping[str, object],
    *,
    project: _OwnedProjectScope,
    workflow_name: str,
    description: str,
    expected_code: int | None = None,
) -> int:
    identity_key = project.identity_kind
    project_identity_key = "projectId" if identity_key == "id" else "projectCode"
    code = value.get(identity_key)
    if not isinstance(code, int) or isinstance(code, bool) or code <= 0:
        message = f"gate-owned workflow omitted its native {identity_key}"
        raise AssertionError(message)
    if expected_code is not None and code != expected_code:
        message = "gate-owned workflow native code changed"
        raise AssertionError(message)
    if (
        value.get("name") != workflow_name
        or value.get("description") != description
        or value.get("releaseState") != "OFFLINE"
        or value.get(project_identity_key) != project.code
        or value.get("schedule") is not None
    ):
        message = "workflow does not prove exact gate ownership and OFFLINE state"
        raise AssertionError(message)
    return code


def _verify_full_workflow_identity_reads(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    trace: list[OperationTraceEntry],
    project: _OwnedProjectScope,
    workflow_name: str,
    workflow_code: int,
    description: str,
) -> None:
    rows = _full_workflow_list(
        invoke,
        project_name=project.name,
        workflow_name=workflow_name,
        trace=trace,
        assertions=("gate-owned-workflow-listed", "native-workflow-code-matched"),
    )
    if len(rows) != 1 or rows[0].get(project.identity_kind) != workflow_code:
        message = "workflow list did not return one exact gate-owned workflow"
        raise AssertionError(message)
    for selector in (workflow_name, str(workflow_code)):
        data = _ok_data(
            invoke(["workflow", "get", selector, "--project", project.name]),
            expected_action="workflow.get",
            label="gate-owned workflow get",
        )
        _require_owned_workflow(
            data,
            project=project,
            workflow_name=workflow_name,
            description=description,
            expected_code=workflow_code,
        )
        _record_success(
            trace,
            "workflow.get",
            argv_shape="workflow get WORKFLOW --project PROJECT",
            assertions=(
                "gate-owned-workflow-matched",
                "native-workflow-code-matched",
                "workflow-offline",
            ),
        )


def _inspect_full_owned_workflow(  # noqa: C901
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    invoke_raw: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
    ds_version: str,
    project: _OwnedProjectScope,
    workflow_name: str,
    workflow_code: int,
    description: str,
    describe_assertions: tuple[str, ...] = (),
) -> _OwnedWorkflowSnapshot:
    selector = [str(workflow_code), "--project", project.name]
    describe = _ok_data(
        invoke(["workflow", "describe", *selector]),
        expected_action="workflow.describe",
        label="gate-owned workflow describe",
    )
    workflow = require_mapping(
        describe.get("workflow"),
        label="gate-owned described workflow",
    )
    _require_owned_workflow(
        workflow,
        project=project,
        workflow_name=workflow_name,
        description=description,
        expected_code=workflow_code,
    )
    workflow_version = _workflow_version(
        workflow.get("version"),
        ds_version=ds_version,
        label="gate-owned workflow version",
    )
    described_tasks = [
        require_mapping(raw, label="gate-owned described task")
        for raw in require_list(
            describe.get("tasks"),
            label="gate-owned described tasks",
        )
    ]
    tasks = tuple(
        sorted(
            (
                _task_snapshot(task, identity_kind=project.identity_kind)
                for task in described_tasks
            ),
            key=_task_key,
        )
    )
    _require_exact_two_owned_tasks(tasks, label="workflow describe")
    for task, raw_task in zip(
        tasks,
        sorted(
            described_tasks,
            key=lambda value: _raw_task_key(
                value,
                identity_kind=project.identity_kind,
            ),
        ),
        strict=True,
    ):
        expected_task_description = (
            f"{project.ownership};resource=task;name={task.name}"
        )
        if (
            project.identity_kind != "id"
            and raw_task.get("description") != expected_task_description
        ):
            message = "gate-owned task ownership description differs"
            raise AssertionError(message)
    roots, edges = _validated_relation_topology(
        describe.get("relations"),
        tasks=tasks,
        identity_kind=project.identity_kind,
    )
    task_codes = {task.name: task.native_id for task in tasks}
    if roots != (task_codes["extract"],) or edges != (
        (task_codes["extract"], task_codes["load"]),
    ):
        message = "gate-owned workflow topology differs from extract-to-load"
        raise AssertionError(message)
    _record_success(
        trace,
        "workflow.describe",
        argv_shape="workflow describe WORKFLOW --project PROJECT",
        assertions=(
            "two-task-dag-matched",
            "relation-endpoints-coherent",
            "workflow-offline",
            *describe_assertions,
        ),
    )

    digest = _ok_data(
        invoke(["workflow", "digest", *selector]),
        expected_action="workflow.digest",
        label="gate-owned workflow digest",
    )
    if set(digest) != _WORKFLOW_DIGEST_KEYS:
        message = "workflow digest top-level projection differs from describe contract"
        raise AssertionError(message)
    digest_workflow = require_mapping(
        digest.get("workflow"),
        label="gate-owned digest workflow",
    )
    digest_workflow_keys = (
        _LEGACY_WORKFLOW_DIGEST_WORKFLOW_KEYS
        if project.identity_kind == "id"
        else _WORKFLOW_DIGEST_WORKFLOW_KEYS
    )
    expected_digest_workflow = {key: workflow.get(key) for key in digest_workflow_keys}
    if (
        set(digest_workflow) != digest_workflow_keys
        or digest_workflow != expected_digest_workflow
    ):
        message = "workflow digest metadata differs from describe"
        raise AssertionError(message)
    raw_global_param_map = workflow.get("globalParamMap")
    global_param_map = (
        {}
        if raw_global_param_map is None
        else require_mapping(
            raw_global_param_map,
            label="gate-owned workflow global parameter map",
        )
    )
    if digest.get("globalParamNames") != sorted(global_param_map):
        message = "workflow digest global parameter names differ from describe"
        raise AssertionError(message)
    if (
        digest_workflow.get(project.identity_kind) != workflow_code
        or digest_workflow.get("name") != workflow_name
        or digest.get("taskCount") != 2
        or digest.get("relationCount") != 1
        or digest.get("taskTypeCounts") != {"SHELL": 2}
    ):
        message = "workflow digest counts or identity differ from describe"
        raise AssertionError(message)
    digest_tasks = [
        require_mapping(raw, label="gate-owned digest task")
        for raw in require_list(digest.get("tasks"), label="gate-owned digest tasks")
    ]
    digest_identities = [
        _digest_task_identity(
            task,
            identity_kind=project.identity_kind,
            label="digest task",
        )
        for task in digest_tasks
    ]
    if (
        len(digest_identities) != 2
        or len({code for code, _name in digest_identities}) != 2
        or len({name for _code, name in digest_identities}) != 2
    ):
        message = "workflow digest did not contain exactly two unique task rows"
        raise AssertionError(message)
    if set(digest_identities) != {(task.native_id, task.name) for task in tasks}:
        message = "workflow digest task identities differ from describe"
        raise AssertionError(message)
    described_by_code = {task.native_id: task for task in tasks}
    known_identities = set(digest_identities)
    digest_downstream_edges: list[tuple[int | str, int | str]] = []
    digest_upstream_edges: list[tuple[int | str, int | str]] = []
    for raw_digest_task, (code, _name) in zip(
        digest_tasks, digest_identities, strict=True
    ):
        expected = described_by_code[code]
        if (
            set(raw_digest_task)
            != {
                project.identity_kind,
                "name",
                "taskType",
                "upstreamTasks",
                "downstreamTasks",
                "isRoot",
                "isLeaf",
            }
            or raw_digest_task.get("taskType") != expected.task_type
        ):
            message = "workflow digest task projection differs from describe"
            raise AssertionError(message)
        upstream = _digest_task_references(
            raw_digest_task.get("upstreamTasks"),
            identity_kind=project.identity_kind,
            label="digest upstream tasks",
        )
        downstream = _digest_task_references(
            raw_digest_task.get("downstreamTasks"),
            identity_kind=project.identity_kind,
            label="digest downstream tasks",
        )
        if not {*upstream, *downstream} <= known_identities:
            message = "workflow digest task references differ from describe"
            raise AssertionError(message)
        digest_upstream_edges.extend(
            (upstream_code, code) for upstream_code, _ in upstream
        )
        digest_downstream_edges.extend(
            (code, downstream_code) for downstream_code, _ in downstream
        )
        if raw_digest_task.get("isRoot") is not (code in roots) or raw_digest_task.get(
            "isLeaf"
        ) is not (code not in {pre_code for pre_code, _post_code in edges}):
            message = "workflow digest task flags differ from describe"
            raise AssertionError(message)
    digest_roots = tuple(
        sorted(
            _digest_task_references(
                digest.get("rootTasks"),
                identity_kind=project.identity_kind,
                label="digest root tasks",
            )
        )
    )
    digest_leaves = tuple(
        sorted(
            _digest_task_references(
                digest.get("leafTasks"),
                identity_kind=project.identity_kind,
                label="digest leaf tasks",
            )
        )
    )
    digest_isolated = _digest_task_references(
        digest.get("isolatedTasks"),
        identity_kind=project.identity_kind,
        label="digest isolated tasks",
    )
    expected_roots = tuple(
        sorted((code, described_by_code[code].name) for code in roots)
    )
    expected_leaf_codes = {task.native_id for task in tasks} - {
        pre_code for pre_code, _ in edges
    }
    expected_leaves = tuple(
        sorted((code, described_by_code[code].name) for code in expected_leaf_codes)
    )
    if (
        tuple(sorted(digest_downstream_edges)) != edges
        or tuple(sorted(digest_upstream_edges)) != edges
        or digest_roots != expected_roots
        or digest_leaves != expected_leaves
        or digest_isolated
    ):
        message = "workflow digest topology differs from describe"
        raise AssertionError(message)
    _record_success(
        trace,
        "workflow.digest",
        argv_shape="workflow digest WORKFLOW --project PROJECT",
        assertions=("digest-counts-match-describe", "digest-topology-match-describe"),
    )

    exported = invoke_raw(["workflow", "export", *selector])
    if exported.exit_code != 0 or exported.stderr:
        message = "gate-owned workflow export failed"
        raise AssertionError(message)
    export_document = require_mapping(
        yaml.safe_load(exported.stdout),
        label="gate-owned workflow export document",
    )
    if set(export_document) != {"workflow", "tasks"}:
        message = "workflow export differs from its canonical structure"
        raise AssertionError(message)
    exported_workflow = require_mapping(
        export_document.get("workflow"),
        label="gate-owned exported workflow",
    )
    expected_exported_workflow = _compact_export_mapping(
        {
            "name": workflow_name,
            "project": project.name,
            "description": description,
            "timeout": workflow.get("timeout"),
            "global_params": workflow.get("globalParamMap"),
            "execution_type": workflow.get("executionType"),
            "release_state": "OFFLINE",
        }
    )
    if project.identity_kind == "id":
        expected_exported_workflow["global_params"] = []
    if dict(exported_workflow) != expected_exported_workflow:
        message = "workflow export differs from its canonical structure"
        raise AssertionError(message)
    exported_tasks = [
        require_mapping(raw, label="gate-owned exported task")
        for raw in require_list(
            export_document.get("tasks"),
            label="gate-owned exported tasks",
        )
    ]
    if len(exported_tasks) != 2:
        message = "workflow export differs from its canonical structure"
        raise AssertionError(message)
    exported_names = [
        _required_text(task.get("name"), label="exported task name")
        for task in exported_tasks
    ]
    if len(set(exported_names)) != 2:
        message = "workflow export differs from its canonical structure"
        raise AssertionError(message)
    exported_by_name = dict(zip(exported_names, exported_tasks, strict=True))
    snapshot_by_name = {task.name: task for task in tasks}
    if set(exported_by_name) != set(snapshot_by_name):
        message = "workflow export task identities differ from describe"
        raise AssertionError(message)
    task_names_by_code = {task.native_id: task.name for task in tasks}
    dependencies_by_code: dict[int | str, list[str]] = {
        task.native_id: [] for task in tasks
    }
    for pre_code, post_code in edges:
        dependencies_by_code[post_code].append(task_names_by_code[pre_code])
    described_by_name = {
        _required_text(task.get("name"), label="described task name"): task
        for task in described_tasks
    }
    for name, exported_task in exported_by_name.items():
        described_task = described_by_name[name]
        described_identity = _task_native_identity(
            described_task,
            identity_kind=project.identity_kind,
            label="described export task",
        )
        expected_export = (
            _canonical_legacy_export_task(
                described_task,
                description=(f"{project.ownership};resource=task;name={name}"),
                depends_on=dependencies_by_code[described_identity],
            )
            if project.identity_kind == "id"
            else _canonical_export_task(
                described_task,
                depends_on=dependencies_by_code[described_identity],
            )
        )
        if dict(exported_task) != expected_export:
            message = "exported task differs from its canonical public task projection"
            raise AssertionError(message)
    if exported_by_name["extract"].get("depends_on", []) != [] or exported_by_name[
        "load"
    ].get("depends_on") != ["extract"]:
        message = "workflow export topology differs from describe"
        raise AssertionError(message)
    _record_success(
        trace,
        "workflow.export",
        argv_shape="workflow export WORKFLOW --project PROJECT",
        assertions=("raw-yaml-dag-matches-describe", "raw-body-not-recorded"),
    )

    task_scope = ["--project", project.name, "--workflow", workflow_name]
    task_list_payload = require_ok_payload(
        invoke(["task", "list", *task_scope]),
        expected_action="task.list",
        label="gate-owned task list",
    )
    task_rows = [
        require_mapping(raw, label="gate-owned task list row")
        for raw in require_list(
            task_list_payload.get("data"),
            label="gate-owned task list data",
        )
    ]
    _require_task_list_row_projection(
        task_rows,
        identity_kind=project.identity_kind,
        label="gate-owned task list",
    )
    listed_identities = [
        (
            _task_native_identity(
                row,
                identity_kind=project.identity_kind,
                label="listed task",
            ),
            _required_text(row.get("name"), label="listed task name"),
            (
                None
                if project.identity_kind == "id"
                else _positive_int(
                    row.get("version"),
                    label="listed task version",
                )
            ),
        )
        for row in task_rows
    ]
    if (
        len(listed_identities) != 2
        or len({code for code, _name, _version in listed_identities}) != 2
        or len({name for _code, name, _version in listed_identities}) != 2
    ):
        message = "task list did not contain exactly two unique task rows"
        raise AssertionError(message)
    if set(listed_identities) != {
        (task.native_id, task.name, task.version) for task in tasks
    }:
        message = "task list differs from workflow describe"
        raise AssertionError(message)
    _record_success(
        trace,
        "task.list",
        argv_shape="task list --project PROJECT --workflow WORKFLOW",
        assertions=(
            "two-task-list-matched-dag",
            (
                "native-task-ids-coherent"
                if project.identity_kind == "id"
                else "task-versions-coherent"
            ),
        ),
    )
    for task in tasks:
        selectors = _task_read_selectors(
            "1.3.9" if project.identity_kind == "id" else "modern",
            task,
        )
        for selector_value in selectors:
            selector_kind = "name" if selector_value == task.name else "native"
            task_data = _ok_data(
                invoke(["task", "get", selector_value, *task_scope]),
                expected_action="task.get",
                label="gate-owned task get",
            )
            if _task_snapshot(task_data, identity_kind=project.identity_kind) != task:
                message = "task get differs from list and workflow DAG"
                raise AssertionError(message)
            if project.identity_kind == "id" and task_data.get("description") != (
                f"{project.ownership};resource=task;name={task.name}"
            ):
                message = "gate-owned legacy task ownership description differs"
                raise AssertionError(message)
            _record_success(
                trace,
                "task.get",
                argv_shape="task get TASK --project PROJECT --workflow WORKFLOW",
                assertions=(
                    "task-get-matched-list-and-dag",
                    f"selector-{selector_kind}-matched",
                ),
            )
    return _OwnedWorkflowSnapshot(
        code=workflow_code,
        name=workflow_name,
        version=workflow_version,
        description=description,
        release_state="OFFLINE",
        non_owned_digest=_workflow_non_owned_digest(
            workflow,
            ds_version=ds_version,
        ),
        tasks=tasks,
        roots=roots,
        edges=edges,
    )


def _validated_relation_topology(
    value: object,
    *,
    tasks: tuple[_OwnedTaskSnapshot, ...],
    identity_kind: IdentityKind,
) -> tuple[
    tuple[int | str, ...],
    tuple[tuple[int | str, int | str], ...],
]:
    task_names = {task.native_id: task.name for task in tasks}
    relations = [
        require_mapping(raw, label="gate-owned workflow relation")
        for raw in require_list(value, label="gate-owned workflow relations")
    ]
    roots: list[int | str] = []
    edges: list[tuple[int | str, int | str]] = []
    if identity_kind == "id":
        return _validated_legacy_relation_topology(relations, task_names=task_names)
    for relation in relations:
        if set(relation) != {
            "preTaskCode",
            "preTaskName",
            "postTaskCode",
            "postTaskName",
        }:
            message = "workflow relation differs from the stable public projection"
            raise AssertionError(message)
        pre_code = _non_negative_int(
            relation.get("preTaskCode"),
            label="relation pre task code",
        )
        post_code = _positive_int(
            relation.get("postTaskCode"),
            label="relation post task code",
        )
        if (
            post_code not in task_names
            or relation.get("postTaskName") != task_names[post_code]
        ):
            message = "relation post-task endpoint differs from the DAG task"
            raise AssertionError(message)
        if pre_code == 0:
            if relation.get("preTaskName") is not None:
                message = "root relation carries a named pre-task endpoint"
                raise AssertionError(message)
            roots.append(post_code)
        else:
            if (
                pre_code not in task_names
                or relation.get("preTaskName") != task_names[pre_code]
            ):
                message = "relation pre-task endpoint differs from the DAG task"
                raise AssertionError(message)
            edges.append((pre_code, post_code))
    return tuple(sorted(roots)), tuple(sorted(edges))


def _validated_legacy_relation_topology(
    relations: Sequence[Mapping[str, object]],
    *,
    task_names: Mapping[int | str, str],
) -> tuple[
    tuple[int | str, ...],
    tuple[tuple[int | str, int | str], ...],
]:
    expected_keys = {"preTaskId", "preTaskName", "postTaskId", "postTaskName"}
    edges: list[tuple[int | str, int | str]] = []
    for relation in relations:
        if set(relation) != expected_keys:
            message = "workflow relation differs from the stable public projection"
            raise AssertionError(message)
        pre_id = _required_text(relation.get("preTaskId"), label="relation pre task id")
        post_id = _required_text(
            relation.get("postTaskId"), label="relation post task id"
        )
        if (
            pre_id not in task_names
            or relation.get("preTaskName") != task_names[pre_id]
            or post_id not in task_names
            or relation.get("postTaskName") != task_names[post_id]
        ):
            message = "relation endpoint differs from the legacy DAG task"
            raise AssertionError(message)
        edges.append((pre_id, post_id))
    incoming = {post_id for _pre_id, post_id in edges}
    roots = [identity for identity in task_names if identity not in incoming]
    return (
        tuple(sorted(roots, key=_task_identity_sort_key)),
        tuple(
            sorted(
                edges,
                key=lambda edge: (
                    _task_identity_sort_key(edge[0]),
                    _task_identity_sort_key(edge[1]),
                ),
            )
        ),
    )


def _digest_task_identity(
    value: Mapping[str, object],
    *,
    identity_kind: IdentityKind,
    label: str,
) -> tuple[int | str, str]:
    return (
        _task_native_identity(
            value,
            identity_kind=identity_kind,
            label=label,
        ),
        _required_text(value.get("name"), label=f"{label} name"),
    )


def _digest_task_references(
    value: object,
    *,
    identity_kind: IdentityKind,
    label: str,
) -> tuple[tuple[int | str, str], ...]:
    references = [
        require_mapping(raw, label=f"{label} reference")
        for raw in require_list(value, label=label)
    ]
    if any(set(reference) != {identity_kind, "name"} for reference in references):
        message = f"{label} differs from the stable public projection"
        raise AssertionError(message)
    return tuple(
        _digest_task_identity(
            reference,
            identity_kind=identity_kind,
            label=f"{label} reference",
        )
        for reference in references
    )


def _task_native_identity(
    value: Mapping[str, object],
    *,
    identity_kind: IdentityKind,
    label: str,
) -> int | str:
    if identity_kind == "id":
        return _required_text(value.get("id"), label=f"{label} id")
    return _positive_int(value.get("code"), label=f"{label} code")


def _require_task_list_row_projection(
    rows: Sequence[Mapping[str, object]],
    *,
    identity_kind: IdentityKind,
    label: str,
) -> None:
    expected = {"id", "name"} if identity_kind == "id" else {"code", "name", "version"}
    if any(set(row) != expected for row in rows):
        message = f"{label} row projection differs from its exact identity epoch"
        raise AssertionError(message)


def _task_snapshot(
    value: Mapping[str, object],
    *,
    identity_kind: IdentityKind,
) -> _OwnedTaskSnapshot:
    if identity_kind == "id":
        native_id: int | str = _required_text(
            value.get("id"),
            label="owned task id",
        )
        version = None
    else:
        native_id = _positive_int(value.get("code"), label="owned task code")
        version = _positive_int(value.get("version"), label="owned task version")
    name = _required_text(value.get("name"), label="owned task name")
    task_type_value = (
        value.get("type")
        if identity_kind == "id" and "type" in value
        else value.get("taskType")
    )
    task_type = _required_text(
        task_type_value,
        label="owned task type",
    )
    if task_type != "SHELL":
        message = "gate-owned task type must remain SHELL"
        raise AssertionError(message)
    params = require_mapping(value.get("taskParams"), label="owned task params")
    command = _required_text(params.get("rawScript"), label="owned SHELL rawScript")
    preserved_params = dict(params)
    preserved_params.pop("rawScript", None)
    if identity_kind == "id":
        # Legacy graph and task-detail reads expose different native field
        # sets. Their exact common preservation seam is task type plus opaque
        # task parameters; list/get identity is checked independently.
        non_owned: dict[str, object] = {
            "taskType": task_type,
            "taskParams": preserved_params,
        }
    else:
        non_owned = {
            key: nested
            for key, nested in value.items()
            # DAG reads expose the task-definition log row while task detail reads
            # expose the current main-table row. Their server-owned row ids and
            # creation timestamps may differ even when the stable code/version
            # identity and task state are equal.
            if key not in {"createTime", "id", "modifyBy", "updateTime", "version"}
        }
        non_owned["taskParams"] = preserved_params
    return _OwnedTaskSnapshot(
        native_id=native_id,
        name=name,
        version=version,
        task_type=task_type,
        command=command,
        non_owned_digest=_canonical_sha256(non_owned),
    )


def _task_identity_sort_key(value: int | str) -> tuple[int, int | str]:
    return (0, value) if isinstance(value, int) else (1, value)


def _task_read_selectors(
    ds_version: str,
    task: _OwnedTaskSnapshot,
) -> tuple[str, ...]:
    if ds_version == "1.3.9":
        return (task.name,)
    return (task.name, str(task.native_id))


def _workflow_non_owned_digest(
    value: Mapping[str, object],
    *,
    ds_version: str,
) -> str:
    allowed_to_change = {"description", "modifyBy", "updateTime", "version"}
    preserved = {
        key: nested for key, nested in value.items() if key not in allowed_to_change
    }
    if (
        ds_version == "1.3.9"
        and {"globalParams", "globalParamMap"} <= preserved.keys()
        and (
            (
                preserved.get("globalParams") is None
                and preserved.get("globalParamMap") is None
            )
            or (
                preserved.get("globalParams") == "[]"
                and preserved.get("globalParamMap") == {}
            )
        )
    ):
        preserved["globalParams"] = "[]"
        preserved["globalParamMap"] = {}
    return _canonical_sha256(preserved)


def _canonical_export_task(
    value: Mapping[str, object],
    *,
    depends_on: list[str],
) -> dict[str, object]:
    task_params = require_mapping(
        value.get("taskParams"),
        label="public task params for export",
    )
    exported: dict[str, object] = {
        "name": value.get("name"),
        "type": value.get("taskType"),
        "description": value.get("description"),
        "task_params": dict(task_params),
        "worker_group": value.get("workerGroup"),
        "priority": value.get("taskPriority"),
        "retry": {
            "times": value.get("failRetryTimes"),
            "interval": value.get("failRetryInterval"),
        },
        "timeout": value.get("timeout"),
        "delay": value.get("delayTime"),
        "depends_on": depends_on,
    }
    flag = value.get("flag")
    if isinstance(flag, str) and flag and flag != "YES":
        exported["flag"] = flag
    environment_code = value.get("environmentCode")
    if isinstance(environment_code, int) and environment_code > 0:
        exported["environment_code"] = environment_code
    timeout = value.get("timeout")
    timeout_notify_strategy = value.get("timeoutNotifyStrategy")
    if (
        isinstance(timeout, int)
        and timeout > 0
        and isinstance(timeout_notify_strategy, str)
        and timeout_notify_strategy
        and timeout_notify_strategy != "WARN"
    ):
        exported["timeout_notify_strategy"] = timeout_notify_strategy
    for source, target in (("cpuQuota", "cpu_quota"), ("memoryMax", "memory_max")):
        resource_limit = value.get(source)
        if isinstance(resource_limit, int) and resource_limit != -1:
            exported[target] = resource_limit
    task_group_id = value.get("taskGroupId")
    if isinstance(task_group_id, int) and task_group_id > 0:
        exported["task_group_id"] = task_group_id
        task_group_priority = value.get("taskGroupPriority")
        if isinstance(task_group_priority, int):
            exported["task_group_priority"] = task_group_priority
    return _compact_export_mapping(exported)


def _canonical_legacy_export_task(
    value: Mapping[str, object],
    *,
    description: str,
    depends_on: list[str],
) -> dict[str, object]:
    task_params = require_mapping(
        value.get("taskParams"),
        label="legacy public task params for export",
    )
    return {
        "name": value.get("name"),
        "type": value.get("type"),
        "description": description,
        "task_params": dict(task_params),
        "flag": "YES",
        "worker_group": "default",
        "priority": "MEDIUM",
        "retry": {"times": 0, "interval": 0},
        "depends_on": depends_on,
    }


def _compact_export_mapping(value: Mapping[str, object]) -> dict[str, object]:
    return {
        key: nested
        for key, nested in value.items()
        if nested is not None and not (isinstance(nested, (dict, list)) and not nested)
    }


def _snapshot_task(
    value: _OwnedWorkflowSnapshot,
    *,
    name: str,
) -> _OwnedTaskSnapshot:
    matches = [task for task in value.tasks if task.name == name]
    if len(matches) != 1:
        message = f"owned workflow snapshot lacks one task named {name!r}"
        raise AssertionError(message)
    return matches[0]


def _require_task_update_transition(
    before: _OwnedWorkflowSnapshot,
    after: _OwnedWorkflowSnapshot,
    *,
    ds_version: str,
    task_name: str,
    command: str,
) -> None:
    if (
        before.code != after.code
        or before.name != after.name
        or before.description != after.description
        or before.release_state != after.release_state
        or before.non_owned_digest != after.non_owned_digest
        or before.roots != after.roots
        or before.edges != after.edges
    ):
        message = "task update changed workflow identity or topology"
        raise AssertionError(message)
    before_tasks = {task.name: task for task in before.tasks}
    after_tasks = {task.name: task for task in after.tasks}
    if set(before_tasks) != set(after_tasks):
        message = "task update changed the workflow task set"
        raise AssertionError(message)
    for name, previous in before_tasks.items():
        current = after_tasks[name]
        if name == task_name:
            if (
                current.native_id != previous.native_id
                or current.task_type != previous.task_type
                or current.command != command
                or not _task_version_transition_is_valid(
                    previous,
                    current,
                    ds_version=ds_version,
                    workflow_before=before,
                    workflow_after=after,
                )
                or current.non_owned_digest != previous.non_owned_digest
            ):
                message = "task update readback differs from its bounded intent"
                raise AssertionError(message)
        elif current != previous:
            message = "task update changed a non-target task"
            raise AssertionError(message)


def _task_version_transition_is_valid(
    before: _OwnedTaskSnapshot,
    after: _OwnedTaskSnapshot,
    *,
    ds_version: str,
    workflow_before: _OwnedWorkflowSnapshot,
    workflow_after: _OwnedWorkflowSnapshot,
) -> bool:
    if before.version is None or after.version is None:
        if before.version is not None or after.version is not None:
            return False
        if ds_version == "1.3.9":
            return workflow_after.version == workflow_before.version
        return workflow_after.version > workflow_before.version
    return after.version > before.version


def _require_workflow_edit_transition(
    before: _OwnedWorkflowSnapshot,
    after: _OwnedWorkflowSnapshot,
    *,
    ds_version: str,
) -> None:
    version_transition_valid = (
        after.version == before.version
        if ds_version == "1.3.9"
        else after.version >= before.version
    )
    if (
        before.code != after.code
        or before.name != after.name
        or before.release_state != after.release_state
        or before.non_owned_digest != after.non_owned_digest
        or before.tasks != after.tasks
        or before.roots != after.roots
        or before.edges != after.edges
        or before.description == after.description
        or not version_transition_valid
    ):
        message = "workflow edit changed state outside its owned description"
        raise AssertionError(message)


def _require_cascade_workflow_version_transition(
    before: _OwnedWorkflowSnapshot,
    after: _OwnedWorkflowSnapshot,
    *,
    expected: tuple[int, int],
    label: str,
) -> None:
    if (before.version, after.version) != expected:
        message = f"{label} did not advance the exact cascade workflow version"
        raise AssertionError(message)


def _task_key(value: _OwnedTaskSnapshot) -> tuple[tuple[int, int | str], str]:
    return _task_identity_sort_key(value.native_id), value.name


def _require_exact_two_owned_tasks(
    tasks: tuple[_OwnedTaskSnapshot, ...],
    *,
    label: str,
) -> None:
    if (
        len(tasks) != 2
        or {task.name for task in tasks} != {"extract", "load"}
        or len({task.native_id for task in tasks}) != 2
    ):
        message = f"{label} did not contain its exact two unique gate-owned tasks"
        raise AssertionError(message)


def _raw_task_key(
    value: Mapping[str, object],
    *,
    identity_kind: IdentityKind,
) -> tuple[tuple[int, int | str], str]:
    return (
        _task_identity_sort_key(
            _task_native_identity(
                value,
                identity_kind=identity_kind,
                label="raw task sort identity",
            )
        ),
        _required_text(value.get("name"), label="raw task sort name"),
    )


def _positive_int(value: object, *, label: str) -> int:
    result = _non_negative_int(value, label=label)
    if result == 0:
        message = f"{label} must be positive"
        raise AssertionError(message)
    return result


def _workflow_version(
    value: object,
    *,
    ds_version: str,
    label: str,
) -> int:
    if ds_version == "1.3.9":
        return _non_negative_int(value, label=label)
    return _positive_int(value, label=label)


def _non_negative_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        message = f"{label} must be a non-negative integer"
        raise AssertionError(message)
    return value


class _GateOwnedProjectGuard:
    """Reconcile one attempted gate project without touching foreign state."""

    def __init__(
        self,
        config: ConformanceBundleGateConfig,
        *,
        invoke: Callable[[list[str]], DsctlCommandResult],
        run_id: str,
    ) -> None:
        self._invoke = invoke
        self._identity_key = _native_identity_kind(config.ds_version)
        self._name = f"dsctl-conformance-{config.ds_version.replace('.', '-')}-{run_id}"
        self._ownership = f"dsctl-conformance-owner:{run_id}"
        self._owned_descriptions = frozenset(
            {
                f"{self._ownership};phase=created",
                f"{self._ownership};phase=updated",
            }
        )
        self._attempted = False
        self._ownership_established = False

    def invoke(self, argv: list[str]) -> DsctlCommandResult:
        """Observe the single gate-owned create attempt at the process boundary."""
        is_gate_create = False
        if argv[:2] == ["project", "create"]:
            name = _argv_option(argv, "--name")
            description = _argv_option(argv, "--description")
            if name == self._name and description in self._owned_descriptions:
                is_gate_create = True
                self._attempted = True
        result = self._invoke(argv)
        if is_gate_create and result.payload.get("ok") is True:
            self._ownership_established = True
        elif (
            is_gate_create
            and not self._ownership_established
            and _result_proves_mutation_not_applied(result)
        ):
            self._attempted = False
        return result

    def cleanup(self) -> None:
        """Delete only a freshly reconciled project with exact ownership proof."""
        if not self._attempted:
            return
        self._cleanup_present_project(require_present=False)

    def _cleanup_present_project(self, *, require_present: bool) -> None:
        exact_rows = self._fresh_exact_rows()
        owned_rows = [row for row in exact_rows if self._is_owned(row)]
        if not owned_rows:
            if require_present:
                message = "recovery found no exact gate-owned project"
                raise AssertionError(message)
            return
        if len(owned_rows) != 1 or len(exact_rows) != 1:
            message = "cleanup could not isolate one exactly gate-owned project"
            raise AssertionError(message)
        native_value = owned_rows[0].get(self._identity_key)
        if (
            not isinstance(native_value, int)
            or isinstance(native_value, bool)
            or native_value <= 0
        ):
            message = "cleanup ownership proof omitted the native project identity"
            raise AssertionError(message)
        self._invoke(["project", "delete", str(native_value), "--force"])
        remaining = [row for row in self._fresh_exact_rows() if self._is_owned(row)]
        if remaining:
            message = "cleanup did not remove the proven gate-owned project"
            raise AssertionError(message)

    def _fresh_exact_rows(self) -> list[dict[str, object]]:
        result = self._invoke(
            [
                "project",
                "list",
                "--search",
                self._name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        )
        data = _ok_data(
            result,
            expected_action="project.list",
            label="gate-owned cleanup reconciliation",
        )
        return [
            row
            for row in _page_rows(data, label="gate-owned cleanup reconciliation")
            if row.get("name") == self._name
        ]

    def _is_owned(self, row: Mapping[str, object]) -> bool:
        description = row.get("description")
        return isinstance(description, str) and description in self._owned_descriptions


class _GateOwnedFullGuard(_GateOwnedProjectGuard):
    """Reconcile a full-bundle workflow before its containing project."""

    def __init__(
        self,
        config: ConformanceBundleGateConfig,
        *,
        invoke: Callable[[list[str]], DsctlCommandResult],
        invoke_task_cleanup: (
            Callable[
                [Literal["prove", "cleanup"], int, int | None, str],
                TaskDefinitionCleanupInvocation,
            ]
            | None
        ),
        run_id: str,
        recovery_mode: bool = False,
    ) -> None:
        super().__init__(config, invoke=invoke, run_id=run_id)
        self._ds_version = config.ds_version
        self._run_id = run_id
        self._recovery_mode = recovery_mode
        self._private_task_cleanup = invoke_task_cleanup
        self._task_cleanup_forbidden = False
        version_slug = config.ds_version.replace(".", "-")
        self._workflow_name = f"dsctl-full-{version_slug}-{run_id}"
        self._workflow_created_description = (
            f"{self._ownership};resource=workflow;phase=created"
        )
        self._workflow_updated_description = (
            f"{self._ownership};resource=workflow;phase=updated"
        )
        self._workflow_descriptions = frozenset(
            {
                self._workflow_created_description,
                self._workflow_updated_description,
            }
        )

    def cleanup(self) -> None:
        """Delete one freshly proven owned workflow, then its owned project."""
        if not self._attempted:
            return
        if self._task_cleanup_forbidden:
            message = "private task-definition cleanup is unsafe to retry"
            raise TaskDefinitionCleanupDoNotRetryError(message)
        self._cleanup_present_state(require_present=False)

    def recover_existing(self) -> None:
        """Delete one preexisting residue only after a complete fresh proof."""
        self._cleanup_present_state(require_present=True)

    def invoke_task_cleanup(
        self,
        operation: Literal["prove", "cleanup"],
        project_code: int,
        workflow_code: int | None,
    ) -> TaskDefinitionCleanupInvocation:
        """Invoke one project/workflow-bound operation, fencing ambiguity."""
        if self._task_cleanup_forbidden:
            message = "private task-definition cleanup is unsafe to retry"
            raise TaskDefinitionCleanupDoNotRetryError(message)
        callback = self._private_task_cleanup
        if callback is None:
            message = "private task-definition cleanup runner is unavailable"
            raise AssertionError(message)
        try:
            return callback(operation, project_code, workflow_code, self._run_id)
        except TaskDefinitionCleanupDoNotRetryError:
            self._task_cleanup_forbidden = True
            raise

    def _cleanup_present_state(self, *, require_present: bool) -> None:
        project_rows = self._fresh_exact_rows()
        owned_projects = [row for row in project_rows if self._is_owned(row)]
        if not owned_projects:
            if require_present:
                message = "full recovery found no exact gate-owned project"
                raise AssertionError(message)
            return
        if len(project_rows) != 1 or len(owned_projects) != 1:
            message = "full cleanup could not isolate one owned project"
            raise AssertionError(message)
        project_code = _positive_int(
            owned_projects[0].get(self._identity_key),
            label=f"full cleanup project {self._identity_key}",
        )
        project = _OwnedProjectScope(
            name=self._name,
            code=project_code,
            identity_kind=self._identity_key,
            ownership=self._ownership,
            created_description=f"{self._ownership};phase=created",
            updated_description=f"{self._ownership};phase=updated",
        )
        workflow_rows = self._fresh_workflow_rows()
        guard_strategy = _full_task_definition_guard_strategy(
            self._ds_version,
            for_recovery=self._recovery_mode,
        )
        workflow_proof = self._prepare_owned_workflow_cleanup(
            workflow_rows,
            project=project,
            strategy=guard_strategy,
        )
        workflow_code = None if workflow_proof is None else workflow_proof.code
        self._require_workflow_instances_absent()
        private_proof_succeeded = self._prove_private_tasks_before_workflow_delete(
            project_code=project_code,
            workflow_code=workflow_code,
            strategy=guard_strategy,
        )
        if (
            guard_strategy == "workflow-cascade-proof-only"
            and workflow_proof is not None
        ):
            if not private_proof_succeeded:
                message = "cascade cleanup task proof did not complete"
                raise AssertionError(message)
            self._require_cascade_task_reads_match(workflow_proof)
        if workflow_code is not None:
            self._invoke(
                [
                    "workflow",
                    "delete",
                    str(workflow_code),
                    "--project",
                    self._name,
                    "--force",
                ]
            )
            if self._fresh_workflow_rows():
                message = "full cleanup did not remove the proven owned workflow"
                raise AssertionError(message)
            if guard_strategy is None:
                self._require_tasks_absent()
        self._reconcile_private_tasks_after_workflow_delete(
            project_code=project_code,
            workflow_code=workflow_code,
            strategy=guard_strategy,
        )
        self._cleanup_present_project(require_present=require_present)

    def _prepare_owned_workflow_cleanup(
        self,
        rows: list[dict[str, object]],
        *,
        project: _OwnedProjectScope,
        strategy: FullTaskDefinitionGuardStrategy | None,
    ) -> _OwnedWorkflowCleanupProof | None:
        if not rows:
            return None
        proof = self._prove_owned_workflow(
            rows,
            project=project,
        )
        if strategy != "workflow-cascade-proof-only":
            self._require_task_reads_match(proof.tasks)
        return proof

    def _prove_private_tasks_before_workflow_delete(
        self,
        *,
        project_code: int,
        workflow_code: int | None,
        strategy: FullTaskDefinitionGuardStrategy | None,
    ) -> bool:
        if strategy is None or workflow_code is None:
            return False
        proof = self.invoke_task_cleanup("prove", project_code, workflow_code)
        _require_task_cleanup_result(
            proof,
            operation="prove",
            ds_version=self._ds_version,
            expected_observed=2,
            expected_released=0,
            expected_deleted=0,
            expected_remaining=2,
            expected_remote_mutations=0,
        )
        return True

    def _reconcile_private_tasks_after_workflow_delete(
        self,
        *,
        project_code: int,
        workflow_code: int | None,
        strategy: FullTaskDefinitionGuardStrategy | None,
    ) -> None:
        if strategy is None:
            return
        cleanup_result = self.invoke_task_cleanup(
            "cleanup",
            project_code,
            workflow_code,
        )
        if strategy == "workflow-cascade-proof-only":
            _require_task_cleanup_result(
                cleanup_result,
                operation="cleanup",
                ds_version=self._ds_version,
                expected_observed=0,
                expected_released=0,
                expected_deleted=0,
                expected_remaining=0,
                expected_remote_mutations=0,
            )
            return
        release_required = (
            self._ds_version in _TASK_DEFINITION_PRE_DELETE_RELEASE_VERSIONS
        )
        recovery_may_resume_offline = self._recovery_mode and release_required
        if recovery_may_resume_offline:
            expected_released = None
            expected_remote_mutations = None
        else:
            expected_released = cleanup_result.observed if release_required else 0
            expected_remote_mutations = cleanup_result.observed + expected_released
        _require_task_cleanup_result(
            cleanup_result,
            operation="cleanup",
            ds_version=self._ds_version,
            expected_released=expected_released,
            expected_deleted=cleanup_result.observed,
            expected_remaining=0,
            expected_remote_mutations=expected_remote_mutations,
        )
        if cleanup_result.observed > 2:
            message = "private task-definition cleanup exceeded the owned scope"
            raise AssertionError(message)

    def _fresh_workflow_rows(self) -> list[dict[str, object]]:
        result = self._invoke(
            [
                "workflow",
                "list",
                "--project",
                self._name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        )
        data = _ok_data(
            result,
            expected_action="workflow.list",
            label="full cleanup workflow reconciliation",
        )
        return _page_rows(
            data,
            label="full cleanup workflow reconciliation",
        )

    def _require_workflow_instances_absent(self) -> None:
        argv = [
            "workflow-instance",
            "list",
            "--project",
            self._name,
        ]
        argv.extend(["--page-no", "1", "--page-size", "20", "--all"])
        data = _ok_data(
            self._invoke(argv),
            expected_action="workflow-instance.list",
            label="full cleanup workflow-instance reconciliation",
        )
        rows = require_list(
            data.get("totalList"),
            label="full cleanup workflow-instance reconciliation rows",
        )
        if rows:
            message = "full cleanup found workflow instances for its owned scope"
            raise AssertionError(message)
        _page_rows(
            data,
            label="full cleanup workflow-instance reconciliation",
        )

    def _prove_owned_workflow(
        self,
        rows: list[dict[str, object]],
        *,
        project: _OwnedProjectScope,
    ) -> _OwnedWorkflowCleanupProof:
        workflow_code, description, version = self._require_fresh_workflow_details(
            rows,
            project=project,
        )
        described = self._load_owned_workflow_description(
            workflow_code,
            description=description,
            version=version,
            project=project,
        )
        tasks = self._require_owned_described_tasks(described)
        roots, edges = _validated_relation_topology(
            described.get("relations"),
            tasks=tasks,
            identity_kind=self._identity_key,
        )
        task_codes = {task.name: task.native_id for task in tasks}
        if roots != (task_codes["extract"],) or edges != (
            (task_codes["extract"], task_codes["load"]),
        ):
            message = "full cleanup workflow topology is not gate-owned"
            raise AssertionError(message)
        return _OwnedWorkflowCleanupProof(
            code=workflow_code,
            description=description,
            version=version,
            tasks=tasks,
        )

    def _require_fresh_workflow_details(
        self,
        rows: list[dict[str, object]],
        *,
        project: _OwnedProjectScope,
    ) -> tuple[int, str, int]:
        if len(rows) != 1 or rows[0].get("name") != self._workflow_name:
            message = "full cleanup could not isolate one owned workflow"
            raise AssertionError(message)
        workflow_code = _positive_int(
            rows[0].get(self._identity_key),
            label=f"full cleanup listed workflow {self._identity_key}",
        )
        details = tuple(
            self._fresh_workflow_detail(selector)
            for selector in (self._workflow_name, str(workflow_code))
        )
        description = _required_text(
            details[0].get("description"),
            label="full cleanup workflow description",
        )
        if description not in self._workflow_descriptions:
            message = "full cleanup workflow has no exact ownership description"
            raise AssertionError(message)
        versions: list[int] = []
        for detail in details:
            _require_owned_workflow(
                detail,
                project=project,
                workflow_name=self._workflow_name,
                description=description,
                expected_code=workflow_code,
            )
            if detail.get("scheduleReleaseState") is not None:
                message = "full cleanup workflow unexpectedly has schedule state"
                raise AssertionError(message)
            versions.append(
                _workflow_version(
                    detail.get("version"),
                    ds_version=self._ds_version,
                    label="full cleanup workflow version",
                )
            )
        if len(set(versions)) != 1:
            message = "full cleanup workflow selectors disagree"
            raise AssertionError(message)
        return workflow_code, description, versions[0]

    def _load_owned_workflow_description(
        self,
        workflow_code: int,
        *,
        description: str,
        version: int,
        project: _OwnedProjectScope,
    ) -> Mapping[str, object]:
        described = _ok_data(
            self._invoke(
                [
                    "workflow",
                    "describe",
                    str(workflow_code),
                    "--project",
                    self._name,
                ]
            ),
            expected_action="workflow.describe",
            label="full cleanup workflow ownership proof",
        )
        described_workflow = require_mapping(
            described.get("workflow"),
            label="full cleanup described workflow",
        )
        _require_owned_workflow(
            described_workflow,
            project=project,
            workflow_name=self._workflow_name,
            description=description,
            expected_code=workflow_code,
        )
        if (
            described_workflow.get("scheduleReleaseState") is not None
            or described_workflow.get("version") != version
        ):
            message = "full cleanup described workflow differs from fresh detail"
            raise AssertionError(message)
        return described

    def _require_owned_described_tasks(
        self,
        described: Mapping[str, object],
    ) -> tuple[_OwnedTaskSnapshot, ...]:
        tasks = tuple(
            sorted(
                (
                    _task_snapshot(
                        require_mapping(
                            raw,
                            label="full cleanup described task",
                        ),
                        identity_kind=self._identity_key,
                    )
                    for raw in require_list(
                        described.get("tasks"),
                        label="full cleanup described tasks",
                    )
                ),
                key=_task_key,
            )
        )
        _require_exact_two_owned_tasks(tasks, label="full cleanup workflow describe")
        described_tasks = [
            require_mapping(raw, label="full cleanup described task")
            for raw in require_list(
                described.get("tasks"),
                label="full cleanup described tasks",
            )
        ]
        for task in described_tasks:
            name = _required_text(task.get("name"), label="full cleanup task name")
            expected = f"{self._ownership};resource=task;name={name}"
            if self._identity_key != "id" and task.get("description") != expected:
                message = "full cleanup task does not prove exact ownership"
                raise AssertionError(message)
            params = require_mapping(
                task.get("taskParams"),
                label="full cleanup task params",
            )
            expected_commands = {
                "extract": {f'printf "%s\\n" "{self._run_id}-extract"\n'},
                "load": {
                    f'printf "%s\\n" "{self._run_id}-load"\n',
                    f'printf "%s\\n" "{self._run_id}-updated-load"\n',
                },
            }
            if (
                params.get("rawScript") not in expected_commands[name]
                or params.get("localParams") != []
                or params.get("resourceList") != []
            ):
                message = "full cleanup task params are not gate-owned"
                raise AssertionError(message)
        return tasks

    def _fresh_workflow_detail(self, selector: str) -> Mapping[str, object]:
        return _ok_data(
            self._invoke(["workflow", "get", selector, "--project", self._name]),
            expected_action="workflow.get",
            label="full cleanup workflow detail ownership proof",
        )

    def _require_task_reads_match(
        self,
        described_tasks: tuple[_OwnedTaskSnapshot, ...],
    ) -> None:
        scope = ["--project", self._name, "--workflow", self._workflow_name]
        listed_payload = require_ok_payload(
            self._invoke(["task", "list", *scope]),
            expected_action="task.list",
            label="full cleanup task list ownership proof",
        )
        listed_rows = [
            require_mapping(raw, label="full cleanup listed task")
            for raw in require_list(
                listed_payload.get("data"),
                label="full cleanup listed tasks",
            )
        ]
        _require_task_list_row_projection(
            listed_rows,
            identity_kind=self._identity_key,
            label="full cleanup task list",
        )
        listed_identities = [
            (
                _task_native_identity(
                    row,
                    identity_kind=self._identity_key,
                    label="full cleanup listed task",
                ),
                _required_text(row.get("name"), label="full cleanup listed task name"),
                (
                    None
                    if self._identity_key == "id"
                    else _positive_int(
                        row.get("version"),
                        label="full cleanup listed task version",
                    )
                ),
            )
            for row in listed_rows
        ]
        expected = {
            (task.native_id, task.name, task.version) for task in described_tasks
        }
        if (
            len(listed_identities) != len(described_tasks)
            or len({code for code, _name, _version in listed_identities})
            != len(described_tasks)
            or len({name for _code, name, _version in listed_identities})
            != len(described_tasks)
            or set(listed_identities) != expected
        ):
            message = "full cleanup task list differs from workflow describe"
            raise AssertionError(message)
        for task in described_tasks:
            for selector in _task_read_selectors(self._ds_version, task):
                detail = _ok_data(
                    self._invoke(["task", "get", selector, *scope]),
                    expected_action="task.get",
                    label="full cleanup task detail ownership proof",
                )
                if _task_snapshot(detail, identity_kind=self._identity_key) != task:
                    message = "full cleanup task selectors disagree with describe"
                    raise AssertionError(message)
                if self._identity_key == "id" and detail.get("description") != (
                    f"{self._ownership};resource=task;name={task.name}"
                ):
                    message = "full cleanup legacy task ownership differs"
                    raise AssertionError(message)

    def _require_cascade_task_reads_match(
        self,
        workflow: _OwnedWorkflowCleanupProof,
    ) -> None:
        """Accept only the proven relation-bound/latest 3.1.9 lineage phases."""
        described_tasks = workflow.tasks
        scope = ["--project", self._name, "--workflow", self._workflow_name]
        listed_payload = require_ok_payload(
            self._invoke(["task", "list", *scope]),
            expected_action="task.list",
            label="cascade cleanup task list ownership proof",
        )
        listed_rows = [
            require_mapping(raw, label="cascade cleanup listed task")
            for raw in require_list(
                listed_payload.get("data"),
                label="cascade cleanup listed tasks",
            )
        ]
        _require_task_list_row_projection(
            listed_rows,
            identity_kind=self._identity_key,
            label="cascade cleanup task list",
        )
        listed_identities = [
            (
                _positive_int(
                    row.get("code"),
                    label="cascade cleanup listed task code",
                ),
                _required_text(
                    row.get("name"),
                    label="cascade cleanup listed task name",
                ),
                _positive_int(
                    row.get("version"),
                    label="cascade cleanup listed task version",
                ),
            )
            for row in listed_rows
        ]
        expected = {
            (task.native_id, task.name, task.version) for task in described_tasks
        }
        if (
            len(listed_identities) != len(described_tasks)
            or len({code for code, _name, _version in listed_identities})
            != len(described_tasks)
            or len({name for _code, name, _version in listed_identities})
            != len(described_tasks)
            or set(listed_identities) != expected
        ):
            message = "cascade cleanup task list differs from workflow describe"
            raise AssertionError(message)

        described_by_name = {task.name: task for task in described_tasks}
        extract_command = f'printf "%s\\n" "{self._run_id}-extract"\n'
        load_command = f'printf "%s\\n" "{self._run_id}-load"\n'
        updated_load_command = f'printf "%s\\n" "{self._run_id}-updated-load"\n'
        extract = described_by_name["extract"]
        load = described_by_name["load"]
        if (extract.version, extract.command) != (1, extract_command) or (
            load.version,
            load.command,
        ) not in {(1, load_command), (2, updated_load_command)}:
            message = "cascade cleanup DAG task phase is not an exact owned phase"
            raise AssertionError(message)

        latest_by_name: dict[str, _OwnedTaskSnapshot] = {}
        for task in described_tasks:
            selector_snapshots = tuple(
                _task_snapshot(
                    _ok_data(
                        self._invoke(["task", "get", selector, *scope]),
                        expected_action="task.get",
                        label="cascade cleanup task detail ownership proof",
                    ),
                    identity_kind=self._identity_key,
                )
                for selector in _task_read_selectors(self._ds_version, task)
            )
            if len(set(selector_snapshots)) != 1:
                message = "cascade cleanup task selectors disagree"
                raise AssertionError(message)
            latest = selector_snapshots[0]
            if (
                latest.native_id,
                latest.name,
                latest.task_type,
                latest.non_owned_digest,
            ) != (
                task.native_id,
                task.name,
                task.task_type,
                task.non_owned_digest,
            ):
                message = "cascade cleanup latest task identity drifted"
                raise AssertionError(message)
            latest_by_name[task.name] = latest

        if latest_by_name["extract"] != extract:
            message = "cascade cleanup extract task drifted"
            raise AssertionError(message)
        latest_load = latest_by_name["load"]
        owned_phase = (
            workflow.version,
            workflow.description,
            load.version,
            load.command,
            latest_load.version,
            latest_load.command,
        )
        allowed_owned_phases = {
            (
                1,
                self._workflow_created_description,
                1,
                load_command,
                1,
                load_command,
            ),
            (
                1,
                self._workflow_created_description,
                1,
                load_command,
                2,
                updated_load_command,
            ),
            (
                2,
                self._workflow_created_description,
                2,
                updated_load_command,
                2,
                updated_load_command,
            ),
            (
                3,
                self._workflow_updated_description,
                2,
                updated_load_command,
                2,
                updated_load_command,
            ),
        }
        if owned_phase not in allowed_owned_phases:
            message = "cascade cleanup workflow/task lineage is not an exact phase"
            raise AssertionError(message)

    def _require_tasks_absent(self) -> None:
        result = self._invoke(
            [
                "task",
                "list",
                "--project",
                self._name,
                "--workflow",
                self._workflow_name,
            ]
        )
        if result.payload.get("ok") is True:
            payload = require_ok_payload(
                result,
                expected_action="task.list",
                label="full cleanup task reconciliation",
            )
            if require_list(
                payload.get("data"),
                label="full cleanup task reconciliation data",
            ):
                message = "full cleanup left gate-owned tasks"
                raise AssertionError(message)
            return
        require_error_payload(
            result,
            expected_action="task.list",
            expected_type="not_found",
            label="full cleanup task reconciliation",
        )


def _load_current_bundle_assessment(
    *,
    ds_version: str,
    bundle_name: str,
) -> BundleAssessment:
    assessment = require_mapping(
        CONFORMANCE_BUNDLE_DATA,
        label="current conformance assessment",
    )
    if (
        assessment.get("schema_version") != 1
        or assessment.get("claim") != "static-action-closure-only"
        or assessment.get("promotion_claimed") is not False
    ):
        message = "Current conformance assessment header is not gate-safe"
        raise ValueError(message)
    assessed_bundles = [
        require_mapping(raw, label="current conformance bundle")
        for raw in require_list(assessment.get("bundles"), label="current bundles")
    ]
    matches = [item for item in assessed_bundles if item.get("name") == bundle_name]
    if len(matches) != 1:
        message = f"Current conformance bundle {bundle_name!r} is missing or duplicated"
        raise ValueError(message)
    assessed = matches[0]
    coordinates = [
        require_mapping(raw, label="current conformance coordinate")
        for raw in require_list(assessed.get("versions"), label="bundle coordinates")
        if require_mapping(raw, label="current conformance coordinate").get("version")
        == ds_version
    ]
    if len(coordinates) != 1:
        message = f"Bundle {bundle_name!r} lacks one exact {ds_version} coordinate"
        raise ValueError(message)
    coordinate = coordinates[0]
    if coordinate.get("status") != "ready" or coordinate.get("blockers") != []:
        message = f"Bundle {bundle_name!r} is not static-ready for {ds_version}"
        raise ValueError(message)
    extends = _required_text_sequence(
        assessed.get("extends"),
        label="bundle extends",
        allow_empty=True,
    )
    inheritance: list[dict[str, object]] = []
    bundles_by_name = {
        _required_text(item.get("name"), label="assessed bundle name"): item
        for item in assessed_bundles
    }
    for parent_name in extends:
        parent = bundles_by_name.get(parent_name)
        if parent is None:
            message = f"Bundle parent {parent_name!r} is absent"
            raise ValueError(message)
        inheritance.append(
            {
                "name": parent_name,
                "bundle_digest": _required_sha256(
                    parent.get("bundle_digest"),
                    label="parent bundle digest",
                ),
                "required_actions": list(
                    _required_text_sequence(
                        parent.get("required_actions"),
                        label="parent required actions",
                    )
                ),
            }
        )
    tested = coordinate.get("tested")
    if type(tested) is not bool:
        message = "Conformance coordinate tested value must be boolean"
        raise TypeError(message)
    return BundleAssessment(
        schema_version=1,
        catalog_digest=_required_sha256(
            assessment.get("catalog_digest"),
            label="assessment catalog digest",
        ),
        assessment_digest=_required_sha256(
            assessment.get("assessment_digest"),
            label="assessment digest",
        ),
        name=bundle_name,
        bundle_digest=_required_sha256(
            assessed.get("bundle_digest"),
            label="bundle digest",
        ),
        coordinate_status="ready",
        support_level=_required_text(
            coordinate.get("support_level"),
            label="coordinate support level",
        ),
        tested=tested,
        extends=extends,
        inheritance=tuple(inheritance),
        direct_actions=_required_text_sequence(
            assessed.get("direct_actions"),
            label="bundle direct actions",
        ),
        required_actions=_required_text_sequence(
            assessed.get("required_actions"),
            label="bundle required actions",
        ),
    )


def _load_installed_bundle_attestation(
    path: Path,
    *,
    ds_version: str,
    bundle: BundleAssessment,
    wheel: Path,
) -> InstalledBundleAttestation:
    from live_gate.runtime_ownership import (  # noqa: PLC0415
        load_wheel_runtime_ownership,
    )
    from tests.live.exact_read_gate import load_exact_manifest  # noqa: PLC0415

    payload = _load_json_object(path, label="installed conformance attestation")
    _require_exact_keys(
        payload,
        {
            "distribution_version",
            "ds_version",
            "bundle",
            "bundle_digest",
            "catalog_digest",
            "assessment_digest",
            "required_actions",
            "profile",
            "actions",
            "manifest",
        },
        label="installed conformance attestation",
    )
    expected_scalars = {
        "ds_version": ds_version,
        "bundle": bundle.name,
        "bundle_digest": bundle.bundle_digest,
        "catalog_digest": bundle.catalog_digest,
        "assessment_digest": bundle.assessment_digest,
    }
    if any(payload.get(key) != value for key, value in expected_scalars.items()):
        message = "Installed conformance identity differs from current assessment"
        raise ValueError(message)
    required_actions = _required_text_sequence(
        payload.get("required_actions"),
        label="installed required actions",
    )
    if required_actions != bundle.required_actions:
        message = "Installed required actions differ from current named bundle"
        raise ValueError(message)
    profile = _validated_installed_profile(
        payload.get("profile"),
        ds_version=ds_version,
        bundle=bundle,
    )
    contract = _validated_installed_contract(
        payload.get("manifest"),
        ds_version=ds_version,
        profile=profile,
    )
    if contract != load_exact_manifest(wheel, ds_version=ds_version):
        message = "Installed conformance contract differs from candidate wheel"
        raise ValueError(message)
    recipes = _validated_action_recipes(
        payload.get("actions"),
        required_actions=required_actions,
        runtime_operations=load_wheel_runtime_ownership(wheel)[ds_version],
    )
    return InstalledBundleAttestation(
        distribution_version=_required_text(
            payload.get("distribution_version"),
            label="installed distribution version",
        ),
        profile=profile,
        action_recipes=recipes,
        contract=contract,
    )


def _validated_installed_profile(
    value: object,
    *,
    ds_version: str,
    bundle: BundleAssessment,
) -> dict[str, object]:
    profile = require_mapping(value, label="installed profile")
    _require_exact_keys(
        profile,
        {
            "server_version",
            "contract_version",
            "family",
            "support_level",
            "tested",
            "source",
            "fingerprints",
        },
        label="installed profile",
    )
    if (
        profile.get("server_version") != ds_version
        or profile.get("contract_version") != ds_version
        or profile.get("support_level") != bundle.support_level
        or profile.get("tested") is not bundle.tested
    ):
        message = "Installed profile differs from the current bundle coordinate"
        raise ValueError(message)
    _required_text(profile.get("family"), label="installed profile family")
    source = require_mapping(profile.get("source"), label="installed profile source")
    _require_exact_keys(source, {"tag", "commit", "tree"}, label="profile source")
    if source.get("tag") != ds_version:
        message = "Installed profile source tag differs from DS version"
        raise ValueError(message)
    _required_git_object(source.get("commit"), label="profile source commit")
    _required_git_object(source.get("tree"), label="profile source tree")
    _validate_fingerprints(
        profile.get("fingerprints"),
        label="installed profile fingerprints",
    )
    return dict(profile)


def _validated_installed_contract(
    value: object,
    *,
    ds_version: str,
    profile: Mapping[str, object],
) -> dict[str, object]:
    contract = require_mapping(value, label="installed contract")
    _require_exact_keys(
        contract,
        {
            "bundle_manifest_schema_version",
            "ds_version",
            "selection",
            "semantic_operations",
            "source_tag",
            "source_commit",
            "source_tree",
            "source_contract_digest",
            "rendered_contract_digest",
            "operation_count",
        },
        label="installed contract",
    )
    bundle_manifest_schema_version = contract.get("bundle_manifest_schema_version")
    if (
        type(bundle_manifest_schema_version) is not int
        or bundle_manifest_schema_version != 2
        or contract.get("ds_version") != ds_version
        or contract.get("selection") != "runtime-slice"
        or contract.get("source_tag") != ds_version
    ):
        message = "Installed contract identity differs from the exact profile"
        raise ValueError(message)
    _required_text_sequence(
        contract.get("semantic_operations"),
        label="contract semantic operations",
        allow_empty=True,
    )
    operation_count = contract.get("operation_count")
    if (
        not isinstance(operation_count, int)
        or isinstance(operation_count, bool)
        or operation_count < 0
    ):
        message = "Installed contract operation_count must be nonnegative"
        raise ValueError(message)
    source = require_mapping(profile.get("source"), label="installed profile source")
    if any(
        contract.get(contract_key) != source.get(source_key)
        for contract_key, source_key in (
            ("source_tag", "tag"),
            ("source_commit", "commit"),
            ("source_tree", "tree"),
        )
    ):
        message = "Installed profile and contract source identities differ"
        raise ValueError(message)
    _required_git_object(contract.get("source_commit"), label="contract source commit")
    _required_git_object(contract.get("source_tree"), label="contract source tree")
    _required_sha256(
        contract.get("source_contract_digest"),
        label="contract source digest",
    )
    _required_sha256(
        contract.get("rendered_contract_digest"),
        label="contract rendered digest",
    )
    return dict(contract)


def _validated_action_recipes(
    value: object,
    *,
    required_actions: tuple[str, ...],
    runtime_operations: frozenset[str],
) -> tuple[dict[str, object], ...]:
    recipes = [
        require_mapping(raw, label="installed action recipe")
        for raw in require_list(value, label="installed action recipes")
    ]
    if len(recipes) != len(required_actions):
        message = "Installed action recipe count differs from the named bundle"
        raise ValueError(message)
    validated: list[dict[str, object]] = []
    for recipe, action in zip(recipes, required_actions, strict=True):
        _require_exact_keys(
            recipe,
            {
                "action",
                "semantic_operation",
                "availability",
                "execution_mode",
                "verification",
                "build_status",
                "fingerprints",
            },
            label="installed action recipe",
        )
        semantic_operation = _required_text(
            recipe.get("semantic_operation"),
            label="recipe semantic operation",
        )
        if (
            recipe.get("action") != action
            or recipe.get("availability") != "supported"
            or recipe.get("build_status") != "accepted"
            or semantic_operation not in runtime_operations
        ):
            message = f"Installed recipe differs for required action {action}"
            raise ValueError(message)
        _required_text(recipe.get("execution_mode"), label="recipe execution mode")
        _required_text(recipe.get("verification"), label="recipe verification")
        _validate_fingerprints(
            recipe.get("fingerprints"),
            label=f"recipe {action} fingerprints",
        )
        validated.append(dict(recipe))
    return tuple(validated)


def _verify_version(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> dict[str, object]:
    data = _ok_data(
        invoke(["version"]),
        expected_action="version",
        label="conformance version preflight",
    )
    profile = config.installation.profile
    expected = {
        "cli": config.installation.distribution_version,
        "ds": config.ds_version,
        "selected_ds_version": config.ds_version,
        "contract_version": config.ds_version,
        "family": profile["family"],
        "support_level": profile["support_level"],
    }
    if any(data.get(key) != value for key, value in expected.items()):
        message = "version preflight differs from installed bundle attestation"
        raise AssertionError(message)
    _record_success(
        trace,
        "version",
        argv_shape="version",
        assertions=("selected-contract-and-family-matched",),
    )
    return data


def _verify_capabilities(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    recipes = {
        cast("str", recipe["action"]): recipe
        for recipe in config.installation.action_recipes
    }
    for action in config.bundle.required_actions:
        data = _ok_data(
            invoke(["capabilities", "--action", action]),
            expected_action="capabilities",
            label=f"conformance capability {action}",
        )
        capability = require_mapping(
            data.get("capability"),
            label=f"capability {action}",
        )
        recipe = recipes[action]
        if (
            capability.get("action") != action
            or capability.get("availability") != "supported"
            or capability.get("verification") != recipe["verification"]
        ):
            message = f"capability preflight differs for {action}"
            raise AssertionError(message)
        _record_success(
            trace,
            "capabilities",
            argv_shape="capabilities --action ACTION",
            assertions=("supported",),
            subject_action=action,
        )


def _verify_doctor(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    data = _ok_data(
        invoke(["doctor"]),
        expected_action="doctor",
        label="conformance doctor",
    )
    checks = {
        item.get("name"): item
        for raw in require_list(data.get("checks"), label="doctor checks")
        for item in [require_mapping(raw, label="doctor check")]
    }
    api = require_mapping(checks.get("api"), label="doctor API check")
    current_user = require_mapping(
        checks.get("current_user"),
        label="doctor current-user check",
    )
    api_ready = api.get("status") == "ok"
    if api.get("status") == "warning":
        doctor_recipes = [
            recipe
            for recipe in config.installation.action_recipes
            if recipe.get("action") == "doctor"
        ]
        api_details = require_mapping(
            api.get("details"),
            label="doctor API warning details",
        )
        api_ready = (
            len(doctor_recipes) == 1
            and doctor_recipes[0].get("semantic_operation") == "identity.current"
            and doctor_recipes[0].get("execution_mode") == "diagnostic_recipe"
            and api_details.get("verification_fallback") == "current_user"
        )
    if not api_ready or current_user.get("status") != "ok":
        message = "doctor did not prove authenticated API readiness"
        raise AssertionError(message)
    details = require_mapping(
        current_user.get("details"),
        label="doctor current-user details",
    )
    user_name = details.get("userName")
    if not isinstance(user_name, str) or not user_name:
        message = "doctor current-user details omitted userName"
        raise AssertionError(message)
    if details.get("userType") != "GENERAL_USER":
        message = "conformance scenario requires a GENERAL_USER persona"
        raise AssertionError(message)
    if identity_hmac(user_name, key=config.attestation_key) != (
        config.cluster.principal_hmac_sha256
    ):
        message = "doctor principal differs from the cluster manifest"
        raise AssertionError(message)
    _record_success(
        trace,
        "doctor",
        argv_shape="doctor",
        assertions=("api-ready", "current-user-bound", "principal-hmac-matched"),
        outcomes=("bound-current-user", "capability-preflight-complete"),
    )


def _read_external_fixture(
    config: ConformanceBundleGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
    observed_outcome: str | None,
) -> dict[str, object]:
    fixture = config.fixture
    identity_key = fixture.project_identity.kind
    project_rows = _project_list(
        invoke,
        project_name=fixture.project_name,
        trace=trace,
    )
    project_row = _matching_row(
        project_rows,
        name=fixture.project_name,
        identity_key=identity_key,
        identity=fixture.project_identity.value,
        label="external project list",
    )
    project_by_name = _project_get(
        invoke,
        selector=fixture.project_name,
        trace=trace,
        assertions=("external-project-matched",),
    )
    project_by_native = _project_get(
        invoke,
        selector=str(fixture.project_identity.value),
        trace=trace,
        assertions=("external-project-native-identity-matched",),
    )
    for row in (project_by_name, project_by_native):
        _matching_row(
            [row],
            name=fixture.project_name,
            identity_key=identity_key,
            identity=fixture.project_identity.value,
            label="external project get",
        )

    workflow_scope = ["--project", fixture.project_name]
    workflow_list = _ok_data(
        invoke(
            [
                "workflow",
                "list",
                *workflow_scope,
                "--search",
                fixture.workflow_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="workflow.list",
        label="external workflow list",
    )
    workflow_rows = _page_rows(workflow_list, label="external workflow list")
    workflow_row = _matching_row(
        workflow_rows,
        name=fixture.workflow_name,
        identity_key=fixture.workflow_identity.kind,
        identity=fixture.workflow_identity.value,
        label="external workflow list",
    )
    _record_success(
        trace,
        "workflow.list",
        argv_shape=(
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=("external-workflow-native-identity-matched",),
    )
    workflow_gets: list[dict[str, object]] = []
    hydrated_schedule_states: list[str] = []
    for selector in (fixture.workflow_name, str(fixture.workflow_identity.value)):
        data = _ok_data(
            invoke(["workflow", "get", selector, *workflow_scope]),
            expected_action="workflow.get",
            label="external workflow get",
        )
        _matching_row(
            [data],
            name=fixture.workflow_name,
            identity_key=fixture.workflow_identity.kind,
            identity=fixture.workflow_identity.value,
            label="external workflow get",
        )
        if data.get("releaseState") != fixture.workflow_release_state:
            message = "external workflow release state differs from its fixture"
            raise AssertionError(message)
        schedule = require_mapping(
            data.get("schedule"),
            label="external workflow schedule",
        )
        if schedule.get("id") != fixture.schedule_id:
            message = "external workflow did not hydrate its fixture schedule"
            raise AssertionError(message)
        schedule_release_state = schedule.get("releaseState")
        if schedule_release_state not in {"ONLINE", "OFFLINE"}:
            message = "external workflow schedule release state is invalid"
            raise AssertionError(message)
        assert isinstance(schedule_release_state, str)
        hydrated_schedule_states.append(schedule_release_state)
        workflow_gets.append(data)
        _record_success(
            trace,
            "workflow.get",
            argv_shape="workflow get WORKFLOW --project PROJECT",
            assertions=("external-workflow-and-schedule-matched",),
        )
    if len(set(hydrated_schedule_states)) != 1:
        message = "external workflow reads disagree on schedule release state"
        raise AssertionError(message)
    authoritative_schedule_state = hydrated_schedule_states[0]

    schedule_data = _ok_data(
        invoke(
            [
                "schedule",
                "list",
                "--project",
                fixture.project_name,
                "--workflow",
                fixture.workflow_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="schedule.list",
        label="external schedule list",
    )
    schedule_rows = _page_rows(schedule_data, label="external schedule list")
    schedule_matches = [
        row
        for row in schedule_rows
        if row.get("id") == fixture.schedule_id
        and row.get("workflowDefinitionCode") == fixture.workflow_identity.value
        and row.get("workflowDefinitionName") == fixture.workflow_name
    ]
    if len(schedule_matches) != 1:
        message = "external schedule list did not return its exact fixture"
        raise AssertionError(message)
    if schedule_matches[0].get("releaseState") != authoritative_schedule_state:
        message = "external schedule release state differs from workflow hydration"
        raise AssertionError(message)
    _record_success(
        trace,
        "schedule.list",
        argv_shape=(
            "schedule list --project PROJECT --workflow WORKFLOW "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=("external-schedule-matched",),
        outcomes=() if observed_outcome is None else (observed_outcome,),
    )
    return {
        "project": project_row,
        "project_by_name": project_by_name,
        "project_by_native": project_by_native,
        "workflow": workflow_row,
        "workflow_by_name": workflow_gets[0],
        "workflow_by_native": workflow_gets[1],
        "schedule": schedule_matches[0],
    }


def _project_list(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    project_name: str,
    trace: list[OperationTraceEntry],
    outcomes: tuple[str, ...] = (),
) -> list[dict[str, object]]:
    data = _ok_data(
        invoke(
            [
                "project",
                "list",
                "--search",
                project_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="project.list",
        label="project list",
    )
    rows = _page_rows(data, label="project list")
    _record_success(
        trace,
        "project.list",
        argv_shape=(
            "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=("bounded-exact-name-search",),
        outcomes=outcomes,
    )
    return rows


def _project_get(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    selector: str,
    trace: list[OperationTraceEntry],
    assertions: tuple[str, ...],
    outcomes: tuple[str, ...] = (),
) -> dict[str, object]:
    data = _ok_data(
        invoke(["project", "get", selector]),
        expected_action="project.get",
        label="project get",
    )
    _record_success(
        trace,
        "project.get",
        argv_shape="project get PROJECT",
        assertions=assertions,
        outcomes=outcomes,
    )
    return data


def _ok_data(
    result: DsctlCommandResult,
    *,
    expected_action: str,
    label: str,
) -> dict[str, object]:
    payload = require_ok_payload(
        result,
        expected_action=expected_action,
        label=label,
    )
    return require_mapping(payload.get("data"), label=f"{label} data")


def _page_rows(value: Mapping[str, object], *, label: str) -> list[dict[str, object]]:
    expected = {"pageNo": 1, "pageSize": 20, "currentPage": 1}
    if any(value.get(key) != item for key, item in expected.items()):
        message = f"{label} pagination metadata differs"
        raise AssertionError(message)
    rows = [
        require_mapping(raw, label=f"{label} row")
        for raw in require_list(value.get("totalList"), label=f"{label} rows")
    ]
    total = value.get("total")
    total_page = value.get("totalPage")
    if (
        not isinstance(total, int)
        or isinstance(total, bool)
        or total != len(rows)
        or not isinstance(total_page, int)
        or isinstance(total_page, bool)
        or total_page not in ({1} if rows else {0, 1})
    ):
        message = f"{label} is not a complete bounded first page"
        raise AssertionError(message)
    return rows


def _owned_project_identity(
    project: Mapping[str, object],
    *,
    identity_key: IdentityKind,
    name: str,
    ownership: str,
    description: str,
) -> int:
    identity = project.get(identity_key)
    if not isinstance(identity, int) or isinstance(identity, bool) or identity <= 0:
        message = f"created project omitted its native {identity_key} identity"
        raise AssertionError(message)
    _require_owned_project(
        [project],
        identity_key=identity_key,
        identity=identity,
        name=name,
        ownership=ownership,
        description=description,
    )
    return identity


def _require_owned_project(
    rows: Sequence[Mapping[str, object]],
    *,
    identity_key: IdentityKind,
    identity: int,
    name: str,
    ownership: str,
    description: str | None = None,
) -> Mapping[str, object]:
    allowed_descriptions = {
        f"{ownership};phase=created",
        f"{ownership};phase=updated",
    }
    matches = [
        row
        for row in rows
        if row.get(identity_key) == identity
        and row.get("name") == name
        and isinstance(row.get("description"), str)
        and row.get("description") in allowed_descriptions
    ]
    if len(matches) != 1:
        message = "project does not prove the gate's exact ownership marker"
        raise AssertionError(message)
    if description is not None and matches[0].get("description") != description:
        message = "gate-owned project description readback differs"
        raise AssertionError(message)
    return matches[0]


def _matching_row(
    rows: Sequence[Mapping[str, object]],
    *,
    name: str,
    identity_key: IdentityKind,
    identity: int,
    label: str,
) -> dict[str, object]:
    matches = [
        dict(row)
        for row in rows
        if row.get("name") == name and row.get(identity_key) == identity
    ]
    if len(matches) != 1:
        message = f"{label} did not return exactly one native fixture identity"
        raise AssertionError(message)
    return matches[0]


def _native_identity_kind(ds_version: str) -> IdentityKind:
    return "id" if ds_version == "1.3.9" else "code"


def _record_success(
    trace: list[OperationTraceEntry],
    action: str,
    *,
    argv_shape: str,
    assertions: tuple[str, ...],
    outcomes: tuple[str, ...] = (),
    subject_action: str | None = None,
) -> None:
    trace.append(
        OperationTraceEntry(
            sequence=len(trace) + 1,
            action=action,
            argv_shape=argv_shape,
            exit_code=0,
            ok=True,
            assertions=assertions,
            outcomes=outcomes,
            subject_action=subject_action,
        )
    )


def _record_failure(
    trace: list[OperationTraceEntry],
    result: DsctlCommandResult,
    action: str,
    *,
    argv_shape: str,
    error_type: str,
    assertions: tuple[str, ...],
) -> None:
    trace.append(
        OperationTraceEntry(
            sequence=len(trace) + 1,
            action=action,
            argv_shape=argv_shape,
            exit_code=result.exit_code,
            ok=False,
            assertions=assertions,
            error_type=error_type,
        )
    )


def _state_hmac(value: object, *, key: bytes) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return identity_hmac(raw, key=key)


def _canonical_sha256(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def identity_hmac(value: str, *, key: bytes) -> str:
    """Bind a low-entropy identity without publishing the identity itself."""
    digest = hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"hmac-sha256:{digest}"


def _load_cluster_identity(
    path: Path,
    *,
    ds_version: str,
    api_url: str,
    attestation_key: bytes,
) -> ClusterIdentity:
    payload = _load_json_object(path, label="cluster manifest")
    _require_exact_keys(
        payload,
        {
            "api_target_hmac_sha256",
            "ds_version",
            "image_id",
            "image_observed_at",
            "image_source",
            "image_provenance",
            "image_ref",
            "persona",
            "principal_hmac_sha256",
            "schema_version",
        },
        label="cluster manifest",
    )
    if payload.get("schema_version") != 2 or payload.get("ds_version") != ds_version:
        message = f"Cluster manifest must declare schema 2 and DS {ds_version}"
        raise ValueError(message)
    image_ref = _required_text(payload.get("image_ref"), label="cluster image_ref")
    image_id = _required_sha256(payload.get("image_id"), label="cluster image_id")
    _validate_image_reference(image_ref, ds_version=ds_version)
    api_target_hmac = _required_hmac_sha256(
        payload.get("api_target_hmac_sha256"),
        label="cluster API target HMAC",
    )
    if api_target_hmac != identity_hmac(api_url, key=attestation_key):
        message = "Cluster API target differs from the explicit profile"
        raise ValueError(message)
    observed_text = _required_text(
        payload.get("image_observed_at"),
        label="cluster image_observed_at",
    )
    observed_at = _parse_timestamp(observed_text, label="cluster image_observed_at")
    now = datetime.now(tz=UTC)
    if observed_at > now + _MAX_CLOCK_SKEW or now - observed_at > _MAX_OBSERVATION_AGE:
        message = "Cluster image observation is outside the live gate window"
        raise ValueError(message)
    persona = _required_text(payload.get("persona"), label="cluster persona")
    if persona != "etl-developer":
        message = "Conformance cluster persona must be 'etl-developer'"
        raise ValueError(message)
    image_source = _required_text(
        payload.get("image_source"),
        label="cluster image_source",
    )
    if image_source != _CLUSTER_IMAGE_SOURCE:
        message = f"Cluster image_source must be {_CLUSTER_IMAGE_SOURCE!r}"
        raise ValueError(message)
    provenance_module = importlib.import_module(
        "live_gate.conformance_image_provenance"
    )
    image_provenance = cast(
        "dict[str, object]",
        provenance_module.validate_image_provenance(
            payload.get("image_provenance"),
            image_ref=image_ref,
            image_id=image_id,
            ds_version=ds_version,
            label="cluster image_provenance",
        ),
    )
    return ClusterIdentity(
        ds_version=ds_version,
        image_ref=image_ref,
        image_id=image_id,
        image_source=image_source,
        image_provenance=image_provenance,
        image_observed_at=observed_text,
        api_target_hmac_sha256=api_target_hmac,
        principal_hmac_sha256=_required_hmac_sha256(
            payload.get("principal_hmac_sha256"),
            label="cluster principal HMAC",
        ),
        persona=persona,
    )


def _load_external_fixture(
    path: Path,
    *,
    ds_version: str,
    cluster: ClusterIdentity,
    attestation_key: bytes,
) -> ExternalFixture:
    payload = _load_json_object(path, label="fixture manifest")
    _require_exact_keys(
        payload,
        {
            "ds_version",
            "image_id",
            "image_ref",
            "project",
            "provisioner",
            "schema_version",
            "workflow",
        },
        label="fixture manifest",
    )
    if payload.get("schema_version") != 1 or payload.get("ds_version") != ds_version:
        message = f"Fixture manifest must declare schema 1 and DS {ds_version}"
        raise ValueError(message)
    if (
        payload.get("image_ref") != cluster.image_ref
        or payload.get("image_id") != cluster.image_id
    ):
        message = "Fixture and cluster manifests identify different images"
        raise ValueError(message)
    project = require_mapping(payload.get("project"), label="fixture project")
    workflow = require_mapping(payload.get("workflow"), label="fixture workflow")
    _require_exact_keys(project, {"identity", "name"}, label="fixture project")
    _require_exact_keys(
        workflow,
        {"identity", "name", "release_state", "schedule_id", "scheduled"},
        label="fixture workflow",
    )
    if workflow.get("scheduled") is not True:
        message = "Conformance fixture workflow must have an attached schedule"
        raise ValueError(message)
    identity_kind: IdentityKind = "id" if ds_version == "1.3.9" else "code"
    project_identity = _load_native_identity(
        project.get("identity"),
        expected_kind=identity_kind,
        label="fixture project identity",
    )
    workflow_identity = _load_native_identity(
        workflow.get("identity"),
        expected_kind=identity_kind,
        label="fixture workflow identity",
    )
    fixture_identity = {"project": project, "workflow": workflow}
    expected_identity_hmac = identity_hmac(
        json.dumps(
            fixture_identity,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        key=attestation_key,
    )
    release_state = _required_text(
        workflow.get("release_state"),
        label="fixture workflow release_state",
    )
    if release_state not in {"ONLINE", "OFFLINE"}:
        message = "Fixture workflow release_state must be ONLINE or OFFLINE"
        raise ValueError(message)
    provisioner = _required_text(
        payload.get("provisioner"),
        label="fixture provisioner",
    )
    if provisioner != _FIXTURE_PROVISIONER:
        message = f"Fixture provisioner must be {_FIXTURE_PROVISIONER!r}"
        raise ValueError(message)
    return ExternalFixture(
        project_name=_required_non_numeric_name(
            project.get("name"),
            label="fixture project name",
        ),
        project_identity=project_identity,
        workflow_name=_required_non_numeric_name(
            workflow.get("name"),
            label="fixture workflow name",
        ),
        workflow_identity=workflow_identity,
        schedule_id=_required_positive_int(
            workflow.get("schedule_id"),
            label="fixture schedule_id",
        ),
        workflow_release_state=release_state,
        provisioner=provisioner,
        manifest_sha256=_file_sha256(path),
        identity_hmac_sha256=expected_identity_hmac,
    )


def _load_native_identity(
    value: object,
    *,
    expected_kind: IdentityKind,
    label: str,
) -> NativeFixtureIdentity:
    identity = require_mapping(value, label=label)
    _require_exact_keys(identity, {"kind", "value"}, label=label)
    if identity.get("kind") != expected_kind:
        message = f"{label} must use exact native kind {expected_kind}"
        raise ValueError(message)
    return NativeFixtureIdentity(
        kind=expected_kind,
        value=_required_positive_int(identity.get("value"), label=f"{label} value"),
    )


def _read_profile_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", maxsplit=1)
        normalized = value.strip()
        if (
            len(normalized) >= 2
            and normalized[0] == normalized[-1]
            and normalized[0] in {"'", '"'}
        ):
            normalized = normalized[1:-1]
        values[key.strip()] = normalized
    return values


def _required_private_file(value: str, *, label: str) -> Path:
    path = _required_file(value, label=label)
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        message = f"{label} must be owner-private"
        raise PermissionError(message)
    return path


def _required_file(value: str, *, label: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        message = f"{label} does not point to a file: {path}"
        raise ValueError(message)
    return path


def _load_json_object(path: Path, *, label: str) -> dict[str, object]:
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        message = f"{label} is not valid UTF-8 JSON"
        raise ValueError(message) from error
    return require_mapping(value, label=label)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    label: str,
) -> None:
    if set(value) != expected:
        message = f"{label} fields differ from its exact schema"
        raise ValueError(message)


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _required_text_sequence(
    value: object,
    *,
    label: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    raw = require_list(value, label=label)
    result = tuple(_required_text(item, label=f"{label} item") for item in raw)
    if (not allow_empty and not result) or len(set(result)) != len(result):
        message = f"{label} must be a non-empty unique sequence"
        raise ValueError(message)
    return result


def _required_positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} must be a positive integer"
        raise TypeError(message)
    return value


def _required_non_numeric_name(value: object, *, label: str) -> str:
    name = _required_text(value, label=label)
    try:
        int(name)
    except ValueError:
        return name
    message = f"{label} must not resolve as a native numeric selector"
    raise ValueError(message)


def _required_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        message = f"{label} must be a prefixed SHA-256 digest"
        raise ValueError(message)
    return text


def _required_hmac_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _HMAC_SHA256.fullmatch(text) is None:
        message = f"{label} must be a prefixed HMAC-SHA-256 digest"
        raise ValueError(message)
    return text


def _required_git_object(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _GIT_OBJECT.fullmatch(text) is None:
        message = f"{label} must be a full Git object identity"
        raise ValueError(message)
    return text


def _validate_fingerprints(value: object, *, label: str) -> None:
    fingerprints = require_mapping(value, label=label)
    expected = {"source", "effective_wire", "consumed_projection", "preservation"}
    _require_exact_keys(fingerprints, expected, label=label)
    for name in expected:
        _required_sha256(fingerprints.get(name), label=f"{label} {name}")


def _validate_image_reference(value: str, *, ds_version: str) -> None:
    provenance_module = importlib.import_module(
        "live_gate.conformance_image_provenance"
    )
    provenance_module.validate_image_reference(value, ds_version=ds_version)


def _parse_timestamp(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        message = f"{label} must be an ISO-8601 timestamp"
        raise ValueError(message) from error
    if parsed.tzinfo is None:
        message = f"{label} must include a timezone"
        raise ValueError(message)
    return parsed.astimezone(UTC)


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _non_empty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _argv_option(argv: list[str], option: str) -> str:
    try:
        value = argv[argv.index(option) + 1]
    except (IndexError, ValueError) as error:
        message = f"scenario invocation omitted required option {option}"
        raise AssertionError(message) from error
    return value


def _result_proves_mutation_not_applied(result: DsctlCommandResult) -> bool:
    if result.payload.get("ok") is not False:
        return False
    error = result.payload.get("error")
    if not isinstance(error, dict):
        return False
    details = error.get("details")
    if isinstance(details, dict):
        if details.get("mutation_applied") is False:
            return True
        if (
            details.get("mutation_applied") is True
            or details.get("mutation_may_have_applied") is True
        ):
            return False
    return error.get("type") in {
        "conflict",
        "invalid_state",
        "not_found",
        "permission_denied",
        "user_input_error",
    }


__all__ = [
    "BundleAssessment",
    "ClusterIdentity",
    "ConformanceBundleGateConfig",
    "ConformanceScenarioCleanupError",
    "ConformanceScenarioResult",
    "ExternalFixture",
    "InstalledBundleAttestation",
    "NativeFixtureIdentity",
    "TaskDefinitionCleanupDoNotRetryError",
    "TaskDefinitionCleanupInvocation",
    "execute_conformance_bundle_scenario",
    "identity_hmac",
    "load_conformance_bundle_gate_config",
    "load_conformance_recovery_run_id",
    "recover_existing_full_conformance_state",
    "write_conformance_bundle_candidate",
]
