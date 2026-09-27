from __future__ import annotations

import ast
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import dsctl

_TOOLS_ROOT = Path(__file__).resolve().parents[2] / "tools"
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Protocol

    from live_gate.exact_profile_policy import (
        ExactProfileGatePolicy,
        exact_profile_gate_policy,
    )
    from live_gate.exact_profile_read_corpus import load_tracked_artifacts
    from live_gate.runtime_ownership import (
        load_current_runtime_ownership,
        load_wheel_runtime_ownership,
    )

    class _GateBundleDigest(Protocol):
        def __call__(self, value: Mapping[str, object]) -> str: ...

    _gate_bundle_digest: _GateBundleDigest
else:
    from exact_342_evidence import (
        canonical_gate_bundle_digest as _gate_bundle_digest,
    )
    from live_gate.exact_profile_policy import (
        ExactProfileGatePolicy,
        exact_profile_gate_policy,
    )
    from live_gate.exact_profile_read_corpus import load_tracked_artifacts
    from live_gate.runtime_ownership import (
        load_current_runtime_ownership,
        load_wheel_runtime_ownership,
    )

_VERSION = "DS_LIVE_EXACT_VERSION"
_ENV_FILE = "DS_LIVE_EXACT_ENV_FILE"
_EXECUTABLE = "DS_LIVE_EXACT_DSCTL"
_PYTHON = "DS_LIVE_EXACT_PYTHON"
_WHEEL = "DS_LIVE_EXACT_WHEEL"
_CLUSTER_MANIFEST = "DS_LIVE_EXACT_CLUSTER_MANIFEST"
_FIXTURE_MANIFEST = "DS_LIVE_EXACT_FIXTURE_MANIFEST"
_ATTESTATION_KEY_FILE = "DS_LIVE_EXACT_ATTESTATION_KEY_FILE"
_EVIDENCE = "DS_LIVE_EXACT_EVIDENCE"
_EXPECTED_PERSONA = "etl-developer"
_EXPECTED_FIXTURE_PROVISIONER = "dsmatrix-exact-read-state-projection/v2"
_REQUIRED_ENV_NAMES = (
    _VERSION,
    _ENV_FILE,
    _EXECUTABLE,
    _PYTHON,
    _WHEEL,
    _CLUSTER_MANIFEST,
    _FIXTURE_MANIFEST,
    _ATTESTATION_KEY_FILE,
    _EVIDENCE,
)
_MANIFEST_FIELDS = {
    "BUNDLE_MANIFEST_SCHEMA_VERSION": "bundle_manifest_schema_version",
    "DS_VERSION": "ds_version",
    "SELECTION": "selection",
    "SEMANTIC_OPERATIONS": "semantic_operations",
    "SOURCE_TAG": "source_tag",
    "SOURCE_COMMIT": "source_commit",
    "SOURCE_TREE": "source_tree",
    "SOURCE_CONTRACT_DIGEST": "source_contract_digest",
    "RENDERED_CONTRACT_DIGEST": "rendered_contract_digest",
    "OPERATION_COUNT": "operation_count",
}
_IMAGE_DIGEST = re.compile(r"^(?P<repository>\S+)@sha256:(?P<digest>[0-9a-f]{64})$")
_HMAC_SHA256 = re.compile(r"^hmac-sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)


@dataclass(frozen=True)
class ClusterIdentity:
    """Non-secret immutable identity of the tested DolphinScheduler cluster."""

    ds_version: str
    image_tag: str
    image_digest: str
    image_source: str
    image_observed_at: str
    api_target_hmac_sha256: str
    principal_hmac_sha256: str
    persona: str


@dataclass(frozen=True)
class ExactProfileFixture:
    """Externally provisioned scheduled workflow with one editable task."""

    project_name: str
    project_code: int
    workflow_name: str
    workflow_code: int
    schedule_id: int
    workflow_release_state: str
    task_name: str
    task_code: int
    task_type: str
    exclusive: bool
    provisioner: str
    manifest_sha256: str


@dataclass(frozen=True)
class ExactProfileGateConfig:
    """Explicit installed-wheel, profile, fixture, and evidence inputs."""

    policy: ExactProfileGatePolicy

    env_file: Path
    executable: Path
    python: Path
    wheel: Path
    cluster_manifest: Path
    fixture_manifest: Path
    evidence_path: Path
    attestation_key: bytes
    cluster: ClusterIdentity
    fixture: ExactProfileFixture


@dataclass(frozen=True)
class InstalledExactProfileAttestation:
    """Allowlisted identity read from the wheel's isolated installation."""

    distribution_version: str
    server_version: str
    family: str
    support_level: str
    tested: bool
    contract_version: str
    profile_fingerprints: dict[str, str]
    action_verifications: dict[str, str]
    gate_recipes: tuple[dict[str, object], ...]
    manifest: dict[str, object]


INSTALLED_EXACT_PROFILE_PROBE_CODE = """
import json
import sys
from importlib.metadata import version
import dsctl
from importlib import import_module
_manifest = import_module(
    "dsctl.generated.versions.ds_" + sys.argv[2].replace(".", "_") + "._manifest"
)
from dsctl.generated.version_profiles import VERSION_PROFILES

profile = VERSION_PROFILES[sys.argv[2]]
gate_decisions = tuple(tuple(item) for item in json.loads(sys.argv[1]))
recipes = []
action_verifications = {}
for action, semantic_operation in gate_decisions:
    decision = profile["build_decisions"][semantic_operation]
    if decision["stable_action"] != action:
        raise AssertionError("gate action does not match generated decision")
    action_verifications[action] = profile["actions"][action]["verification"]
    recipes.append({
        "action": action,
        "semantic_operation": decision["semantic_operation"],
        "build_status": decision["build_status"],
        "fingerprints": decision["fingerprints"],
    })
manifest = {
    "bundle_manifest_schema_version": _manifest.BUNDLE_MANIFEST_SCHEMA_VERSION,
    "ds_version": _manifest.DS_VERSION,
    "selection": _manifest.SELECTION,
    "semantic_operations": list(_manifest.SEMANTIC_OPERATIONS),
    "source_tag": _manifest.SOURCE_TAG,
    "source_commit": _manifest.SOURCE_COMMIT,
    "source_tree": _manifest.SOURCE_TREE,
    "source_contract_digest": _manifest.SOURCE_CONTRACT_DIGEST,
    "rendered_contract_digest": _manifest.RENDERED_CONTRACT_DIGEST,
    "operation_count": _manifest.OPERATION_COUNT,
}
print(json.dumps({
    "distribution_version": version("dolphinscheduler-cli"),
    "module_file": dsctl.__file__,
    "server_version": profile["server_version"],
    "family": profile["family"],
    "support_level": profile["support_level"],
    "tested": profile["tested"],
    "contract_version": profile["contract_version"],
    "profile_source": profile["source"],
    "profile_fingerprints": profile["fingerprints"],
    "action_verifications": action_verifications,
    "gate_recipes": recipes,
    "manifest": manifest,
}))
"""


@dataclass(frozen=True)
class ExternalTaskCleanupAttestation:
    """Verified restoration state for the externally managed task fixture."""

    mutated: bool
    restored: bool
    exact_command_restored: bool
    restored_dag_version_consistent: bool
    non_owned_fields_restored: bool
    dag_topology_preserved: bool
    workflow_release_state_preserved: bool

    def __post_init__(self) -> None:
        if not all(
            (
                self.mutated,
                self.restored,
                self.exact_command_restored,
                self.restored_dag_version_consistent,
                self.non_owned_fields_restored,
                self.dag_topology_preserved,
                self.workflow_release_state_preserved,
            )
        ):
            message = "Passing task cleanup attestation requires exact restoration"
            raise ValueError(message)


@dataclass(frozen=True)
class CleanupAttestation:
    """Bounded cleanup result for resources created by the exact gate itself."""

    confirmed: bool
    project_leftovers: int
    external_task: ExternalTaskCleanupAttestation

    def __post_init__(self) -> None:
        if not self.confirmed or self.project_leftovers != 0:
            message = "Passing cleanup attestation requires zero gate-created projects"
            raise ValueError(message)

    def to_data(self) -> dict[str, object]:
        return {
            "gate_created_projects": {
                "confirmed": self.confirmed,
                "leftovers": self.project_leftovers,
            },
            "external_fixture": {
                "scope": "externally-managed-editable-offline-shell-task",
                "mutated_by_gate": self.external_task.mutated,
                "restored_by_gate": self.external_task.restored,
                "exact_command_restored": self.external_task.exact_command_restored,
                "restored_dag_version_consistent": (
                    self.external_task.restored_dag_version_consistent
                ),
                "non_owned_fields_restored": (
                    self.external_task.non_owned_fields_restored
                ),
                "dag_topology_preserved": (self.external_task.dag_topology_preserved),
                "workflow_release_state_preserved": (
                    self.external_task.workflow_release_state_preserved
                ),
            },
        }


@dataclass(frozen=True)
class OperationTraceEntry:
    """One redacted CLI action outcome from the live gate."""

    sequence: int
    argv_shape: str
    action: str
    exit_code: int
    ok: bool
    assertions: tuple[str, ...]
    selector_kind: str | None = None
    error_type: str | None = None

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            message = "operation trace sequence must be positive"
            raise ValueError(message)
        if not self.argv_shape.strip() or not self.action.strip():
            message = "operation trace action and argv shape must not be blank"
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

    def to_data(self) -> dict[str, object]:
        data: dict[str, object] = {
            "sequence": self.sequence,
            "argv_shape": self.argv_shape,
            "action": self.action,
            "exit_code": self.exit_code,
            "ok": self.ok,
            "assertions": list(self.assertions),
        }
        if self.selector_kind is not None:
            data["selector_kind"] = self.selector_kind
        if self.error_type is not None:
            data["error_type"] = self.error_type
        return data


def load_exact_profile_gate_config(
    environment: Mapping[str, str],
) -> ExactProfileGateConfig:
    """Load every explicit input and cross-check cluster/fixture identity."""
    missing = [
        name for name in _REQUIRED_ENV_NAMES if not environment.get(name, "").strip()
    ]
    if missing:
        message = "Missing exact profile live-gate inputs: " + ", ".join(missing)
        raise ValueError(message)

    policy = exact_profile_gate_policy(environment[_VERSION])
    env_file = _required_file(environment[_ENV_FILE], label=_ENV_FILE)
    executable = _required_file(environment[_EXECUTABLE], label=_EXECUTABLE)
    python = _required_file(environment[_PYTHON], label=_PYTHON)
    wheel = _required_file(environment[_WHEEL], label=_WHEEL)
    cluster_manifest = _required_file(
        environment[_CLUSTER_MANIFEST],
        label=_CLUSTER_MANIFEST,
    )
    fixture_manifest = _required_file(
        environment[_FIXTURE_MANIFEST],
        label=_FIXTURE_MANIFEST,
    )
    attestation_key_file = _required_file(
        environment[_ATTESTATION_KEY_FILE],
        label=_ATTESTATION_KEY_FILE,
    )
    if wheel.suffix != ".whl":
        message = f"{_WHEEL} must point to a built wheel"
        raise ValueError(message)
    if executable.parent != python.parent:
        message = "Exact gate dsctl and Python must come from the same virtualenv"
        raise ValueError(message)

    attestation_key = _load_attestation_key(attestation_key_file)
    cluster = _load_cluster_identity(
        cluster_manifest,
        policy=policy,
        env_file=env_file,
        attestation_key=attestation_key,
    )
    fixture = _load_fixture(fixture_manifest, cluster=cluster)
    return ExactProfileGateConfig(
        policy=policy,
        env_file=env_file,
        executable=executable,
        python=python,
        wheel=wheel,
        cluster_manifest=cluster_manifest,
        fixture_manifest=fixture_manifest,
        evidence_path=Path(environment[_EVIDENCE]).expanduser(),
        attestation_key=attestation_key,
        cluster=cluster,
        fixture=fixture,
    )


def load_exact_profile_manifest(wheel: Path, *, ds_version: str) -> dict[str, object]:
    """Read the exact generated-contract identity shipped inside one wheel."""
    policy = exact_profile_gate_policy(ds_version)
    with zipfile.ZipFile(wheel) as archive:
        try:
            source = archive.read(policy.wheel_manifest_path).decode("utf-8")
        except KeyError as error:
            message = f"Wheel does not contain {policy.wheel_manifest_path}"
            raise ValueError(message) from error

    assignments: dict[str, object] = {}
    filename = f"{wheel}!/{policy.wheel_manifest_path}"
    for statement in ast.parse(source, filename=filename).body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name) or target.id not in _MANIFEST_FIELDS:
            continue
        assignments[target.id] = ast.literal_eval(statement.value)

    missing = sorted(set(_MANIFEST_FIELDS) - assignments.keys())
    if missing:
        message = "Wheel contract manifest is incomplete: " + ", ".join(missing)
        raise ValueError(message)
    return _validated_exact_profile_manifest(assignments, policy=policy)


def _validated_exact_profile_manifest(
    assignments: Mapping[str, object],
    *,
    policy: ExactProfileGatePolicy,
) -> dict[str, object]:
    if assignments["DS_VERSION"] != policy.ds_version:
        message = (
            f"Wheel contract manifest is not for DolphinScheduler {policy.ds_version}"
        )
        raise ValueError(message)
    if assignments["SELECTION"] != "runtime-slice":
        message = (
            f"DolphinScheduler {policy.ds_version} wheel contract "
            "must be a runtime slice"
        )
        raise ValueError(message)
    bundle_manifest_schema_version = assignments["BUNDLE_MANIFEST_SCHEMA_VERSION"]
    if (
        type(bundle_manifest_schema_version) is not int
        or bundle_manifest_schema_version != 2
    ):
        message = (
            f"DolphinScheduler {policy.ds_version} wheel manifest schema is unsupported"
        )
        raise ValueError(message)
    if assignments["SOURCE_TAG"] != policy.ds_version:
        message = (
            f"DolphinScheduler {policy.ds_version} wheel source tag is inconsistent"
        )
        raise ValueError(message)

    manifest = {
        output_name: assignments[source_name]
        for source_name, output_name in _MANIFEST_FIELDS.items()
    }
    semantic_operations = manifest["semantic_operations"]
    if not isinstance(semantic_operations, tuple) or not all(
        isinstance(item, str) for item in semantic_operations
    ):
        message = "Wheel semantic operations must be a tuple of strings"
        raise ValueError(message)
    manifest["semantic_operations"] = list(semantic_operations)
    operation_count = manifest["operation_count"]
    if not isinstance(operation_count, int) or isinstance(operation_count, bool):
        message = "Wheel operation count must be an integer"
        raise TypeError(message)
    if operation_count < 0:
        message = "Wheel operation count must be nonnegative"
        raise ValueError(message)
    return manifest


def inspect_installed_exact_profile(
    config: ExactProfileGateConfig,
) -> InstalledExactProfileAttestation:
    """Cross-check the installed profile against its immutable wheel archive."""
    policy = config.policy
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [
            str(config.python),
            "-I",
            "-c",
            INSTALLED_EXACT_PROFILE_PROBE_CODE,
            json.dumps(policy.recipes),
            policy.ds_version,
        ],
        capture_output=True,
        check=False,
        cwd=config.python.parent,
        env=environment,
        text=True,
        timeout=30.0,
    )
    if completed.returncode != 0:
        message = (
            f"Could not inspect the isolated DolphinScheduler {policy.ds_version} wheel"
        )
        raise AssertionError(message)
    payload = _required_mapping(
        json.loads(completed.stdout),
        label="installed-wheel attestation",
    )
    _require_exact_keys(
        payload,
        {
            "action_verifications",
            "contract_version",
            "distribution_version",
            "family",
            "gate_recipes",
            "manifest",
            "module_file",
            "profile_fingerprints",
            "profile_source",
            "server_version",
            "support_level",
            "tested",
        },
        label="installed-wheel attestation",
    )
    module_file = _required_text(payload.get("module_file"), label="module_file")
    venv_root = config.python.parent.parent.resolve()
    if not Path(module_file).resolve().is_relative_to(venv_root):
        message = "Installed dsctl did not import from the gate virtualenv"
        raise AssertionError(message)

    archive_manifest = load_exact_profile_manifest(
        config.wheel, ds_version=policy.ds_version
    )
    installed_manifest = _required_mapping(
        payload.get("manifest"),
        label="installed manifest",
    )
    if installed_manifest != archive_manifest:
        message = "Installed manifest does not match the snapshotted wheel archive"
        raise AssertionError(message)
    _validate_profile_source(
        payload.get("profile_source"),
        manifest=archive_manifest,
    )
    profile_fingerprints = _load_fingerprints(
        payload.get("profile_fingerprints"),
        label="profile fingerprints",
    )
    action_verifications = _load_action_verifications(
        payload.get("action_verifications"),
        label="profile action verifications",
        policy=policy,
    )
    if set(action_verifications.values()) != {"live_smoke"}:
        message = "Installed exact profile gate actions are not all live_smoke"
        raise AssertionError(message)
    gate_recipes = _load_gate_recipes(
        payload.get("gate_recipes"),
        policy=policy,
        manifest=installed_manifest,
        runtime_operations=load_wheel_runtime_ownership(config.wheel)[
            policy.ds_version
        ],
    )
    attestation = InstalledExactProfileAttestation(
        distribution_version=_required_text(
            payload.get("distribution_version"),
            label="distribution_version",
        ),
        server_version=_required_text(
            payload.get("server_version"),
            label="server_version",
        ),
        family=_required_text(payload.get("family"), label="family"),
        support_level=_required_text(
            payload.get("support_level"),
            label="support_level",
        ),
        tested=_required_bool(payload.get("tested"), label="tested"),
        contract_version=_required_text(
            payload.get("contract_version"),
            label="contract_version",
        ),
        profile_fingerprints=profile_fingerprints,
        action_verifications=action_verifications,
        gate_recipes=gate_recipes,
        manifest=dict(installed_manifest),
    )
    expected_profile = {
        "server_version": policy.ds_version,
        "family": policy.family,
        "support_level": policy.support_level,
        "tested": policy.tested,
        "contract_version": policy.ds_version,
    }
    actual_profile = {key: getattr(attestation, key) for key in expected_profile}
    if actual_profile != expected_profile:
        message = "Installed wheel does not expose the exact profile semantic profile"
        raise AssertionError(message)
    return attestation


def _validate_profile_source(
    value: object,
    *,
    manifest: Mapping[str, object],
) -> None:
    source = _required_mapping(value, label="profile source")
    _require_exact_keys(source, {"tag", "commit", "tree"}, label="profile source")
    expected = {
        "tag": manifest["source_tag"],
        "commit": manifest["source_commit"],
        "tree": manifest["source_tree"],
    }
    if source != expected:
        message = "Installed profile source does not match the exact manifest"
        raise AssertionError(message)


def _load_fingerprints(value: object, *, label: str) -> dict[str, str]:
    fingerprints = _required_mapping(value, label=label)
    expected_keys = {
        "consumed_projection",
        "effective_wire",
        "preservation",
        "source",
    }
    _require_exact_keys(fingerprints, expected_keys, label=label)
    normalized = {
        key: _required_text(fingerprints.get(key), label=f"{label} {key}")
        for key in sorted(expected_keys)
    }
    if any(_SHA256.fullmatch(item) is None for item in normalized.values()):
        message = f"{label} must contain SHA-256 identities"
        raise ValueError(message)
    return normalized


def _load_action_verifications(
    value: object, *, label: str, policy: ExactProfileGatePolicy
) -> dict[str, str]:
    raw = _required_mapping(value, label=label)
    expected_actions = {action for action, _operation in policy.recipes}
    _require_exact_keys(raw, expected_actions, label=label)
    allowed = {"static", "contract_tested", "live_smoke", "live_full"}
    normalized: dict[str, str] = {}
    for action, _operation in policy.recipes:
        verification = _required_text(
            raw.get(action),
            label=f"{label} {action}",
        )
        if verification not in allowed:
            message = f"{label} {action} is not a recognized verification level"
            raise ValueError(message)
        normalized[action] = verification
    return normalized


def _load_gate_recipes(
    value: object,
    *,
    policy: ExactProfileGatePolicy,
    manifest: Mapping[str, object],
    runtime_operations: frozenset[str],
) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        message = "gate recipes must be an array"
        raise TypeError(message)
    if len(value) != len(policy.recipes):
        message = "Installed profile did not expose all 15 exact gate recipes"
        raise AssertionError(message)
    recipes: list[dict[str, object]] = []
    for raw_recipe, (expected_action, expected_operation) in zip(
        value,
        policy.recipes,
        strict=True,
    ):
        recipe = _required_mapping(raw_recipe, label="gate recipe")
        _require_exact_keys(
            recipe,
            {"action", "semantic_operation", "build_status", "fingerprints"},
            label="gate recipe",
        )
        if (
            recipe.get("action") != expected_action
            or recipe.get("semantic_operation") != expected_operation
            or recipe.get("build_status") != "accepted"
        ):
            message = "Installed gate recipe does not match the accepted exact bundle"
            raise AssertionError(message)
        recipes.append(
            {
                "action": expected_action,
                "semantic_operation": expected_operation,
                "build_status": "accepted",
                "fingerprints": _load_fingerprints(
                    recipe.get("fingerprints"),
                    label=f"{expected_action} fingerprints",
                ),
            }
        )
    operations = manifest.get("semantic_operations")
    if not isinstance(operations, list):
        message = "Installed manifest semantic operations must be an array"
        raise TypeError(message)
    recipe_operations = {operation for _action, operation in policy.recipes}
    if not recipe_operations <= runtime_operations:
        message = "Installed gate recipes are absent from the exact runtime"
        raise AssertionError(message)
    return tuple(recipes)


def write_exact_profile_evidence(
    config: ExactProfileGateConfig,
    *,
    version_data: Mapping[str, object],
    installation: InstalledExactProfileAttestation,
    operation_trace: Sequence[OperationTraceEntry],
    cleanup: CleanupAttestation,
    recorded_at: datetime | None = None,
) -> Path:
    """Atomically write a bounded, credential-free record for a passing gate."""
    policy = config.policy
    timestamp = recorded_at or datetime.now(tz=UTC)
    expected_version_data = {
        "cli": installation.distribution_version,
        "ds": installation.server_version,
        "selected_ds_version": installation.server_version,
        "contract_version": installation.contract_version,
        "family": installation.family,
        "support_level": installation.support_level,
    }
    for key, expected in expected_version_data.items():
        if version_data.get(key) != expected:
            message = f"Black-box CLI {key} does not match installed-wheel attestation"
            raise AssertionError(message)
    gate_bundle_base: dict[str, object] = {
        "actions": [action for action, _operation in policy.recipes],
        "action_verifications": dict(installation.action_verifications),
        "recipes": list(installation.gate_recipes),
    }
    gate_bundle = {
        **gate_bundle_base,
        "digest": _gate_bundle_digest(gate_bundle_base),
    }
    evidence = {
        "schema_version": policy.current_schema_version,
        "sanitization_schema_version": 1,
        "gate": policy.gate_id,
        "status": "passed",
        "recorded_at": timestamp.isoformat().replace("+00:00", "Z"),
        "runner": {
            "artifact": "installed-wheel-console-script",
            "cli_version": installation.distribution_version,
            "wheel_filename": config.wheel.name,
            "wheel_sha256": f"sha256:{_sha256(config.wheel)}",
        },
        "dolphinscheduler": {
            "release": config.cluster.ds_version,
            "image_tag": config.cluster.image_tag,
            "image_digest": config.cluster.image_digest,
            "image_source": config.cluster.image_source,
            "image_observed_at": config.cluster.image_observed_at,
            "api_target_hmac_sha256": config.cluster.api_target_hmac_sha256,
            "principal_hmac_sha256": config.cluster.principal_hmac_sha256,
            "persona": config.cluster.persona,
        },
        "profile": {
            "ds": installation.server_version,
            "selected_ds_version": installation.server_version,
            "contract_version": installation.contract_version,
            "family": installation.family,
            "support_level": installation.support_level,
            "tested": installation.tested,
            "fingerprints": dict(installation.profile_fingerprints),
        },
        "contract": dict(installation.manifest),
        "gate_bundle": gate_bundle,
        "contract_operation_test": policy.contract_operation_test,
        "fixture": {
            "manifest_sha256": f"sha256:{config.fixture.manifest_sha256}",
            "provisioner": config.fixture.provisioner,
            "scheduled_workflow": True,
            "editable_offline_shell_task": True,
            "exclusive_fixture": config.fixture.exclusive,
        },
        "operation_trace": [entry.to_data() for entry in operation_trace],
        "cleanup": cleanup.to_data(),
        "secrets_recorded": False,
    }
    validate_exact_profile_evidence_payload(
        evidence, wheel=config.wheel, ds_version=policy.ds_version
    )
    raw = json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    _reject_protected_values(raw, config=config)
    config.evidence_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=config.evidence_path.parent,
        prefix=f".{config.evidence_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temporary_path = Path(stream.name)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    temporary_path.replace(config.evidence_path)
    return config.evidence_path


def validate_exact_profile_evidence_payload(
    value: object, *, ds_version: str, wheel: Path | None = None
) -> None:
    """Validate current compiled ownership before the selected receipt schema."""
    policy = exact_profile_gate_policy(ds_version)
    compiled_operations: frozenset[str] = frozenset()
    if (
        isinstance(value, dict)
        and value.get("schema_version") == policy.current_schema_version
    ):
        if wheel is None:
            source_root = _TOOLS_ROOT.parent
            artifacts = load_tracked_artifacts(source_root)
            runtime_operations = load_current_runtime_ownership(
                source_root, contracts=artifacts.contracts
            )
            contract = artifacts.contracts[policy.ds_version]
        else:
            runtime_operations = load_wheel_runtime_ownership(wheel)
            contract = load_exact_profile_manifest(wheel, ds_version=policy.ds_version)
        legacy_operations = frozenset(
            cast("list[str]", contract["semantic_operations"])
        )
        compiled_operations = runtime_operations[policy.ds_version] - legacy_operations
    policy.validate_receipt(
        value,
        expected_cli_version=dsctl.__version__,
        compiled_semantic_operations=compiled_operations,
    )


def canonical_gate_bundle_digest(value: Mapping[str, object]) -> str:
    """Expose the independent schema digest to exact-gate tests."""
    return _gate_bundle_digest(value)


def _load_cluster_identity(
    path: Path,
    *,
    policy: ExactProfileGatePolicy,
    env_file: Path,
    attestation_key: bytes,
) -> ClusterIdentity:
    payload = _load_json_object(path, label="cluster manifest")
    if (
        payload.get("schema_version") != 2
        or payload.get("ds_version") != policy.ds_version
    ):
        message = "Cluster manifest must declare schema 2 and DS profile"
        raise ValueError(message)
    image_tag = _required_text(payload.get("image_tag"), label="cluster image tag")
    image_digest = _required_text(
        payload.get("image_digest"),
        label="cluster image digest",
    )
    _validate_image_identity(
        image_tag=image_tag, image_digest=image_digest, policy=policy
    )
    api_target_hmac_sha256 = _required_hmac_sha256(
        payload.get("api_target_hmac_sha256"),
        label="cluster API target HMAC",
    )
    profile_values = _read_profile_values(env_file)
    profile_url = _required_text(
        profile_values.get("DS_API_URL"),
        label="profile DS_API_URL",
    ).rstrip("/")
    if api_target_hmac_sha256 != identity_hmac(
        profile_url,
        key=attestation_key,
    ):
        message = "Cluster manifest API target does not match the gate profile"
        raise ValueError(message)
    image_observed_at = _required_timestamp(
        payload.get("image_observed_at"),
        label="image_observed_at",
    )
    _validate_observation_window(image_observed_at, reference=datetime.now(tz=UTC))
    persona = _required_text(payload.get("persona"), label="persona")
    if persona != _EXPECTED_PERSONA:
        message = f"Cluster manifest persona must be {_EXPECTED_PERSONA!r}"
        raise ValueError(message)
    return ClusterIdentity(
        ds_version=policy.ds_version,
        image_tag=image_tag,
        image_digest=image_digest,
        image_source=_required_text(
            payload.get("image_source"),
            label="image_source",
        ),
        image_observed_at=image_observed_at,
        api_target_hmac_sha256=api_target_hmac_sha256,
        principal_hmac_sha256=_required_hmac_sha256(
            payload.get("principal_hmac_sha256"),
            label="cluster principal HMAC",
        ),
        persona=persona,
    )


def _load_fixture(path: Path, *, cluster: ClusterIdentity) -> ExactProfileFixture:
    policy = exact_profile_gate_policy(cluster.ds_version)
    payload = _load_json_object(path, label="fixture manifest")
    if (
        payload.get("schema_version") != 2
        or payload.get("ds_version") != policy.ds_version
    ):
        message = "Fixture manifest must declare schema 2 and DS profile"
        raise ValueError(message)
    if (
        payload.get("image_tag") != cluster.image_tag
        or payload.get("image_digest") != cluster.image_digest
    ):
        message = (
            "Fixture and cluster manifests must identify the same image tag/digest"
        )
        raise ValueError(message)
    if payload.get("exclusive") is not True:
        message = (
            "Exact profile editable fixture must be reserved exclusively for the gate"
        )
        raise ValueError(message)
    project = _required_mapping(payload.get("project"), label="fixture project")
    workflow = _required_mapping(payload.get("workflow"), label="fixture workflow")
    if workflow.get("scheduled") is not True:
        message = "Exact profile fixture workflow must have an attached schedule"
        raise ValueError(message)
    release_state = _required_text(
        workflow.get("release_state"),
        label="fixture workflow release_state",
    )
    if release_state != "OFFLINE":
        message = "Exact profile editable task fixture workflow must be OFFLINE"
        raise ValueError(message)
    task = _required_mapping(
        workflow.get("editable_task"),
        label="fixture editable task",
    )
    task_type = _required_text(task.get("type"), label="fixture task type")
    if task_type != "SHELL":
        message = "Exact profile editable task fixture must be a SHELL task"
        raise ValueError(message)
    provisioner = _required_text(
        payload.get("provisioner"),
        label="fixture provisioner",
    )
    if provisioner != _EXPECTED_FIXTURE_PROVISIONER:
        message = (
            f"Exact profile fixture provisioner must be {_EXPECTED_FIXTURE_PROVISIONER}"
        )
        raise ValueError(message)
    return ExactProfileFixture(
        project_name=_required_text(project.get("name"), label="fixture project name"),
        project_code=_required_int(project.get("code"), label="fixture project code"),
        workflow_name=_required_text(
            workflow.get("name"),
            label="fixture workflow name",
        ),
        workflow_code=_required_int(
            workflow.get("code"),
            label="fixture workflow code",
        ),
        schedule_id=_required_int(
            workflow.get("schedule_id"),
            label="fixture schedule id",
        ),
        workflow_release_state=release_state,
        task_name=_required_text(task.get("name"), label="fixture task name"),
        task_code=_required_int(task.get("code"), label="fixture task code"),
        task_type=task_type,
        exclusive=True,
        provisioner=provisioner,
        manifest_sha256=_sha256(path),
    )


def _validate_image_identity(
    *, image_tag: str, image_digest: str, policy: ExactProfileGatePolicy
) -> None:
    tag_match = re.fullmatch(
        rf"(?P<repository>\S+):{re.escape(policy.ds_version)}", image_tag
    )
    digest_match = _IMAGE_DIGEST.fullmatch(image_digest)
    if tag_match is None:
        message = (
            f"Cluster image tag must identify DolphinScheduler {policy.ds_version}"
        )
        raise ValueError(message)
    if digest_match is None:
        message = "Cluster image digest must include an immutable @sha256 identity"
        raise ValueError(message)
    if tag_match["repository"] != digest_match["repository"]:
        message = "Cluster image tag and digest must use the same repository"
        raise ValueError(message)


def _reject_protected_values(
    raw: str,
    *,
    config: ExactProfileGateConfig,
) -> None:
    protected_values = [
        *_profile_secret_values(config.env_file),
        _printable_secret(config.attestation_key),
        config.fixture.project_name,
        config.fixture.workflow_name,
        config.fixture.task_name,
        *[
            value
            for value in (
                str(config.fixture.project_code),
                str(config.fixture.workflow_code),
                str(config.fixture.schedule_id),
                str(config.fixture.task_code),
            )
            if len(value) >= 8
        ],
    ]
    leaked = next((value for value in protected_values if value and value in raw), None)
    if leaked is not None:
        message = "Live evidence contains a protected profile or fixture value"
        raise ValueError(message)


def _profile_secret_values(path: Path) -> list[str]:
    values = _read_profile_values(path)
    return [
        value
        for key in ("DS_API_TOKEN", "DS_API_URL")
        for value in [values.get(key, "")]
        if value
    ]


def _read_profile_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", maxsplit=1)
        normalized = value.strip().strip("'\"")
        values[key.strip()] = normalized
    return values


def identity_hmac(value: str, *, key: bytes) -> str:
    """Bind a low-entropy identity without publishing a reversible digest."""
    digest = hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"hmac-sha256:{digest}"


def _load_attestation_key(path: Path) -> bytes:
    key = path.read_bytes().strip()
    if len(key) < 32:
        message = "Exact profile attestation key must contain at least 32 bytes"
        raise ValueError(message)
    return key


def _printable_secret(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def _load_json_object(path: Path, *, label: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    return payload


def _required_file(value: str, *, label: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        message = f"{label} does not point to a file: {path}"
        raise ValueError(message)
    return path


def _required_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


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
    details = []
    if missing:
        details.append("missing: " + ", ".join(missing))
    if extra:
        details.append("extra: " + ", ".join(extra))
    message = f"{label} has invalid fields; {'; '.join(details)}"
    raise ValueError(message)


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value.strip()


def _required_hmac_sha256(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    if _HMAC_SHA256.fullmatch(text) is None:
        message = f"{label} must be a keyed HMAC-SHA256 identity"
        raise ValueError(message)
    return text


def _required_timestamp(value: object, *, label: str) -> str:
    text = _required_text(value, label=label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        message = f"{label} must be an ISO-8601 timestamp"
        raise ValueError(message) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        message = f"{label} must include a timezone"
        raise ValueError(message)
    return text


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def _validate_observation_window(value: str, *, reference: datetime) -> None:
    observed_at = _parse_timestamp(value)
    normalized_reference = reference.astimezone(UTC)
    if observed_at > normalized_reference + _MAX_CLOCK_SKEW:
        message = "image_observed_at is implausibly later than the gate run"
        raise ValueError(message)
    if normalized_reference - observed_at > _MAX_OBSERVATION_AGE:
        message = "image_observed_at is too stale for exact profile evidence"
        raise ValueError(message)


def _required_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"{label} must be an integer"
        raise TypeError(message)
    return value


def _required_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        message = f"{label} must be a boolean"
        raise TypeError(message)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
