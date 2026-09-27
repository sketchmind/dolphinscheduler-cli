from __future__ import annotations

import ast
import hashlib
import hmac
import importlib
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
from typing import TYPE_CHECKING, Literal, cast

from tests.live.support import (
    DsctlCommandResult,
    require_list,
    require_mapping,
    require_ok_payload,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import Protocol

    class _EvidenceValidator(Protocol):
        def __call__(
            self,
            value: object,
            *,
            expected_cli_version: str,
            expected_ds_version: str,
            compiled_semantic_operations: frozenset[str] = frozenset(),
        ) -> None: ...

    class _ReadBundleDigest(Protocol):
        def __call__(self, value: Mapping[str, object]) -> str: ...

    _validate_read_evidence: _EvidenceValidator
    _read_bundle_digest: _ReadBundleDigest

_TOOLS_ROOT = Path(__file__).resolve().parents[2] / "tools"
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))
_READ_EVIDENCE_SCHEMA = importlib.import_module("live_gate.exact_profile_read_evidence")
_RUNTIME_OWNERSHIP = importlib.import_module("live_gate.runtime_ownership")
EXACT_PROFILE_READ_ACTIONS = cast(
    "tuple[str, ...]",
    _READ_EVIDENCE_SCHEMA.EXACT_PROFILE_READ_ACTIONS,
)
EXACT_PROFILE_READ_CAPABILITY_ACTIONS = cast(
    "tuple[str, ...]",
    _READ_EVIDENCE_SCHEMA.EXACT_PROFILE_READ_CAPABILITY_ACTIONS,
)
EXACT_PROFILE_READ_RECIPES = cast(
    "tuple[tuple[str, str], ...]",
    _READ_EVIDENCE_SCHEMA.EXACT_PROFILE_READ_RECIPES,
)
CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION = cast(
    "int",
    _READ_EVIDENCE_SCHEMA.CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION,
)
if not TYPE_CHECKING:
    _read_bundle_digest = _READ_EVIDENCE_SCHEMA.canonical_read_bundle_digest
    _validate_read_evidence = (
        _READ_EVIDENCE_SCHEMA.validate_exact_profile_read_evidence_payload
    )

IdentityKind = Literal["id", "code"]
_REQUIRED_READ_OPERATIONS = frozenset(
    {"project.get", "project.page", "workflow.get", "workflow.page"}
)
_EXPECTED_PERSONA = "etl-developer"
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)
_HMAC_SHA256 = re.compile(r"^hmac-sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_WORKFLOW_LIST_SCHEDULE_VERSIONS = frozenset(
    {
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    }
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
_RUNTIME_ENV_NAMES = {
    "version": "DS_LIVE_EXACT_READ_VERSION",
    "env_file": "DS_LIVE_EXACT_READ_ENV_FILE",
    "executable": "DS_LIVE_EXACT_READ_DSCTL",
    "python": "DS_LIVE_EXACT_READ_PYTHON",
    "wheel": "DS_LIVE_EXACT_READ_WHEEL",
    "cluster_manifest": "DS_LIVE_EXACT_READ_CLUSTER_MANIFEST",
    "fixture_manifest": "DS_LIVE_EXACT_READ_FIXTURE_MANIFEST",
    "attestation_key_file": "DS_LIVE_EXACT_READ_ATTESTATION_KEY_FILE",
    "evidence": "DS_LIVE_EXACT_READ_EVIDENCE",
}
INSTALLED_READ_PROBE_CODE = """
import importlib
import json
import sys
from importlib.metadata import version as distribution_version

import dsctl
from dsctl.generated.version_profiles import VERSION_PROFILES

target = sys.argv[1]
slug = target.replace(".", "_")
manifest_module = importlib.import_module(
    f"dsctl.generated.versions.ds_{slug}._manifest"
)
profile = VERSION_PROFILES[target]
read_decisions = tuple(tuple(item) for item in json.loads(sys.argv[2]))
recipes = []
action_verifications = {}
for action, semantic_operation in read_decisions:
    decision = profile["build_decisions"][semantic_operation]
    if decision["stable_action"] != action:
        raise AssertionError("read action does not match generated decision")
    action_verifications[action] = profile["actions"][action]["verification"]
    recipes.append({
        "action": action,
        "semantic_operation": decision["semantic_operation"],
        "build_status": decision["build_status"],
        "fingerprints": decision["fingerprints"],
    })
manifest = {
    "bundle_manifest_schema_version": manifest_module.BUNDLE_MANIFEST_SCHEMA_VERSION,
    "ds_version": manifest_module.DS_VERSION,
    "selection": manifest_module.SELECTION,
    "semantic_operations": list(manifest_module.SEMANTIC_OPERATIONS),
    "source_tag": manifest_module.SOURCE_TAG,
    "source_commit": manifest_module.SOURCE_COMMIT,
    "source_tree": manifest_module.SOURCE_TREE,
    "source_contract_digest": manifest_module.SOURCE_CONTRACT_DIGEST,
    "rendered_contract_digest": manifest_module.RENDERED_CONTRACT_DIGEST,
    "operation_count": manifest_module.OPERATION_COUNT,
}
print(json.dumps({
    "distribution_version": distribution_version("dolphinscheduler-cli"),
    "module_file": dsctl.__file__,
    "server_version": profile["server_version"],
    "contract_version": profile["contract_version"],
    "family": profile["family"],
    "support_level": profile["support_level"],
    "tested": profile["tested"],
    "profile_source": profile["source"],
    "profile_fingerprints": profile["fingerprints"],
    "action_verifications": action_verifications,
    "read_recipes": recipes,
    "manifest": manifest,
}))
"""


@dataclass(frozen=True)
class NativeFixtureIdentity:
    """One exact-version native identity used by a read-only fixture."""

    kind: IdentityKind
    value: int

    def __post_init__(self) -> None:
        if self.value <= 0:
            message = "fixture native identity must be positive"
            raise ValueError(message)


@dataclass(frozen=True)
class ClusterIdentity:
    """Non-secret identity of the exact DolphinScheduler deployment."""

    ds_version: str
    image_ref: str
    image_id: str
    image_source: str
    image_observed_at: str
    api_target_hmac_sha256: str
    principal_hmac_sha256: str
    persona: str


@dataclass(frozen=True)
class ExactReadFixture:
    """Externally provisioned project and scheduled workflow for read smoke."""

    project_name: str
    project_identity: NativeFixtureIdentity
    workflow_name: str
    workflow_identity: NativeFixtureIdentity
    schedule_id: int
    workflow_release_state: str
    provisioner: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        for label, name in (
            ("project", self.project_name),
            ("workflow", self.workflow_name),
        ):
            normalized = name.strip()
            if not normalized:
                message = f"fixture {label} name must be non-empty"
                raise ValueError(message)
            try:
                int(normalized)
            except ValueError:
                continue
            message = f"fixture {label} name must not resolve as a numeric selector"
            raise ValueError(message)
        if self.workflow_release_state not in {"ONLINE", "OFFLINE"}:
            message = "fixture workflow release state must be ONLINE or OFFLINE"
            raise ValueError(message)


@dataclass(frozen=True)
class ExactReadGateConfig:
    """Expected profile, cluster, and fixture facts for one read scenario."""

    ds_version: str
    family: str
    support_level: str
    tested: bool
    attestation_key: bytes
    cluster: ClusterIdentity
    fixture: ExactReadFixture

    def __post_init__(self) -> None:
        expected_kind: IdentityKind = "id" if self.ds_version == "1.3.9" else "code"
        identities = (
            self.fixture.project_identity,
            self.fixture.workflow_identity,
        )
        if any(identity.kind != expected_kind for identity in identities):
            message = (
                f"DS {self.ds_version} fixtures must use {expected_kind} identities"
            )
            raise ValueError(message)


@dataclass(frozen=True)
class ExactReadRuntimeConfig:
    """Explicit files and verified external facts for one installed-wheel run."""

    ds_version: str
    env_file: Path
    executable: Path
    python: Path
    wheel: Path
    cluster_manifest: Path
    fixture_manifest: Path
    evidence_path: Path
    attestation_key: bytes
    cluster: ClusterIdentity
    fixture: ExactReadFixture


@dataclass(frozen=True)
class InstalledReadAttestation:
    """Allowlisted exact profile facts imported from the isolated wheel."""

    distribution_version: str
    contract_version: str
    family: str
    support_level: str
    tested: bool
    profile_fingerprints: dict[str, str]
    action_verifications: dict[str, str]
    read_recipes: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class OperationTraceEntry:
    """One sanitized black-box command outcome from the read gate."""

    sequence: int
    argv_shape: str
    action: str
    exit_code: int
    ok: bool
    assertions: tuple[str, ...]
    selector_kind: str | None = None

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
        return data


@dataclass(frozen=True)
class ExactReadEffects:
    """Explicit side-effect claim for the bounded read-only scenario."""

    remote_mutations: int = 0
    fixture_mutated: bool = False

    def to_data(self) -> dict[str, object]:
        return {
            "remote_mutations": self.remote_mutations,
            "fixture_mutated": self.fixture_mutated,
        }


@dataclass(frozen=True)
class ExactReadResult:
    """Passing version metadata, trace, and side-effect claim."""

    version_data: Mapping[str, object]
    operation_trace: tuple[OperationTraceEntry, ...]
    effects: ExactReadEffects


def load_exact_manifest(wheel: Path, *, ds_version: str) -> dict[str, object]:
    """Read and validate one exact generated manifest without executing it."""
    version_slug = ds_version.replace(".", "_")
    manifest_path = f"dsctl/generated/versions/ds_{version_slug}/_manifest.py"
    with zipfile.ZipFile(wheel) as archive:
        try:
            source = archive.read(manifest_path).decode("utf-8")
        except KeyError as error:
            message = f"Wheel does not contain {manifest_path}"
            raise ValueError(message) from error

    assignments = _parse_manifest_assignments(
        source,
        filename=f"{wheel}!/{manifest_path}",
    )
    manifest = _validated_manifest(assignments, ds_version=ds_version)
    _validate_manifest_operations(
        selection=manifest["selection"],
        operations=cast("tuple[object, ...]", assignments["SEMANTIC_OPERATIONS"]),
        operation_count=cast("int", manifest["operation_count"]),
        runtime_operations=_RUNTIME_OWNERSHIP.load_wheel_runtime_ownership(wheel)[
            ds_version
        ],
    )
    return manifest


def _parse_manifest_assignments(
    source: str,
    *,
    filename: str,
) -> dict[str, object]:
    assignments: dict[str, object] = {}
    for statement in ast.parse(source, filename=filename).body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name) or target.id not in _MANIFEST_FIELDS:
            continue
        assignments[target.id] = ast.literal_eval(statement.value)
    return assignments


def _validated_manifest(
    assignments: dict[str, object],
    *,
    ds_version: str,
) -> dict[str, object]:
    missing = sorted(set(_MANIFEST_FIELDS) - assignments.keys())
    if missing:
        message = "Wheel exact contract manifest is incomplete: " + ", ".join(missing)
        raise ValueError(message)
    bundle_manifest_schema_version = assignments["BUNDLE_MANIFEST_SCHEMA_VERSION"]
    if (
        type(bundle_manifest_schema_version) is not int
        or bundle_manifest_schema_version != 2
    ):
        message = "Wheel exact contract manifest schema is unsupported"
        raise ValueError(message)
    if assignments["DS_VERSION"] != ds_version:
        message = f"Wheel exact contract manifest is not for DS {ds_version}"
        raise ValueError(message)
    if assignments["SOURCE_TAG"] != ds_version:
        message = "Wheel exact contract source tag does not match the DS version"
        raise ValueError(message)

    selection = assignments["SELECTION"]
    if selection not in {"full", "runtime-slice"}:
        message = "Wheel exact contract selection is unsupported"
        raise ValueError(message)
    operations = assignments["SEMANTIC_OPERATIONS"]
    if not isinstance(operations, tuple) or not all(
        isinstance(item, str) and item for item in operations
    ):
        message = "Wheel semantic operations must be a tuple of strings"
        raise ValueError(message)
    operation_count = assignments["OPERATION_COUNT"]
    if not isinstance(operation_count, int) or isinstance(operation_count, bool):
        message = "Wheel operation count must be an integer"
        raise TypeError(message)
    manifest = {
        output_name: assignments[source_name]
        for source_name, output_name in _MANIFEST_FIELDS.items()
    }
    manifest["semantic_operations"] = list(operations)
    return manifest


def _validate_manifest_operations(
    *,
    selection: object,
    operations: tuple[object, ...],
    operation_count: int,
    runtime_operations: frozenset[str],
) -> None:
    if selection == "full":
        if operations or operation_count <= 0:
            message = (
                "Full wheel manifest must declare an empty root set and operations"
            )
            raise ValueError(message)
    elif operation_count < 0 or not runtime_operations >= _REQUIRED_READ_OPERATIONS:
        message = "Runtime-slice manifest does not close the exact read operations"
        raise ValueError(message)


def load_exact_read_gate_config(
    environment: Mapping[str, str],
) -> ExactReadRuntimeConfig:
    """Load and cross-check all explicit generic exact-read gate inputs."""
    missing = [
        name
        for name in _RUNTIME_ENV_NAMES.values()
        if not environment.get(name, "").strip()
    ]
    if missing:
        message = "Missing exact-profile read-gate inputs: " + ", ".join(missing)
        raise ValueError(message)

    ds_version = environment[_RUNTIME_ENV_NAMES["version"]].strip()
    paths = {
        key: _required_file(environment[name], label=name)
        for key, name in _RUNTIME_ENV_NAMES.items()
        if key not in {"version", "evidence"}
    }
    env_file = paths["env_file"]
    executable = paths["executable"]
    python = paths["python"]
    wheel = paths["wheel"]
    if executable.parent != python.parent:
        message = "Exact read dsctl and Python must come from the same virtualenv"
        raise ValueError(message)
    if wheel.suffix != ".whl":
        message = "Exact-profile read wheel input must end in .whl"
        raise ValueError(message)
    profile = _read_profile_values(env_file)
    if profile.get("DS_VERSION") != ds_version:
        message = "Exact-profile read DS_VERSION does not match the requested version"
        raise ValueError(message)

    attestation_key = paths["attestation_key_file"].read_bytes().strip()
    if len(attestation_key) < 32:
        message = "Exact-profile read attestation key must contain at least 32 bytes"
        raise ValueError(message)
    cluster = _load_cluster_identity(
        paths["cluster_manifest"],
        ds_version=ds_version,
        env_file=env_file,
        attestation_key=attestation_key,
    )
    fixture = _load_read_fixture(
        paths["fixture_manifest"],
        ds_version=ds_version,
        cluster=cluster,
    )
    # Reuse the scenario's identity-epoch validation at the input boundary.
    ExactReadGateConfig(
        ds_version=ds_version,
        family="pending-installed-inspection",
        support_level="experimental",
        tested=False,
        attestation_key=attestation_key,
        cluster=cluster,
        fixture=fixture,
    )
    return ExactReadRuntimeConfig(
        ds_version=ds_version,
        env_file=env_file,
        executable=executable,
        python=python,
        wheel=wheel,
        cluster_manifest=paths["cluster_manifest"],
        fixture_manifest=paths["fixture_manifest"],
        evidence_path=Path(environment[_RUNTIME_ENV_NAMES["evidence"]]).expanduser(),
        attestation_key=attestation_key,
        cluster=cluster,
        fixture=fixture,
    )


def inspect_installed_read(
    config: ExactReadRuntimeConfig,
) -> InstalledReadAttestation:
    """Cross-check the installed profile against its immutable wheel archive."""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [
            str(config.python),
            "-I",
            "-c",
            INSTALLED_READ_PROBE_CODE,
            config.ds_version,
            json.dumps(EXACT_PROFILE_READ_RECIPES),
        ],
        capture_output=True,
        check=False,
        cwd=config.python.parent,
        env=environment,
        text=True,
        timeout=30.0,
    )
    if completed.returncode != 0:
        message = f"Could not inspect the isolated DS {config.ds_version} wheel"
        raise AssertionError(message)
    payload = _required_mapping(
        json.loads(completed.stdout),
        label="installed read attestation",
    )
    _require_exact_keys(
        payload,
        {
            "contract_version",
            "action_verifications",
            "distribution_version",
            "family",
            "manifest",
            "module_file",
            "profile_fingerprints",
            "profile_source",
            "read_recipes",
            "server_version",
            "support_level",
            "tested",
        },
        label="installed read attestation",
    )
    module_file = Path(
        _required_text(payload.get("module_file"), label="installed module_file")
    ).resolve()
    if not module_file.is_relative_to(config.python.parent.parent.resolve()):
        message = "Installed dsctl did not import from the read-gate virtualenv"
        raise AssertionError(message)
    if payload.get("server_version") != config.ds_version:
        message = "Installed profile server version does not match the gate"
        raise AssertionError(message)

    archive_manifest = load_exact_manifest(config.wheel, ds_version=config.ds_version)
    installed_manifest = _required_mapping(
        payload.get("manifest"),
        label="installed manifest",
    )
    if installed_manifest != archive_manifest:
        message = "Installed manifest does not match the snapshotted wheel"
        raise AssertionError(message)
    _validate_profile_source(
        payload.get("profile_source"),
        manifest=archive_manifest,
    )
    fingerprints = _load_fingerprints(
        payload.get("profile_fingerprints"),
        label="profile fingerprints",
    )
    action_verifications = _load_action_verifications(
        payload.get("action_verifications"),
        label="profile action verifications",
    )
    if set(action_verifications.values()) != {"live_smoke"}:
        message = "Installed exact read actions are not all live_smoke"
        raise AssertionError(message)
    recipes = _load_read_recipes(payload.get("read_recipes"))
    attestation = InstalledReadAttestation(
        distribution_version=_required_text(
            payload.get("distribution_version"),
            label="distribution_version",
        ),
        contract_version=_required_text(
            payload.get("contract_version"),
            label="contract_version",
        ),
        family=_required_text(payload.get("family"), label="family"),
        support_level=_required_text(
            payload.get("support_level"),
            label="support_level",
        ),
        tested=_required_bool(payload.get("tested"), label="tested"),
        profile_fingerprints=fingerprints,
        action_verifications=action_verifications,
        read_recipes=recipes,
    )
    _validate_installed_profile(attestation, ds_version=config.ds_version)
    return attestation


def _validate_profile_source(
    value: object,
    *,
    manifest: Mapping[str, object],
) -> None:
    source = _required_mapping(value, label="profile source")
    _require_exact_keys(source, {"commit", "tag", "tree"}, label="profile source")
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


def _load_read_recipes(value: object) -> tuple[dict[str, object], ...]:
    raw_recipes = _required_list(value, label="read recipes")
    recipes: list[dict[str, object]] = []
    for raw_recipe, (expected_action, expected_operation) in zip(
        raw_recipes,
        EXACT_PROFILE_READ_RECIPES,
        strict=True,
    ):
        recipe = _required_mapping(raw_recipe, label="read recipe")
        _require_exact_keys(
            recipe,
            {"action", "build_status", "fingerprints", "semantic_operation"},
            label="read recipe",
        )
        if (
            recipe.get("action") != expected_action
            or recipe.get("semantic_operation") != expected_operation
            or recipe.get("build_status") != "accepted"
        ):
            message = "Installed read recipe does not match the accepted read bundle"
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
    if len(recipes) != len(EXACT_PROFILE_READ_RECIPES):
        message = "Installed profile did not expose all four exact read recipes"
        raise AssertionError(message)
    return tuple(recipes)


def _load_action_verifications(value: object, *, label: str) -> dict[str, str]:
    raw = _required_mapping(value, label=label)
    _require_exact_keys(raw, set(EXACT_PROFILE_READ_ACTIONS), label=label)
    allowed = {"static", "contract_tested", "live_smoke", "live_full"}
    normalized: dict[str, str] = {}
    for action in EXACT_PROFILE_READ_ACTIONS:
        verification = _required_text(
            raw.get(action),
            label=f"{label} {action}",
        )
        if verification not in allowed:
            message = f"{label} {action} is not a recognized verification level"
            raise ValueError(message)
        normalized[action] = verification
    return normalized


def _validate_installed_profile(
    attestation: InstalledReadAttestation,
    *,
    ds_version: str,
) -> None:
    if attestation.contract_version != ds_version:
        message = f"Installed wheel does not expose the exact DS {ds_version} profile"
        raise AssertionError(message)
    if attestation.support_level not in {"experimental", "full", "legacy_core"}:
        message = "Installed profile support level is not recognized"
        raise AssertionError(message)
    if (attestation.support_level in {"full", "legacy_core"}) != attestation.tested:
        message = "Installed profile promotion metadata is inconsistent"
        raise AssertionError(message)


def execute_exact_read_gate(
    config: ExactReadGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
) -> ExactReadResult:
    """Execute the fixed project/workflow read bundle through installed dsctl."""
    trace: list[OperationTraceEntry] = []
    version_data = _verify_version(config, invoke=invoke, trace=trace)
    _verify_capabilities(invoke=invoke, trace=trace)
    _verify_doctor(config, invoke=invoke, trace=trace)
    _verify_project_reads(config, invoke=invoke, trace=trace)
    _verify_workflow_reads(config, invoke=invoke, trace=trace)
    return ExactReadResult(
        version_data=version_data,
        operation_trace=tuple(trace),
        effects=ExactReadEffects(),
    )


def write_exact_read_evidence(
    config: ExactReadRuntimeConfig,
    *,
    installation: InstalledReadAttestation,
    result: ExactReadResult,
    recorded_at: datetime | None = None,
) -> Path:
    """Atomically write one sanitized, wheel-bound read-only receipt."""
    timestamp = recorded_at or datetime.now(tz=UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        message = "Exact-profile read evidence timestamp must include a timezone"
        raise ValueError(message)
    if config.evidence_path.exists():
        message = (
            f"Refusing to overwrite existing live evidence: {config.evidence_path}"
        )
        raise FileExistsError(message)

    read_bundle_base: dict[str, object] = {
        "actions": list(EXACT_PROFILE_READ_ACTIONS),
        "action_verifications": installation.action_verifications,
        "recipes": list(installation.read_recipes),
    }
    read_bundle = {
        **read_bundle_base,
        "digest": _read_bundle_digest(read_bundle_base),
    }
    version_data = result.version_data
    contract = load_exact_manifest(config.wheel, ds_version=config.ds_version)
    evidence = {
        "schema_version": CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION,
        "sanitization_schema_version": 1,
        "gate": "exact-profile-read",
        "status": "passed",
        "recorded_at": timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "runner": {
            "artifact": "installed-wheel-console-script",
            "cli_version": installation.distribution_version,
            "wheel_filename": config.wheel.name,
            "wheel_sha256": f"sha256:{_sha256(config.wheel)}",
        },
        "dolphinscheduler": {
            "release": config.cluster.ds_version,
            "image_ref": config.cluster.image_ref,
            "image_id": config.cluster.image_id,
            "image_source": config.cluster.image_source,
            "image_observed_at": config.cluster.image_observed_at,
            "api_target_hmac_sha256": config.cluster.api_target_hmac_sha256,
            "principal_hmac_sha256": config.cluster.principal_hmac_sha256,
            "persona": config.cluster.persona,
        },
        "profile": {
            "ds": version_data["ds"],
            "selected_ds_version": version_data["selected_ds_version"],
            "contract_version": version_data["contract_version"],
            "family": version_data["family"],
            "support_level": version_data["support_level"],
            "tested": installation.tested,
            "fingerprints": installation.profile_fingerprints,
        },
        "contract": contract,
        "read_bundle": read_bundle,
        "fixture": {
            "manifest_sha256": f"sha256:{config.fixture.manifest_sha256}",
            "provisioner": config.fixture.provisioner,
            "project_identity_kind": config.fixture.project_identity.kind,
            "workflow_identity_kind": config.fixture.workflow_identity.kind,
            "scheduled_workflow": True,
            "workflow_release_state": config.fixture.workflow_release_state,
        },
        "operation_trace": [entry.to_data() for entry in result.operation_trace],
        "effects": result.effects.to_data(),
        "secrets_recorded": False,
    }
    if version_data.get("cli") != installation.distribution_version:
        message = "Black-box CLI version does not match installed-wheel attestation"
        raise AssertionError(message)
    validate_exact_profile_read_evidence_payload(
        evidence,
        expected_cli_version=installation.distribution_version,
        expected_ds_version=config.ds_version,
        compiled_semantic_operations=(
            _RUNTIME_OWNERSHIP.load_wheel_runtime_ownership(config.wheel)[
                config.ds_version
            ]
            - frozenset(cast("list[str]", contract["semantic_operations"]))
        ),
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
    try:
        os.link(temporary_path, config.evidence_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return config.evidence_path


def canonical_read_bundle_digest(value: Mapping[str, object]) -> str:
    """Expose the independently implemented receipt digest to gate tests."""
    return _read_bundle_digest(value)


def validate_exact_profile_read_evidence_payload(
    value: object,
    *,
    expected_cli_version: str,
    expected_ds_version: str,
    compiled_semantic_operations: frozenset[str] = frozenset(),
) -> None:
    """Validate a read receipt through the independent release-grade schema."""
    _validate_read_evidence(
        value,
        expected_cli_version=expected_cli_version,
        expected_ds_version=expected_ds_version,
        compiled_semantic_operations=compiled_semantic_operations,
    )


def _reject_protected_values(
    raw: str,
    *,
    config: ExactReadRuntimeConfig,
) -> None:
    profile = _read_profile_values(config.env_file)
    protected_values = [
        profile.get("DS_API_TOKEN", ""),
        profile.get("DS_API_URL", ""),
        _printable_secret(config.attestation_key),
        config.fixture.project_name,
        config.fixture.workflow_name,
        *[
            value
            for value in (
                str(config.fixture.project_identity.value),
                str(config.fixture.workflow_identity.value),
                str(config.fixture.schedule_id),
            )
            if len(value) >= 8
        ],
    ]
    if any(value and value in raw for value in protected_values):
        message = "Exact-profile read evidence contains a protected input value"
        raise ValueError(message)


def _printable_secret(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return ""


def _verify_version(
    config: ExactReadGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> dict[str, object]:
    payload = require_ok_payload(
        invoke(["version"]),
        expected_action="version",
        label=f"exact {config.ds_version} version preflight",
    )
    data = require_mapping(payload.get("data"), label="exact version data")
    expected = {
        "ds": config.ds_version,
        "selected_ds_version": config.ds_version,
        "contract_version": config.ds_version,
        "family": config.family,
        "support_level": config.support_level,
    }
    for key, value in expected.items():
        if data.get(key) != value:
            message = f"version field {key} did not match exact profile"
            raise AssertionError(message)
    cli_version = data.get("cli")
    if not isinstance(cli_version, str) or not cli_version:
        message = "version data must include the installed CLI version"
        raise AssertionError(message)
    _record(
        trace,
        "version",
        argv_shape="version",
        assertions=("selected-contract-and-family-matched",),
    )
    return data


def _verify_capabilities(
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    for action in EXACT_PROFILE_READ_CAPABILITY_ACTIONS:
        payload = require_ok_payload(
            invoke(["capabilities", "--action", action]),
            expected_action="capabilities",
            label=f"read capability {action}",
        )
        data = require_mapping(payload.get("data"), label="capability data")
        capability = require_mapping(
            data.get("capability"),
            label=f"capability {action}",
        )
        if capability.get("action") != action:
            message = f"capability response did not identify {action}"
            raise AssertionError(message)
        if capability.get("availability") != "supported":
            message = f"exact profile does not support required read {action}"
            raise AssertionError(message)
        verification = capability.get("verification")
        if verification != "live_smoke":
            message = f"capability {action} is not attested as live_smoke"
            raise AssertionError(message)
        _record(
            trace,
            "capabilities",
            argv_shape="capabilities --action ACTION",
            assertions=(f"{action}-supported",),
        )


def _verify_doctor(
    config: ExactReadGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    payload = require_ok_payload(
        invoke(["doctor"]),
        expected_action="doctor",
        label=f"exact {config.ds_version} doctor",
    )
    data = require_mapping(payload.get("data"), label="doctor data")
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
    api_status = api.get("status")
    if api_status not in {"ok", "warning"} or current_user.get("status") != "ok":
        message = "doctor did not prove authenticated current-user readiness"
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
        message = "exact read gate requires a GENERAL_USER persona"
        raise AssertionError(message)
    if identity_hmac(user_name, key=config.attestation_key) != (
        config.cluster.principal_hmac_sha256
    ):
        message = "doctor principal did not match the cluster manifest"
        raise AssertionError(message)
    _record(
        trace,
        "doctor",
        argv_shape="doctor",
        assertions=(
            ("api-health-ok" if api_status == "ok" else "api-health-warning-recorded"),
            "current-user-ok",
            "general-user-confirmed",
            "principal-hmac-matched",
        ),
    )


def _verify_project_reads(
    config: ExactReadGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    fixture = config.fixture
    list_payload = require_ok_payload(
        invoke(
            [
                "project",
                "list",
                "--search",
                fixture.project_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="project.list",
        label="exact project list",
    )
    list_data = require_mapping(list_payload.get("data"), label="project list data")
    _require_page_shape(list_data)
    rows = [
        require_mapping(item, label="project row")
        for item in require_list(list_data.get("totalList"), label="project rows")
    ]
    _require_matching_row(
        rows,
        name=fixture.project_name,
        identity=fixture.project_identity,
        label="project list",
    )
    _record(
        trace,
        "project.list",
        argv_shape="project list --search PROJECT_NAME --page-no 1 --page-size 20",
        selector_kind="search",
        assertions=("native-project-identity-matched", "pagination-search-matched"),
    )

    for selector, selector_kind in (
        (fixture.project_name, "project-name"),
        (
            str(fixture.project_identity.value),
            f"project-{fixture.project_identity.kind}",
        ),
    ):
        payload = require_ok_payload(
            invoke(["project", "get", selector]),
            expected_action="project.get",
            label=f"project get by {selector_kind}",
        )
        data = require_mapping(payload.get("data"), label="project get data")
        _require_matching_row(
            [data],
            name=fixture.project_name,
            identity=fixture.project_identity,
            label="project get",
        )
        _record(
            trace,
            "project.get",
            argv_shape="project get PROJECT",
            selector_kind=selector_kind,
            assertions=("native-project-identity-matched",),
        )


def _verify_workflow_reads(
    config: ExactReadGateConfig,
    *,
    invoke: Callable[[list[str]], DsctlCommandResult],
    trace: list[OperationTraceEntry],
) -> None:
    fixture = config.fixture
    scope = ["--project", fixture.project_name]
    list_payload = require_ok_payload(
        invoke(
            [
                "workflow",
                "list",
                *scope,
                "--search",
                fixture.workflow_name,
                "--page-no",
                "1",
                "--page-size",
                "20",
            ]
        ),
        expected_action="workflow.list",
        label="exact workflow list",
    )
    list_data = require_mapping(list_payload.get("data"), label="workflow list data")
    _require_page_shape(list_data)
    rows = [
        require_mapping(item, label="workflow row")
        for item in require_list(list_data.get("totalList"), label="workflow rows")
    ]
    row = _require_matching_row(
        rows,
        name=fixture.workflow_name,
        identity=fixture.workflow_identity,
        label="workflow list",
    )
    if (
        config.ds_version in _WORKFLOW_LIST_SCHEDULE_VERSIONS
        and row.get("scheduleId") != fixture.schedule_id
    ):
        message = "workflow list did not return the attached schedule identity"
        raise AssertionError(message)
    _record(
        trace,
        "workflow.list",
        argv_shape=(
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no 1 --page-size 20"
        ),
        selector_kind="project-name+workflow-search",
        assertions=("native-workflow-identity-matched", "pagination-search-matched"),
    )

    for selector, selector_kind in (
        (fixture.workflow_name, "workflow-name"),
        (
            str(fixture.workflow_identity.value),
            f"workflow-{fixture.workflow_identity.kind}",
        ),
    ):
        payload = require_ok_payload(
            invoke(["workflow", "get", selector, *scope]),
            expected_action="workflow.get",
            label=f"workflow get by {selector_kind}",
        )
        data = require_mapping(payload.get("data"), label="workflow get data")
        _require_matching_row(
            [data],
            name=fixture.workflow_name,
            identity=fixture.workflow_identity,
            label="workflow get",
        )
        if data.get("releaseState") != fixture.workflow_release_state:
            message = "workflow get did not preserve the fixture release state"
            raise AssertionError(message)
        schedule = require_mapping(
            data.get("schedule"),
            label="workflow attached schedule",
        )
        if schedule.get("id") != fixture.schedule_id:
            message = "workflow get did not hydrate the attached schedule"
            raise AssertionError(message)
        _record(
            trace,
            "workflow.get",
            argv_shape="workflow get WORKFLOW --project PROJECT",
            selector_kind=selector_kind,
            assertions=(
                "native-workflow-identity-matched",
                "schedule-hydrated",
                "release-state-matched",
            ),
        )


def _require_page_shape(data: Mapping[str, object]) -> None:
    expected = {"pageNo": 1, "pageSize": 20, "currentPage": 1}
    has_expected_page = all(
        type(data.get(key)) is int and data.get(key) == value
        for key, value in expected.items()
    )
    has_positive_totals = all(
        type(data.get(key)) is int and cast("int", data.get(key)) > 0
        for key in ("total", "totalPage")
    )
    if not has_expected_page or not has_positive_totals:
        message = "read page did not preserve complete positive pagination metadata"
        raise AssertionError(message)


def _require_matching_row(
    rows: list[dict[str, object]],
    *,
    name: str,
    identity: NativeFixtureIdentity,
    label: str,
) -> dict[str, object]:
    matches = [
        row
        for row in rows
        if row.get("name") == name and row.get(identity.kind) == identity.value
    ]
    if len(matches) != 1:
        message = f"{label} did not return exactly one native fixture identity"
        raise AssertionError(message)
    return matches[0]


def _record(
    trace: list[OperationTraceEntry],
    action: str,
    *,
    argv_shape: str,
    assertions: tuple[str, ...],
    selector_kind: str | None = None,
) -> None:
    trace.append(
        OperationTraceEntry(
            sequence=len(trace) + 1,
            argv_shape=argv_shape,
            action=action,
            exit_code=0,
            ok=True,
            assertions=assertions,
            selector_kind=selector_kind,
        )
    )


def identity_hmac(value: str, *, key: bytes) -> str:
    """Bind one low-entropy identity without recording the identity itself."""
    digest = hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"hmac-sha256:{digest}"


def _load_cluster_identity(
    path: Path,
    *,
    ds_version: str,
    env_file: Path,
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
            "image_ref",
            "persona",
            "principal_hmac_sha256",
            "schema_version",
        },
        label="cluster manifest",
    )
    if payload.get("schema_version") != 1 or payload.get("ds_version") != ds_version:
        message = f"Cluster manifest must declare schema 1 and DS {ds_version}"
        raise ValueError(message)
    image_ref = _required_text(payload.get("image_ref"), label="cluster image_ref")
    image_id = _required_text(
        payload.get("image_id"),
        label="cluster image_id",
    )
    _validate_image_identity(
        image_ref=image_ref,
        image_id=image_id,
        ds_version=ds_version,
    )
    profile = _read_profile_values(env_file)
    api_url = _required_text(profile.get("DS_API_URL"), label="profile DS_API_URL")
    expected_target = identity_hmac(api_url.rstrip("/"), key=attestation_key)
    api_target = _required_hmac_sha256(
        payload.get("api_target_hmac_sha256"),
        label="cluster API target HMAC",
    )
    if api_target != expected_target:
        message = "Cluster manifest API target does not match the explicit profile"
        raise ValueError(message)
    image_observed_at = _required_timestamp(
        payload.get("image_observed_at"),
        label="cluster image_observed_at",
    )
    _validate_observation_window(image_observed_at, reference=datetime.now(tz=UTC))
    persona = _required_text(payload.get("persona"), label="cluster persona")
    if persona != _EXPECTED_PERSONA:
        message = f"Cluster manifest persona must be {_EXPECTED_PERSONA!r}"
        raise ValueError(message)
    return ClusterIdentity(
        ds_version=ds_version,
        image_ref=image_ref,
        image_id=image_id,
        image_source=_required_text(
            payload.get("image_source"),
            label="cluster image_source",
        ),
        image_observed_at=image_observed_at,
        api_target_hmac_sha256=api_target,
        principal_hmac_sha256=_required_hmac_sha256(
            payload.get("principal_hmac_sha256"),
            label="cluster principal HMAC",
        ),
        persona=persona,
    )


def _load_read_fixture(
    path: Path,
    *,
    ds_version: str,
    cluster: ClusterIdentity,
) -> ExactReadFixture:
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
        message = "Fixture and cluster manifests must identify the same image"
        raise ValueError(message)
    project = _required_mapping(payload.get("project"), label="fixture project")
    workflow = _required_mapping(payload.get("workflow"), label="fixture workflow")
    _require_exact_keys(
        project,
        {"identity", "name"},
        label="fixture project",
    )
    _require_exact_keys(
        workflow,
        {
            "identity",
            "name",
            "release_state",
            "schedule_id",
            "scheduled",
        },
        label="fixture workflow",
    )
    if workflow.get("scheduled") is not True:
        message = "Exact-profile read fixture must contain an attached schedule"
        raise ValueError(message)
    return ExactReadFixture(
        project_name=_required_text(project.get("name"), label="project name"),
        project_identity=_load_native_identity(
            project.get("identity"),
            label="project identity",
        ),
        workflow_name=_required_text(workflow.get("name"), label="workflow name"),
        workflow_identity=_load_native_identity(
            workflow.get("identity"),
            label="workflow identity",
        ),
        schedule_id=_required_positive_int(
            workflow.get("schedule_id"),
            label="workflow schedule_id",
        ),
        workflow_release_state=_required_text(
            workflow.get("release_state"),
            label="workflow release_state",
        ),
        provisioner=_required_text(
            payload.get("provisioner"),
            label="fixture provisioner",
        ),
        manifest_sha256=_sha256(path),
    )


def _load_native_identity(value: object, *, label: str) -> NativeFixtureIdentity:
    identity = _required_mapping(value, label=label)
    _require_exact_keys(identity, {"kind", "value"}, label=label)
    kind = identity.get("kind")
    if kind not in {"id", "code"}:
        message = f"{label} kind must be id or code"
        raise ValueError(message)
    return NativeFixtureIdentity(
        kind=cast("IdentityKind", kind),
        value=_required_positive_int(identity.get("value"), label=f"{label} value"),
    )


def _validate_image_identity(
    *,
    image_ref: str,
    image_id: str,
    ds_version: str,
) -> None:
    tagged_ref, digest_separator, published_digest = image_ref.partition("@")
    repository, tag_separator, tag = tagged_ref.rpartition(":")
    if (
        not tag_separator
        or not repository
        or tag != ds_version
        or any(character.isspace() for character in repository)
    ):
        message = f"Cluster image reference must identify DolphinScheduler {ds_version}"
        raise ValueError(message)
    if digest_separator and _SHA256.fullmatch(published_digest) is None:
        message = "Cluster image reference contains an invalid published digest"
        raise ValueError(message)
    if _SHA256.fullmatch(image_id) is None:
        message = "Cluster image ID must be an immutable SHA-256 identity"
        raise ValueError(message)


def _read_profile_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", maxsplit=1)
        values[key.strip()] = _strip_optional_quotes(value.strip())
    return values


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_json_object(path: Path, *, label: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _required_mapping(payload, label=label)


def _required_file(value: str, *, label: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        message = f"{label} does not point to a file: {path}"
        raise ValueError(message)
    return path


def _required_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        message = f"{label} must be an object with string keys"
        raise TypeError(message)
    return value


def _required_list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        message = f"{label} must be a list"
        raise TypeError(message)
    return value


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    label: str,
) -> None:
    actual = set(value)
    if actual != expected:
        message = (
            f"{label} keys differ: expected {sorted(expected)}, got {sorted(actual)}"
        )
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


def _validate_observation_window(value: str, *, reference: datetime) -> None:
    observed_at = datetime.fromisoformat(value).astimezone(UTC)
    normalized_reference = reference.astimezone(UTC)
    if observed_at > normalized_reference + _MAX_CLOCK_SKEW:
        message = "image_observed_at is implausibly later than the gate run"
        raise ValueError(message)
    if normalized_reference - observed_at > _MAX_OBSERVATION_AGE:
        message = "image_observed_at is too stale for exact-profile read evidence"
        raise ValueError(message)


def _required_positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} must be a positive integer"
        raise TypeError(message)
    return value


def _required_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        message = f"{label} must be a boolean"
        raise TypeError(message)
    return value


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
