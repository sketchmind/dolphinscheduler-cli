from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import zipfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dsctl.generated.conformance_bundles import (
    CONFORMANCE_BUNDLE_DATA as _TRACKED_CONFORMANCE_DATA,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    TARGET_TASK_DEFINITION_CLEANUP_VERSIONS as _PRIVATE_TASK_CLEANUP_VERSIONS,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    TASK_DEFINITION_CLEANUP_PROFILE_DATA as _TRACKED_TASK_CLEANUP_DATA,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION as _PRIVATE_TASK_CLEANUP_ROOT,
)
from live_gate import installed_wheel as gate_harness
from live_gate.runtime_ownership import load_wheel_runtime_ownership
from private_manifest_io import absolute_path, load_private_json_object
from runtime_bundle_manifest import runtime_bundle_versions

ROOT = Path(__file__).resolve().parents[1]
_PYTEST_ENTRY_ARGUMENT = "tests/live/test_exact_conformance_bundle.py"
_PYTEST_ENTRY_PATH = ROOT / _PYTEST_ENTRY_ARGUMENT
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RECOVERY_RUN_ID = re.compile(r"[a-z0-9]{16,32}\Z")
_CROSS_PROCESS_TASK_RECOVERY_VERSIONS = CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS
_PROFILE_FINGERPRINT_KEYS = frozenset(
    {"source", "effective_wire", "consumed_projection", "preservation"}
)
_MANIFEST_FIELDS = frozenset(
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
    }
)
_MANIFEST_ASSIGNMENTS = {
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
_WHEEL_CONFORMANCE_PATH = "dsctl/generated/conformance_bundles.py"
_WHEEL_TASK_CLEANUP_PROFILE_PATH = "dsctl/generated/task_definition_cleanup_profiles.py"
_UNASSESSED_AXES = ["facets", "scenarios", "freshness", "live_evidence"]

if TYPE_CHECKING:
    from collections.abc import Callable

    EvidenceValidator = Callable[..., object]
    InstallWheel = Callable[..., gate_harness.InstalledWheel]
    ProcessRunner = Callable[..., subprocess.CompletedProcess[str]]


INSTALLED_CONFORMANCE_PROBE_CODE = r"""
import hashlib
import json
import sys
from importlib import import_module
from importlib.metadata import version as distribution_version
from pathlib import Path

import dsctl
from dsctl.generated.conformance_bundles import CONFORMANCE_BUNDLE_DATA
from dsctl.generated.version_profiles import VERSION_PROFILES
from dsctl.upstream import get_version_support

request = json.loads(sys.argv[1])
if set(request) != {
    "bundle", "ds_version", "task_cleanup_digest", "runtime_operations"
}:
    raise AssertionError("installed conformance probe request fields differ")
bundle_name = request["bundle"]
ds_version = request["ds_version"]
task_cleanup_digest = request["task_cleanup_digest"]
runtime_operations = request["runtime_operations"]
if (
    not isinstance(bundle_name, str)
    or not isinstance(ds_version, str)
    or not isinstance(task_cleanup_digest, str)
):
    raise AssertionError("installed conformance probe request is not textual")
if (
    not isinstance(runtime_operations, list)
    or not runtime_operations
    or not all(isinstance(item, str) and item for item in runtime_operations)
    or len(runtime_operations) != len(set(runtime_operations))
):
    raise AssertionError("verified runtime operations are invalid")

if (
    CONFORMANCE_BUNDLE_DATA.get("schema_version") != 1
    or CONFORMANCE_BUNDLE_DATA.get("claim") != "static-action-closure-only"
    or CONFORMANCE_BUNDLE_DATA.get("promotion_claimed") is not False
):
    raise AssertionError("installed conformance assessment header differs")
bundles = [
    item
    for item in CONFORMANCE_BUNDLE_DATA["bundles"]
    if item.get("name") == bundle_name
]
if len(bundles) != 1:
    raise AssertionError("installed conformance bundle is missing or duplicated")
bundle = bundles[0]
coordinates = [
    item for item in bundle["versions"] if item.get("version") == ds_version
]
if len(coordinates) != 1:
    raise AssertionError("installed conformance coordinate is missing or duplicated")
coordinate = coordinates[0]
if coordinate.get("status") != "ready" or coordinate.get("blockers") != []:
    raise AssertionError("installed conformance coordinate is not static-ready")

required_actions = bundle.get("required_actions")
if (
    not isinstance(required_actions, list)
    or not required_actions
    or not all(isinstance(item, str) and item for item in required_actions)
    or len(set(required_actions)) != len(required_actions)
):
    raise AssertionError("installed conformance required actions differ")
profile = VERSION_PROFILES[ds_version]
support = get_version_support(ds_version)
for field in (
    "server_version",
    "contract_version",
    "family",
    "support_level",
    "tested",
):
    if getattr(support, field) != profile[field]:
        raise AssertionError("installed profile and support metadata differ")
manifest_module = import_module(
    "dsctl.generated.versions.ds_" + ds_version.replace(".", "_") + "._manifest"
)
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
cleanup_profiles = import_module(
    "dsctl.generated.task_definition_cleanup_profiles"
)
cleanup_data = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA
cleanup_payload = json.dumps(
    cleanup_data,
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
).encode("utf-8")
observed_cleanup_digest = "sha256:" + hashlib.sha256(cleanup_payload).hexdigest()
if observed_cleanup_digest != task_cleanup_digest:
    raise AssertionError("installed private cleanup profile differs")
cleanup_versions = tuple(cleanup_data["target_versions"])
cleanup_root = cleanup_data["semantic_operation"]
if (
    not isinstance(cleanup_root, str)
    or not cleanup_root
    or len(cleanup_versions) != len(set(cleanup_versions))
    or not set(cleanup_data["full_core_reconciliation_versions"]).issubset(
        cleanup_versions
    )
    or not set(cleanup_data["full_core_versions"]).issubset(cleanup_versions)
    or not set(cleanup_data["cross_process_recovery_versions"]).issubset(
        cleanup_versions
    )
):
    raise AssertionError("installed private cleanup profile header differs")
if (cleanup_root in runtime_operations) != (
    ds_version in cleanup_versions
):
    raise AssertionError("installed private cleanup semantic root differs")
if ds_version in cleanup_versions:
    if (
        cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_SCHEMA_VERSION
        != cleanup_data["schema_version"]
        or tuple(cleanup_profiles.TARGET_TASK_DEFINITION_CLEANUP_VERSIONS)
        != cleanup_versions
        or tuple(
            cleanup_profiles.FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
        )
        != tuple(cleanup_data["full_core_reconciliation_versions"])
        or tuple(cleanup_profiles.FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS)
        != tuple(cleanup_data["full_core_versions"])
        or tuple(
            cleanup_profiles.CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS
        )
        != tuple(cleanup_data["cross_process_recovery_versions"])
        or cleanup_profiles.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
        != cleanup_root
        or cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES
        != cleanup_data["profiles"]
        or ds_version not in cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES
    ):
        raise AssertionError("installed private cleanup profile differs")
    package_root = Path(dsctl.__file__).resolve().parent
    for module_name in (
        "dsctl.release_gate.task_definition_cleanup",
        "dsctl.release_gate.task_definition_cleanup_runner",
        "dsctl.upstream.task_definition_cleanup",
    ):
        module = import_module(module_name)
        module_file = getattr(module, "__file__", None)
        if (
            not isinstance(module_file, str)
            or not Path(module_file).resolve().is_relative_to(package_root)
        ):
            raise AssertionError("installed private cleanup module escaped wheel")

actions = []
for action in required_actions:
    action_fact = profile["actions"][action]
    catalog_fact = support.catalog.entries[action]
    if action_fact["availability"] != catalog_fact.availability.value:
        raise AssertionError("installed profile and catalog availability differ")
    if action_fact["verification"] != catalog_fact.verification.value:
        raise AssertionError("installed profile and catalog verification differ")
    decisions = [
        decision
        for decision in profile["build_decisions"].values()
        if decision["stable_action"] == action
    ]
    if len(decisions) != 1:
        raise AssertionError("installed action decision is missing or duplicated")
    decision = decisions[0]
    if (
        action_fact["availability"] != "supported"
        or decision["build_status"] != "accepted"
        or decision["semantic_operation"] not in runtime_operations
    ):
        raise AssertionError("installed required action is not executable")
    actions.append({
        "action": action,
        "availability": action_fact["availability"],
        "execution_mode": action_fact["execution_mode"],
        "verification": action_fact["verification"],
        "semantic_operation": decision["semantic_operation"],
        "build_status": decision["build_status"],
        "fingerprints": decision["fingerprints"],
    })

print(json.dumps({
    "distribution_version": distribution_version("dolphinscheduler-cli"),
    "module_file": dsctl.__file__,
    "ds_version": ds_version,
    "bundle": bundle_name,
    "coordinate_status": coordinate["status"],
    "blockers": coordinate["blockers"],
    "required_actions": required_actions,
    "bundle_digest": bundle["bundle_digest"],
    "catalog_digest": CONFORMANCE_BUNDLE_DATA["catalog_digest"],
    "assessment_digest": CONFORMANCE_BUNDLE_DATA["assessment_digest"],
    "profile": {
        "server_version": profile["server_version"],
        "contract_version": profile["contract_version"],
        "family": profile["family"],
        "support_level": profile["support_level"],
        "tested": profile["tested"],
        "source": profile["source"],
        "fingerprints": profile["fingerprints"],
    },
    "actions": actions,
    "manifest": manifest,
}))
"""


@dataclass(frozen=True)
class ConformanceGateInputs(gate_harness.GateFiles):
    """Immutable inputs for one exact-version named conformance gate."""

    ds_version: str
    bundle: str
    recovery_run_id_file: Path | None = None
    run_id_output_file: Path | None = None


@dataclass(frozen=True)
class TrackedConformanceCoordinate:
    """Static coordinate identity from the tracked generated assessment."""

    bundle: str
    bundle_digest: str
    catalog_digest: str
    assessment_digest: str
    required_actions: tuple[str, ...]
    support_level: str
    tested: bool


@dataclass(frozen=True)
class InstalledConformanceAttestation:
    """Exact named-bundle identity read from one isolated wheel installation."""

    distribution_version: str
    ds_version: str
    bundle: str
    bundle_digest: str
    catalog_digest: str
    assessment_digest: str
    required_actions: tuple[str, ...]
    profile: dict[str, object]
    actions: tuple[dict[str, object], ...]
    manifest: dict[str, object]


def run_gate(
    inputs: ConformanceGateInputs,
    *,
    process_runner: ProcessRunner = subprocess.run,
    install_wheel: InstallWheel | None = None,
    evidence_validator: EvidenceValidator | None = None,
) -> int:
    """Run one installed-wheel named conformance gate."""
    coordinate = _validate_inputs(inputs)
    recovery_run_id = _validated_recovery_run_id(inputs)
    run_id_output_file = _validated_run_id_output_file(inputs)
    if not _PYTEST_ENTRY_PATH.is_file():
        message = (
            f"Conformance gate fixed pytest entry does not exist: {_PYTEST_ENTRY_PATH}"
        )
        raise FileNotFoundError(message)
    installer = install_wheel or gate_harness.install_wheel
    validator = evidence_validator or _default_evidence_validator
    prefix = f"dsctl-conformance-{inputs.ds_version.replace('.', '-')}-"
    with gate_harness.private_workspace(prefix=prefix) as workspace:
        snapshot = _snapshot_inputs(
            inputs,
            workspace / "inputs",
            recovery_run_id=recovery_run_id,
        )
        expected_dsctl = gate_harness.venv_binary(workspace / "venv", "dsctl")
        installed = installer(
            snapshot.wheel,
            workspace,
            missing_executable_message=(
                f"Installed wheel did not create {expected_dsctl}"
            ),
        )
        attestation = inspect_installed_conformance(
            snapshot,
            installed=installed,
            coordinate=coordinate,
            process_runner=process_runner,
        )
        attestation_path = workspace / "installed-attestation.json"
        _write_private_json(attestation_path, asdict(attestation))
        candidate_evidence = workspace / "candidate-evidence.json"
        if run_id_output_file is not None:
            _write_new_private_run_id(run_id_output_file)
        environment = _conformance_environment(
            snapshot,
            executable=installed.executable,
            python=installed.python,
            attestation=attestation_path,
            candidate_evidence=candidate_evidence,
            run_id_output_file=run_id_output_file,
        )
        completed = process_runner(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                _PYTEST_ENTRY_ARGUMENT,
                "-m",
                "live_exact_conformance",
            ],
            check=False,
            cwd=ROOT,
            env=environment,
        )
        if completed.returncode != 0:
            candidate_evidence.unlink(missing_ok=True)
            return completed.returncode
        if recovery_run_id is not None:
            if candidate_evidence.exists():
                candidate_evidence.unlink(missing_ok=True)
                message = "Conformance recovery must not create candidate evidence"
                raise RuntimeError(message)
            return 0
        if not candidate_evidence.is_file():
            message = "Passing conformance test did not produce candidate evidence"
            raise RuntimeError(message)
        receipt = _load_strict_candidate_receipt(candidate_evidence)
        canonical_receipt = _canonical_json(receipt)
        validator(
            receipt,
            expected_ds_version=inputs.ds_version,
            expected_bundle=inputs.bundle,
            expected_wheel_filename=snapshot.wheel.name,
            expected_wheel_sha256=f"sha256:{_file_sha256(snapshot.wheel)}",
        )
        validated_candidate = workspace / "validated-candidate-evidence.json"
        _write_private_text(validated_candidate, canonical_receipt)
        gate_harness.publish_passing_evidence(
            validated_candidate,
            inputs.evidence,
            invalid_candidate_message=(
                "Conformance candidate evidence is not a passing receipt"
            ),
        )
        return 0


def inspect_installed_conformance(
    inputs: ConformanceGateInputs,
    *,
    installed: gate_harness.InstalledWheel,
    coordinate: TrackedConformanceCoordinate,
    process_runner: ProcessRunner = subprocess.run,
) -> InstalledConformanceAttestation:
    """Inspect one requested coordinate through the installed wheel only."""
    archive_conformance = _load_wheel_conformance_data(inputs.wheel)
    if archive_conformance != _TRACKED_CONFORMANCE_DATA:
        message = "Candidate wheel archive conformance differs from tracked assessment"
        raise AssertionError(message)
    archive_cleanup = _load_wheel_task_cleanup_profile_data(inputs.wheel)
    if archive_cleanup != _TRACKED_TASK_CLEANUP_DATA:
        message = (
            "Candidate wheel private cleanup profile differs from tracked "
            "generated truth"
        )
        raise AssertionError(message)
    archive_manifest = _load_wheel_manifest(inputs.wheel, inputs.ds_version)
    runtime_operations = load_wheel_runtime_ownership(inputs.wheel)[inputs.ds_version]
    environment = gate_harness.isolated_environment()
    completed = process_runner(
        [
            str(installed.python),
            "-I",
            "-c",
            INSTALLED_CONFORMANCE_PROBE_CODE,
            json.dumps(
                {
                    "bundle": inputs.bundle,
                    "ds_version": inputs.ds_version,
                    "runtime_operations": sorted(runtime_operations),
                    "task_cleanup_digest": _canonical_digest(
                        _TRACKED_TASK_CLEANUP_DATA
                    ),
                },
                sort_keys=True,
            ),
        ],
        capture_output=True,
        check=False,
        cwd=installed.python.parent,
        env=environment,
        text=True,
        timeout=30.0,
    )
    if completed.returncode != 0:
        message = "Could not inspect the isolated conformance wheel"
        raise AssertionError(message)
    try:
        payload: object = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        message = "Installed conformance probe did not return JSON"
        raise ValueError(message) from error
    attestation = _validate_installed_attestation(
        payload,
        inputs=inputs,
        installed=installed,
        coordinate=coordinate,
        runtime_operations=runtime_operations,
    )
    if attestation.manifest != archive_manifest:
        message = "Installed runtime manifest differs from candidate wheel archive"
        raise AssertionError(message)
    return attestation


def _validate_inputs(
    inputs: ConformanceGateInputs,
) -> TrackedConformanceCoordinate:
    coordinate = _require_tracked_static_ready_coordinate(inputs)
    gate_harness.validate_gate_files(
        inputs,
        messages=gate_harness.GateValidationMessages(
            missing_template="Conformance gate {label} does not exist: {path}",
            wheel="Conformance gate requires a wheel artifact",
            profile_private="Conformance gate profile must be private",
            attestation_private="Conformance gate attestation key must be private",
            overwrite_template="Refusing to overwrite conformance evidence: {path}",
        ),
    )
    profile_version = gate_harness.read_env_file(inputs.env_file).get("DS_VERSION")
    if profile_version != inputs.ds_version:
        displayed_version = profile_version or "<missing>"
        message = (
            f"Conformance profile DS_VERSION {displayed_version} does not match "
            f"requested {inputs.ds_version}"
        )
        raise ValueError(message)
    return coordinate


def _validated_recovery_run_id(inputs: ConformanceGateInputs) -> str | None:
    recovery_file = inputs.recovery_run_id_file
    if recovery_file is None:
        return None
    if (
        inputs.bundle != "full_core/v1"
        or inputs.ds_version not in _CROSS_PROCESS_TASK_RECOVERY_VERSIONS
    ):
        supported = ", ".join(
            f"{version}/full_core/v1"
            for version in _CROSS_PROCESS_TASK_RECOVERY_VERSIONS
        )
        message = f"Conformance recovery is only supported for {supported}"
        raise ValueError(message)
    payload = load_private_json_object(
        recovery_file,
        label="Conformance recovery run-id file",
    )
    if set(payload) != {"schema_version", "run_id"}:
        message = "Conformance recovery run-id file fields differ"
        raise ValueError(message)
    if payload.get("schema_version") != 1:
        message = "Conformance recovery run-id schema differs"
        raise ValueError(message)
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or _RECOVERY_RUN_ID.fullmatch(run_id) is None:
        message = "Conformance recovery run_id is not 16-32 lowercase safe characters"
        raise ValueError(message)
    return run_id


def _validated_run_id_output_file(inputs: ConformanceGateInputs) -> Path | None:
    path = inputs.run_id_output_file
    if path is None:
        return None
    if inputs.recovery_run_id_file is not None:
        message = "Conformance run-id output and recovery are mutually exclusive"
        raise ValueError(message)
    if inputs.bundle != "full_core/v1":
        message = "Conformance run-id output requires full_core/v1"
        raise ValueError(message)
    if path.name == inputs.evidence.name and (
        path.parent.resolve() == inputs.evidence.parent.resolve()
    ):
        message = "Conformance run-id output must differ from evidence"
        raise ValueError(message)
    os.close(_open_run_id_output_parent(path, check_new=True))
    return path


def _open_run_id_output_parent(path: Path, *, check_new: bool) -> int:
    parent = path.parent
    status = parent.lstat()
    if not stat.S_ISDIR(status.st_mode):
        message = "Conformance run-id output parent must be owner-private"
        raise PermissionError(message)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(parent, flags)
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or opened.st_uid != os.geteuid()
        or opened.st_mode & 0o077
        or (opened.st_dev, opened.st_ino) != (status.st_dev, status.st_ino)
    ):
        os.close(descriptor)
        message = "Conformance run-id output parent must be owner-private"
        raise PermissionError(message)
    if check_new:
        try:
            os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            os.close(descriptor)
            message = "Refusing to overwrite conformance run-id output"
            raise FileExistsError(message)
    return descriptor


def _write_new_private_run_id(path: Path) -> None:
    parent = _open_run_id_output_parent(path, check_new=True)
    try:
        descriptor = os.open(
            path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(
                _canonical_json({"schema_version": 1, "run_id": secrets.token_hex(8)})
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(parent)
    finally:
        os.close(parent)


def _require_tracked_static_ready_coordinate(
    inputs: ConformanceGateInputs,
    *,
    assessment: object | None = None,
) -> TrackedConformanceCoordinate:
    if assessment is None:
        assessment = _TRACKED_CONFORMANCE_DATA
    if not isinstance(assessment, Mapping):
        message = "Tracked conformance assessment must be an object"
        raise TypeError(message)
    _validate_tracked_assessment_header(assessment)
    bundles = assessment.get("bundles")
    if not isinstance(bundles, list) or not bundles:
        message = "Tracked conformance assessment requires named bundles"
        raise ValueError(message)
    matches = [
        item
        for item in bundles
        if isinstance(item, Mapping) and item.get("name") == inputs.bundle
    ]
    if len(matches) != 1:
        message = f"Unknown conformance bundle: {inputs.bundle!r}"
        raise ValueError(message)
    bundle = matches[0]
    coordinate = _tracked_version_coordinate(bundle, inputs=inputs)
    required_actions = _required_text_list(
        bundle.get("required_actions"),
        label=f"Tracked conformance bundle {inputs.bundle} required_actions",
    )
    support_level, tested = _tracked_support_metadata(coordinate)
    return TrackedConformanceCoordinate(
        bundle=inputs.bundle,
        bundle_digest=_required_digest(
            bundle.get("bundle_digest"),
            label=f"Tracked conformance bundle {inputs.bundle} digest",
        ),
        catalog_digest=_required_digest(
            assessment.get("catalog_digest"),
            label="Tracked conformance catalog digest",
        ),
        assessment_digest=_required_digest(
            assessment.get("assessment_digest"),
            label="Tracked conformance assessment digest",
        ),
        required_actions=required_actions,
        support_level=support_level,
        tested=tested,
    )


def _validate_tracked_assessment_header(assessment: Mapping[object, object]) -> None:
    if (
        assessment.get("schema_version") != 1
        or assessment.get("kind") != "dsctl-conformance-bundle-static-assessment"
        or assessment.get("claim") != "static-action-closure-only"
        or assessment.get("promotion_claimed") is not False
        or assessment.get("support_level_changes") is not False
        or assessment.get("tested_changes") is not False
        or assessment.get("unassessed_axes") != _UNASSESSED_AXES
        or assessment.get("target_versions") != list(runtime_bundle_versions())
    ):
        message = "Tracked conformance assessment header is invalid"
        raise ValueError(message)
    observed_assessment_digest = _required_digest(
        assessment.get("assessment_digest"),
        label="Tracked conformance assessment digest",
    )
    expected_assessment_digest = _canonical_digest(
        {key: value for key, value in assessment.items() if key != "assessment_digest"}
    )
    if observed_assessment_digest != expected_assessment_digest:
        message = "Tracked conformance assessment digest does not match its payload"
        raise ValueError(message)


def _tracked_version_coordinate(
    bundle: Mapping[object, object],
    *,
    inputs: ConformanceGateInputs,
) -> Mapping[object, object]:
    versions = bundle.get("versions")
    if not isinstance(versions, list):
        message = f"Tracked conformance bundle {inputs.bundle} has no coordinates"
        raise TypeError(message)
    coordinates = [
        item
        for item in versions
        if isinstance(item, Mapping) and item.get("version") == inputs.ds_version
    ]
    if len(coordinates) != 1:
        message = (
            f"Tracked conformance coordinate {inputs.ds_version}/{inputs.bundle} "
            "is missing or duplicated"
        )
        raise ValueError(message)
    coordinate = coordinates[0]
    blockers = coordinate.get("blockers")
    if coordinate.get("status") != "ready" or blockers != []:
        message = (
            f"Conformance coordinate {inputs.ds_version}/{inputs.bundle} "
            "is not static-ready"
        )
        raise ValueError(message)
    return coordinate


def _tracked_support_metadata(
    coordinate: Mapping[object, object],
) -> tuple[str, bool]:
    support_level = _required_text(
        coordinate.get("support_level"),
        label="Tracked conformance coordinate support_level",
    )
    if support_level not in {"experimental", "legacy_core", "full"}:
        message = "Tracked conformance coordinate support_level is invalid"
        raise ValueError(message)
    tested = coordinate.get("tested")
    if not isinstance(tested, bool):
        message = "Tracked conformance coordinate tested must be boolean"
        raise TypeError(message)
    return support_level, tested


def _tracked_bundle_names() -> tuple[str, ...]:
    if not isinstance(_TRACKED_CONFORMANCE_DATA, Mapping):
        message = "Tracked conformance assessment must be an object"
        raise TypeError(message)
    bundles = _TRACKED_CONFORMANCE_DATA.get("bundles")
    if not isinstance(bundles, list) or not bundles:
        message = "Tracked conformance assessment requires named bundles"
        raise ValueError(message)
    return _required_text_list(
        [item.get("name") if isinstance(item, Mapping) else None for item in bundles],
        label="Tracked conformance bundle names",
    )


def _required_text_list(value: object, *, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        message = f"{label} must be a non-empty string array"
        raise ValueError(message)
    normalized = tuple(value)
    if len(set(normalized)) != len(normalized):
        message = f"{label} must not contain duplicates"
        raise ValueError(message)
    return normalized


def _required_digest(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        message = f"{label} must be a SHA-256 digest"
        raise ValueError(message)
    return value


def _canonical_digest(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _load_wheel_conformance_data(wheel: Path) -> dict[str, object]:
    archive_label = f"{wheel}!/{_WHEEL_CONFORMANCE_PATH}"
    assignments = _load_literal_assignments_from_source(
        _read_wheel_text(wheel, _WHEEL_CONFORMANCE_PATH),
        filename=archive_label,
        names=frozenset({"_CONFORMANCE_BUNDLE_JSON"}),
    )
    payload_text = assignments["_CONFORMANCE_BUNDLE_JSON"]
    if not isinstance(payload_text, str):
        message = "Candidate wheel conformance payload must be text"
        raise TypeError(message)
    loaded: object = json.loads(payload_text)
    if not isinstance(loaded, dict):
        message = "Candidate wheel conformance payload must be an object"
        raise TypeError(message)
    return loaded


def _load_wheel_task_cleanup_profile_data(wheel: Path) -> dict[str, object]:
    archive_label = f"{wheel}!/{_WHEEL_TASK_CLEANUP_PROFILE_PATH}"
    assignments = _load_literal_assignments_from_source(
        _read_wheel_text(wheel, _WHEEL_TASK_CLEANUP_PROFILE_PATH),
        filename=archive_label,
        names=frozenset({"_TASK_DEFINITION_CLEANUP_PROFILE_JSON"}),
    )
    payload_text = assignments["_TASK_DEFINITION_CLEANUP_PROFILE_JSON"]
    if not isinstance(payload_text, str):
        message = "Candidate wheel private cleanup profile payload must be text"
        raise TypeError(message)
    try:
        loaded: object = json.loads(
            payload_text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_non_finite_json,
        )
    except json.JSONDecodeError as error:
        message = "Candidate wheel private cleanup profile must be strict JSON"
        raise ValueError(message) from error
    if not isinstance(loaded, dict):
        message = "Candidate wheel private cleanup profile must be an object"
        raise TypeError(message)
    return loaded


def _load_wheel_manifest(wheel: Path, ds_version: str) -> dict[str, object]:
    package_slug = f"ds_{ds_version.replace('.', '_')}"
    archive_path = f"dsctl/generated/versions/{package_slug}/_manifest.py"
    assignments = _load_literal_assignments_from_source(
        _read_wheel_text(wheel, archive_path),
        filename=f"{wheel}!/{archive_path}",
        names=frozenset(_MANIFEST_ASSIGNMENTS),
    )
    normalized = {
        output_name: assignments[source_name]
        for source_name, output_name in _MANIFEST_ASSIGNMENTS.items()
    }
    semantic_operations = normalized.get("semantic_operations")
    if isinstance(semantic_operations, tuple):
        normalized["semantic_operations"] = list(semantic_operations)
    return _validate_installed_manifest(normalized, ds_version=ds_version)


def _load_literal_assignments_from_source(
    source: str,
    *,
    filename: str,
    names: frozenset[str],
) -> dict[str, object]:
    try:
        statements = ast.parse(source, filename=filename).body
    except SyntaxError as error:
        message = f"{filename}: generated source is not valid Python"
        raise ValueError(message) from error
    assignments: dict[str, object] = {}
    for statement in statements:
        stored_names = {
            node.id
            for node in ast.walk(statement)
            if isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Store)
            and node.id in names
        }
        if not stored_names:
            continue
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            labels = ", ".join(sorted(stored_names))
            message = f"{filename}: {labels} must use one simple literal assignment"
            raise ValueError(message)
        target = statement.targets[0]
        if (
            not isinstance(target, ast.Name)
            or target.id not in names
            or stored_names != {target.id}
        ):
            labels = ", ".join(sorted(stored_names))
            message = f"{filename}: {labels} must use one simple literal assignment"
            raise ValueError(message)
        name = target.id
        if name in assignments:
            message = (
                f"{filename}: {name} must use one simple literal assignment; "
                "generated source assigns it more than once"
            )
            raise ValueError(message)
        try:
            assignments[name] = ast.literal_eval(statement.value)
        except (TypeError, ValueError) as error:
            message = f"{filename}: {name} must be a simple literal assignment"
            raise ValueError(message) from error
    missing = sorted(names - assignments.keys())
    if missing:
        message = f"{filename}: generated source lacks {', '.join(missing)}"
        raise ValueError(message)
    return assignments


def _read_wheel_text(wheel: Path, archive_path: str) -> str:
    try:
        with zipfile.ZipFile(wheel) as archive:
            return archive.read(archive_path).decode("utf-8")
    except KeyError as error:
        message = f"Candidate wheel does not contain {archive_path}"
        raise ValueError(message) from error


def _validate_installed_attestation(
    value: object,
    *,
    inputs: ConformanceGateInputs,
    installed: gate_harness.InstalledWheel,
    coordinate: TrackedConformanceCoordinate,
    runtime_operations: frozenset[str],
) -> InstalledConformanceAttestation:
    payload = _required_mapping(
        value,
        label="Installed conformance attestation",
        fields={
            "distribution_version",
            "module_file",
            "ds_version",
            "bundle",
            "coordinate_status",
            "blockers",
            "required_actions",
            "bundle_digest",
            "catalog_digest",
            "assessment_digest",
            "profile",
            "actions",
            "manifest",
        },
    )
    module_file = _required_text(payload.get("module_file"), label="module_file")
    venv_root = installed.python.parent.parent.resolve()
    if not Path(module_file).resolve().is_relative_to(venv_root):
        message = "Installed dsctl did not import from the conformance gate virtualenv"
        raise AssertionError(message)
    expected_identity = {
        "ds_version": inputs.ds_version,
        "bundle": inputs.bundle,
        "coordinate_status": "ready",
        "blockers": [],
        "required_actions": list(coordinate.required_actions),
        "bundle_digest": coordinate.bundle_digest,
        "catalog_digest": coordinate.catalog_digest,
        "assessment_digest": coordinate.assessment_digest,
    }
    for field, expected in expected_identity.items():
        if payload.get(field) != expected:
            message = f"Installed conformance {field} does not match tracked coordinate"
            raise AssertionError(message)
    manifest = _validate_installed_manifest(
        payload.get("manifest"),
        ds_version=inputs.ds_version,
    )
    cleanup_root_present = _PRIVATE_TASK_CLEANUP_ROOT in runtime_operations
    cleanup_root_expected = inputs.ds_version in _PRIVATE_TASK_CLEANUP_VERSIONS
    if cleanup_root_present is not cleanup_root_expected:
        message = "Installed private cleanup semantic root differs"
        raise AssertionError(message)
    profile = _validate_installed_profile(
        payload.get("profile"),
        ds_version=inputs.ds_version,
        manifest=manifest,
        support_level=coordinate.support_level,
        tested=coordinate.tested,
    )
    actions = _validate_installed_actions(
        payload.get("actions"),
        required_actions=coordinate.required_actions,
        runtime_operations=runtime_operations,
    )
    return InstalledConformanceAttestation(
        distribution_version=_required_text(
            payload.get("distribution_version"),
            label="distribution_version",
        ),
        ds_version=inputs.ds_version,
        bundle=inputs.bundle,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
        required_actions=coordinate.required_actions,
        profile=profile,
        actions=actions,
        manifest=manifest,
    )


def _validate_installed_manifest(
    value: object,
    *,
    ds_version: str,
) -> dict[str, object]:
    manifest = _required_mapping(
        value,
        label="Installed runtime manifest",
        fields=_MANIFEST_FIELDS,
    )
    bundle_manifest_schema_version = manifest.get("bundle_manifest_schema_version")
    if (
        type(bundle_manifest_schema_version) is not int
        or bundle_manifest_schema_version != 2
        or manifest.get("ds_version") != ds_version
        or manifest.get("source_tag") != ds_version
        or manifest.get("selection") not in {"full", "runtime-slice"}
    ):
        message = "Installed runtime manifest does not match requested exact version"
        raise AssertionError(message)
    raw_operations = manifest.get("semantic_operations")
    if not isinstance(raw_operations, list) or not all(
        isinstance(item, str) and item for item in raw_operations
    ):
        message = "Installed runtime manifest semantic_operations must be textual"
        raise TypeError(message)
    operations = tuple(raw_operations)
    if len(set(operations)) != len(operations):
        message = "Installed runtime manifest semantic_operations contain duplicates"
        raise ValueError(message)
    count = manifest.get("operation_count")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or (manifest["selection"] == "full" and count == 0)
    ):
        message = "Installed runtime manifest operation count differs"
        raise AssertionError(message)
    if manifest["selection"] == "full" and operations:
        message = "Installed full manifest must not declare semantic slice roots"
        raise AssertionError(message)
    for field in ("source_contract_digest", "rendered_contract_digest"):
        _required_digest(manifest.get(field), label=f"Installed manifest {field}")
    for field in ("source_commit", "source_tree"):
        _required_text(manifest.get(field), label=f"Installed manifest {field}")
    normalized = dict(manifest)
    normalized["semantic_operations"] = list(operations)
    return normalized


def _validate_installed_profile(
    value: object,
    *,
    ds_version: str,
    manifest: Mapping[str, object],
    support_level: str,
    tested: bool,
) -> dict[str, object]:
    profile = _required_mapping(
        value,
        label="Installed exact profile",
        fields={
            "server_version",
            "contract_version",
            "family",
            "support_level",
            "tested",
            "source",
            "fingerprints",
        },
    )
    if (
        profile.get("server_version") != ds_version
        or profile.get("contract_version") != ds_version
        or profile.get("support_level") != support_level
        or profile.get("tested") is not tested
    ):
        message = "Installed exact profile does not match requested version"
        raise AssertionError(message)
    _required_text(profile.get("family"), label="Installed profile family")
    source = _required_mapping(
        profile.get("source"),
        label="Installed profile source",
        fields={"tag", "commit", "tree"},
    )
    expected_source = {
        "tag": manifest["source_tag"],
        "commit": manifest["source_commit"],
        "tree": manifest["source_tree"],
    }
    if source != expected_source:
        message = "Installed profile source does not match runtime manifest"
        raise AssertionError(message)
    fingerprints = _validate_fingerprints(
        profile.get("fingerprints"),
        label="Installed profile fingerprints",
    )
    normalized = dict(profile)
    normalized["source"] = dict(source)
    normalized["fingerprints"] = fingerprints
    return normalized


def _validate_installed_actions(
    value: object,
    *,
    required_actions: tuple[str, ...],
    runtime_operations: frozenset[str],
) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list) or len(value) != len(required_actions):
        message = "Installed conformance action count differs"
        raise AssertionError(message)
    normalized: list[dict[str, object]] = []
    for raw, expected_action in zip(value, required_actions, strict=True):
        action = _required_mapping(
            raw,
            label=f"Installed conformance action {expected_action}",
            fields={
                "action",
                "availability",
                "execution_mode",
                "verification",
                "semantic_operation",
                "build_status",
                "fingerprints",
            },
        )
        semantic_operation = _required_text(
            action.get("semantic_operation"),
            label=f"Installed conformance action {expected_action} operation",
        )
        if (
            action.get("action") != expected_action
            or action.get("availability") != "supported"
            or action.get("build_status") != "accepted"
            or semantic_operation not in runtime_operations
        ):
            message = f"Installed conformance action {expected_action} is not accepted"
            raise AssertionError(message)
        _required_text(
            action.get("execution_mode"),
            label=f"Installed conformance action {expected_action} execution_mode",
        )
        _required_text(
            action.get("verification"),
            label=f"Installed conformance action {expected_action} verification",
        )
        item = dict(action)
        item["fingerprints"] = _validate_fingerprints(
            action.get("fingerprints"),
            label=f"Installed conformance action {expected_action} fingerprints",
        )
        normalized.append(item)
    return tuple(normalized)


def _validate_fingerprints(value: object, *, label: str) -> dict[str, str]:
    fingerprints = _required_mapping(
        value,
        label=label,
        fields=_PROFILE_FINGERPRINT_KEYS,
    )
    return {
        field: _required_digest(fingerprints.get(field), label=f"{label} {field}")
        for field in sorted(_PROFILE_FINGERPRINT_KEYS)
    }


def _required_mapping(
    value: object,
    *,
    label: str,
    fields: set[str] | frozenset[str],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise TypeError(message)
    if set(value) != set(fields):
        message = f"{label} fields differ"
        raise ValueError(message)
    return value


def _required_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{label} must be non-empty text"
        raise ValueError(message)
    return value


def _default_evidence_validator(
    receipt: object,
    *,
    expected_ds_version: str,
    expected_bundle: str,
    expected_wheel_filename: str,
    expected_wheel_sha256: str,
) -> object:
    from live_gate.conformance_bundle_evidence import (  # noqa: PLC0415
        validate_conformance_bundle_evidence,
    )

    return validate_conformance_bundle_evidence(
        receipt,
        expected_ds_version=expected_ds_version,
        expected_bundle=expected_bundle,
        expected_wheel_filename=expected_wheel_filename,
        expected_wheel_sha256=expected_wheel_sha256,
    )


def _write_private_json(path: Path, value: object) -> None:
    _write_private_text(path, _canonical_json(value))


def _load_strict_candidate_receipt(path: Path) -> object:
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_non_finite_json,
        )
    except (UnicodeError, json.JSONDecodeError) as error:
        message = "Conformance candidate evidence is not strict UTF-8 JSON"
        raise ValueError(message) from error


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, nested in pairs:
        if key in value:
            message = f"Conformance candidate has duplicate JSON key {key!r}"
            raise ValueError(message)
        value[key] = nested
    return value


def _reject_non_finite_json(value: str) -> object:
    message = f"Conformance candidate contains non-finite JSON value {value}"
    raise ValueError(message)


def _canonical_json(value: object) -> str:
    return (
        json.dumps(
            value,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def _write_private_text(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _snapshot_inputs(
    inputs: ConformanceGateInputs,
    destination: Path,
    *,
    recovery_run_id: str | None = None,
) -> ConformanceGateInputs:
    snapshot = gate_harness.snapshot_gate_files(
        inputs,
        destination,
        changed_template="Conformance gate input changed while snapshotting: {path}",
    )
    recovery_run_id_file: Path | None = None
    if recovery_run_id is not None:
        recovery_run_id_file = destination / "recovery-run-id.json"
        _write_private_json(
            recovery_run_id_file,
            {"schema_version": 1, "run_id": recovery_run_id},
        )
    return ConformanceGateInputs(
        ds_version=inputs.ds_version,
        bundle=inputs.bundle,
        wheel=snapshot.wheel,
        env_file=snapshot.env_file,
        attestation_key_file=snapshot.attestation_key_file,
        cluster_manifest=snapshot.cluster_manifest,
        fixture_manifest=snapshot.fixture_manifest,
        evidence=snapshot.evidence,
        recovery_run_id_file=recovery_run_id_file,
    )


def _conformance_environment(
    inputs: ConformanceGateInputs,
    *,
    executable: Path,
    python: Path,
    attestation: Path,
    candidate_evidence: Path,
    run_id_output_file: Path | None = None,
) -> dict[str, str]:
    environment = gate_harness.isolated_environment()
    environment.update(
        {
            "DSCTL_RUN_LIVE_TESTS": "1",
            "DS_LIVE_CONFORMANCE_VERSION": inputs.ds_version,
            "DS_LIVE_CONFORMANCE_BUNDLE": inputs.bundle,
            "DS_LIVE_CONFORMANCE_ENV_FILE": str(inputs.env_file),
            "DS_LIVE_CONFORMANCE_ATTESTATION_KEY_FILE": str(
                inputs.attestation_key_file
            ),
            "DS_LIVE_CONFORMANCE_DSCTL": str(executable),
            "DS_LIVE_CONFORMANCE_PYTHON": str(python),
            "DS_LIVE_CONFORMANCE_WHEEL": str(inputs.wheel),
            "DS_LIVE_CONFORMANCE_CLUSTER_MANIFEST": str(inputs.cluster_manifest),
            "DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST": str(inputs.fixture_manifest),
            "DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION": str(attestation),
            "DS_LIVE_CONFORMANCE_EVIDENCE": str(candidate_evidence),
        }
    )
    if inputs.recovery_run_id_file is not None:
        environment["DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE"] = str(
            inputs.recovery_run_id_file
        )
    if run_id_output_file is not None:
        environment["DS_LIVE_CONFORMANCE_RUN_ID_FILE"] = str(run_id_output_file)
    gate_harness.bind_direct_api_target(
        environment,
        inputs.env_file,
        invalid_url_message=(
            "Conformance gate profile must contain a valid DS_API_URL"
        ),
    )
    return environment


def _parse_args(argv: list[str] | None = None) -> ConformanceGateInputs:
    parser = argparse.ArgumentParser(
        description="Run one installed-wheel named conformance bundle gate.",
    )
    parser.add_argument(
        "--ds-version", choices=runtime_bundle_versions(), required=True
    )
    parser.add_argument("--bundle", choices=_tracked_bundle_names(), required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--attestation-key-file", type=Path, required=True)
    parser.add_argument("--cluster-manifest", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--recovery-run-id-file", type=Path)
    parser.add_argument("--run-id-output-file", type=Path)
    args = parser.parse_args(argv)
    return ConformanceGateInputs(
        ds_version=args.ds_version,
        bundle=args.bundle,
        wheel=args.wheel.expanduser().resolve(),
        env_file=args.env_file.expanduser().resolve(),
        attestation_key_file=args.attestation_key_file.expanduser().resolve(),
        cluster_manifest=args.cluster_manifest.expanduser().resolve(),
        fixture_manifest=args.fixture_manifest.expanduser().resolve(),
        evidence=args.evidence.expanduser().resolve(),
        recovery_run_id_file=(
            absolute_path(args.recovery_run_id_file)
            if args.recovery_run_id_file is not None
            else None
        ),
        run_id_output_file=(
            absolute_path(args.run_id_output_file)
            if args.run_id_output_file is not None
            else None
        ),
    )


def main(argv: list[str] | None = None) -> int:
    return run_gate(_parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
