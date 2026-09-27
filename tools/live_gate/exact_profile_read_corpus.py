"""Validation for a complete promotion-grade exact-profile read corpus."""

from __future__ import annotations

import argparse
import ast
import copy
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, cast

from live_gate.evidence_values import required_mapping as _required_mapping
from live_gate.evidence_values import required_text as _required_text
from live_gate.exact_profile_read_evidence import (
    CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION,
    EXACT_PROFILE_READ_ACTIONS,
    EXACT_PROFILE_READ_RECIPES,
    validate_exact_profile_read_evidence_payload,
)
from live_gate.runtime_ownership import load_current_runtime_ownership
from runtime_bundle_manifest import runtime_bundle_versions

if TYPE_CHECKING:
    from collections.abc import Mapping


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE_ROOT = ROOT / "docs" / "development" / "live-evidence" / "exact-read"
_CURRENT_BUNDLE_MANIFEST_SCHEMA_VERSION = 2
_RECEIPT_FILENAME = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})-(?P<wheel_sha12>[0-9a-f]{12})\.json$"
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
_PROFILE_BINDINGS = {
    "ds": "server_version",
    "selected_ds_version": "server_version",
    "contract_version": "contract_version",
    "family": "family",
    "support_level": "support_level",
    "tested": "tested",
    "fingerprints": "fingerprints",
}
_VERSION_BINDINGS = (
    ("dolphinscheduler.release", ("dolphinscheduler", "release")),
    ("profile.ds", ("profile", "ds")),
    ("profile.selected_ds_version", ("profile", "selected_ds_version")),
    ("profile.contract_version", ("profile", "contract_version")),
    ("contract.ds_version", ("contract", "ds_version")),
    ("contract.source_tag", ("contract", "source_tag")),
)


@dataclass(frozen=True)
class ExactProfileReadCorpus:
    """Identity of one complete, same-wheel exact-profile read corpus."""

    evidence_root: Path
    versions: tuple[str, ...]
    cli_version: str
    wheel_filename: str
    wheel_sha256: str
    receipts: tuple[Path, ...]


@dataclass(frozen=True)
class TrackedArtifacts:
    """Safely materialized generated profiles, manifests, and package version."""

    cli_version: str
    versions: tuple[str, ...]
    profiles: Mapping[str, object]
    contracts: Mapping[str, Mapping[str, object]]


def check_exact_profile_read_evidence_corpus(
    evidence_root: Path,
    *,
    expected_wheel_filename: str | None = None,
    expected_wheel_sha256: str | None = None,
    source_root: Path = ROOT,
) -> ExactProfileReadCorpus:
    """Validate the complete tracked generic exact-profile read corpus."""
    artifacts = load_tracked_artifacts(source_root)
    runtime_operations = load_current_runtime_ownership(
        source_root, contracts=artifacts.contracts
    )
    version_directories = _validate_corpus_root(evidence_root, artifacts.versions)
    receipts: list[Path] = []
    shared_runner: tuple[str, str, str] | None = None
    for version in artifacts.versions:
        receipt_path = _single_receipt(version_directories[version], version=version)
        receipt = _load_receipt(receipt_path)
        if receipt.get("schema_version") != CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION:
            message = (
                f"{receipt_path}: schema "
                f"{CURRENT_EXACT_PROFILE_READ_SCHEMA_VERSION} is required for "
                "promotion evidence"
            )
            raise ValueError(message)
        read_bundle = _required_mapping(
            receipt.get("read_bundle"),
            label=f"{receipt_path}: read_bundle",
        )
        if "action_verifications" not in read_bundle:
            message = (
                f"{receipt_path}: read_bundle.action_verifications is required for "
                "promotion evidence"
            )
            raise ValueError(message)
        _validate_version_bindings(receipt, version=version, path=receipt_path)
        try:
            validate_exact_profile_read_evidence_payload(
                receipt,
                expected_cli_version=artifacts.cli_version,
                expected_ds_version=version,
                compiled_semantic_operations=runtime_operations[version]
                - frozenset(
                    cast(
                        "list[str]",
                        artifacts.contracts[version]["semantic_operations"],
                    )
                ),
            )
        except (TypeError, ValueError) as error:
            message = f"{receipt_path}: invalid exact-profile read receipt: {error}"
            raise ValueError(message) from error
        _validate_tracked_bindings(
            receipt,
            version=version,
            artifacts=artifacts,
            path=receipt_path,
        )
        runner = _runner_identity(receipt, path=receipt_path)
        validate_receipt_filename(receipt_path, wheel_sha256=runner[2])
        if expected_wheel_filename is not None and runner[1] != expected_wheel_filename:
            message = (
                f"{receipt_path}: wheel filename differs from expected value; "
                f"expected {expected_wheel_filename!r}, got {runner[1]!r}"
            )
            raise ValueError(message)
        if expected_wheel_sha256 is not None and runner[2] != expected_wheel_sha256:
            message = (
                f"{receipt_path}: wheel SHA-256 differs from expected value; "
                f"expected {expected_wheel_sha256!r}, got {runner[2]!r}"
            )
            raise ValueError(message)
        if shared_runner is None:
            shared_runner = runner
        elif runner != shared_runner:
            message = (
                f"{receipt_path}: runner identity differs from the corpus wheel; "
                f"expected cli={shared_runner[0]!r}, wheel={shared_runner[1]!r}, "
                f"digest={shared_runner[2]!r}"
            )
            raise ValueError(message)
        receipts.append(receipt_path)

    if shared_runner is None:  # pragma: no cover - the fixed matrix is non-empty
        message = "exact-profile read corpus has no receipts"
        raise ValueError(message)
    return ExactProfileReadCorpus(
        evidence_root=evidence_root,
        versions=artifacts.versions,
        cli_version=shared_runner[0],
        wheel_filename=shared_runner[1],
        wheel_sha256=shared_runner[2],
        receipts=tuple(receipts),
    )


def load_tracked_artifacts(source_root: Path) -> TrackedArtifacts:
    """Load current generated evidence bindings without executing source."""

    cli_version = _load_cli_version(source_root / "pyproject.toml")
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    profile_data = _load_profile_data(profile_path)
    versions = _required_text_tuple(
        profile_data.get("target_versions"),
        label=f"{profile_path}: TARGET_DS_VERSIONS",
    )
    if versions != runtime_bundle_versions():
        message = (
            f"{profile_path}: generated target versions differ from the canonical "
            f"runtime bundle manifest; expected {runtime_bundle_versions()!r}, "
            f"got {versions!r}"
        )
        raise ValueError(message)
    profiles = _required_mapping(
        _materialize_profiles(profile_data, versions=versions, path=profile_path),
        label=f"{profile_path}: VERSION_PROFILES",
    )
    if tuple(profiles) != versions:
        message = f"{profile_path}: generated profile keys do not match target versions"
        raise ValueError(message)
    contracts = {
        version: _load_contract(source_root, version=version) for version in versions
    }
    return TrackedArtifacts(
        cli_version=cli_version,
        versions=versions,
        profiles=profiles,
        contracts=contracts,
    )


def _load_profile_data(path: Path) -> dict[str, object]:
    source = path.read_text(encoding="utf-8")
    raw = _load_literal_assignment(source, name="_PROFILE_JSON", path=path)
    if not isinstance(raw, str):
        message = f"{path}: _PROFILE_JSON must be a string literal"
        raise TypeError(message)
    try:
        payload: object = json.loads(raw)
    except json.JSONDecodeError as error:
        message = f"{path}: _PROFILE_JSON is not valid JSON: {error.msg}"
        raise ValueError(message) from error
    data = _required_mapping(payload, label=f"{path}: _PROFILE_JSON")
    if data.get("schema_version") != 1:
        message = f"{path}: generated profile schema is unsupported"
        raise ValueError(message)
    return data


def _materialize_profiles(
    data: Mapping[str, object],
    *,
    versions: tuple[str, ...],
    path: Path,
) -> dict[str, object]:
    stable_actions = _required_text_tuple(
        data.get("stable_actions"), label=f"{path}: stable_actions"
    )
    metadata = _required_mapping(
        data.get("profile_metadata"), label=f"{path}: profile_metadata"
    )
    bindings = _required_mapping(
        data.get("profile_bindings"), label=f"{path}: profile_bindings"
    )
    capabilities = _required_mapping(
        data.get("capability_records"), label=f"{path}: capability_records"
    )
    build_records = _required_mapping(
        data.get("build_records"), label=f"{path}: build_records"
    )
    build_fields = _required_mapping(
        data.get("build_fields"), label=f"{path}: build_fields"
    )
    expected_fields = {
        "source_operations",
        "type_closure",
        "selector_semantics",
        "evidence_sources",
        "fingerprints",
    }
    if set(build_fields) != expected_fields:
        message = f"{path}: build_fields differ from the named record schema"
        raise ValueError(message)
    field_pools = {
        field: _required_mapping(records, label=f"{path}: build_fields.{field} records")
        for field, records in build_fields.items()
    }
    if set(metadata) != set(versions) or set(bindings) != set(versions):
        message = f"{path}: named profile version keys are inconsistent"
        raise ValueError(message)
    profiles: dict[str, object] = {}
    operations: set[str] | None = None
    for version in versions:
        label = f"{path}: profile_bindings[{version!r}]"
        profile_bindings = _required_mapping(bindings[version], label=label)
        if set(profile_bindings) != {"actions", "build_decisions"}:
            message = f"{label} keys differ from the named binding schema"
            raise ValueError(message)
        action_bindings = _required_mapping(
            profile_bindings["actions"], label=f"{label}.actions"
        )
        if set(action_bindings) != set(stable_actions):
            message = f"{label}.actions do not cover every stable action"
            raise ValueError(message)
        decision_bindings = _required_mapping(
            profile_bindings["build_decisions"], label=f"{label}.build_decisions"
        )
        if operations is None:
            operations = set(decision_bindings)
        elif set(decision_bindings) != operations:
            message = f"{label} has a different build-operation domain"
            raise ValueError(message)
        decisions = {
            operation: _materialize_decision(
                operation=operation,
                record=_named_record(build_records, name, label=f"{label}:{operation}"),
                fields=field_pools,
                stable_actions=stable_actions,
                label=f"{label}:{operation}",
            )
            for operation, name in decision_bindings.items()
        }
        profile = copy.deepcopy(
            _required_mapping(metadata[version], label=f"{path}: metadata[{version!r}]")
        )
        profile["actions"] = {
            action: copy.deepcopy(
                _named_record(capabilities, name, label=f"{label}:{action}")
            )
            for action, name in action_bindings.items()
        }
        profile["build_decisions"] = decisions
        profiles[version] = profile
    return profiles


def _materialize_decision(
    *,
    operation: str,
    record: dict[str, object],
    fields: Mapping[str, Mapping[str, object]],
    stable_actions: tuple[str, ...],
    label: str,
) -> dict[str, object]:
    if record.get("semantic_operation") != operation:
        message = f"{label} binds a different semantic operation"
        raise ValueError(message)
    action = _required_text(record.get("stable_action"), label=f"{label}.stable_action")
    if action not in stable_actions:
        message = f"{label} selects an unknown stable action"
        raise ValueError(message)
    _required_text(record.get("build_status"), label=f"{label}.build_status")
    references: dict[str, str] = {}
    for field, records in fields.items():
        reference = _required_text(
            record.get(field), label=f"{label}.{field} reference"
        )
        if reference not in records:
            message = f"{label}.{field} references unknown record {reference!r}"
            raise ValueError(message)
        references[field] = reference
    fingerprints = _required_mapping(
        fields["fingerprints"][references["fingerprints"]],
        label=f"{label}.fingerprints",
    )
    if set(fingerprints) != {
        "source",
        "effective_wire",
        "consumed_projection",
        "preservation",
    }:
        message = f"{label} must bind four fingerprints"
        raise ValueError(message)
    for axis, fingerprint in fingerprints.items():
        if (
            not isinstance(fingerprint, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint) is None
        ):
            message = f"{label}.fingerprints.{axis} must be a SHA-256 fingerprint"
            raise ValueError(message)
    return {
        "semantic_operation": operation,
        "stable_action": action,
        "build_status": record["build_status"],
        "fingerprints": copy.deepcopy(fingerprints),
    }


def _named_record(
    records: Mapping[str, object], reference: object, *, label: str
) -> dict[str, object]:
    name = _required_text(reference, label=f"{label} record name")
    if name not in records:
        message = f"{label} references unknown record {name!r}"
        raise ValueError(message)
    return _required_mapping(records[name], label=f"{label} record {name!r}")


def _load_cli_version(path: Path) -> str:
    payload = _required_mapping(
        tomllib.loads(path.read_text(encoding="utf-8")),
        label=f"{path}: pyproject",
    )
    project = _required_mapping(payload.get("project"), label=f"{path}: project")
    return _required_text(project.get("version"), label=f"{path}: project.version")


def _load_contract(source_root: Path, *, version: str) -> dict[str, object]:
    slug = version.replace(".", "_")
    path = (
        source_root
        / "src"
        / "dsctl"
        / "generated"
        / "versions"
        / f"ds_{slug}"
        / "_manifest.py"
    )
    source = path.read_text(encoding="utf-8")
    assignments = _load_literal_assignments(
        source,
        names=frozenset(_MANIFEST_FIELDS),
        path=path,
    )
    contract: dict[str, object] = {}
    for source_name, receipt_name in _MANIFEST_FIELDS.items():
        if source_name not in assignments:
            message = f"{path}: generated manifest lacks {source_name}"
            raise ValueError(message)
        contract[receipt_name] = assignments[source_name]
    bundle_manifest_schema_version = contract["bundle_manifest_schema_version"]
    if (
        type(bundle_manifest_schema_version) is not int
        or bundle_manifest_schema_version != _CURRENT_BUNDLE_MANIFEST_SCHEMA_VERSION
    ):
        message = f"{path}: generated bundle manifest schema is unsupported"
        raise ValueError(message)
    semantic_operations = contract["semantic_operations"]
    if not isinstance(semantic_operations, tuple) or not all(
        isinstance(item, str) for item in semantic_operations
    ):
        message = f"{path}: SEMANTIC_OPERATIONS must be a string tuple"
        raise TypeError(message)
    contract["semantic_operations"] = list(semantic_operations)
    return contract


def _load_literal_assignment(source: str, *, name: str, path: Path) -> object:
    assignments = _load_literal_assignments(
        source,
        names=frozenset({name}),
        path=path,
    )
    if name not in assignments:
        message = f"{path}: generated source lacks {name}"
        raise ValueError(message)
    return assignments[name]


def _load_literal_assignments(
    source: str,
    *,
    names: frozenset[str],
    path: Path,
) -> dict[str, object]:
    assignments: dict[str, object] = {}
    try:
        statements = ast.parse(source, filename=str(path)).body
    except SyntaxError as error:
        message = f"{path}: generated source is not valid Python"
        raise ValueError(message) from error
    for statement in statements:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if not isinstance(target, ast.Name) or target.id not in names:
            continue
        if target.id in assignments:
            message = f"{path}: generated source assigns {target.id} more than once"
            raise ValueError(message)
        try:
            assignments[target.id] = ast.literal_eval(statement.value)
        except (TypeError, ValueError) as error:
            message = f"{path}: {target.id} must be a literal assignment"
            raise ValueError(message) from error
    return assignments


def _validate_corpus_root(
    root: Path,
    versions: tuple[str, ...],
) -> dict[str, Path]:
    if not root.exists():
        message = (
            f"exact-profile read evidence root does not exist: {root}; "
            "track one current receipt for every exact version before running this gate"
        )
        raise FileNotFoundError(message)
    if not root.is_dir():
        message = f"exact-profile read evidence root is not a directory: {root}"
        raise NotADirectoryError(message)
    entries = {path.name: path for path in root.iterdir()}
    expected = set(versions)
    missing = sorted(expected - entries.keys())
    unexpected = sorted(entries.keys() - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing version directories: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected root entries: " + ", ".join(unexpected))
        message = (
            f"{root}: exact-profile read corpus is incomplete; {'; '.join(details)}"
        )
        raise ValueError(message)
    for version, path in entries.items():
        if not path.is_dir():
            message = f"{path}: expected a receipt directory for DS {version}"
            raise NotADirectoryError(message)
    return entries


def _single_receipt(version_root: Path, *, version: str) -> Path:
    entries = sorted(version_root.iterdir(), key=lambda path: path.name)
    if len(entries) != 1:
        names = ", ".join(path.name for path in entries) or "none"
        message = (
            f"{version_root}: DS {version} requires exactly one current receipt; "
            f"found {len(entries)} ({names})"
        )
        raise ValueError(message)
    receipt = entries[0]
    if not receipt.is_file():
        message = f"{receipt}: DS {version} receipt must be a regular JSON file"
        raise ValueError(message)
    return receipt


def _load_receipt(path: Path) -> dict[str, object]:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        message = f"{path}: receipt is not valid JSON: {error.msg}"
        raise ValueError(message) from error
    return _required_mapping(payload, label=f"{path}: receipt")


def _validate_version_bindings(
    receipt: Mapping[str, object],
    *,
    version: str,
    path: Path,
) -> None:
    for label, keys in _VERSION_BINDINGS:
        value: object = receipt
        for key in keys:
            value = _required_mapping(value, label=f"{path}: {label}").get(key)
        if value != version:
            message = (
                f"{path}: {label} must match directory version {version!r}; "
                f"got {value!r}"
            )
            raise ValueError(message)


def _validate_tracked_bindings(
    receipt: Mapping[str, object],
    *,
    version: str,
    artifacts: TrackedArtifacts,
    path: Path,
) -> None:
    contract = _required_mapping(receipt.get("contract"), label=f"{path}: contract")
    _require_current_mapping(
        contract,
        artifacts.contracts[version],
        label=f"{path}: contract",
    )
    profile = _required_mapping(receipt.get("profile"), label=f"{path}: profile")
    tracked_profile = _required_mapping(
        artifacts.profiles.get(version),
        label=f"tracked profile {version}",
    )
    expected_profile = {
        receipt_name: tracked_profile.get(tracked_name)
        for receipt_name, tracked_name in _PROFILE_BINDINGS.items()
    }
    _require_current_mapping(
        {name: profile.get(name) for name in _PROFILE_BINDINGS},
        expected_profile,
        label=f"{path}: profile",
    )
    _validate_current_recipes(
        receipt,
        tracked_profile=tracked_profile,
        path=path,
    )
    _validate_current_action_verifications(
        receipt,
        tracked_profile=tracked_profile,
        path=path,
    )


def _validate_current_recipes(
    receipt: Mapping[str, object],
    *,
    tracked_profile: Mapping[str, object],
    path: Path,
) -> None:
    read_bundle = _required_mapping(
        receipt.get("read_bundle"),
        label=f"{path}: read_bundle",
    )
    recipes = read_bundle.get("recipes")
    if not isinstance(recipes, list):
        message = f"{path}: read_bundle.recipes must be a list"
        raise TypeError(message)
    decisions = _required_mapping(
        tracked_profile.get("build_decisions"),
        label=f"tracked profile recipes for {path.parent.name}",
    )
    for index, (action, operation) in enumerate(EXACT_PROFILE_READ_RECIPES):
        recipe = _required_mapping(
            recipes[index],
            label=f"{path}: read_bundle.recipes[{index}]",
        )
        decision = _required_mapping(
            decisions.get(operation),
            label=f"tracked profile decision {path.parent.name}:{operation}",
        )
        selected_action = decision.get("stable_action")
        if selected_action != action:
            message = (
                f"tracked profile {path.parent.name}:{operation} selects action "
                f"{selected_action!r}, expected {action!r}"
            )
            raise ValueError(message)
        expected = {
            "action": action,
            "semantic_operation": decision.get("semantic_operation"),
            "build_status": decision.get("build_status"),
            "fingerprints": decision.get("fingerprints"),
        }
        _require_current_mapping(
            recipe,
            expected,
            label=f"{path}: read recipe {action}",
        )


def _validate_current_action_verifications(
    receipt: Mapping[str, object],
    *,
    tracked_profile: Mapping[str, object],
    path: Path,
) -> None:
    read_bundle = _required_mapping(
        receipt.get("read_bundle"),
        label=f"{path}: read_bundle",
    )
    raw_verifications = read_bundle.get("action_verifications")
    if raw_verifications is None:
        message = (
            f"{path}: read_bundle.action_verifications is required for "
            "promotion evidence"
        )
        raise ValueError(message)
    verifications = _required_mapping(
        raw_verifications,
        label=f"{path}: read_bundle.action_verifications",
    )
    actions = _required_mapping(
        tracked_profile.get("actions"),
        label=f"tracked profile actions for {path.parent.name}",
    )
    expected: dict[str, object] = {}
    for action in EXACT_PROFILE_READ_ACTIONS:
        capability = _required_mapping(
            actions.get(action),
            label=f"tracked profile capability {path.parent.name}:{action}",
        )
        verification = capability.get("verification")
        if verification != "live_smoke":
            message = (
                f"tracked profile {path.parent.name}:{action} must be live_smoke "
                "before its read receipt can be promotion evidence"
            )
            raise ValueError(message)
        expected[action] = verification
    _require_current_mapping(
        verifications,
        expected,
        label=f"{path}: read action verifications",
    )


def _runner_identity(
    receipt: Mapping[str, object],
    *,
    path: Path,
) -> tuple[str, str, str]:
    runner = _required_mapping(receipt.get("runner"), label=f"{path}: runner")
    return (
        _required_text(runner.get("cli_version"), label=f"{path}: runner.cli_version"),
        _required_text(
            runner.get("wheel_filename"),
            label=f"{path}: runner.wheel_filename",
        ),
        _required_text(
            runner.get("wheel_sha256"),
            label=f"{path}: runner.wheel_sha256",
        ),
    )


def validate_receipt_filename(path: Path, *, wheel_sha256: str) -> None:
    """Require the governed date-and-wheel-digest receipt filename."""

    match = _RECEIPT_FILENAME.fullmatch(path.name)
    if match is None:
        message = (
            f"{path}: receipt filename must match "
            "YYYY-MM-DD-<first-12-wheel-sha256>.json"
        )
        raise ValueError(message)
    try:
        date.fromisoformat(match["date"])
    except ValueError as error:
        message = f"{path}: receipt filename date is not a calendar date"
        raise ValueError(message) from error
    expected_suffix = wheel_sha256.removeprefix("sha256:")[:12]
    if match["wheel_sha12"] != expected_suffix:
        message = (
            f"{path}: filename digest suffix must be {expected_suffix!r} "
            "from runner.wheel_sha256"
        )
        raise ValueError(message)


def _require_current_mapping(
    actual: Mapping[str, object],
    expected: Mapping[str, object],
    *,
    label: str,
) -> None:
    if actual == expected:
        return
    differing = sorted(
        key
        for key in set(actual) | set(expected)
        if actual.get(key) != expected.get(key)
    )
    message = (
        f"{label} differs from the current tracked generated artifact at: "
        + ", ".join(differing)
        + "; rerun the installed-wheel gate with the current canonical wheel"
    )
    raise ValueError(message)


def _required_text_tuple(value: object, *, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, (list, tuple))
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        message = f"{label} must be a non-empty string tuple"
        raise TypeError(message)
    return tuple(value)


def run_exact_profile_read_evidence_cli(
    argv: list[str] | None = None,
    *,
    default_evidence_root: Path = DEFAULT_EVIDENCE_ROOT,
) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the complete tracked same-wheel exact-profile read receipt "
            "corpus."
        )
    )
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=default_evidence_root,
        help="receipt corpus root (defaults to the tracked exact-read evidence tree)",
    )
    parser.add_argument(
        "--expected-wheel-filename",
        help="require every receipt to name this exact wheel basename",
    )
    parser.add_argument(
        "--expected-wheel-sha256",
        help="require every receipt to bind this full sha256:<64-hex> identity",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=ROOT,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        checked = check_exact_profile_read_evidence_corpus(
            args.evidence_root.expanduser().resolve(),
            expected_wheel_filename=args.expected_wheel_filename,
            expected_wheel_sha256=args.expected_wheel_sha256,
            source_root=args.source_root.expanduser().resolve(),
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"exact-profile read evidence check failed: {error}", file=sys.stderr)
        return 1
    print(
        "exact-profile read evidence check passed: "
        f"{len(checked.receipts)} versions; cli={checked.cli_version}; "
        f"wheel={checked.wheel_filename}; digest={checked.wheel_sha256}"
    )
    return 0


__all__ = [
    "DEFAULT_EVIDENCE_ROOT",
    "ExactProfileReadCorpus",
    "TrackedArtifacts",
    "check_exact_profile_read_evidence_corpus",
    "load_tracked_artifacts",
    "run_exact_profile_read_evidence_cli",
    "validate_receipt_filename",
]
