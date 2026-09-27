"""Load current source, image and cleanup facts without executing source."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

from live_gate.conformance_evidence.types import (
    _CurrentTruth,
    _ImageContract,
    _TaskDefinitionCleanupContract,
)
from live_gate.conformance_evidence.values import (
    _exact_keys,
    _text_sequence,
)
from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_bool as _boolean
from live_gate.evidence_values import required_int as _integer
from live_gate.evidence_values import required_list as _sequence
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text
from live_gate.exact_profile_read_corpus import load_tracked_artifacts
from live_gate.runtime_ownership import load_current_runtime_ownership

if TYPE_CHECKING:
    from collections.abc import Mapping

_CONFORMANCE_BUNDLE_ASSIGNMENT = "_CONFORMANCE_BUNDLE_JSON"


_CONFORMANCE_IMAGE_CONTRACT_ASSIGNMENT = "_CONFORMANCE_IMAGE_CONTRACT_JSON"


_TASK_DEFINITION_CLEANUP_PROFILE_ASSIGNMENT = "_TASK_DEFINITION_CLEANUP_PROFILE_JSON"


_TASK_DEFINITION_PROFILE_ASSIGNMENT = "_TASK_DEFINITION_PROFILE_JSON"


_MANIFEST_ASSIGNMENTS = frozenset(
    {
        "BUNDLE_MANIFEST_SCHEMA_VERSION",
        "DS_VERSION",
        "OPERATION_COUNT",
        "RENDERED_CONTRACT_DIGEST",
        "SELECTION",
        "SEMANTIC_OPERATIONS",
        "SOURCE_COMMIT",
        "SOURCE_CONTRACT_DIGEST",
        "SOURCE_TAG",
        "SOURCE_TREE",
    }
)


_MANAGED_LOCK_PRESENCE_FIELDS = frozenset(
    {
        "base_manifest",
        "binary_sha512",
        "published_manifest",
        "schema_sha256",
        "source_commit",
        "source_sha512",
    }
)


def _load_current_truth(source_root: object) -> _CurrentTruth:
    if not isinstance(source_root, Path):
        message = "source_root must be a pathlib.Path"
        raise TypeError(message)
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    raw_profile = _load_literal_assignment(profile_path, name="_PROFILE_JSON")
    if not isinstance(raw_profile, str):
        message = f"{profile_path}: _PROFILE_JSON must be a string literal"
        raise TypeError(message)
    artifacts = load_tracked_artifacts(source_root)
    for version in artifacts.versions:
        slug = version.replace(".", "_")
        manifest_path = (
            source_root
            / "src"
            / "dsctl"
            / "generated"
            / "versions"
            / f"ds_{slug}"
            / "_manifest.py"
        )
        _load_literal_assignments(
            manifest_path,
            names=_MANIFEST_ASSIGNMENTS,
        )
    assessment_path = (
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py"
    )
    raw_assessment = _load_literal_assignment(
        assessment_path,
        name=_CONFORMANCE_BUNDLE_ASSIGNMENT,
    )
    if not isinstance(raw_assessment, str):
        message = (
            f"{assessment_path}: {_CONFORMANCE_BUNDLE_ASSIGNMENT} "
            "must be a string literal"
        )
        raise TypeError(message)
    assessment = _load_strict_json_object(
        raw_assessment,
        label=f"{assessment_path}: {_CONFORMANCE_BUNDLE_ASSIGNMENT}",
    )
    image_contract_path = (
        source_root / "tools" / "live_gate" / "conformance_image_contract.py"
    )
    raw_image_contract = _load_literal_assignment(
        image_contract_path,
        name=_CONFORMANCE_IMAGE_CONTRACT_ASSIGNMENT,
    )
    if not isinstance(raw_image_contract, str):
        message = (
            f"{image_contract_path}: {_CONFORMANCE_IMAGE_CONTRACT_ASSIGNMENT} "
            "must be a string literal"
        )
        raise TypeError(message)
    image_contract = _validate_current_image_contract(
        _load_strict_json_object(
            raw_image_contract,
            label=(f"{image_contract_path}: {_CONFORMANCE_IMAGE_CONTRACT_ASSIGNMENT}"),
        ),
        versions=artifacts.versions,
    )
    cleanup_profile_path = (
        source_root
        / "src"
        / "dsctl"
        / "generated"
        / "task_definition_cleanup_profiles.py"
    )
    raw_cleanup_profile = _load_literal_assignment(
        cleanup_profile_path,
        name=_TASK_DEFINITION_CLEANUP_PROFILE_ASSIGNMENT,
    )
    if not isinstance(raw_cleanup_profile, str):
        message = (
            f"{cleanup_profile_path}: "
            f"{_TASK_DEFINITION_CLEANUP_PROFILE_ASSIGNMENT} must be a string literal"
        )
        raise TypeError(message)
    runtime_operations = load_current_runtime_ownership(
        source_root, contracts=artifacts.contracts
    )
    task_cleanup = _validate_current_task_cleanup_contract(
        _load_strict_json_object(
            raw_cleanup_profile,
            label=(
                f"{cleanup_profile_path}: {_TASK_DEFINITION_CLEANUP_PROFILE_ASSIGNMENT}"
            ),
        ),
        versions=artifacts.versions,
        runtime_operations=runtime_operations,
    )
    task_profile_path = (
        source_root / "src" / "dsctl" / "generated" / "task_definition_profiles.py"
    )
    raw_task_profile = _load_literal_assignment(
        task_profile_path,
        name=_TASK_DEFINITION_PROFILE_ASSIGNMENT,
    )
    if not isinstance(raw_task_profile, str):
        message = (
            f"{task_profile_path}: "
            f"{_TASK_DEFINITION_PROFILE_ASSIGNMENT} must be a string literal"
        )
        raise TypeError(message)
    dependency_update_upstream_limited_versions = (
        _validate_current_task_definition_update_policy(
            _load_strict_json_object(
                raw_task_profile,
                label=(f"{task_profile_path}: {_TASK_DEFINITION_PROFILE_ASSIGNMENT}"),
            ),
            versions=artifacts.versions,
        )
    )
    return _CurrentTruth(
        cli_version=artifacts.cli_version,
        versions=artifacts.versions,
        profiles=artifacts.profiles,
        contracts=artifacts.contracts,
        runtime_operations=runtime_operations,
        assessment=assessment,
        image_contract=image_contract,
        task_cleanup=task_cleanup,
        dependency_update_upstream_limited_versions=(
            dependency_update_upstream_limited_versions
        ),
    )


def _validate_current_task_definition_update_policy(
    value: Mapping[str, object],
    *,
    versions: tuple[str, ...],
) -> frozenset[str]:
    _exact_keys(
        value,
        {"schema_version", "target_versions", "profile_pool", "profile_specs"},
        label="task-definition profile",
    )
    _constant(
        _integer(
            value.get("schema_version"),
            label="task-definition profile schema_version",
        ),
        5,
        label="task-definition profile schema_version",
    )
    target_versions = tuple(
        _text_sequence(
            value.get("target_versions"),
            label="task-definition profile target_versions",
            allow_empty=False,
        )
    )
    _constant(
        target_versions,
        versions,
        label="task-definition profile target_versions",
    )
    profile_pool = _sequence(
        value.get("profile_pool"),
        label="task-definition profile_pool",
    )
    if not profile_pool:
        message = "task-definition profile_pool must not be empty"
        raise ValueError(message)
    profile_specs = _mapping(
        value.get("profile_specs"),
        label="task-definition profile_specs",
    )
    if tuple(profile_specs) != target_versions:
        message = "task-definition profile_specs do not cover target versions"
        raise ValueError(message)

    limited_versions: set[str] = set()
    for version in target_versions:
        profile_index = _integer(
            profile_specs.get(version),
            label=f"DS {version} task-definition profile index",
        )
        if not 0 <= profile_index < len(profile_pool):
            message = f"DS {version} task-definition profile index is out of range"
            raise ValueError(message)
        profile = _mapping(
            profile_pool[profile_index],
            label=f"DS {version} task-definition profile",
        )
        update_executable = _boolean(
            profile.get("update_executable"),
            label=f"DS {version} task-definition update_executable",
        )
        dependency_update = _boolean(
            profile.get("dependency_update"),
            label=f"DS {version} task-definition dependency_update",
        )
        whole_workflow_update = _boolean(
            profile.get("whole_workflow_update"),
            label=f"DS {version} task-definition whole_workflow_update",
        )
        executable = _boolean(
            profile.get("executable"),
            label=f"DS {version} task-definition executable",
        )
        can_update = update_executable or (executable and whole_workflow_update)
        if dependency_update and not can_update:
            message = f"DS {version} dependency update is not executable"
            raise ValueError(message)
        if can_update and not dependency_update:
            limited_versions.add(version)
    return frozenset(limited_versions)


def _validate_current_task_cleanup_contract(
    value: Mapping[str, object],
    *,
    versions: tuple[str, ...],
    runtime_operations: Mapping[str, frozenset[str]],
) -> _TaskDefinitionCleanupContract:
    _exact_keys(
        value,
        {
            "schema_version",
            "semantic_operation",
            "target_versions",
            "full_core_reconciliation_versions",
            "full_core_versions",
            "cross_process_recovery_versions",
            "profiles",
        },
        label="task-definition cleanup profile",
    )
    schema_version = _integer(
        value.get("schema_version"),
        label="task-definition cleanup profile schema_version",
    )
    _constant(
        schema_version,
        4,
        label="task-definition cleanup profile schema_version",
    )
    semantic_operation = _text(
        value.get("semantic_operation"),
        label="task-definition cleanup semantic operation",
    )
    target_versions = tuple(
        _text_sequence(
            value.get("target_versions"),
            label="task-definition cleanup target versions",
            allow_empty=False,
        )
    )
    if target_versions != tuple(
        version for version in versions if version in set(target_versions)
    ):
        message = "task-definition cleanup target versions are not canonical"
        raise ValueError(message)
    full_core_reconciliation_versions = _canonical_cleanup_version_subset(
        value.get("full_core_reconciliation_versions"),
        target_versions=target_versions,
        label="task-definition cleanup full-core reconciliation versions",
    )
    full_core_versions = _canonical_cleanup_version_subset(
        value.get("full_core_versions"),
        target_versions=target_versions,
        label="task-definition cleanup full-core versions",
    )
    cross_process_recovery_versions = _canonical_cleanup_version_subset(
        value.get("cross_process_recovery_versions"),
        target_versions=target_versions,
        label="task-definition cleanup cross-process recovery versions",
    )
    profiles = _mapping(
        value.get("profiles"),
        label="task-definition cleanup profiles",
    )
    if tuple(profiles) != target_versions:
        message = "task-definition cleanup profiles do not cover target versions"
        raise ValueError(message)
    profile_contracts = {
        version: _validate_current_task_cleanup_profile(
            profiles.get(version),
            version=version,
        )
        for version in target_versions
    }
    strategies = {version: profile_contracts[version][0] for version in target_versions}
    pre_delete_release_versions = frozenset(
        version
        for version in target_versions
        if profile_contracts[version][1] == "offline"
    )
    if any(strategies[version] != "direct-delete" for version in full_core_versions):
        message = "task-definition cleanup mutation versions are not direct-delete"
        raise ValueError(message)
    if any(
        strategies[version] != "direct-delete"
        for version in pre_delete_release_versions
    ):
        message = "task-definition pre-delete release versions are not direct-delete"
        raise ValueError(message)
    for version in versions:
        operations = runtime_operations[version]
        if (semantic_operation in operations) != (version in target_versions):
            message = "task-definition cleanup root and runtime ownership differ"
            raise ValueError(message)
    return _TaskDefinitionCleanupContract(
        semantic_operation=semantic_operation,
        target_versions=target_versions,
        full_core_reconciliation_versions=frozenset(full_core_reconciliation_versions),
        full_core_versions=frozenset(full_core_versions),
        cross_process_recovery_versions=frozenset(cross_process_recovery_versions),
        strategies=strategies,
        pre_delete_release_versions=pre_delete_release_versions,
    )


def _canonical_cleanup_version_subset(
    value: object,
    *,
    target_versions: tuple[str, ...],
    label: str,
) -> tuple[str, ...]:
    selected = tuple(_text_sequence(value, label=label, allow_empty=False))
    if selected != tuple(
        version for version in target_versions if version in set(selected)
    ):
        message = f"{label} are not canonical"
        raise ValueError(message)
    return selected


def _validate_current_task_cleanup_profile(
    value: object,
    *,
    version: str,
) -> tuple[str, str]:
    profile = _mapping(
        value,
        label=f"DS {version} task-definition cleanup profile",
    )
    _exact_keys(
        profile,
        {
            "strategy",
            "page_params_epoch",
            "page_model",
            "row_fields",
            "execute_types",
            "workflow_binding_fields",
            "history_model",
            "delete_response",
            "pre_delete_release",
        },
        label=f"DS {version} task-definition cleanup profile",
    )
    strategy = _text(
        profile.get("strategy"),
        label=f"DS {version} cleanup strategy",
    )
    if strategy not in {"direct-delete", "workflow-cascade-proof-only"}:
        message = f"DS {version} cleanup strategy is not recognized"
        raise ValueError(message)
    epoch = _text(
        profile.get("page_params_epoch"),
        label=f"DS {version} cleanup page_params_epoch",
    )
    if epoch not in {"task-search", "workflow-task-search", "execute-type"}:
        message = f"DS {version} cleanup page_params_epoch is not recognized"
        raise ValueError(message)
    _text(
        profile.get("page_model"),
        label=f"DS {version} cleanup page_model",
    )
    row_fields = _text_sequence(
        profile.get("row_fields"),
        label=f"DS {version} cleanup row_fields",
        allow_empty=False,
    )
    execute_types = _text_sequence(
        profile.get("execute_types"),
        label=f"DS {version} cleanup execute_types",
        allow_empty=True,
    )
    if len(row_fields) != 3:
        message = f"DS {version} cleanup row_fields must contain three fields"
        raise ValueError(message)
    if bool(execute_types) != (epoch == "execute-type"):
        message = f"DS {version} cleanup execute_types discriminator differs"
        raise ValueError(message)
    workflow_binding_value = profile.get("workflow_binding_fields")
    history_model_value = profile.get("history_model")
    delete_response = _text(
        profile.get("delete_response"),
        label=f"DS {version} cleanup delete_response",
    )
    pre_delete_release_value = profile.get("pre_delete_release")
    pre_delete_release = _text(
        pre_delete_release_value,
        label=f"DS {version} cleanup pre_delete_release",
    )
    if pre_delete_release not in {"none", "offline"}:
        message = f"DS {version} cleanup pre-delete release is not recognized"
        raise ValueError(message)
    if strategy == "direct-delete":
        structurally_valid = (
            workflow_binding_value is None
            and history_model_value is None
            and delete_response in {"void", "optional-process-definition"}
        )
    else:
        workflow_binding_fields = _text_sequence(
            workflow_binding_value,
            label=f"DS {version} cleanup workflow binding fields",
            allow_empty=False,
        )
        structurally_valid = (
            epoch == "execute-type"
            and len(workflow_binding_fields) == 4
            and isinstance(history_model_value, str)
            and bool(history_model_value.strip())
            and delete_response == "unavailable"
            and pre_delete_release == "none"
        )
    if not structurally_valid:
        message = f"DS {version} cleanup strategy structure drifted"
        raise ValueError(message)
    return strategy, pre_delete_release


def _validate_current_image_contract(
    value: Mapping[str, object],
    *,
    versions: tuple[str, ...],
) -> _ImageContract:
    _exact_keys(
        value,
        {
            "api_image_repositories",
            "managed_api_versions",
            "managed_lock_presence",
            "managed_provenance_labels",
            "schema_version",
        },
        label="conformance image contract",
    )
    _constant(value.get("schema_version"), 1, label="image contract schema_version")
    repositories = _mapping(
        value.get("api_image_repositories"),
        label="image contract API repositories",
    )
    if tuple(repositories) != versions:
        message = "image contract repositories do not cover the exact target versions"
        raise ValueError(message)
    normalized_repositories = {
        version: _text(repositories.get(version), label=f"DS {version} API repository")
        for version in versions
    }
    managed_versions = frozenset(
        _text_sequence(
            value.get("managed_api_versions"),
            label="image contract managed API versions",
            allow_empty=False,
        )
    )
    if not managed_versions.issubset(versions):
        message = "image contract managed versions are outside the target matrix"
        raise ValueError(message)
    raw_lock_presence = _mapping(
        value.get("managed_lock_presence"),
        label="image contract managed lock presence",
    )
    if set(raw_lock_presence) != set(managed_versions):
        message = "image contract lock presence must cover exact managed versions"
        raise ValueError(message)
    lock_presence: dict[str, Mapping[str, bool]] = {}
    for version in sorted(managed_versions):
        fields = _mapping(
            raw_lock_presence.get(version),
            label=f"DS {version} managed lock presence",
        )
        _exact_keys(
            fields,
            set(_MANAGED_LOCK_PRESENCE_FIELDS),
            label=f"DS {version} managed lock presence",
        )
        normalized_fields: dict[str, bool] = {}
        for field in _MANAGED_LOCK_PRESENCE_FIELDS:
            required = fields.get(field)
            if not isinstance(required, bool):
                message = f"DS {version} managed lock presence values must be boolean"
                raise TypeError(message)
            normalized_fields[field] = required
        lock_presence[version] = MappingProxyType(normalized_fields)
    managed_labels = frozenset(
        _text_sequence(
            value.get("managed_provenance_labels"),
            label="image contract managed provenance labels",
            allow_empty=False,
        )
    )
    return _ImageContract(
        repositories=MappingProxyType(normalized_repositories),
        managed_versions=managed_versions,
        managed_lock_presence=MappingProxyType(lock_presence),
        managed_labels=managed_labels,
    )


def _load_literal_assignment(path: Path, *, name: str) -> object:
    return _load_literal_assignments(path, names=frozenset({name}))[name]


def _load_literal_assignments(
    path: Path,
    *,
    names: frozenset[str],
) -> dict[str, object]:
    try:
        statements = ast.parse(
            path.read_text(encoding="utf-8"),
            filename=str(path),
        ).body
    except SyntaxError as error:
        message = f"{path}: generated source is not valid Python"
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
            message = f"{path}: {labels} must use one simple literal assignment"
            raise ValueError(message)
        target = statement.targets[0]
        if (
            not isinstance(target, ast.Name)
            or target.id not in names
            or stored_names != {target.id}
        ):
            labels = ", ".join(sorted(stored_names))
            message = f"{path}: {labels} must use one simple literal assignment"
            raise ValueError(message)
        name = target.id
        if name in assignments:
            message = (
                f"{path}: {name} must use one simple literal assignment; "
                "generated source assigns it more than once"
            )
            raise ValueError(message)
        try:
            assignments[name] = ast.literal_eval(statement.value)
        except (TypeError, ValueError) as error:
            message = f"{path}: {name} must be a literal assignment"
            raise ValueError(message) from error
    missing = sorted(names - assignments.keys())
    if missing:
        message = f"{path}: generated source lacks {', '.join(missing)}"
        raise ValueError(message)
    return assignments


def _load_strict_json_object(source: str, *, label: str) -> Mapping[str, object]:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                message = f"{label}: JSON key {key!r} is duplicated"
                raise ValueError(message)
            result[key] = value
        return result

    try:
        value: object = json.loads(source, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        message = f"{label} is not valid JSON: {error.msg}"
        raise ValueError(message) from error
    return _mapping(value, label=label)
