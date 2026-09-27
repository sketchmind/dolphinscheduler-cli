"""Immutable records exchanged by the internal conformance evidence validators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class ConformanceBundleEvidenceSummary:
    """Validated identity of one passing named-bundle receipt."""

    schema_version: int
    ds_version: str
    bundle: str
    wheel_filename: str
    wheel_sha256: str
    receipt_digest: str
    required_actions: tuple[str, ...]
    authoring_mode: str


@dataclass(frozen=True)
class ConformanceBundleAssessmentBundle:
    """Validated identity and exact coordinates of one current named bundle."""

    name: str
    extends: tuple[str, ...]
    coordinates: Mapping[str, str]


@dataclass(frozen=True)
class ConformanceBundleAssessment:
    """Read-only current generated conformance facts loaded without execution."""

    versions: tuple[str, ...]
    bundles: tuple[ConformanceBundleAssessmentBundle, ...]


@dataclass(frozen=True)
class _AssessmentCoordinate:
    support_level: str
    tested: bool
    status: str


@dataclass(frozen=True)
class _AssessmentBundle:
    name: str
    bundle_digest: str
    extends: tuple[str, ...]
    direct_actions: tuple[str, ...]
    required_actions: tuple[str, ...]
    coordinates: Mapping[str, _AssessmentCoordinate]


@dataclass(frozen=True)
class _Assessment:
    schema_version: int
    catalog_digest: str
    assessment_digest: str
    bundles: Mapping[str, _AssessmentBundle]


@dataclass(frozen=True)
class _CurrentTruth:
    cli_version: str
    versions: tuple[str, ...]
    profiles: Mapping[str, object]
    contracts: Mapping[str, Mapping[str, object]]
    runtime_operations: Mapping[str, frozenset[str]]
    assessment: Mapping[str, object]
    image_contract: _ImageContract
    task_cleanup: _TaskDefinitionCleanupContract
    dependency_update_upstream_limited_versions: frozenset[str]


@dataclass(frozen=True)
class _ImageContract:
    repositories: Mapping[str, str]
    managed_versions: frozenset[str]
    managed_lock_presence: Mapping[str, Mapping[str, bool]]
    managed_labels: frozenset[str]


@dataclass(frozen=True)
class _TaskDefinitionCleanupContract:
    semantic_operation: str
    target_versions: tuple[str, ...]
    full_core_reconciliation_versions: frozenset[str]
    full_core_versions: frozenset[str]
    cross_process_recovery_versions: frozenset[str]
    strategies: Mapping[str, str]
    pre_delete_release_versions: frozenset[str]


@dataclass(frozen=True)
class _ProfileBinding:
    source: tuple[str, str, str]
    support_level: str
    tested: bool


@dataclass(frozen=True)
class _ContractBinding:
    schema_version: int
    ds_version: str
    selection: str
    semantic_operations: tuple[str, ...]
    source: tuple[str, str, str]
    source_contract_digest: str
    rendered_contract_digest: str
    operation_count: int


@dataclass(frozen=True)
class _ActionRecipeBinding:
    semantic_operation: str
    availability: str
    execution_mode: str
    verification: str
    build_status: str
    fingerprints: Mapping[str, object]


@dataclass(frozen=True)
class _TraceEntry:
    sequence: int
    action: str
    argv_shape: str
    exit_code: int
    ok: bool
    assertions: tuple[str, ...]
    outcomes: tuple[str, ...]
    error_type: str | None
    subject_action: str | None
