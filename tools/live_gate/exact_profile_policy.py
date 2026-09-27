"""Reviewed exact-profile gate policies; action supersets do not imply equivalence.

The external-shell/v1 scenario preserves an exclusive pre-existing SHELL task,
including its non-owned state, dependencies and workflow topology. The generic
full_core/v1 scenario does not establish that restoration contract. Keep the
schema 3–7 receipt lineage and its ordered proofs under their original validator
until a replacement independently proves every obligation.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from exact_342_evidence import (
    CURRENT_EXACT_342_SCHEMA_VERSION,
    EXACT_342_GATE_RECIPES,
    SUPPORTED_EXACT_342_SCHEMA_VERSIONS,
    validate_exact_342_evidence_payload,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path


class ExactReceiptValidator(Protocol):
    """Validate one policy's receipt without trusting its claimed runtime roots."""

    def __call__(
        self,
        value: object,
        *,
        expected_cli_version: str,
        compiled_semantic_operations: frozenset[str] = frozenset(),
    ) -> None:
        """Reject invalid receipt fields, trace obligations or runtime ownership."""
        ...


@dataclass(frozen=True)
class ExactProfileGatePolicy:
    """Exact reviewed scenario and immutable receipt compatibility boundaries."""

    ds_version: str
    family: str
    support_level: str
    tested: bool
    scenario: str
    gate_id: str
    current_schema_version: int
    supported_schema_versions: frozenset[int]
    recipes: tuple[tuple[str, str], ...]
    unclaimed_read_actions: tuple[str, ...]
    validate_receipt: ExactReceiptValidator

    @property
    def actions(self) -> tuple[str, ...]:
        """Return the ordered recipe closure required by this exact gate."""
        return tuple(action for action, _operation in self.recipes)

    @property
    def wheel_manifest_path(self) -> str:
        """Return the exact generated manifest inside an installed wheel."""
        slug = self.ds_version.replace(".", "_")
        return f"dsctl/generated/versions/ds_{slug}/_manifest.py"

    @property
    def contract_operation_test(self) -> str:
        """Retain the source-contract test identity serialized in receipts."""
        slug = self.ds_version.replace(".", "_")
        return f"tests/upstream/test_ds_{slug}.py"

    def evidence_directory(self, source_root: Path) -> Path:
        """Locate current receipts by scenario and exact version."""
        scenario_name = self.scenario.partition("/")[0]
        return (
            source_root
            / "docs"
            / "development"
            / "live-evidence"
            / scenario_name
            / self.ds_version
        )


EXACT_PROFILE_GATE_POLICIES: Mapping[str, ExactProfileGatePolicy] = MappingProxyType(
    {
        "3.4.2": ExactProfileGatePolicy(
            ds_version="3.4.2",
            family="workflow-3.3-plus",
            support_level="experimental",
            tested=False,
            scenario="external-shell/v1",
            gate_id="exact-profile-3.4.2",
            current_schema_version=CURRENT_EXACT_342_SCHEMA_VERSION,
            supported_schema_versions=SUPPORTED_EXACT_342_SCHEMA_VERSIONS,
            recipes=EXACT_342_GATE_RECIPES,
            unclaimed_read_actions=(
                "project.get",
                "project.list",
                "workflow.get",
                "workflow.list",
            ),
            validate_receipt=validate_exact_342_evidence_payload,
        ),
    }
)

# Release publication still requires this exact external-fixture restoration
# proof in addition to the all-version named-bundle corpus.
CURRENT_RELEASE_EXACT_GATE_POLICY = EXACT_PROFILE_GATE_POLICIES["3.4.2"]


def exact_profile_gate_policy(ds_version: str) -> ExactProfileGatePolicy:
    """Select an explicitly reviewed policy without inferring adjacent support."""
    try:
        return EXACT_PROFILE_GATE_POLICIES[ds_version]
    except KeyError as error:
        message = f"No reviewed exact-profile gate policy for DS {ds_version!r}"
        raise ValueError(message) from error
