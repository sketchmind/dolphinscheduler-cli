"""Public validation API for artifact-bound named conformance-bundle receipts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Self

from live_gate.conformance_evidence.assessment import (
    _current_assessment,
)
from live_gate.conformance_evidence.current_truth import (
    _load_current_truth,
)
from live_gate.conformance_evidence.receipt import (
    _validate_conformance_bundle_evidence,
)
from live_gate.conformance_evidence.types import (
    ConformanceBundleAssessment,
    ConformanceBundleAssessmentBundle,
    ConformanceBundleEvidenceSummary,
    _Assessment,
    _CurrentTruth,
)
from live_gate.conformance_evidence.values import (
    canonical_conformance_bundle_receipt_digest,
)

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ConformanceBundleEvidenceValidator:
    """Prepared current-truth snapshot for one or more receipt validations."""

    _current: _CurrentTruth
    _assessment: _Assessment

    @classmethod
    def load(cls, *, source_root: Path = ROOT) -> Self:
        """Load and validate one current-truth snapshot without executing source."""
        current = _load_current_truth(source_root)
        return cls(
            _current=current,
            _assessment=_current_assessment(current),
        )

    @property
    def assessment(self) -> ConformanceBundleAssessment:
        """Return the immutable public assessment projected from this snapshot."""
        return _conformance_bundle_assessment(
            current=self._current,
            assessment=self._assessment,
        )

    def validate(
        self,
        receipt: object,
        *,
        expected_ds_version: str | None = None,
        expected_bundle: str | None = None,
        expected_wheel_filename: str | None = None,
        expected_wheel_sha256: str | None = None,
    ) -> ConformanceBundleEvidenceSummary:
        """Validate one receipt against this exact current-truth snapshot."""
        return _validate_conformance_bundle_evidence(
            receipt,
            expected_ds_version=expected_ds_version,
            expected_bundle=expected_bundle,
            expected_wheel_filename=expected_wheel_filename,
            expected_wheel_sha256=expected_wheel_sha256,
            current=self._current,
            assessment=self._assessment,
        )


def load_conformance_bundle_assessment(
    *,
    source_root: Path = ROOT,
) -> ConformanceBundleAssessment:
    """Safely load fully validated current bundle/version terminal facts."""
    return ConformanceBundleEvidenceValidator.load(
        source_root=source_root,
    ).assessment


def _conformance_bundle_assessment(
    *,
    current: _CurrentTruth,
    assessment: _Assessment,
) -> ConformanceBundleAssessment:
    bundles = tuple(
        ConformanceBundleAssessmentBundle(
            name=bundle.name,
            extends=bundle.extends,
            coordinates=MappingProxyType(
                {
                    version: bundle.coordinates[version].status
                    for version in current.versions
                }
            ),
        )
        for bundle in assessment.bundles.values()
    )
    return ConformanceBundleAssessment(
        versions=current.versions,
        bundles=bundles,
    )


def validate_conformance_bundle_evidence(
    receipt: object,
    *,
    expected_ds_version: str | None = None,
    expected_bundle: str | None = None,
    expected_wheel_filename: str | None = None,
    expected_wheel_sha256: str | None = None,
    source_root: Path = ROOT,
) -> ConformanceBundleEvidenceSummary:
    """Validate one passing named conformance-bundle live receipt."""
    return ConformanceBundleEvidenceValidator.load(
        source_root=source_root,
    ).validate(
        receipt,
        expected_ds_version=expected_ds_version,
        expected_bundle=expected_bundle,
        expected_wheel_filename=expected_wheel_filename,
        expected_wheel_sha256=expected_wheel_sha256,
    )


__all__ = [
    "ConformanceBundleAssessment",
    "ConformanceBundleAssessmentBundle",
    "ConformanceBundleEvidenceSummary",
    "ConformanceBundleEvidenceValidator",
    "canonical_conformance_bundle_receipt_digest",
    "load_conformance_bundle_assessment",
    "validate_conformance_bundle_evidence",
]
