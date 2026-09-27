"""CLI wrapper for the exact-version conformance receipt corpus validator."""

from __future__ import annotations

from live_gate.conformance_bundle_corpus import (
    DEFAULT_EVIDENCE_ROOT,
    ConformanceBundleCorpus,
    ConformanceBundleCorpusReceipt,
    check_conformance_bundle_evidence_corpus,
    run_conformance_bundle_evidence_cli,
)


def main(argv: list[str] | None = None) -> int:
    """Validate the tracked conformance receipt corpus."""
    return run_conformance_bundle_evidence_cli(
        argv,
        default_evidence_root=DEFAULT_EVIDENCE_ROOT,
    )


__all__ = [
    "DEFAULT_EVIDENCE_ROOT",
    "ConformanceBundleCorpus",
    "ConformanceBundleCorpusReceipt",
    "check_conformance_bundle_evidence_corpus",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
