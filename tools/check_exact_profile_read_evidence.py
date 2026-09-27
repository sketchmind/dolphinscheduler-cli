"""CLI wrapper for the exact-profile read promotion corpus validator."""

from __future__ import annotations

from live_gate.exact_profile_read_corpus import (
    DEFAULT_EVIDENCE_ROOT,
    ExactProfileReadCorpus,
    check_exact_profile_read_evidence_corpus,
    run_exact_profile_read_evidence_cli,
)


def main(argv: list[str] | None = None) -> int:
    """Run the promotion corpus validator from the tools entry point."""
    return run_exact_profile_read_evidence_cli(
        argv,
        default_evidence_root=DEFAULT_EVIDENCE_ROOT,
    )


__all__ = [
    "DEFAULT_EVIDENCE_ROOT",
    "ExactProfileReadCorpus",
    "check_exact_profile_read_evidence_corpus",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
