"""CLI entry point for policy-selected exact-profile promotion evidence."""

from __future__ import annotations

from live_gate.exact_profile_promotion_evidence import (
    run_exact_profile_promotion_evidence_cli,
)


def main(argv: list[str] | None = None) -> int:
    """Run the exact-profile promotion evidence checker."""
    return run_exact_profile_promotion_evidence_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
