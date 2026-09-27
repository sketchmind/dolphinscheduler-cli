"""Report terminal support coverage for every exact DS profile coordinate."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ds_codegen.support_coverage import (
    analyze_support_coverage,
    render_support_coverage,
)
from dsctl.generated.version_profiles import PROFILE_DATA


def build_parser() -> argparse.ArgumentParser:
    """Build the exact support-coverage parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Classify every materialized version/action coordinate as supported, "
            "reviewed upstream-limited/absent, or still pending."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write deterministic JSON here instead of stdout.",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit 1 while any exact version/action coordinate is still pending.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Render current coverage and optionally enforce terminal completeness."""
    args = build_parser().parse_args(argv)
    try:
        report = analyze_support_coverage(PROFILE_DATA)
        rendered = render_support_coverage(report)
        if args.output is None:
            sys.stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.require_complete and report["complete"] is not True:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
