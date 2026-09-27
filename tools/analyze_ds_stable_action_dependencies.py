from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.stable_action_dependencies import (
    DEFAULT_BASELINE_VERSION,
    analyze_repository_stable_action_dependencies,
    render_stable_action_dependency_report,
)

DEFAULT_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    """Build the stable-action dependency analyzer parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Trace every stable dsctl action through services and exact "
            "generated operations. Unresolved evidence makes the report "
            "incomplete and exits with status 1."
        )
    )
    parser.add_argument(
        "--baseline-version",
        choices=REVIEWED_DS_VERSIONS,
        default=DEFAULT_BASELINE_VERSION,
        help="Exact DS implementation to use as the dependency baseline.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=DEFAULT_REPOSITORY_ROOT,
        help="Repository root to analyze. Defaults to this tool's checkout.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write structured JSON here instead of stdout.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the stable-action dependency analyzer."""
    args = build_parser().parse_args(argv)
    repository_root = args.repo_root.resolve()
    try:
        report = analyze_repository_stable_action_dependencies(
            repository_root, baseline_version=args.baseline_version
        )
        rendered = render_stable_action_dependency_report(report)
        if args.output is None:
            sys.stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
    except (OSError, SyntaxError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
