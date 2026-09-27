"""Analyze DolphinScheduler contract drift across upstream versions."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ds_codegen.api import (
    analyze_contract_versions,
    parse_contract_inputs,
    render_version_diff_reports,
)
from ds_codegen.source_matrix import load_exact_source_specs


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the version-diff analyzer."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare generated DolphinScheduler controller contracts across "
            "versions. Inputs can be existing snapshot JSON files or checked-out "
            "DS source trees from git tags/worktrees."
        )
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="Add one existing generate_ds_contract.py JSON snapshot.",
    )
    parser.add_argument(
        "--ds-source",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help=(
            "Add one DolphinScheduler source tree. The tool runs the codegen "
            "extractor against this tree before comparing."
        ),
    )
    parser.add_argument(
        "--scope",
        choices=("full", "cli"),
        default="full",
        help=(
            "Compare the full source inventory (default), or the reviewed base "
            "version's CLI closure. CLI scope requires current exact snapshots."
        ),
    )
    parser.add_argument(
        "--source-manifest",
        type=Path,
        help="Add an exact-source inventory; only selected base/targets are loaded.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root for source-manifest paths.",
    )
    parser.add_argument(
        "--base",
        required=True,
        help="Input label to use as the comparison base.",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help=(
            "Input label to compare against --base. Repeat for multiple targets; "
            "when omitted, all non-base inputs are compared."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
        help="Report format.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the report to a file instead of stdout.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50,
        help=(
            "Maximum added/removed/changed items per Markdown section. Use 0 "
            "for no limit. JSON output is always complete."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the analyzer and return a process exit code."""
    args = build_parser().parse_args(argv)
    try:
        sources = list(args.ds_source)
        if args.source_manifest is not None:
            repo_root = args.repo_root.resolve()
            manifest = args.source_manifest
            if not manifest.is_absolute():
                manifest = repo_root / manifest
            sources.extend(
                f"{spec.version}={spec.source_root}"
                for spec in load_exact_source_specs(repo_root, manifest)
            )
        inputs = parse_contract_inputs(
            snapshot_values=args.snapshot,
            source_values=sources,
            minimum_count=2,
            require_version_labels=args.scope == "cli",
        )
        reports = analyze_contract_versions(
            inputs,
            base_label=args.base,
            target_labels=args.target,
            scope=args.scope,
        )
        text = render_version_diff_reports(
            reports,
            output_format=args.format,
            max_items=args.max_items,
        )
        if args.output is None:
            sys.stdout.write(text)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8")
    except (FileNotFoundError, KeyError, TypeError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
