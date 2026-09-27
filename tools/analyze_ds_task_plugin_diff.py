"""Analyze DolphinScheduler task-plugin contract drift across versions."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ds_codegen.api import (
    analyze_task_plugin_versions,
    apply_task_plugin_impact_review,
    load_task_plugin_impact_review,
    parse_contract_inputs,
    render_task_plugin_diff_reports,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the task-plugin diff command-line parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare server-side DolphinScheduler task-plugin authoring "
            "contracts. Inputs may be cached task-plugin snapshots or checked-"
            "out DS source trees. Structural changes remain review-required; "
            "the tool does not infer CLI policy from Java validation."
        )
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="Add one cached task-plugin contract snapshot.",
    )
    parser.add_argument(
        "--ds-source",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="Add one checked-out DolphinScheduler source tree.",
    )
    parser.add_argument("--base", required=True, help="Comparison base label.")
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Target label; repeat or omit to compare every non-base input.",
    )
    parser.add_argument(
        "--task-type",
        action="append",
        default=[],
        help="Limit source extraction to one task type; repeat as needed.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
        help="Report format.",
    )
    parser.add_argument(
        "--review",
        type=Path,
        help=(
            "Apply one source-fingerprint-guarded impact review. This requires "
            "a single base/target report."
        ),
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
        help="Maximum review items in Markdown; 0 means unlimited.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the task-plugin diff and return a process exit code."""
    args = build_parser().parse_args(argv)
    try:
        inputs = parse_contract_inputs(
            snapshot_values=args.snapshot,
            source_values=args.ds_source,
            minimum_count=2,
            require_version_labels=False,
        )
        reports = analyze_task_plugin_versions(
            inputs,
            base_label=args.base,
            target_labels=args.target,
            task_types=args.task_type or None,
        )
        if args.review is not None:
            _require_single_review_report(reports)
            reports = [
                apply_task_plugin_impact_review(
                    reports[0],
                    load_task_plugin_impact_review(args.review),
                    source_roots={
                        item.label: item.path
                        for item in inputs
                        if item.kind == "ds-source"
                    },
                )
            ]
        output = render_task_plugin_diff_reports(
            reports,
            output_format=args.format,
            max_items=args.max_items,
        )
        if args.output is None:
            sys.stdout.write(output)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(output, encoding="utf-8")
    except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.review is None:
        return 0
    return 0 if all(report.get("review_complete") is True for report in reports) else 1


def _require_single_review_report(reports: list[dict[str, object]]) -> None:
    if len(reports) == 1:
        return
    message = "--review requires exactly one base/target report"
    raise ValueError(message)


if __name__ == "__main__":
    raise SystemExit(main())
