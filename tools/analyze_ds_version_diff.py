"""Analyze DolphinScheduler contract drift across upstream versions."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ds_codegen.version_diff import (
    build_snapshot_from_ds_source,
    compare_contract_snapshots,
    load_snapshot,
    render_markdown_report,
    report_to_json_text,
)

if TYPE_CHECKING:
    from ds_codegen.ir import ContractSnapshot

InputKind = Literal["snapshot", "ds-source"]


@dataclass(frozen=True)
class SnapshotInput:
    """One labeled source of a DS contract snapshot."""

    label: str
    path: Path
    kind: InputKind


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
        inputs = _parse_inputs(args.snapshot, args.ds_source)
        snapshots = _load_inputs(inputs)
        reports = _build_reports(
            snapshots,
            base_label=args.base,
            target_labels=args.target,
        )
        text = _render_reports(
            reports,
            output_format=args.format,
            max_items=args.max_items,
        )
        if args.output is None:
            sys.stdout.write(text)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8")
    except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def _parse_inputs(
    snapshot_values: list[str],
    ds_source_values: list[str],
) -> list[SnapshotInput]:
    inputs = [
        SnapshotInput(label=label, path=path, kind="snapshot")
        for label, path in (_parse_labeled_path(item) for item in snapshot_values)
    ]
    inputs.extend(
        SnapshotInput(label=label, path=path, kind="ds-source")
        for label, path in (_parse_labeled_path(item) for item in ds_source_values)
    )
    labels = [item.label for item in inputs]
    duplicate_labels = sorted({label for label in labels if labels.count(label) > 1})
    if duplicate_labels:
        message = f"duplicate input labels: {', '.join(duplicate_labels)}"
        raise ValueError(message)
    if len(inputs) < 2:
        message = "at least two inputs are required"
        raise ValueError(message)
    return inputs


def _parse_labeled_path(value: str) -> tuple[str, Path]:
    label, separator, raw_path = value.partition("=")
    if not separator or not label.strip() or not raw_path.strip():
        message = f"invalid input {value!r}; expected LABEL=PATH"
        raise ValueError(message)
    return label.strip(), Path(raw_path).expanduser()


def _load_inputs(inputs: list[SnapshotInput]) -> dict[str, ContractSnapshot]:
    snapshots: dict[str, ContractSnapshot] = {}
    for item in inputs:
        if item.kind == "snapshot":
            snapshots[item.label] = load_snapshot(item.path)
        else:
            snapshots[item.label] = build_snapshot_from_ds_source(item.path)
    return snapshots


def _build_reports(
    snapshots: dict[str, ContractSnapshot],
    *,
    base_label: str,
    target_labels: list[str],
) -> list[dict[str, Any]]:
    if base_label not in snapshots:
        message = f"unknown base label {base_label!r}"
        raise KeyError(message)
    selected_targets = target_labels or [
        label for label in snapshots if label != base_label
    ]
    reports = []
    for target_label in selected_targets:
        if target_label not in snapshots:
            message = f"unknown target label {target_label!r}"
            raise KeyError(message)
        reports.append(
            compare_contract_snapshots(
                base_label=base_label,
                base=snapshots[base_label],
                target_label=target_label,
                target=snapshots[target_label],
            )
        )
    return reports


def _render_reports(
    reports: list[dict[str, Any]],
    *,
    output_format: str,
    max_items: int,
) -> str:
    if output_format == "json":
        if len(reports) == 1:
            return report_to_json_text(reports[0])
        return report_to_json_text({"reports": reports})
    return (
        "\n".join(
            render_markdown_report(report, max_items=max_items).rstrip()
            for report in reports
        )
        + "\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
