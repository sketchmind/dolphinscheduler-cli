"""Generate an exact multi-version DolphinScheduler task-plugin inventory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ds_codegen.api import (
    ContractInput,
    generate_task_plugin_inventory,
    parse_contract_inputs,
)
from ds_codegen.source_matrix import (
    DEFAULT_EXACT_SOURCE_MANIFEST,
    load_exact_source_specs,
)

ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    """Build the task-plugin inventory command-line parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Build a deterministic task-plugin inventory from clean exact-tag "
            "DS sources or provenance-bearing cached snapshots."
        )
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add one cached task-plugin contract snapshot.",
    )
    parser.add_argument(
        "--ds-source",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add one clean exact-tag DolphinScheduler source tree.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=ROOT,
        help="Repository root used to resolve source-manifest paths.",
    )
    parser.add_argument(
        "--source-manifest",
        type=Path,
        help=(
            "Load the complete exact source matrix instead of repeating "
            "--ds-source; the tracked manifest is "
            f"{DEFAULT_EXACT_SOURCE_MANIFEST.relative_to(ROOT)}."
        ),
    )
    parser.add_argument(
        "--task-type",
        action="append",
        default=[],
        help="Limit extraction to one task type; repeat as needed.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        help="Persist each exact extracted task-plugin snapshot here.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Build the inventory and retain diagnostics for each failed input."""
    args = build_parser().parse_args(argv)
    try:
        inputs = _load_inputs(args)
        report = generate_task_plugin_inventory(
            inputs,
            task_types=args.task_type or None,
            snapshot_dir=args.snapshot_dir,
        )
        output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.output is None:
            sys.stdout.write(output)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(output, encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0 if report["complete"] else 1


def _load_inputs(args: argparse.Namespace) -> list[ContractInput]:
    if args.source_manifest is None:
        return parse_contract_inputs(
            snapshot_values=args.snapshot,
            source_values=args.ds_source,
            minimum_count=1,
            require_version_labels=True,
        )
    if args.snapshot or args.ds_source:
        message = "--source-manifest cannot be combined with --snapshot or --ds-source"
        raise ValueError(message)
    repo_root = args.repo_root.resolve()
    manifest_path = args.source_manifest
    if not manifest_path.is_absolute():
        manifest_path = repo_root / manifest_path
    specs = load_exact_source_specs(repo_root, manifest_path)
    return [
        ContractInput(spec.version, spec.source_root, "ds-source") for spec in specs
    ]


if __name__ == "__main__":
    raise SystemExit(main())
