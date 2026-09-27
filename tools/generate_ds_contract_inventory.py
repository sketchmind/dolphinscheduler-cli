"""Generate a deterministic multi-version DolphinScheduler contract inventory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ds_codegen.api import generate_contract_inventory, parse_contract_inputs


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Build one deterministic source-contract inventory from existing "
            "snapshots or checked-out DolphinScheduler source trees. Each input "
            "is processed independently so one extraction failure does not hide "
            "the remaining diagnostics."
        )
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add a generate_ds_contract.py JSON snapshot.",
    )
    parser.add_argument(
        "--ds-source",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add a checked-out DolphinScheduler source tree to extract.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        help=(
            "Persist each successfully loaded exact snapshot for cached later "
            "inventory or diff runs."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Build the inventory, retaining diagnostics for every failed input."""
    args = build_parser().parse_args(argv)
    try:
        inputs = parse_contract_inputs(
            snapshot_values=args.snapshot,
            source_values=args.ds_source,
            minimum_count=1,
            require_version_labels=True,
        )
        report = generate_contract_inventory(
            inputs,
            snapshot_dir=args.snapshot_dir,
            progress=lambda message: print(message, file=sys.stderr),
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


if __name__ == "__main__":
    raise SystemExit(main())
