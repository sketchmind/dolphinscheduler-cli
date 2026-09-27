"""Analyze reviewed semantic bindings against an extracted DS inventory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ds_codegen.compatibility_impact import (
    READ_OPERATION_BINDINGS,
    REVIEWED_DS_VERSIONS,
    ReviewedBinding,
    analyze_semantic_bindings,
)

SEMANTIC_READ_OPERATIONS = tuple(
    sorted(
        {
            semantic_operation
            for bindings in READ_OPERATION_BINDINGS.values()
            for semantic_operation in bindings
        }
    )
)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate reviewed project/workflow read bindings against an exact "
            "source inventory and group matching source contracts. Matching "
            "groups are compatibility candidates, not support claims."
        )
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        required=True,
        help="JSON produced by generate_ds_contract_inventory.py.",
    )
    parser.add_argument(
        "--version",
        action="append",
        choices=REVIEWED_DS_VERSIONS,
        default=[],
        help="Analyze one exact version; repeat as needed. Defaults to all.",
    )
    parser.add_argument(
        "--semantic-operation",
        action="append",
        choices=SEMANTIC_READ_OPERATIONS,
        default=[],
        help="Analyze one stable read operation; repeat as needed. Defaults to all.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON to this path instead of stdout.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the semantic-binding impact analyzer."""
    args = build_parser().parse_args(argv)
    try:
        inventory = _load_inventory(args.inventory)
        bindings = _select_bindings(
            versions=args.version,
            semantic_operations=args.semantic_operation,
        )
        report = analyze_semantic_bindings(inventory, bindings=bindings)
        rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.output is None:
            sys.stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0 if report["complete"] else 1


def _load_inventory(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"inventory must contain a JSON object: {path}"
        raise TypeError(message)
    return payload


def _select_bindings(
    *,
    versions: list[str],
    semantic_operations: list[str],
) -> dict[str, dict[str, ReviewedBinding]]:
    selected_versions = versions or list(REVIEWED_DS_VERSIONS)
    selected_operations = set(semantic_operations or SEMANTIC_READ_OPERATIONS)
    return {
        version: {
            semantic_operation: source_operation
            for semantic_operation, source_operation in READ_OPERATION_BINDINGS[
                version
            ].items()
            if semantic_operation in selected_operations
        }
        for version in selected_versions
    }


if __name__ == "__main__":
    raise SystemExit(main())
