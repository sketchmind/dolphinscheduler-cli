"""Build the all-version mechanical candidate matrix for stable dsctl actions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.contract_inputs import ContractInput, parse_contract_inputs
from ds_codegen.stable_action_dependencies import (
    DEFAULT_BASELINE_VERSION,
    analyze_repository_stable_action_dependencies,
)
from ds_codegen.stable_action_version_matrix import (
    analyze_stable_action_version_matrix,
    render_stable_action_version_matrix,
)

DEFAULT_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT_DIRECTORY = Path("build/ds_contract/snapshots-v2")


def build_parser() -> argparse.ArgumentParser:
    """Build the stable-action version-matrix parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Project every stable dsctl action's exact wire dependencies across "
            "all reviewed exact DolphinScheduler contracts. The output contains "
            "mechanical candidates only and never declares semantic support."
        )
    )
    parser.add_argument(
        "--baseline-version",
        choices=REVIEWED_DS_VERSIONS,
        help=(
            "Exact repository baseline (default 3.4.1); when reading a dependency "
            "report, its validated baseline is authoritative."
        ),
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=DEFAULT_REPOSITORY_ROOT,
        help="Repository root to analyze. Defaults to this tool's checkout.",
    )
    parser.add_argument(
        "--dependency-report",
        type=Path,
        help=(
            "Read a stable-action dependency report instead of analyzing the "
            "repository source."
        ),
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add one exact cached contract snapshot; specify all reviewed versions.",
    )
    parser.add_argument(
        "--ds-source",
        action="append",
        default=[],
        metavar="VERSION=PATH",
        help="Add one clean exact-tag DS source tree; specify all reviewed versions.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIRECTORY,
        help=(
            "Default directory containing ds-VERSION-contract.json snapshots; "
            "ignored when --snapshot or --ds-source is supplied."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write deterministic JSON here instead of stdout.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the matrix analysis and return a structural analysis status."""
    args = build_parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    try:
        dependencies = _dependency_report(
            repo_root,
            dependency_report=args.dependency_report,
            baseline_version=args.baseline_version,
        )
        inputs = _contract_inputs(
            repo_root,
            snapshot_values=args.snapshot,
            source_values=args.ds_source,
            snapshot_dir=args.snapshot_dir,
        )
        report = analyze_stable_action_version_matrix(
            dependency_report=dependencies,
            inputs=inputs,
        )
        rendered = render_stable_action_version_matrix(report)
        if args.output is None:
            sys.stdout.write(rendered)
        else:
            output = _resolve(repo_root, args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0 if report["complete"] else 1


def _dependency_report(
    repo_root: Path,
    *,
    dependency_report: Path | None,
    baseline_version: str | None = None,
) -> dict[str, object]:
    if dependency_report is None:
        return analyze_repository_stable_action_dependencies(
            repo_root, baseline_version=baseline_version or DEFAULT_BASELINE_VERSION
        )
    path = _resolve(repo_root, dependency_report)
    loaded: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = "stable-action dependency report must be a JSON object"
        raise TypeError(message)
    if baseline_version is not None:
        baseline = loaded.get("baseline")
        if (
            not isinstance(baseline, dict)
            or baseline.get("ds_version") != baseline_version
        ):
            message = "selected baseline differs from the dependency report baseline"
            raise ValueError(message)
    return loaded


def _contract_inputs(
    repo_root: Path,
    *,
    snapshot_values: list[str],
    source_values: list[str],
    snapshot_dir: Path,
) -> list[ContractInput]:
    if snapshot_values or source_values:
        inputs = parse_contract_inputs(
            snapshot_values=snapshot_values,
            source_values=source_values,
            minimum_count=len(REVIEWED_DS_VERSIONS),
            require_version_labels=True,
        )
        return [
            ContractInput(item.label, _resolve(repo_root, item.path), item.kind)
            for item in inputs
        ]
    directory = _resolve(repo_root, snapshot_dir)
    return [
        ContractInput(
            version,
            directory / f"ds-{version}-contract.json",
            "snapshot",
        )
        for version in REVIEWED_DS_VERSIONS
    ]


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


if __name__ == "__main__":
    raise SystemExit(main())
