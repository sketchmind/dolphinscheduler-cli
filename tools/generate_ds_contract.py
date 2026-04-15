from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

from ds_codegen.api import (
    build_contract_snapshot,
    write_contract_snapshot,
    write_generated_package,
    write_python_registry,
    write_requests_client,
    write_requests_example,
    write_requests_package,
)

if TYPE_CHECKING:
    from collections.abc import Callable


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a raw DS 3.4.1 controller contract snapshot"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing references/dolphinscheduler",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/ds_contract/ds341_raw_contract.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--python-output",
        type=Path,
        default=None,
        help="Optional Python registry output path",
    )
    parser.add_argument(
        "--requests-example-output",
        type=Path,
        default=None,
        help="Optional requests-based generated example output path",
    )
    parser.add_argument(
        "--requests-client-output",
        type=Path,
        default=None,
        help="Optional full requests-based generated client output path",
    )
    parser.add_argument(
        "--package-output",
        type=Path,
        default=None,
        help=(
            "Optional output root for a versioned generated package tree "
            "(writes generated/versions/ds_<version>/... only)"
        ),
    )
    parser.add_argument(
        "--requests-package-output",
        type=Path,
        default=None,
        help=(
            "Optional output root for a full requests-based sample package tree "
            "(writes generated/versions/ds_<version>/... and upstream/...)"
        ),
    )
    return parser


def _resolve_path(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def _write_if_requested(
    repo_root: Path,
    path: Path | None,
    writer: Callable[..., None],
    *writer_args: object,
) -> None:
    if path is None:
        return
    writer(*writer_args, _resolve_path(repo_root, path))


def main() -> None:
    args = _build_parser().parse_args()

    repo_root = args.repo_root.resolve()
    snapshot = build_contract_snapshot(repo_root)
    output_path = _resolve_path(repo_root, args.output)
    write_contract_snapshot(snapshot, output_path)
    _write_if_requested(repo_root, args.python_output, write_python_registry, snapshot)
    _write_if_requested(
        repo_root,
        args.requests_example_output,
        write_requests_example,
        snapshot,
    )
    _write_if_requested(
        repo_root,
        args.requests_client_output,
        write_requests_client,
        snapshot,
    )
    _write_if_requested(
        repo_root,
        args.package_output,
        write_generated_package,
        repo_root,
        snapshot,
    )
    _write_if_requested(
        repo_root,
        args.requests_package_output,
        write_requests_package,
        repo_root,
        snapshot,
    )
    print(
        f"wrote {snapshot.operation_count} operations, {snapshot.dto_count} dtos, "
        f"{snapshot.model_count} models, and {snapshot.enum_count} enums to "
        f"{output_path}"
    )


if __name__ == "__main__":
    main()
