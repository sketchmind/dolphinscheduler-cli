from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from ds_codegen.api import (
    build_contract_snapshot,
    configured_runtime_contract_slice,
    contract_snapshot_digest,
    load_contract_input,
    parse_contract_inputs,
    write_contract_snapshot,
    write_generated_package,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a raw DolphinScheduler controller contract snapshot"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing references/dolphinscheduler",
    )
    parser.add_argument(
        "--ds-source",
        default=None,
        metavar="VERSION=PATH",
        help=(
            "Optional clean exact-tag DolphinScheduler source input. Relative "
            "paths resolve from --repo-root."
        ),
    )
    parser.add_argument(
        "--runtime-slice",
        action="store_true",
        help=(
            "Generate only the reviewed semantic runtime closure configured "
            "for the extracted exact version."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/ds_contract/ds_raw_contract.json"),
        help="Output JSON path",
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
    return parser


def _resolve_path(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    provenance: dict[str, object] | None = None
    if args.ds_source is None:
        snapshot = build_contract_snapshot(repo_root)
    else:
        source_input = parse_contract_inputs(
            snapshot_values=[],
            source_values=[args.ds_source],
            minimum_count=1,
            require_version_labels=True,
        )[0]
        source_input = replace(
            source_input,
            path=_resolve_path(repo_root, source_input.path),
        )
        loaded = load_contract_input(source_input, require_exact=True)
        snapshot = loaded.snapshot
        provenance = loaded.provenance

    if args.runtime_slice:
        snapshot = configured_runtime_contract_slice(snapshot)
        if provenance is not None:
            provenance = {
                **provenance,
                "contract_digest": contract_snapshot_digest(snapshot),
            }

    output_path = _resolve_path(repo_root, args.output)
    write_contract_snapshot(snapshot, output_path, provenance=provenance)
    if args.package_output is not None:
        write_generated_package(
            snapshot,
            _resolve_path(repo_root, args.package_output),
        )
    print(
        f"wrote {snapshot.operation_count} operations, {snapshot.dto_count} dtos, "
        f"{snapshot.model_count} models, and {snapshot.enum_count} enums to "
        f"{output_path}"
    )


if __name__ == "__main__":
    main()
