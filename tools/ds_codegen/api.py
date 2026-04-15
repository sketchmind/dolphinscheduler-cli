from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ds_codegen.extract import build_contract_snapshot
from ds_codegen.render import (
    write_python_registry,
    write_requests_client,
    write_requests_example,
    write_upstream_adapter_package,
)
from ds_codegen.render.package import (
    write_generated_package as _write_generated_package,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot


def write_contract_snapshot(snapshot: ContractSnapshot, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            snapshot.to_json_dict(),
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
        + "\n"
    )


def write_generated_package(
    repo_root: Path,
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    _write_generated_package(repo_root, snapshot, output_root)


def write_requests_package(
    repo_root: Path,
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    write_generated_package(repo_root, snapshot, output_root)
    write_upstream_adapter_package(snapshot, output_root)


__all__ = [
    "build_contract_snapshot",
    "write_contract_snapshot",
    "write_generated_package",
    "write_python_registry",
    "write_requests_client",
    "write_requests_example",
    "write_requests_package",
]
