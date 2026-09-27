"""Build deterministic inventories from extracted DS source contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from typing import TYPE_CHECKING, Any

from ds_codegen.contract_inputs import (
    ExactProvenanceError,
    LoadedContract,
    contract_snapshot_digest,
    load_contract_input,
    natural_version_key,
    require_exact_provenance,
    write_contract_snapshot_document,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from ds_codegen.contract_inputs import ContractInput
    from ds_codegen.ir import ContractSnapshot

INVENTORY_SCHEMA_VERSION = 2
_DOCUMENTATION_KEYS = frozenset(
    {
        "description",
        "documentation",
        "example",
        "parameter_docs",
        "returns_doc",
        "summary",
    }
)


def build_contract_inventory(
    contracts: Sequence[LoadedContract],
) -> dict[str, object]:
    """Return a deterministic structural inventory for exact DS versions."""
    labels = [contract.label for contract in contracts]
    duplicates = sorted({label for label in labels if labels.count(label) > 1})
    if duplicates:
        message = f"duplicate snapshot labels: {', '.join(duplicates)}"
        raise ValueError(message)

    targets = [_build_target(contract) for contract in contracts]
    targets.sort(key=lambda target: natural_version_key(str(target["ds_version"])))
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "kind": "dolphinscheduler-source-contract-inventory",
        "targets": targets,
    }


def generate_contract_inventory(
    inputs: Sequence[ContractInput],
    *,
    snapshot_dir: Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Compile exact inputs independently and retain bounded diagnostics."""
    contracts: list[LoadedContract] = []
    diagnostics: list[dict[str, object]] = []
    for item in inputs:
        if progress is not None:
            progress(f"[inventory] {item.label}: loading {item.kind} {item.path}")
        try:
            contract = load_contract_input(item, require_exact=False)
            build_contract_inventory([contract])
        except Exception as exc:  # Each source is an independent compiler input.
            if progress is not None:
                progress(f"[inventory] {item.label}: failed ({type(exc).__name__})")
            diagnostic: dict[str, object] = {
                "label": item.label,
                "input_kind": item.kind,
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
            if isinstance(exc, ExactProvenanceError):
                diagnostic["provenance"] = exc.provenance
            diagnostics.append(diagnostic)
        else:
            if progress is not None:
                progress(f"[inventory] {item.label}: ok")
            contracts.append(contract)
    contracts.sort(key=lambda item: natural_version_key(item.label))
    diagnostics.sort(key=lambda item: natural_version_key(str(item["label"])))
    if snapshot_dir is not None:
        _write_snapshots(contracts, snapshot_dir)
    return {
        **build_contract_inventory(contracts),
        "complete": not diagnostics,
        "diagnostics": diagnostics,
    }


def _build_target(contract: LoadedContract) -> dict[str, object]:
    label = contract.label
    snapshot = contract.snapshot
    if label != snapshot.ds_version:
        message = (
            f"input label {label!r} does not match snapshot version "
            f"{snapshot.ds_version!r}"
        )
        raise ValueError(message)
    _validate_counts(snapshot)
    require_exact_provenance(contract.provenance, label=label)
    expected_digest = contract_snapshot_digest(snapshot)
    if contract.provenance.get("contract_digest") != expected_digest:
        message = f"contract provenance digest does not match snapshot {label!r}"
        raise ValueError(message)

    surfaces = {
        "operations": _surface_entries(
            snapshot.operations,
            key=lambda item: item.operation_id,
            summary=lambda item: {
                "http_method": item.http_method,
                "path": item.path,
            },
        ),
        "dtos": _surface_entries(
            snapshot.dtos,
            key=lambda item: item.import_path,
            summary=lambda item: {"name": item.name},
        ),
        "models": _surface_entries(
            snapshot.models,
            key=lambda item: item.import_path,
            summary=lambda item: {"kind": item.kind, "name": item.name},
        ),
        "enums": _surface_entries(
            snapshot.enums,
            key=lambda item: item.import_path,
            summary=lambda item: {"name": item.name},
        ),
    }
    return {
        "label": label,
        "ds_version": snapshot.ds_version,
        "contract_fingerprint": _fingerprint({"surfaces": surfaces}),
        "provenance": contract.provenance,
        "counts": {
            "operations": snapshot.operation_count,
            "dtos": snapshot.dto_count,
            "models": snapshot.model_count,
            "enums": snapshot.enum_count,
        },
        "surfaces": surfaces,
    }


def _write_snapshots(contracts: Sequence[LoadedContract], output_dir: Path) -> None:
    for contract in contracts:
        write_contract_snapshot_document(
            contract.snapshot,
            output_dir / f"ds-{contract.label}-contract.json",
            provenance=contract.provenance,
        )


def _validate_counts(snapshot: ContractSnapshot) -> None:
    collections = (
        ("operation_count", snapshot.operation_count, len(snapshot.operations)),
        ("dto_count", snapshot.dto_count, len(snapshot.dtos)),
        ("model_count", snapshot.model_count, len(snapshot.models)),
        ("enum_count", snapshot.enum_count, len(snapshot.enums)),
    )
    for field, declared, actual in collections:
        if declared != actual:
            message = f"{field} declares {declared} but contains {actual} items"
            raise ValueError(message)


def _surface_entries(
    items: Sequence[Any],
    *,
    key: Callable[[Any], str],
    summary: Callable[[Any], dict[str, object]],
) -> list[dict[str, object]]:
    entries = []
    for item in sorted(items, key=key):
        canonical = _canonicalize(item)
        entries.append(
            {
                "key": key(item),
                "fingerprint": _fingerprint(canonical),
                **summary(item),
            }
        )
    return entries


def _canonicalize(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonicalize(asdict(value))
    if isinstance(value, dict):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if key not in _DOCUMENTATION_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    return value


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


__all__ = ["build_contract_inventory", "generate_contract_inventory"]
