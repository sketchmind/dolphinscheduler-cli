from __future__ import annotations

from typing import TYPE_CHECKING

from ds_codegen.contract_analysis import (
    analyze_contract_versions,
    render_version_diff_reports,
)
from ds_codegen.contract_inputs import (
    ContractInput,
    ContractInputKind,
    ExactProvenanceError,
    LoadedContract,
    build_source_provenance,
    contract_extractor_fingerprint,
    contract_snapshot_digest,
    load_contract_input,
    load_contract_snapshot,
    natural_version_key,
    parse_contract_inputs,
    require_exact_provenance,
    write_contract_snapshot_document,
)
from ds_codegen.contract_inventory import (
    build_contract_inventory,
    generate_contract_inventory,
)
from ds_codegen.extract import build_contract_snapshot
from ds_codegen.ir import ContractSnapshot
from ds_codegen.render.package import (
    write_generated_package as _write_generated_package,
)
from ds_codegen.runtime_contract import (
    configured_runtime_contract_slice,
    runtime_semantic_operations,
    slice_contract_for_bindings,
)
from ds_codegen.task_plugins import (
    TaskPluginSnapshot,
    analyze_task_plugin_versions,
    apply_task_plugin_impact_review,
    build_task_plugin_snapshot_from_source,
    compare_task_plugin_snapshots,
    generate_task_plugin_inventory,
    load_task_plugin_impact_review,
    load_task_plugin_snapshot,
    render_task_plugin_diff_reports,
    task_plugin_snapshot_digest,
    write_task_plugin_snapshot,
)

if TYPE_CHECKING:
    from pathlib import Path


def write_contract_snapshot(
    snapshot: ContractSnapshot,
    output_path: Path,
    *,
    provenance: dict[str, object] | None = None,
) -> None:
    write_contract_snapshot_document(
        snapshot,
        output_path,
        provenance=provenance,
    )


def write_generated_package(
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    _write_generated_package(snapshot, output_root)


__all__ = [
    "ContractInput",
    "ContractInputKind",
    "ContractSnapshot",
    "ExactProvenanceError",
    "LoadedContract",
    "TaskPluginSnapshot",
    "analyze_contract_versions",
    "analyze_task_plugin_versions",
    "apply_task_plugin_impact_review",
    "build_contract_inventory",
    "build_contract_snapshot",
    "build_source_provenance",
    "build_task_plugin_snapshot_from_source",
    "compare_task_plugin_snapshots",
    "configured_runtime_contract_slice",
    "contract_extractor_fingerprint",
    "contract_snapshot_digest",
    "generate_contract_inventory",
    "generate_task_plugin_inventory",
    "load_contract_input",
    "load_contract_snapshot",
    "load_task_plugin_impact_review",
    "load_task_plugin_snapshot",
    "natural_version_key",
    "parse_contract_inputs",
    "render_task_plugin_diff_reports",
    "render_version_diff_reports",
    "require_exact_provenance",
    "runtime_semantic_operations",
    "slice_contract_for_bindings",
    "task_plugin_snapshot_digest",
    "write_contract_snapshot",
    "write_generated_package",
    "write_task_plugin_snapshot",
]
