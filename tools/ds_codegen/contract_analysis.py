"""Compare source inventories or a reviewed CLI closure without admitting versions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from ds_codegen.adapter_wire_candidates import (
    effective_request_candidate,
    effective_response_candidate,
)
from ds_codegen.contract_inputs import (
    contract_extractor_fingerprint,
    load_contract_input,
    natural_version_key,
)
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
    slice_contract_for_bindings,
)
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from ds_codegen.version_diff import (
    compare_contract_snapshots,
    render_markdown_report,
    report_to_json_text,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ds_codegen.contract_inputs import ContractInput, LoadedContract
    from ds_codegen.ir import ContractSnapshot

ComparisonScope = Literal["full", "cli"]
JsonObject = dict[str, Any]


def analyze_contract_versions(
    inputs: Sequence[ContractInput],
    *,
    base_label: str,
    target_labels: list[str],
    scope: ComparisonScope = "full",
) -> list[JsonObject]:
    """Load only selected inputs; CLI comparisons require current exact evidence."""
    if scope not in {"full", "cli"}:
        message = f"unsupported comparison scope {scope!r}"
        raise ValueError(message)
    available = {item.label: item for item in inputs}
    if base_label not in available:
        message = f"unknown base label {base_label!r}"
        raise KeyError(message)
    targets = target_labels or sorted(
        (label for label in available if label != base_label), key=natural_version_key
    )
    selected = dict.fromkeys([base_label, *targets])
    for label in selected:
        if label not in available:
            message = f"unknown target label {label!r}"
            raise KeyError(message)
    loaded = {}
    for label in selected:
        contract = load_contract_input(available[label], require_exact=scope == "cli")
        if scope == "cli" and contract.snapshot.ds_version != label:
            message = (
                f"snapshot {label!r} contains a different DS version "
                f"{contract.snapshot.ds_version!r}"
            )
            raise ValueError(message)
        if scope == "cli" and (
            contract.provenance.get("extractor_fingerprint")
            != contract_extractor_fingerprint()
        ):
            message = f"snapshot {label!r} uses a different extractor; regenerate it"
            raise ValueError(message)
        loaded[label] = contract
    base = loaded[base_label]
    if scope == "cli":
        return [_compare_cli_contracts(base, loaded[label]) for label in targets]
    return [
        compare_contract_snapshots(
            base_label=base_label,
            base=base.snapshot,
            target_label=label,
            target=loaded[label].snapshot,
        )
        for label in targets
    ]


def _compare_cli_contracts(base: LoadedContract, target: LoadedContract) -> JsonObject:
    """Use the base's reviewed roots while retaining the candidate's own version."""
    reference, candidate = base.snapshot, target.snapshot
    bindings = {
        **runtime_operation_bindings(reference.ds_version),
        **runtime_auxiliary_operation_bindings(reference.ds_version),
    }
    roots = sorted({root for b in bindings.values() for root in b.source_operations})
    available_ops = {op.operation_id for op in candidate.operations}
    available_types = {
        (surface, item.import_path)
        for surface, items in (
            ("models", candidate.models),
            ("dtos", candidate.dtos),
            ("enums", candidate.enums),
        )
        for item in items
    }
    missing_operations = sorted(set(roots) - available_ops)
    missing_types = sorted(
        {
            f"{ref.surface}:{ref.key}"
            for binding in bindings.values()
            for ref in binding.type_closure
            if (ref.surface, ref.key) not in available_types
        }
    )
    # Invalid reference evidence is an error, never a candidate diagnostic.
    reference_slice = slice_contract_for_bindings(reference, bindings)
    closure = None
    closure_error = None
    if not missing_operations and not missing_types:
        try:
            candidate_slice = slice_contract_for_bindings(candidate, bindings)
        except (KeyError, TypeError, ValueError) as error:
            closure_error = str(error)
        else:
            closure = compare_contract_snapshots(
                base_label=base.label,
                base=reference_slice,
                target_label=target.label,
                target=candidate_slice,
            )
    wire_changes = _compare_wire_operations(reference, candidate, roots)
    complete = closure is not None and not any(
        "unresolved_type" in item["changes"] for item in wire_changes
    )
    equal = (
        closure is not None
        and complete
        and not wire_changes
        and all(not any(counts.values()) for counts in closure["summary"].values())
    )
    return {
        "scope": "cli",
        "base": {"label": base.label, "version": reference.ds_version},
        "target": {"label": target.label, "version": candidate.ds_version},
        "assessment": (
            "incomplete_evidence" if not complete else "equal" if equal else "different"
        ),
        "semantic_support": "not_assessed",
        "source_operation_roots": len(roots),
        "missing_operations": missing_operations,
        "missing_types": missing_types,
        "closure_error": closure_error,
        "closure_diff": closure,
        "wire_operation_differences": wire_changes,
        "provenance": {"base": base.provenance, "target": target.provenance},
    }


def _compare_wire_operations(
    reference: ContractSnapshot, candidate: ContractSnapshot, roots: list[str]
) -> list[JsonObject]:
    old_ops = {op.operation_id: op for op in reference.operations}
    new_ops = {op.operation_id: op for op in candidate.operations}
    old_resolver = SnapshotTypeResolver.compile(reference)
    new_resolver = SnapshotTypeResolver.compile(candidate)
    differences: list[JsonObject] = []
    for root in roots:
        old, new = old_ops[root], new_ops.get(root)
        expected = (
            effective_request_candidate(old, reference, resolver=old_resolver),
            effective_response_candidate(old, reference, resolver=old_resolver),
        )
        if new is None:
            differences.append({"operation": root, "changes": ["missing_operation"]})
            continue
        try:
            actual = (
                effective_request_candidate(new, candidate, resolver=new_resolver),
                effective_response_candidate(new, candidate, resolver=new_resolver),
            )
        except (KeyError, TypeError, ValueError) as error:
            differences.append(
                {
                    "operation": root,
                    "changes": ["unresolved_type"],
                    "detail": str(error),
                }
            )
            continue
        changes = [
            name
            for name, previous, current in zip(
                ("request", "response"), expected, actual, strict=True
            )
            if previous != current
        ]
        if old.response_projection != new.response_projection:
            changes.append("response_projection")
        if changes:
            differences.append({"operation": root, "changes": changes})
    return differences


def render_version_diff_reports(
    reports: list[JsonObject], *, output_format: str, max_items: int
) -> str:
    """Render complete machine reports or bounded readable comparisons."""
    if output_format == "json":
        return report_to_json_text(
            reports[0] if len(reports) == 1 else {"reports": reports}
        )
    if output_format != "markdown":
        message = f"unsupported version-diff output format {output_format!r}"
        raise ValueError(message)
    return (
        "\n".join(
            (
                _render_cli_report(report, max_items=max_items)
                if report.get("scope") == "cli"
                else render_markdown_report(report, max_items=max_items)
            ).rstrip()
            for report in reports
        )
        + "\n"
    )


def _render_cli_report(report: JsonObject, *, max_items: int) -> str:
    lines = [
        f"# CLI Contract Diff: {report['base']['label']} -> "
        f"{report['target']['label']}",
        "",
        f"Assessment: `{report['assessment']}`.",
        "Static comparison of reviewed roots; task behavior and semantic support "
        "are not assessed.",
        "",
        f"Source operation roots: {report['source_operation_roots']}.",
    ]
    for field in ("missing_operations", "missing_types"):
        if report[field]:
            lines.extend(["", f"{field.replace('_', ' ').capitalize()}:", ""])
            values = report[field][:max_items] if max_items > 0 else report[field]
            lines.extend(f"- `{value}`" for value in values)
    if report["closure_error"]:
        lines.extend(["", f"Closure error: {report['closure_error']}"])
    changes = report["wire_operation_differences"]
    if changes:
        lines.extend(["", "Normalized operation differences:", ""])
        lines.extend(
            f"- `{change['operation']}`: {', '.join(change['changes'])}"
            for change in (changes[:max_items] if max_items > 0 else changes)
        )
    if report["closure_diff"] is not None:
        lines.extend(
            ["", render_markdown_report(report["closure_diff"], max_items=max_items)]
        )
    return "\n".join(lines)
