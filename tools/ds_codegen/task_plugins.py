"""Extract and compare DolphinScheduler task-plugin authoring contracts."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, cast

import javalang
import yaml

from ds_codegen.contract_inputs import (
    ContractInput,
    ExactProvenanceError,
    build_source_contract_provenance,
    cached_contract_provenance,
    natural_version_key,
    require_exact_provenance,
)
from ds_codegen.extract.type_extraction import extract_enum_specs, extract_model_specs
from ds_codegen.extract.type_lookup import _render_reference_name, _render_type
from ds_codegen.ir import (
    DtoFieldSpec,
    EnumFieldSpec,
    EnumSpec,
    EnumValueSpec,
    ModelSpec,
)
from ds_codegen.java_source import (
    JavaParseCache,
    SourceResolutionScope,
    build_import_map,
    clear_java_source_caches,
    load_type_declaration,
    parse_java_compilation_unit,
    resolve_import_path,
    resolve_referenced_import_path,
)
from ds_codegen.source import codegen_repo_root_for_ds_source, read_ds_source_version

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from pathlib import Path

RegistrationKind = Literal["legacy_switch", "logic_switch", "spi_factory"]
ExecutionKind = Literal["legacy", "physical", "logic"]
JsonObject = dict[str, Any]
TASK_PLUGIN_SCHEMA_VERSION = 1
_MODEL_CONTRACT_FIELDS = ("name", "kind", "extends")
_FIELD_CONTRACT_FIELDS = (
    "name",
    "java_type",
    "wire_name",
    "required",
    "default_value",
    "nullable",
    "default_factory",
    "allowable_values",
)


@dataclass(frozen=True)
class TaskPluginDiagnostic:
    """One bounded source-discovery problem."""

    code: str
    message: str
    path: str | None = None
    task_type: str | None = None


@dataclass(frozen=True)
class TaskPluginMethodEvidence:
    """Structural evidence for one plugin behavior method."""

    method_name: str
    declared_in: str
    referenced_fields: list[str]
    fingerprint: str


@dataclass(frozen=True)
class TaskPluginSpec:
    """One server-side task type bound to its parameter contract."""

    task_type: str
    parameter_model_import: str
    model_imports: list[str]
    enum_imports: list[str]
    execution_kind: ExecutionKind
    registration_kind: RegistrationKind
    binding_evidence: list[str]
    source_paths: list[str]
    source_fingerprint: str
    semantic_fingerprint: str
    validation: TaskPluginMethodEvidence | None
    resource_references: TaskPluginMethodEvidence | None


@dataclass(frozen=True)
class TaskPluginSnapshot:
    """Normalized task-plugin evidence extracted from one DS source tree."""

    schema_version: int
    ds_version: str
    source_complete: bool
    plugins: list[TaskPluginSpec]
    models: list[ModelSpec]
    enums: list[EnumSpec]
    diagnostics: list[TaskPluginDiagnostic]

    def to_json_dict(self) -> JsonObject:
        """Return a deterministic JSON-compatible snapshot payload."""
        return asdict(self)


@dataclass(frozen=True)
class _Binding:
    task_type: str
    parameter_import: str
    execution_kind: ExecutionKind
    registration_kind: RegistrationKind
    evidence: list[str]
    source_imports: list[str]


@dataclass(frozen=True)
class _LoadedTaskPluginContract:
    label: str
    snapshot: TaskPluginSnapshot
    provenance: JsonObject


class _TaskPluginExtractionError(ValueError):
    """A source-supported task binding whose contract closure is incomplete."""


def write_task_plugin_snapshot(
    snapshot: TaskPluginSnapshot,
    output_path: Path,
    *,
    provenance: Mapping[str, object] | None = None,
) -> None:
    """Persist one task-plugin snapshot without controller-contract fields."""
    payload: JsonObject = {
        "kind": "dolphinscheduler-task-plugin-contract",
        **snapshot.to_json_dict(),
    }
    if provenance is not None:
        payload["provenance"] = dict(provenance)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_task_plugin_snapshot(path: Path) -> TaskPluginSnapshot:
    """Load one cached task-plugin snapshot document."""
    payload = _read_task_plugin_document(path)
    return _task_plugin_snapshot_from_json(payload)


def _read_task_plugin_document(path: Path) -> JsonObject:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"Task-plugin snapshot must contain a JSON object: {path}"
        raise TypeError(message)
    if payload.get("kind") != "dolphinscheduler-task-plugin-contract":
        message = f"Not a DolphinScheduler task-plugin snapshot: {path}"
        raise ValueError(message)
    if payload.get("schema_version") != TASK_PLUGIN_SCHEMA_VERSION:
        message = f"Unsupported task-plugin snapshot schema: {path}"
        raise ValueError(message)
    return cast("JsonObject", payload)


def task_plugin_snapshot_digest(snapshot: TaskPluginSnapshot) -> str:
    """Return the canonical semantic digest of one cached snapshot."""
    return _canonical_digest(snapshot.to_json_dict())


def generate_task_plugin_inventory(
    inputs: Sequence[ContractInput],
    *,
    task_types: Sequence[str] | None = None,
    snapshot_dir: Path | None = None,
) -> JsonObject:
    """Build an exact, deterministic task-plugin inventory for many versions."""
    contracts: list[_LoadedTaskPluginContract] = []
    diagnostics: list[JsonObject] = []
    for item in inputs:
        try:
            contract = _load_task_plugin_input(
                item,
                task_types=task_types,
                require_exact=True,
            )
        except Exception as exc:  # Each exact source is an independent input.
            diagnostic: JsonObject = {
                "label": item.label,
                "input_kind": item.kind,
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
            if isinstance(exc, ExactProvenanceError):
                diagnostic["provenance"] = exc.provenance
            diagnostics.append(diagnostic)
            continue
        contracts.append(contract)
        diagnostics.extend(
            {
                "label": item.label,
                "input_kind": item.kind,
                "error_type": "TaskPluginSourceDiagnostic",
                "message": diagnostic.message,
                "source_diagnostic": asdict(diagnostic),
            }
            for diagnostic in contract.snapshot.diagnostics
        )
    contracts.sort(key=lambda item: natural_version_key(item.label))
    diagnostics.sort(key=lambda item: natural_version_key(str(item["label"])))
    if snapshot_dir is not None:
        for contract in contracts:
            write_task_plugin_snapshot(
                contract.snapshot,
                snapshot_dir / f"ds-{contract.label}-task-plugins.json",
                provenance=contract.provenance,
            )
    targets = [_task_plugin_inventory_target(contract) for contract in contracts]
    source_complete = all(
        contract.snapshot.source_complete for contract in contracts
    ) and len(contracts) == len(inputs)
    return {
        "schema_version": TASK_PLUGIN_SCHEMA_VERSION,
        "kind": "dolphinscheduler-task-plugin-contract-inventory",
        "complete": source_complete and not diagnostics,
        "source_complete": source_complete,
        "targets": targets,
        "diagnostics": diagnostics,
    }


def analyze_task_plugin_versions(
    inputs: Sequence[ContractInput],
    *,
    base_label: str,
    target_labels: list[str],
    task_types: Sequence[str] | None = None,
) -> list[JsonObject]:
    """Compare selected task-plugin snapshots from shared contract inputs."""
    contracts = {
        item.label: _load_task_plugin_input(
            item,
            task_types=task_types,
            require_exact=False,
        )
        for item in inputs
    }
    if base_label not in contracts:
        message = f"unknown base label {base_label!r}"
        raise KeyError(message)
    selected_targets = target_labels or sorted(
        (label for label in contracts if label != base_label),
        key=natural_version_key,
    )
    reports: list[JsonObject] = []
    for target_label in selected_targets:
        if target_label not in contracts:
            message = f"unknown target label {target_label!r}"
            raise KeyError(message)
        reports.append(
            compare_task_plugin_snapshots(
                base_label=base_label,
                base=contracts[base_label].snapshot,
                target_label=target_label,
                target=contracts[target_label].snapshot,
                base_provenance=contracts[base_label].provenance,
                target_provenance=contracts[target_label].provenance,
            )
        )
    return reports


def render_task_plugin_diff_reports(
    reports: list[JsonObject],
    *,
    output_format: str,
    max_items: int,
) -> str:
    """Render task-plugin compatibility reports as JSON or Markdown."""
    if output_format == "json":
        payload: object = reports[0] if len(reports) == 1 else {"reports": reports}
        return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if output_format != "markdown":
        message = f"unsupported task-plugin diff output format {output_format!r}"
        raise ValueError(message)
    return (
        "\n".join(
            _render_task_plugin_markdown(report, max_items=max_items).rstrip()
            for report in reports
        )
        + "\n"
    )


def load_task_plugin_impact_review(path: Path) -> JsonObject:
    """Load one source-guarded task-plugin impact review from YAML or JSON."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"Task-plugin impact review must contain an object: {path}"
        raise TypeError(message)
    return cast("JsonObject", payload)


def apply_task_plugin_impact_review(
    report: Mapping[str, object],
    review: Mapping[str, object],
    *,
    source_roots: Mapping[str, Path] | None = None,
) -> JsonObject:
    """Materialize reviewed facets and CLI impact over one mechanical diff."""
    if review.get("schema_version") != TASK_PLUGIN_SCHEMA_VERSION:
        message = "unsupported task-plugin impact review schema"
        raise ValueError(message)
    if review.get("kind") != "dolphinscheduler-task-plugin-impact-review":
        message = "invalid task-plugin impact review kind"
        raise ValueError(message)
    _require_review_endpoint(report, review, side="base")
    _require_review_endpoint(report, review, side="target")
    evidence_planes = _validate_review_source_evidence(
        review,
        source_roots=source_roots,
    )

    review_items = _require_list(
        report.get("unmapped_changes"),
        label="unmapped_changes",
    )
    known_by_id = {
        str(_require_mapping(item, label="review item")["id"]): item
        for item in review_items
    }
    facet_ids = _review_facet_ids(review)
    decisions = _require_list(review.get("decisions"), label="review.decisions")
    mapped_by_id: dict[str, str] = {}
    normalized_decisions: list[JsonObject] = []
    for raw_decision in decisions:
        decision = _require_mapping(raw_decision, label="impact decision")
        decision_id = decision.get("id")
        rationale = decision.get("rationale")
        if not isinstance(decision_id, str) or not decision_id.strip():
            message = "impact decision id must be a nonempty string"
            raise ValueError(message)
        if not isinstance(rationale, str) or not rationale.strip():
            message = f"impact decision {decision_id!r} requires a rationale"
            raise ValueError(message)
        _validate_impact_decision(
            decision,
            facet_ids=facet_ids,
            evidence_planes=evidence_planes,
        )
        change_ids = _require_list(
            decision.get("change_ids"),
            label=f"impact decision {decision_id}.change_ids",
        )
        for raw_change_id in change_ids:
            if not isinstance(raw_change_id, str):
                message = f"impact decision {decision_id!r} has a non-string change id"
                raise TypeError(message)
            if raw_change_id not in known_by_id:
                message = (
                    f"impact decision {decision_id!r} references stale or unknown "
                    f"change {raw_change_id!r}"
                )
                raise ValueError(message)
            previous = mapped_by_id.get(raw_change_id)
            if previous is not None:
                message = (
                    f"task-plugin change {raw_change_id!r} is mapped by both "
                    f"{previous!r} and {decision_id!r}"
                )
                raise ValueError(message)
            mapped_by_id[raw_change_id] = decision_id
        normalized_decisions.append(copy.deepcopy(decision))

    mapped_ids = sorted(mapped_by_id)
    missing_ids = sorted(set(known_by_id) - set(mapped_by_id))
    materialized = cast("JsonObject", copy.deepcopy(dict(report)))
    materialized["unmapped_changes"] = [known_by_id[item] for item in missing_ids]
    materialized["review_complete"] = (
        report.get("source_complete") is True and not missing_ids
    )
    materialized_review = cast("JsonObject", copy.deepcopy(dict(review)))
    materialized_review["decisions"] = normalized_decisions
    materialized_review["mapped_change_ids"] = mapped_ids
    materialized_review["missing_change_ids"] = missing_ids
    materialized["review"] = materialized_review
    return materialized


def _require_review_endpoint(
    report: Mapping[str, object],
    review: Mapping[str, object],
    *,
    side: str,
) -> None:
    report_endpoint = _require_mapping(report.get(side), label=side)
    review_endpoint = _require_mapping(review.get(side), label=f"review.{side}")
    if review_endpoint.get("label") != report_endpoint.get("label"):
        message = f"{side} review label does not match the diff report"
        raise ValueError(message)
    if review_endpoint.get("contract_fingerprint") != report_endpoint.get(
        "contract_fingerprint"
    ):
        message = f"{side} contract fingerprint is stale"
        raise ValueError(message)
    expected_source_identity = review_endpoint.get("source_identity_fingerprint")
    if "source_identity_fingerprint" not in review_endpoint:
        return
    if not _is_sha256_digest(expected_source_identity):
        message = f"{side} source identity fingerprint must be a SHA-256 digest"
        raise ValueError(message)
    if report_endpoint.get("source_exact") is not True:
        message = f"{side} source identity is not exact"
        raise ValueError(message)
    if expected_source_identity != report_endpoint.get("source_identity_fingerprint"):
        message = f"{side} source identity fingerprint is stale"
        raise ValueError(message)


def _validate_review_source_evidence(
    review: Mapping[str, object],
    *,
    source_roots: Mapping[str, Path] | None,
) -> set[str]:
    endpoints = [
        _require_mapping(review.get(side), label=f"review.{side}")
        for side in ("base", "target")
    ]
    decisions = _require_list(review.get("decisions"), label="review.decisions")
    facets = _require_list(review.get("facets", []), label="review.facets")
    has_review_claims = bool(
        decisions
        or facets
        or review.get("evidence") is not None
        or any("source_identity_fingerprint" in endpoint for endpoint in endpoints)
    )
    if not has_review_claims:
        return set()
    if not all("source_identity_fingerprint" in endpoint for endpoint in endpoints):
        message = "source-guarded review requires both endpoint identities"
        raise ValueError(message)
    raw_evidence = review.get("evidence")
    if raw_evidence is None:
        message = "source-guarded review requires explicit evidence paths"
        raise ValueError(message)
    evidence = _require_mapping(raw_evidence, label="review.evidence")
    if not evidence:
        message = "source-guarded review requires explicit evidence paths"
        raise ValueError(message)
    evidence_paths: list[str] = []
    evidence_planes: set[str] = set()
    for plane, raw_paths in evidence.items():
        if not isinstance(plane, str) or not plane.strip():
            message = "review evidence plane names must be nonempty strings"
            raise ValueError(message)
        paths = _require_list(raw_paths, label=f"review.evidence.{plane}")
        if not paths or any(
            not isinstance(path, str) or not path.strip() for path in paths
        ):
            message = f"review evidence plane {plane!r} requires source paths"
            raise ValueError(message)
        evidence_planes.add(plane)
        for raw_path in paths:
            path = cast("str", raw_path)
            parsed = PurePosixPath(path)
            if parsed.is_absolute() or ".." in parsed.parts:
                message = f"review evidence path must be repository-relative: {path}"
                raise ValueError(message)
            evidence_paths.append(path)
    _validate_review_evidence_paths(
        endpoints,
        evidence_paths,
        source_roots=source_roots,
    )
    return evidence_planes


def _validate_review_evidence_paths(
    endpoints: Sequence[Mapping[str, object]],
    evidence_paths: Sequence[str],
    *,
    source_roots: Mapping[str, Path] | None,
) -> None:
    if source_roots is None:
        return
    labels = [endpoint.get("label") for endpoint in endpoints]
    if not all(isinstance(label, str) and label in source_roots for label in labels):
        return
    roots = [source_roots[cast("str", label)] for label in labels]
    for path in evidence_paths:
        if any((root / path).is_file() for root in roots):
            continue
        message = f"review evidence path does not exist in either source tree: {path}"
        raise ValueError(message)


def _review_facet_ids(review: Mapping[str, object]) -> set[str]:
    raw_facets = review.get("facets", [])
    facets = _require_list(raw_facets, label="review.facets")
    facet_ids: set[str] = set()
    for raw_facet in facets:
        facet = _require_mapping(raw_facet, label="review facet")
        facet_id = facet.get("id")
        if not isinstance(facet_id, str) or not facet_id.strip():
            message = "review facet id must be a nonempty string"
            raise ValueError(message)
        if facet_id in facet_ids:
            message = f"duplicate review facet id {facet_id!r}"
            raise ValueError(message)
        for field in ("compatibility", "implementation_status"):
            value = facet.get(field)
            if not isinstance(value, str) or not value.strip():
                message = f"review facet {facet_id!r} requires {field}"
                raise ValueError(message)
        facet_ids.add(facet_id)
    return facet_ids


def _validate_impact_decision(
    decision: Mapping[str, object],
    *,
    facet_ids: set[str],
    evidence_planes: set[str],
) -> None:
    decision_id = str(decision["id"])
    cli_impact = decision.get("cli_impact")
    if cli_impact not in {"affected", "none"}:
        message = (
            f"impact decision {decision_id!r} requires cli_impact 'affected' or 'none'"
        )
        raise ValueError(message)
    raw_evidence_planes = _require_list(
        decision.get("evidence_planes"),
        label=f"impact decision {decision_id}.evidence_planes",
    )
    if not raw_evidence_planes:
        message = f"impact decision {decision_id!r} requires evidence planes"
        raise ValueError(message)
    for raw_plane in raw_evidence_planes:
        if not isinstance(raw_plane, str) or raw_plane not in evidence_planes:
            message = (
                f"impact decision {decision_id!r} references unknown evidence "
                f"plane {raw_plane!r}"
            )
            raise ValueError(message)
    raw_facets = _require_list(
        decision.get("facets", []),
        label=f"impact decision {decision_id}.facets",
    )
    decision_facets: list[str] = []
    for raw_facet in raw_facets:
        if not isinstance(raw_facet, str) or raw_facet not in facet_ids:
            message = (
                f"impact decision {decision_id!r} references unknown facet "
                f"{raw_facet!r}"
            )
            raise ValueError(message)
        decision_facets.append(raw_facet)
    raw_actions = _require_list(
        decision.get("affected_actions", []),
        label=f"impact decision {decision_id}.affected_actions",
    )
    if cli_impact == "none":
        if raw_actions:
            message = (
                f"impact decision {decision_id!r} declares no CLI impact but "
                "lists affected actions"
            )
            raise ValueError(message)
        return
    if not decision_facets:
        message = f"impact decision {decision_id!r} requires at least one facet"
        raise ValueError(message)
    implementation_status = decision.get("implementation_status")
    if not isinstance(implementation_status, str) or not implementation_status.strip():
        message = f"impact decision {decision_id!r} requires implementation_status"
        raise ValueError(message)
    if not raw_actions:
        message = f"impact decision {decision_id!r} requires affected actions"
        raise ValueError(message)
    for raw_action in raw_actions:
        action = _require_mapping(
            raw_action,
            label=f"impact decision {decision_id}.affected action",
        )
        for field in ("action", "role"):
            value = action.get(field)
            if not isinstance(value, str) or not value.strip():
                message = (
                    f"impact decision {decision_id!r} affected action requires {field}"
                )
                raise ValueError(message)


def _render_task_plugin_markdown(
    report: Mapping[str, object],
    *,
    max_items: int,
) -> str:
    base = _require_mapping(report.get("base"), label="base")
    target = _require_mapping(report.get("target"), label="target")
    summary = _require_mapping(report.get("summary"), label="summary")
    task_summary = _require_mapping(summary.get("task_types"), label="task_types")
    review_status = (
        "complete" if report.get("review_complete") is True else "review required"
    )
    lines = [
        f"# DolphinScheduler Task-Plugin Diff: {base['label']} -> {target['label']}",
        "",
        "| Gate | Status |",
        "| --- | --- |",
        (
            "| Source extraction | "
            f"{'complete' if report.get('source_complete') is True else 'incomplete'} |"
        ),
        f"| Compatibility review | {review_status} |",
        "",
        "## Summary",
        "",
        "| Surface | Added | Removed | Changed |",
        "| --- | ---: | ---: | ---: |",
        (
            f"| task types | {task_summary['added']} | {task_summary['removed']} | "
            f"{task_summary['changed']} |"
        ),
        "",
        "## Review gate",
        "",
    ]
    review_items = _require_list(
        report.get("unmapped_changes"),
        label="unmapped_changes",
    )
    selected = review_items if max_items == 0 else review_items[:max_items]
    if not selected:
        lines.append("No unmapped semantic changes.")
    else:
        lines.extend(
            f"- [ ] `{_require_mapping(item, label='review item')['id']}`"
            for item in selected
        )
        if max_items and len(review_items) > max_items:
            lines.append(f"- ... {len(review_items) - max_items} more")
    return "\n".join(lines).rstrip() + "\n"


def _load_task_plugin_input(
    item: ContractInput,
    *,
    task_types: Sequence[str] | None,
    require_exact: bool,
) -> _LoadedTaskPluginContract:
    if item.kind == "ds-source":
        snapshot = build_task_plugin_snapshot_from_source(
            item.path,
            task_types=task_types,
        )
        digest = task_plugin_snapshot_digest(snapshot)
        provenance = build_source_contract_provenance(
            item.path,
            label=item.label,
            ds_version=snapshot.ds_version,
            contract_digest=digest,
        )
    else:
        payload = _read_task_plugin_document(item.path)
        snapshot = _task_plugin_snapshot_from_json(payload)
        _require_cached_task_types(snapshot, task_types=task_types, path=item.path)
        digest = task_plugin_snapshot_digest(snapshot)
        provenance = cached_contract_provenance(
            payload.get("provenance"),
            contract_digest=digest,
            path=item.path,
        )
    if require_exact:
        require_exact_provenance(provenance, label=item.label)
    if snapshot.ds_version != item.label:
        message = (
            f"input label {item.label!r} does not match task-plugin snapshot "
            f"version {snapshot.ds_version!r}"
        )
        raise ValueError(message)
    return _LoadedTaskPluginContract(
        label=item.label,
        snapshot=snapshot,
        provenance=provenance,
    )


def _require_cached_task_types(
    snapshot: TaskPluginSnapshot,
    *,
    task_types: Sequence[str] | None,
    path: Path,
) -> None:
    if task_types is None:
        return
    expected = {task_type.strip().upper() for task_type in task_types}
    actual = {plugin.task_type.upper() for plugin in snapshot.plugins}
    if actual == expected:
        return
    message = (
        f"cached task-plugin snapshot selection does not match requested task "
        f"types at {path}: expected {sorted(expected)}, found {sorted(actual)}"
    )
    raise ValueError(message)


def _task_plugin_inventory_target(
    contract: _LoadedTaskPluginContract,
) -> JsonObject:
    snapshot = contract.snapshot
    return {
        "label": contract.label,
        "ds_version": snapshot.ds_version,
        "source_complete": snapshot.source_complete,
        "contract_fingerprint": task_plugin_snapshot_digest(snapshot),
        "provenance": contract.provenance,
        "counts": {
            "task_types": len(snapshot.plugins),
            "models": len(snapshot.models),
            "enums": len(snapshot.enums),
        },
        "task_types": [
            {
                "key": plugin.task_type,
                "parameter_model_import": plugin.parameter_model_import,
                "semantic_fingerprint": plugin.semantic_fingerprint,
                "registration_kind": plugin.registration_kind,
            }
            for plugin in snapshot.plugins
        ],
    }


def _task_plugin_snapshot_from_json(
    payload: Mapping[str, object],
) -> TaskPluginSnapshot:
    plugins = [
        TaskPluginSpec(
            **{
                **_require_mapping(item, label="task plugin"),
                "validation": _method_evidence_from_json(
                    _require_mapping(item, label="task plugin").get("validation")
                ),
                "resource_references": _method_evidence_from_json(
                    _require_mapping(item, label="task plugin").get(
                        "resource_references"
                    )
                ),
            }
        )
        for item in _require_list(payload.get("plugins"), label="plugins")
    ]
    models = [
        ModelSpec(
            **{
                **_require_mapping(item, label="model"),
                "fields": [
                    DtoFieldSpec(**_require_mapping(field, label="model field"))
                    for field in _require_list(
                        _require_mapping(item, label="model").get("fields"),
                        label="model.fields",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("models"), label="models")
    ]
    enums = [
        EnumSpec(
            **{
                **_require_mapping(item, label="enum"),
                "fields": [
                    EnumFieldSpec(**_require_mapping(field, label="enum field"))
                    for field in _require_list(
                        _require_mapping(item, label="enum").get("fields"),
                        label="enum.fields",
                    )
                ],
                "values": [
                    EnumValueSpec(**_require_mapping(value, label="enum value"))
                    for value in _require_list(
                        _require_mapping(item, label="enum").get("values"),
                        label="enum.values",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("enums"), label="enums")
    ]
    diagnostics = [
        TaskPluginDiagnostic(**_require_mapping(item, label="diagnostic"))
        for item in _require_list(payload.get("diagnostics"), label="diagnostics")
    ]
    return TaskPluginSnapshot(
        schema_version=_require_int(
            payload.get("schema_version"), label="schema_version"
        ),
        ds_version=str(payload["ds_version"]),
        source_complete=bool(payload["source_complete"]),
        plugins=plugins,
        models=models,
        enums=enums,
        diagnostics=diagnostics,
    )


def _method_evidence_from_json(value: object) -> TaskPluginMethodEvidence | None:
    if value is None:
        return None
    return TaskPluginMethodEvidence(**_require_mapping(value, label="method evidence"))


def _require_mapping(value: object, *, label: str) -> JsonObject:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return cast("JsonObject", value)


def _require_list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        message = f"{label} must be a list"
        raise TypeError(message)
    return value


def _require_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"{label} must be an integer"
        raise TypeError(message)
    return value


def build_task_plugin_snapshot_from_source(
    ds_source_root: Path,
    *,
    task_types: Sequence[str] | None = None,
) -> TaskPluginSnapshot:
    """Extract task-plugin contracts from one checked-out DS source tree."""
    ds_version = read_ds_source_version(ds_source_root)
    requested = (
        {task_type.strip().upper() for task_type in task_types}
        if task_types is not None
        else None
    )
    parse_cache: JavaParseCache = {}
    try:
        with codegen_repo_root_for_ds_source(ds_source_root) as repo_root:
            bindings, diagnostics = _discover_bindings(
                repo_root,
                requested=requested,
                parse_cache=parse_cache,
            )
            plugins: list[TaskPluginSpec] = []
            models_by_import: dict[str, ModelSpec] = {}
            enums_by_import: dict[str, EnumSpec] = {}
            for binding in bindings:
                try:
                    plugin, models, enums = _extract_plugin_contract(
                        repo_root,
                        binding,
                        parse_cache=parse_cache,
                    )
                except _TaskPluginExtractionError as exc:
                    diagnostics.append(
                        TaskPluginDiagnostic(
                            code="parameter_contract_not_resolved",
                            message=str(exc),
                            path=_source_path_for_import(
                                repo_root,
                                binding.parameter_import,
                            ),
                            task_type=binding.task_type,
                        )
                    )
                    continue
                plugins.append(plugin)
                models_by_import.update({model.import_path: model for model in models})
                enums_by_import.update({enum.import_path: enum for enum in enums})
    finally:
        parse_cache.clear()
        clear_java_source_caches()

    return TaskPluginSnapshot(
        schema_version=TASK_PLUGIN_SCHEMA_VERSION,
        ds_version=ds_version,
        source_complete=not diagnostics,
        plugins=sorted(plugins, key=lambda item: item.task_type),
        models=sorted(models_by_import.values(), key=lambda item: item.import_path),
        enums=sorted(enums_by_import.values(), key=lambda item: item.import_path),
        diagnostics=sorted(
            diagnostics,
            key=lambda item: (
                item.task_type or "",
                item.code,
                item.path or "",
                item.message,
            ),
        ),
    )


def _discover_bindings(
    repo_root: Path,
    *,
    requested: set[str] | None,
    parse_cache: JavaParseCache,
) -> tuple[list[_Binding], list[TaskPluginDiagnostic]]:
    spi_bindings, diagnostics = _discover_spi_bindings(
        repo_root,
        parse_cache=parse_cache,
    )
    logic_bindings, logic_diagnostics = _discover_logic_switch_bindings(
        repo_root,
        parse_cache=parse_cache,
    )
    diagnostics.extend(logic_diagnostics)
    legacy_bindings, legacy_diagnostics = _discover_legacy_bindings(repo_root)
    diagnostics.extend(legacy_diagnostics)
    if not spi_bindings and not logic_bindings and not legacy_bindings:
        diagnostics.append(
            TaskPluginDiagnostic(
                code="registration_strategy_not_found",
                message=(
                    "No TaskChannelFactory or TaskParametersUtils source was found."
                ),
            )
        )

    spi_by_task_type = _bindings_by_task_type(spi_bindings)
    logic_by_task_type = _bindings_by_task_type(logic_bindings)
    legacy_by_task_type = _bindings_by_task_type(legacy_bindings)

    selected: list[_Binding] = []
    available_types = (
        set(spi_by_task_type) | set(logic_by_task_type) | set(legacy_by_task_type)
    )
    target_types = requested if requested is not None else available_types
    for task_type in sorted(target_types):
        candidates = (
            spi_by_task_type.get(task_type)
            or logic_by_task_type.get(task_type)
            or legacy_by_task_type.get(task_type, [])
        )
        unique_candidates = {
            (candidate.parameter_import, candidate.registration_kind): candidate
            for candidate in candidates
        }
        if not candidates:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="task_type_not_bound",
                    message=(
                        f"No source binding resolved task type {task_type!r} to a "
                        "parameter class."
                    ),
                    task_type=task_type,
                )
            )
            continue
        if len(unique_candidates) != 1:
            imports = ", ".join(sorted(item.parameter_import for item in candidates))
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="ambiguous_task_type_binding",
                    message=f"Task type {task_type!r} maps to: {imports}",
                    task_type=task_type,
                )
            )
            continue
        selected.append(next(iter(unique_candidates.values())))
    if requested is not None:
        global_codes = {
            "ambiguous_legacy_registry",
            "legacy_registry_method_missing",
            "legacy_registry_parse_failed",
            "registration_strategy_not_found",
        }
        diagnostics = [
            diagnostic
            for diagnostic in diagnostics
            if diagnostic.task_type is None
            or diagnostic.task_type in requested
            or diagnostic.code in global_codes
        ]
    return selected, diagnostics


def _bindings_by_task_type(bindings: Iterable[_Binding]) -> dict[str, list[_Binding]]:
    by_task_type: dict[str, list[_Binding]] = {}
    for binding in bindings:
        by_task_type.setdefault(binding.task_type, []).append(binding)
    return by_task_type


def _discover_spi_bindings(
    repo_root: Path,
    *,
    parse_cache: JavaParseCache,
) -> tuple[list[_Binding], list[TaskPluginDiagnostic]]:
    source_root = repo_root / "references" / "dolphinscheduler"
    bindings: list[_Binding] = []
    diagnostics: list[TaskPluginDiagnostic] = []
    for factory_path in sorted(source_root.rglob("*TaskChannelFactory.java")):
        if not _is_main_java_source(factory_path):
            continue
        loaded = _load_primary_class(factory_path)
        if loaded is None:
            if factory_path.name == "TaskChannelFactory.java":
                continue
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="factory_class_not_resolved",
                    message="Task-channel factory did not contain a primary class.",
                    path=_relative_source_path(repo_root, factory_path),
                )
            )
            continue
        factory, factory_imports, factory_package, factory_import = loaded
        if _has_annotation(factory, "VisibleForTesting"):
            continue
        task_type = _factory_task_type(
            repo_root,
            factory,
            import_map=factory_imports,
            package_name=factory_package,
            parse_cache=parse_cache,
        )
        channel_name = _factory_channel_type(factory)
        if task_type is None:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="factory_task_type_not_resolved",
                    message=f"{factory_import}.getName() was not statically resolved.",
                    path=_relative_source_path(repo_root, factory_path),
                )
            )
            continue
        if channel_name is None:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="factory_channel_not_resolved",
                    message=(
                        f"{factory_import}.create() did not return one statically "
                        "resolvable channel class."
                    ),
                    path=_relative_source_path(repo_root, factory_path),
                    task_type=task_type,
                )
            )
            continue
        channel_import = resolve_referenced_import_path(
            repo_root,
            channel_name,
            SourceResolutionScope(
                factory_imports,
                factory_package,
                factory_import,
            ),
        )
        if channel_import is None:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="channel_not_resolved",
                    message=(
                        f"{factory_import}.create() returned unresolved type "
                        f"{channel_name!r}."
                    ),
                    path=_relative_source_path(repo_root, factory_path),
                    task_type=task_type,
                )
            )
            continue
        parameter_candidates, source_imports = _parameter_candidates_from_channel(
            repo_root,
            channel_import,
            parse_cache=parse_cache,
        )
        if len(parameter_candidates) != 1:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="parameter_model_not_resolved",
                    message=(
                        f"{channel_import} resolved {len(parameter_candidates)} "
                        "parameter classes from its parsing chain."
                    ),
                    path=_source_path_for_import(repo_root, channel_import),
                    task_type=task_type,
                )
            )
            continue
        parameter_import = next(iter(parameter_candidates))
        bindings.append(
            _Binding(
                task_type=task_type,
                parameter_import=parameter_import,
                execution_kind=(
                    "logic" if "LogicTaskChannel" in channel_import else "physical"
                ),
                registration_kind="spi_factory",
                evidence=[
                    f"{factory_import}.getName() -> {task_type}",
                    (f"{channel_import}.parseParameters() -> {parameter_import}"),
                ],
                source_imports=[factory_import, channel_import, *source_imports],
            )
        )
    return bindings, diagnostics


def _discover_logic_switch_bindings(
    repo_root: Path,
    *,
    parse_cache: JavaParseCache,
) -> tuple[list[_Binding], list[TaskPluginDiagnostic]]:
    """Discover transitional logic tasks parsed directly by TaskPluginManager."""
    source_root = repo_root / "references" / "dolphinscheduler"
    candidates = sorted(
        path
        for path in source_root.rglob("TaskPluginManager.java")
        if _is_main_java_source(path)
    )
    bindings: list[_Binding] = []
    diagnostics: list[TaskPluginDiagnostic] = []
    for path in candidates:
        loaded = _load_primary_class(path)
        if loaded is None:
            diagnostics.append(
                TaskPluginDiagnostic(
                    code="logic_registry_parse_failed",
                    message="TaskPluginManager did not contain a primary class.",
                    path=_relative_source_path(repo_root, path),
                )
            )
            continue
        declaration, import_map, package_name, registry_import = loaded
        method = next(
            (
                method
                for method in declaration.methods
                if method.name == "getParameters"
            ),
            None,
        )
        if method is None:
            continue
        for _, switch_case in method.filter(javalang.tree.SwitchStatementCase):
            task_types = _resolved_switch_case_task_types(
                repo_root,
                switch_case,
                import_map=import_map,
                package_name=package_name,
                parse_cache=parse_cache,
            )
            if not task_types:
                continue
            parameter_imports = {
                item
                for item in _class_reference_imports(
                    repo_root,
                    switch_case,
                    import_map=import_map,
                    package_name=package_name,
                )
                if item.endswith("Parameters")
            }
            if len(parameter_imports) != 1:
                diagnostics.extend(
                    TaskPluginDiagnostic(
                        code="logic_parameter_model_not_resolved",
                        message=(
                            f"Logic task type {task_type!r} resolved "
                            f"{len(parameter_imports)} parameter classes."
                        ),
                        path=_relative_source_path(repo_root, path),
                        task_type=task_type,
                    )
                    for task_type in task_types
                )
                continue
            parameter_import = next(iter(parameter_imports))
            bindings.extend(
                _Binding(
                    task_type=task_type,
                    parameter_import=parameter_import,
                    execution_kind="logic",
                    registration_kind="logic_switch",
                    evidence=[
                        (
                            f"{registry_import}.getParameters({task_type}) -> "
                            f"{parameter_import}"
                        )
                    ],
                    source_imports=[registry_import, parameter_import],
                )
                for task_type in task_types
            )
    return bindings, diagnostics


def _discover_legacy_bindings(
    repo_root: Path,
) -> tuple[list[_Binding], list[TaskPluginDiagnostic]]:
    source_root = repo_root / "references" / "dolphinscheduler"
    candidates = sorted(
        path
        for path in source_root.rglob("TaskParametersUtils.java")
        if _is_main_java_source(path)
    )
    if not candidates:
        return [], []
    if len(candidates) != 1:
        return [], [
            TaskPluginDiagnostic(
                code="ambiguous_legacy_registry",
                message=f"Found {len(candidates)} TaskParametersUtils sources.",
            )
        ]
    path = candidates[0]
    loaded = _load_primary_class(path)
    if loaded is None:
        return [], [
            TaskPluginDiagnostic(
                code="legacy_registry_parse_failed",
                message="TaskParametersUtils did not contain a primary class.",
                path=_relative_source_path(repo_root, path),
            )
        ]
    declaration, import_map, package_name, registry_import = loaded
    method = next(
        (method for method in declaration.methods if method.name == "getParameters"),
        None,
    )
    if method is None:
        return [], [
            TaskPluginDiagnostic(
                code="legacy_registry_method_missing",
                message="TaskParametersUtils.getParameters() was not found.",
                path=_relative_source_path(repo_root, path),
            )
        ]

    bindings: list[_Binding] = []
    diagnostics: list[TaskPluginDiagnostic] = []
    for _, switch_case in method.filter(javalang.tree.SwitchStatementCase):
        task_types = _switch_case_task_types(switch_case)
        if not task_types:
            continue
        parameter_imports = _class_reference_imports(
            repo_root,
            switch_case,
            import_map=import_map,
            package_name=package_name,
        )
        parameter_imports = {
            item for item in parameter_imports if item.endswith("Parameters")
        }
        if len(parameter_imports) != 1:
            diagnostics.extend(
                TaskPluginDiagnostic(
                    code="legacy_parameter_model_not_resolved",
                    message=(
                        f"Legacy task type {task_type!r} resolved "
                        f"{len(parameter_imports)} parameter classes."
                    ),
                    path=_relative_source_path(repo_root, path),
                    task_type=task_type,
                )
                for task_type in task_types
            )
            continue
        parameter_import = next(iter(parameter_imports))
        bindings.extend(
            _Binding(
                task_type=task_type,
                parameter_import=parameter_import,
                execution_kind="legacy",
                registration_kind="legacy_switch",
                evidence=[
                    (
                        f"{registry_import}.getParameters({task_type}) -> "
                        f"{parameter_import}"
                    )
                ],
                source_imports=[registry_import, parameter_import],
            )
            for task_type in task_types
        )
    return bindings, diagnostics


def _extract_plugin_contract(
    repo_root: Path,
    binding: _Binding,
    *,
    parse_cache: JavaParseCache,
) -> tuple[TaskPluginSpec, list[ModelSpec], list[EnumSpec]]:
    declaration_kind_cache: dict[str, Literal["class", "enum"] | None] = {}
    models, enum_imports, model_imports = extract_model_specs(
        repo_root,
        {binding.parameter_import},
        parse_cache,
        declaration_kind_cache,
    )
    root_model = next(
        (model for model in models if model.import_path == binding.parameter_import),
        None,
    )
    if root_model is None:
        message = f"Unable to extract parameter model {binding.parameter_import}"
        raise _TaskPluginExtractionError(message)
    enums = extract_enum_specs(repo_root, enum_imports, parse_cache)
    extracted_model_imports = {model.import_path for model in models}
    missing_model_imports = sorted(model_imports - extracted_model_imports)
    extracted_enum_imports = {enum.import_path for enum in enums}
    missing_enum_imports = sorted(enum_imports - extracted_enum_imports)
    if missing_model_imports or missing_enum_imports:
        missing = ", ".join([*missing_model_imports, *missing_enum_imports])
        message = (
            f"Task type {binding.task_type!r} has an incomplete parameter "
            f"contract closure: {missing}"
        )
        raise _TaskPluginExtractionError(message)
    field_names = {
        name for field in root_model.fields for name in (field.name, field.wire_name)
    }
    validation = _find_behavior_method(
        repo_root,
        binding.parameter_import,
        method_names=("checkParameters",),
        field_names=field_names,
        parse_cache=parse_cache,
    )
    resource_references = _find_behavior_method(
        repo_root,
        binding.parameter_import,
        method_names=("getResourceFilesList", "getResources"),
        field_names=field_names,
        parse_cache=parse_cache,
    )
    source_imports = {
        *binding.source_imports,
        *model_imports,
        *(enum_spec.import_path for enum_spec in enums),
    }
    if validation is not None:
        source_imports.add(validation.declared_in)
    if resource_references is not None:
        source_imports.add(resource_references.declared_in)
    source_paths = sorted(
        path
        for import_path in source_imports
        if (path := _source_path_for_import(repo_root, import_path)) is not None
    )
    source_fingerprint = _source_files_fingerprint(repo_root, source_paths)
    semantic_payload = {
        "task_type": binding.task_type,
        "parameter_model_import": binding.parameter_import,
        "execution_kind": binding.execution_kind,
        "registration_kind": binding.registration_kind,
        "models": [
            _semantic_model_payload(model)
            for model in sorted(models, key=lambda item: item.import_path)
        ],
        "enums": [_semantic_enum_payload(enum) for enum in enums],
        "validation": asdict(validation) if validation is not None else None,
        "resource_references": (
            asdict(resource_references) if resource_references is not None else None
        ),
    }
    plugin = TaskPluginSpec(
        task_type=binding.task_type,
        parameter_model_import=binding.parameter_import,
        model_imports=sorted(model_imports),
        enum_imports=sorted(enum_imports),
        execution_kind=binding.execution_kind,
        registration_kind=binding.registration_kind,
        binding_evidence=binding.evidence,
        source_paths=source_paths,
        source_fingerprint=source_fingerprint,
        semantic_fingerprint=_canonical_digest(semantic_payload),
        validation=validation,
        resource_references=resource_references,
    )
    return plugin, models, enums


def compare_task_plugin_snapshots(
    *,
    base_label: str,
    base: TaskPluginSnapshot,
    target_label: str,
    target: TaskPluginSnapshot,
    base_provenance: Mapping[str, object] | None = None,
    target_provenance: Mapping[str, object] | None = None,
) -> JsonObject:
    """Return a structural task-plugin diff with unmapped review items."""
    base_plugins = {plugin.task_type: plugin for plugin in base.plugins}
    target_plugins = {plugin.task_type: plugin for plugin in target.plugins}
    base_models = {model.import_path: model for model in base.models}
    target_models = {model.import_path: model for model in target.models}
    base_enums = {enum.import_path: enum for enum in base.enums}
    target_enums = {enum.import_path: enum for enum in target.enums}

    added = [
        _plugin_summary(target_plugins[key])
        for key in sorted(set(target_plugins) - set(base_plugins))
    ]
    removed = [
        _plugin_summary(base_plugins[key])
        for key in sorted(set(base_plugins) - set(target_plugins))
    ]
    changed: list[JsonObject] = []
    unmapped: list[JsonObject] = []
    for task_type in sorted(set(base_plugins) & set(target_plugins)):
        old_plugin = base_plugins[task_type]
        new_plugin = target_plugins[task_type]
        change, change_items = _plugin_change(
            old_plugin,
            new_plugin,
            base_models=base_models,
            target_models=target_models,
            base_enums=base_enums,
            target_enums=target_enums,
        )
        if change_items:
            changed.append({"key": task_type, **change})
            unmapped.extend(change_items)

    for item in added:
        task_type = str(item["task_type"])
        unmapped.append(_review_item(task_type, "task_type", task_type, "added"))
    for item in removed:
        task_type = str(item["task_type"])
        unmapped.append(_review_item(task_type, "task_type", task_type, "removed"))
    unmapped.sort(key=lambda item: str(item["id"]))
    diagnostics = [
        {"side": side, **asdict(diagnostic)}
        for side, snapshot in (("base", base), ("target", target))
        for diagnostic in snapshot.diagnostics
    ]
    task_types_diff = {"added": added, "removed": removed, "changed": changed}
    return {
        "schema_version": TASK_PLUGIN_SCHEMA_VERSION,
        "kind": "dolphinscheduler-task-plugin-contract-diff",
        "base": _snapshot_summary(
            base_label,
            base,
            provenance=base_provenance,
        ),
        "target": _snapshot_summary(
            target_label,
            target,
            provenance=target_provenance,
        ),
        "source_complete": base.source_complete and target.source_complete,
        "review_complete": not unmapped,
        "summary": {
            "task_types": {
                "added": len(added),
                "removed": len(removed),
                "changed": len(changed),
            }
        },
        "task_types": task_types_diff,
        "unmapped_changes": unmapped,
        "diagnostics": diagnostics,
    }


def _plugin_change(
    base: TaskPluginSpec,
    target: TaskPluginSpec,
    *,
    base_models: Mapping[str, ModelSpec],
    target_models: Mapping[str, ModelSpec],
    base_enums: Mapping[str, EnumSpec],
    target_enums: Mapping[str, EnumSpec],
) -> tuple[JsonObject, list[JsonObject]]:
    binding_changes = _value_changes(
        base,
        target,
        fields=("parameter_model_import", "execution_kind", "registration_kind"),
    )
    base_plugin_models = {
        key: base_models[key] for key in base.model_imports if key in base_models
    }
    target_plugin_models = {
        key: target_models[key] for key in target.model_imports if key in target_models
    }
    models = _model_diff(base_plugin_models, target_plugin_models)
    fields = _model_field_diff(
        base_plugin_models,
        target_plugin_models,
        root_import=(
            base.parameter_model_import
            if base.parameter_model_import == target.parameter_model_import
            else None
        ),
    )
    enums = _enum_diff(
        {key: base_enums[key] for key in base.enum_imports if key in base_enums},
        {key: target_enums[key] for key in target.enum_imports if key in target_enums},
    )
    behavior = _behavior_diff(base, target)
    change: JsonObject = {
        "before_semantic_fingerprint": base.semantic_fingerprint,
        "after_semantic_fingerprint": target.semantic_fingerprint,
        "source_evidence_changed": (
            base.source_fingerprint != target.source_fingerprint
        ),
        "binding": {"changes": binding_changes},
        "models": models,
        "fields": fields,
        "enums": enums,
        "behavior": behavior,
    }
    review_items = _review_items_for_change(base.task_type, change)
    if base.semantic_fingerprint != target.semantic_fingerprint and not review_items:
        change["unclassified_semantic_change"] = True
        review_items.append(
            _review_item(
                base.task_type,
                "semantic_contract",
                base.task_type,
                "changed",
            )
        )
    return change, review_items


def _model_diff(
    base_models: Mapping[str, ModelSpec],
    target_models: Mapping[str, ModelSpec],
) -> JsonObject:
    added = [
        _model_summary(target_models[key])
        for key in sorted(set(target_models) - set(base_models))
    ]
    removed = [
        _model_summary(base_models[key])
        for key in sorted(set(base_models) - set(target_models))
    ]
    changed: list[JsonObject] = []
    for key in sorted(set(base_models) & set(target_models)):
        changes = _value_changes(
            base_models[key],
            target_models[key],
            fields=_MODEL_CONTRACT_FIELDS,
        )
        if changes:
            changed.append({"key": key, "changes": changes})
    return {"added": added, "removed": removed, "changed": changed}


def _model_field_diff(
    base_models: Mapping[str, ModelSpec],
    target_models: Mapping[str, ModelSpec],
    *,
    root_import: str | None,
) -> JsonObject:
    combined: JsonObject = {"added": [], "removed": [], "changed": []}
    for import_path in sorted(set(base_models) & set(target_models)):
        key_prefix = "" if import_path == root_import else f"{import_path}#"
        section = _field_diff(
            base_models[import_path],
            target_models[import_path],
            key_prefix=key_prefix,
        )
        for kind in ("added", "removed", "changed"):
            cast("list[JsonObject]", combined[kind]).extend(
                cast("list[JsonObject]", section[kind])
            )
    return combined


def _field_diff(
    base_model: ModelSpec | None,
    target_model: ModelSpec | None,
    *,
    key_prefix: str = "",
) -> JsonObject:
    base_fields = (
        {field.wire_name: field for field in base_model.fields}
        if base_model is not None
        else {}
    )
    target_fields = (
        {field.wire_name: field for field in target_model.fields}
        if target_model is not None
        else {}
    )
    added = [
        _field_summary(target_fields[key], key=f"{key_prefix}{key}")
        for key in sorted(set(target_fields) - set(base_fields))
    ]
    removed = [
        _field_summary(base_fields[key], key=f"{key_prefix}{key}")
        for key in sorted(set(base_fields) - set(target_fields))
    ]
    changed: list[JsonObject] = []
    for key in sorted(set(base_fields) & set(target_fields)):
        changes = _value_changes(
            base_fields[key],
            target_fields[key],
            fields=_FIELD_CONTRACT_FIELDS,
        )
        if changes:
            changed.append({"key": f"{key_prefix}{key}", "changes": changes})
    return {"added": added, "removed": removed, "changed": changed}


def _enum_diff(
    base_enums: Mapping[str, EnumSpec],
    target_enums: Mapping[str, EnumSpec],
) -> JsonObject:
    added = [
        _enum_summary(target_enums[key])
        for key in sorted(set(target_enums) - set(base_enums))
    ]
    removed = [
        _enum_summary(base_enums[key])
        for key in sorted(set(base_enums) - set(target_enums))
    ]
    changed: list[JsonObject] = []
    for key in sorted(set(base_enums) & set(target_enums)):
        base_payload = _semantic_enum_payload(base_enums[key])
        target_payload = _semantic_enum_payload(target_enums[key])
        if base_payload != target_payload:
            changed.append(
                {
                    "key": key,
                    "before": _enum_summary(base_enums[key]),
                    "after": _enum_summary(target_enums[key]),
                }
            )
    return {"added": added, "removed": removed, "changed": changed}


def _behavior_diff(base: TaskPluginSpec, target: TaskPluginSpec) -> JsonObject:
    base_methods = {
        item.method_name: item
        for item in (base.validation, base.resource_references)
        if item is not None
    }
    target_methods = {
        item.method_name: item
        for item in (target.validation, target.resource_references)
        if item is not None
    }
    added = [
        _method_summary(target_methods[key])
        for key in sorted(set(target_methods) - set(base_methods))
    ]
    removed = [
        _method_summary(base_methods[key])
        for key in sorted(set(base_methods) - set(target_methods))
    ]
    changed: list[JsonObject] = []
    for key in sorted(set(base_methods) & set(target_methods)):
        before = base_methods[key]
        after = target_methods[key]
        if before.fingerprint == after.fingerprint:
            continue
        changed.append(
            {
                "key": key,
                "before_referenced_fields": before.referenced_fields,
                "after_referenced_fields": after.referenced_fields,
            }
        )
    return {"added": added, "removed": removed, "changed": changed}


def _review_items_for_change(task_type: str, change: JsonObject) -> list[JsonObject]:
    binding = cast("JsonObject", change["binding"])
    items = [
        _review_item(task_type, "binding", str(item["field"]), "changed")
        for item in cast("list[JsonObject]", binding["changes"])
    ]
    for surface in ("models", "fields", "enums", "behavior"):
        section = cast("JsonObject", change[surface])
        singular = surface.removesuffix("s")
        for kind in ("added", "removed", "changed"):
            items.extend(
                _review_item(task_type, singular, str(item["key"]), kind)
                for item in cast("list[JsonObject]", section[kind])
            )
    return items


def _review_item(
    task_type: str,
    surface: str,
    key: str,
    change: str,
) -> JsonObject:
    return {
        "id": f"{task_type}:{surface}:{key}:{change}",
        "task_type": task_type,
        "surface": surface,
        "key": key,
        "change": change,
    }


def _snapshot_summary(
    label: str,
    snapshot: TaskPluginSnapshot,
    *,
    provenance: Mapping[str, object] | None,
) -> JsonObject:
    summary: JsonObject = {
        "label": label,
        "ds_version": snapshot.ds_version,
        "source_complete": snapshot.source_complete,
        "task_type_count": len(snapshot.plugins),
        "contract_fingerprint": _canonical_digest(
            [
                {
                    "task_type": plugin.task_type,
                    "semantic_fingerprint": plugin.semantic_fingerprint,
                }
                for plugin in snapshot.plugins
            ]
        ),
    }
    if provenance is not None:
        origin = _require_mapping(provenance.get("origin"), label="provenance.origin")
        summary["source_exact"] = provenance.get("exact") is True
        summary["source_identity_fingerprint"] = _canonical_digest(origin)
    return summary


def _plugin_summary(plugin: TaskPluginSpec) -> JsonObject:
    return {
        "key": plugin.task_type,
        "task_type": plugin.task_type,
        "parameter_model_import": plugin.parameter_model_import,
        "semantic_fingerprint": plugin.semantic_fingerprint,
    }


def _model_summary(model: ModelSpec) -> JsonObject:
    return {
        "key": model.import_path,
        "name": model.name,
        "kind": model.kind,
        "extends": model.extends,
    }


def _field_summary(field: DtoFieldSpec, *, key: str | None = None) -> JsonObject:
    return {
        "key": key or field.wire_name,
        "java_type": field.java_type,
        "wire_name": field.wire_name,
        "default_value": field.default_value,
    }


def _enum_summary(enum: EnumSpec) -> JsonObject:
    return {
        "key": enum.import_path,
        "name": enum.name,
        "values": [value.name for value in enum.values],
    }


def _method_summary(method: TaskPluginMethodEvidence) -> JsonObject:
    return {
        "key": method.method_name,
        "referenced_fields": method.referenced_fields,
    }


def _value_changes(
    base: object,
    target: object,
    *,
    fields: Sequence[str],
) -> list[JsonObject]:
    changes: list[JsonObject] = []
    for field in fields:
        before = getattr(base, field)
        after = getattr(target, field)
        if before != after:
            changes.append({"field": field, "before": before, "after": after})
    return changes


def _load_primary_class(
    source_path: Path,
) -> (
    tuple[
        javalang.tree.ClassDeclaration,
        dict[str, str],
        str | None,
        str,
    ]
    | None
):
    compilation_unit = parse_java_compilation_unit(source_path.read_text())
    declaration = next(
        (
            item
            for item in compilation_unit.types
            if isinstance(item, javalang.tree.ClassDeclaration)
        ),
        None,
    )
    if declaration is None:
        return None
    package_name = (
        compilation_unit.package.name if compilation_unit.package is not None else None
    )
    import_path = (
        f"{package_name}.{declaration.name}" if package_name else declaration.name
    )
    return declaration, build_import_map(compilation_unit), package_name, import_path


def _is_main_java_source(path: Path) -> bool:
    parts = path.parts
    return any(
        parts[index : index + 3] == ("src", "main", "java")
        for index in range(len(parts) - 2)
    )


def _has_annotation(
    declaration: javalang.tree.TypeDeclaration,
    simple_name: str,
) -> bool:
    return any(
        str(annotation.name).rsplit(".", 1)[-1] == simple_name
        for annotation in getattr(declaration, "annotations", None) or []
    )


def _factory_task_type(
    repo_root: Path,
    declaration: javalang.tree.ClassDeclaration,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    parse_cache: JavaParseCache,
) -> str | None:
    constants: dict[str, str] = {}
    for field in declaration.fields:
        for declarator in field.declarators:
            value = _string_literal(declarator.initializer)
            if value is not None:
                constants[declarator.name] = value
    method = next(
        (method for method in declaration.methods if method.name == "getName"),
        None,
    )
    if method is None:
        return None
    returned = _single_return_expression(method)
    value = _string_literal(returned)
    if value is not None:
        return value.upper()
    if isinstance(returned, javalang.tree.MemberReference):
        local_value = constants.get(cast("str", returned.member))
        if local_value is not None:
            return local_value.upper()
        imported_value = _resolve_static_string_constant(
            repo_root,
            returned,
            import_map=import_map,
            package_name=package_name,
            parse_cache=parse_cache,
        )
        return imported_value.upper() if imported_value is not None else None
    return None


def _resolve_static_string_constant(
    repo_root: Path,
    reference: javalang.tree.MemberReference,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    parse_cache: JavaParseCache,
) -> str | None:
    member = cast("str", reference.member)
    qualifier = cast("str | None", reference.qualifier) or None
    owner_import: str | None = None
    if qualifier is not None:
        owner_import = resolve_referenced_import_path(
            repo_root,
            qualifier,
            SourceResolutionScope(import_map, package_name),
        )
    else:
        static_import = import_map.get(f"@static:{member}")
        if static_import is not None:
            owner_import, _, imported_member = static_import.rpartition(".")
            if imported_member != member:
                return None
    if owner_import is None:
        return None
    loaded = load_type_declaration(repo_root, owner_import, parse_cache)
    if loaded is None:
        return None
    _, declaration, _, _ = loaded
    for field in getattr(declaration, "fields", None) or []:
        for declarator in field.declarators:
            if declarator.name != member:
                continue
            return _string_literal(declarator.initializer)
    return None


def _factory_channel_type(
    declaration: javalang.tree.ClassDeclaration,
) -> str | None:
    method = next(
        (method for method in declaration.methods if method.name == "create"),
        None,
    )
    returned = _single_return_expression(method) if method is not None else None
    if not isinstance(returned, javalang.tree.ClassCreator):
        return None
    return _render_type(returned.type)


def _parameter_candidates_from_channel(
    repo_root: Path,
    channel_import: str,
    *,
    parse_cache: JavaParseCache,
) -> tuple[set[str], list[str]]:
    loaded = load_type_declaration(repo_root, channel_import, parse_cache)
    if loaded is None:
        return set(), []
    _, declaration, import_map, package_name = loaded
    direct = _parameter_class_references(
        repo_root,
        declaration,
        import_map=import_map,
        package_name=package_name,
        method_names={"parseParameters"},
    )
    if direct:
        return direct, []

    linked_types = _created_class_imports(
        repo_root,
        declaration,
        import_map=import_map,
        package_name=package_name,
        method_names={"createTask"},
    )
    candidates: set[str] = set()
    visited: list[str] = []
    for linked_import in sorted(linked_types):
        linked = load_type_declaration(repo_root, linked_import, parse_cache)
        if linked is None:
            continue
        visited.append(linked_import)
        _, linked_declaration, linked_imports, linked_package = linked
        candidates.update(
            _parameter_class_references(
                repo_root,
                linked_declaration,
                import_map=linked_imports,
                package_name=linked_package,
                method_names=None,
            )
        )
    return candidates, visited


def _parameter_class_references(
    repo_root: Path,
    declaration: javalang.tree.TypeDeclaration,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    method_names: set[str] | None,
) -> set[str]:
    candidates: set[str] = set()
    for body_item in getattr(declaration, "body", None) or []:
        is_searchable = isinstance(
            body_item,
            (javalang.tree.MethodDeclaration, javalang.tree.ConstructorDeclaration),
        )
        if not is_searchable:
            continue
        if (
            method_names is not None
            and getattr(body_item, "name", None) not in method_names
        ):
            continue
        candidates.update(
            item
            for item in _class_reference_imports(
                repo_root,
                body_item,
                import_map=import_map,
                package_name=package_name,
            )
            if item.endswith("Parameters")
        )
    return candidates


def _class_reference_imports(
    repo_root: Path,
    node: javalang.ast.Node,
    *,
    import_map: dict[str, str],
    package_name: str | None,
) -> set[str]:
    imports: set[str] = set()
    for _, reference in node.filter(javalang.tree.ClassReference):
        type_name = _render_type(reference.type)
        import_path = resolve_referenced_import_path(
            repo_root,
            type_name,
            SourceResolutionScope(import_map, package_name),
        )
        if import_path is not None:
            imports.add(import_path)
    return imports


def _created_class_imports(
    repo_root: Path,
    declaration: javalang.tree.TypeDeclaration,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    method_names: set[str],
) -> set[str]:
    imports: set[str] = set()
    for method in getattr(declaration, "methods", None) or []:
        if method.name not in method_names:
            continue
        for _, creator in method.filter(javalang.tree.ClassCreator):
            import_path = resolve_referenced_import_path(
                repo_root,
                _render_type(creator.type),
                SourceResolutionScope(import_map, package_name),
            )
            if import_path is not None:
                imports.add(import_path)
    return imports


def _find_behavior_method(
    repo_root: Path,
    import_path: str,
    *,
    method_names: tuple[str, ...],
    field_names: set[str],
    parse_cache: JavaParseCache,
) -> TaskPluginMethodEvidence | None:
    current_import = import_path
    visited: set[str] = set()
    while current_import not in visited:
        visited.add(current_import)
        loaded = load_type_declaration(repo_root, current_import, parse_cache)
        if loaded is None:
            return None
        _, declaration, import_map, package_name = loaded
        for method_name in method_names:
            method = next(
                (
                    item
                    for item in getattr(declaration, "methods", None) or []
                    if item.name == method_name
                ),
                None,
            )
            if method is not None:
                references = sorted(
                    {
                        cast("str", reference.member)
                        for _, reference in method.filter(javalang.tree.MemberReference)
                        if reference.member in field_names
                    }
                )
                return TaskPluginMethodEvidence(
                    method_name=method_name,
                    declared_in=current_import,
                    referenced_fields=references,
                    fingerprint=_canonical_digest(_canonical_ast(method)),
                )
        extends = getattr(declaration, "extends", None)
        if extends is None:
            return None
        parent_import = resolve_referenced_import_path(
            repo_root,
            _render_reference_name(extends),
            SourceResolutionScope(import_map, package_name, current_import),
        )
        if parent_import is None:
            return None
        current_import = parent_import
    return None


def _single_return_expression(
    method: javalang.tree.MethodDeclaration,
) -> object | None:
    returns = [
        statement.expression
        for _, statement in method.filter(javalang.tree.ReturnStatement)
    ]
    return returns[0] if len(returns) == 1 else None


def _string_literal(expression: object | None) -> str | None:
    if not isinstance(expression, javalang.tree.Literal):
        return None
    value = cast("str", expression.value)
    if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return None


def _switch_case_task_types(
    switch_case: javalang.tree.SwitchStatementCase,
) -> list[str]:
    task_types: list[str] = []
    for value in switch_case.case:
        if isinstance(value, str):
            task_types.append(value.upper())
            continue
        literal = _string_literal(value)
        if literal is not None:
            task_types.append(literal.upper())
    return task_types


def _resolved_switch_case_task_types(
    repo_root: Path,
    switch_case: javalang.tree.SwitchStatementCase,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    parse_cache: JavaParseCache,
) -> list[str]:
    task_types = _switch_case_task_types(switch_case)
    for value in switch_case.case:
        if not isinstance(value, javalang.tree.MemberReference):
            continue
        resolved = _resolve_static_string_constant(
            repo_root,
            value,
            import_map=import_map,
            package_name=package_name,
            parse_cache=parse_cache,
        )
        if resolved is not None:
            task_types.append(resolved.upper())
    return task_types


def _semantic_model_payload(model: ModelSpec) -> JsonObject:
    return {
        "name": model.name,
        "import_path": model.import_path,
        "kind": model.kind,
        "extends": model.extends,
        "fields": [
            {
                "name": field.name,
                "wire_name": field.wire_name,
                "java_type": field.java_type,
                "required": field.required,
                "default_value": field.default_value,
                "nullable": field.nullable,
                "default_factory": field.default_factory,
                "allowable_values": field.allowable_values,
            }
            for field in model.fields
        ],
    }


def _semantic_enum_payload(enum: EnumSpec) -> JsonObject:
    return {
        "import_path": enum.import_path,
        "json_value_field": enum.json_value_field,
        "fields": [
            {
                "name": field.name,
                "java_type": field.java_type,
                "annotations": field.annotations,
            }
            for field in enum.fields
        ],
        "values": [
            {"name": value.name, "arguments": value.arguments} for value in enum.values
        ],
    }


def _source_path_for_import(repo_root: Path, import_path: str) -> str | None:
    source_path = resolve_import_path(repo_root, import_path)
    if source_path is None:
        return None
    return _relative_source_path(repo_root, source_path)


def _relative_source_path(repo_root: Path, source_path: Path) -> str:
    source_root = repo_root / "references" / "dolphinscheduler"
    return source_path.relative_to(source_root).as_posix()


def _source_files_fingerprint(repo_root: Path, source_paths: Iterable[str]) -> str:
    source_root = repo_root / "references" / "dolphinscheduler"
    payload = [
        {
            "path": path,
            "digest": _bytes_digest((source_root / path).read_bytes()),
        }
        for path in source_paths
    ]
    return _canonical_digest(payload)


def _canonical_ast(value: object) -> object:
    if isinstance(value, javalang.ast.Node):
        return {
            "node": type(value).__name__,
            **{
                attribute: _canonical_ast(getattr(value, attribute))
                for attribute in value.attrs
                if attribute != "documentation"
            },
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_ast(item) for item in value]
    if isinstance(value, set):
        items = [_canonical_ast(item) for item in value]
        return sorted(items, key=_canonical_json)
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_digest(value: object) -> str:
    encoded = _canonical_json(value).encode()
    return _bytes_digest(encoded)


def _bytes_digest(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _is_sha256_digest(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(
        character in "0123456789abcdef" for character in digest
    )


__all__ = [
    "TaskPluginSnapshot",
    "analyze_task_plugin_versions",
    "apply_task_plugin_impact_review",
    "build_task_plugin_snapshot_from_source",
    "compare_task_plugin_snapshots",
    "generate_task_plugin_inventory",
    "load_task_plugin_impact_review",
    "load_task_plugin_snapshot",
    "render_task_plugin_diff_reports",
    "task_plugin_snapshot_digest",
    "write_task_plugin_snapshot",
]
