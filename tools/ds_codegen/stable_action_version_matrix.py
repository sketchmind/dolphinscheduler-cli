"""Project stable CLI actions across the exact DolphinScheduler source matrix.

This module deliberately produces mechanical candidates, not compatibility
claims.  A route match can discover a renamed controller operation, but only a
reviewed version adapter may turn that candidate into supported behavior.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ds_codegen.adapter_wire_candidates import (
    effective_request_candidate,
    effective_response_candidate,
    load_exact_target_contracts,
    operation_by_source_id,
    operations_by_exact_route,
    resolve_operation_anchor,
    source_contract_summary,
)
from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from ds_codegen.stable_action_dependencies import (
    DEFAULT_BASELINE_VERSION,
    analyze_repository_stable_action_dependencies,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from ds_codegen.contract_inputs import ContractInput, LoadedContract
    from ds_codegen.ir import ContractSnapshot, OperationSpec


JsonObject = dict[str, Any]

_KNOWN_SEAM_DIAGNOSTICS = frozenset(
    {
        "adapter-direct-transport",
        "adapter-non-wire-method",
    }
)


def analyze_repository_stable_action_version_matrix(
    project_root: Path,
    *,
    inputs: Sequence[ContractInput],
    baseline_version: str = DEFAULT_BASELINE_VERSION,
) -> JsonObject:
    """Analyze the repository's stable actions against all exact contracts."""
    dependencies = analyze_repository_stable_action_dependencies(
        project_root, baseline_version=baseline_version
    )
    return analyze_stable_action_version_matrix(
        dependency_report=dependencies,
        inputs=inputs,
    )


def analyze_stable_action_version_matrix(
    *,
    dependency_report: Mapping[str, object],
    inputs: Sequence[ContractInput],
) -> JsonObject:
    """Join stable action dependencies to 36 exact wire-contract snapshots."""
    baseline_version = _validate_dependency_report(dependency_report)
    contracts = load_exact_target_contracts(inputs)
    baseline = contracts[baseline_version].snapshot
    resolvers = {
        version: SnapshotTypeResolver.compile(contract.snapshot)
        for version, contract in contracts.items()
    }
    actions_payload = _require_list(
        dependency_report.get("actions"),
        label="dependency report.actions",
    )
    operation_specs = _collect_operation_specs(actions_payload)
    operation_candidates = [
        _operation_candidate_report(
            operation_spec=operation_specs[source_operation],
            baseline=baseline,
            contracts=contracts,
            resolvers=resolvers,
        )
        for source_operation in sorted(operation_specs)
    ]
    candidates_by_operation = {
        _require_text(item.get("source_operation"), label="source operation"): item
        for item in operation_candidates
    }
    actions = [
        _action_report(
            action_payload=_require_mapping(item, label="dependency action"),
            baseline_version=baseline_version,
            candidates_by_operation=candidates_by_operation,
        )
        for item in actions_payload
    ]
    actions.sort(key=lambda item: str(item["action"]))

    unknown_diagnostics = [
        {
            "action": item["action"],
            "diagnostic": diagnostic,
        }
        for item in actions
        for diagnostic in item["unresolved_dependency_diagnostics"]
    ]
    mechanical_blockers = sum(
        bool(version["mechanical_blocker"])
        for action in actions
        for version in action["versions"]
    )
    return {
        "schema_version": 1,
        "kind": "dolphinscheduler-stable-action-version-candidates",
        "claim": "mechanical-candidates-only",
        "automatic_support": False,
        "semantic_support_claimed": False,
        "upstream_absence_claimed": False,
        "complete": not unknown_diagnostics,
        "mechanical_gate_passed": mechanical_blockers == 0,
        "baseline": {
            "version": baseline_version,
            "dependency_report_kind": dependency_report["kind"],
            "dependency_report_complete": dependency_report.get("complete") is True,
        },
        "candidate_scope": {
            "source_id": "exact source operation id",
            "route": "exact HTTP method and normalized snapshot path",
            "request": (
                "adapter-wire candidate request normalization excluding route, "
                "including consumes, parameters, and transitive request types"
            ),
            "response": (
                "normalized snapshot logical-return type and transitive response "
                "wire-type closure candidate"
            ),
            "rename": (
                "a unique exact-route match is a review candidate only and never "
                "a semantic-support assertion"
            ),
            "absence": (
                "no exact-id or route candidate is candidate_missing/needs_review; "
                "this analyzer never infers unsupported-by-upstream"
            ),
        },
        "sources": [
            source_contract_summary(contracts[version])
            for version in REVIEWED_DS_VERSIONS
        ],
        "summary": _summary(actions, operation_candidates),
        "version_summary": _version_summary(actions),
        "domain_summary": _domain_summary(actions),
        "review_queue": _review_queue(actions),
        "unresolved_dependency_diagnostics": unknown_diagnostics,
        "operation_candidates": operation_candidates,
        "actions": actions,
    }


def render_stable_action_version_matrix(report: Mapping[str, object]) -> str:
    """Serialize one candidate matrix deterministically."""
    return json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _validate_dependency_report(report: Mapping[str, object]) -> str:
    if report.get("kind") != "dolphinscheduler-stable-action-dependencies":
        message = "invalid stable-action dependency report kind"
        raise ValueError(message)
    baseline = _require_mapping(
        report.get("baseline"),
        label="dependency report.baseline",
    )
    version = _require_text(
        baseline.get("ds_version"), label="dependency baseline.ds_version"
    )
    if version not in REVIEWED_DS_VERSIONS:
        message = f"unknown exact dependency baseline {version!r}"
        raise ValueError(message)
    if baseline.get("generated_package") != "ds_" + version.replace(".", "_"):
        message = "dependency baseline version and generated package differ"
        raise ValueError(message)
    actions = _require_list(report.get("actions"), label="dependency report.actions")
    names = [
        _require_text(
            _require_mapping(item, label="dependency action").get("action"),
            label="dependency action.action",
        )
        for item in actions
    ]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        message = f"duplicate stable actions: {', '.join(duplicates)}"
        raise ValueError(message)
    return version


def _collect_operation_specs(
    actions: list[object],
) -> dict[str, JsonObject]:
    collected: dict[str, JsonObject] = {}
    client_operations: dict[str, set[str]] = defaultdict(set)
    for raw_action in actions:
        action = _require_mapping(raw_action, label="dependency action")
        for raw_operation in _require_list(
            action.get("generated_operations"),
            label="dependency action.generated_operations",
        ):
            operation = _require_mapping(
                raw_operation,
                label="generated operation",
            )
            source_operation = _require_text(
                operation.get("source_operation"),
                label="generated operation.source_operation",
            )
            normalized = {
                "source_operation": source_operation,
                "http_method": _require_text(
                    operation.get("http_method"),
                    label="generated operation.http_method",
                ),
                "path": _require_text(
                    operation.get("path"),
                    label="generated operation.path",
                ).strip("/"),
            }
            previous = collected.get(source_operation)
            if previous is not None and previous != normalized:
                message = f"inconsistent baseline metadata for {source_operation!r}"
                raise ValueError(message)
            collected[source_operation] = normalized
            client_operations[source_operation].add(
                _require_text(
                    operation.get("client_operation"),
                    label="generated operation.client_operation",
                )
            )
    for source_operation, operation in collected.items():
        operation["client_operations"] = sorted(client_operations[source_operation])
    return collected


def _operation_candidate_report(
    *,
    operation_spec: Mapping[str, object],
    baseline: ContractSnapshot,
    contracts: Mapping[str, LoadedContract],
    resolvers: Mapping[str, SnapshotTypeResolver],
) -> JsonObject:
    source_operation = _require_text(
        operation_spec.get("source_operation"),
        label="operation source_operation",
    )
    http_method = _require_text(
        operation_spec.get("http_method"),
        label=f"{source_operation}.http_method",
    )
    path = _require_text(
        operation_spec.get("path"),
        label=f"{source_operation}.path",
    )
    baseline_operation = resolve_operation_anchor(
        baseline,
        source_operation=source_operation,
        http_method=http_method,
        path=path,
    )
    baseline_resolver = resolvers[baseline.ds_version]
    baseline_request = _request_shape_candidate(
        baseline_operation,
        baseline,
        resolver=baseline_resolver,
    )
    baseline_response = effective_response_candidate(
        baseline_operation,
        baseline,
        resolver=baseline_resolver,
    )
    versions = [
        _compare_operation_candidate(
            baseline_operation=baseline_operation,
            baseline_request=baseline_request,
            baseline_response=baseline_response,
            target=contracts[version].snapshot,
            resolver=resolvers[version],
        )
        for version in REVIEWED_DS_VERSIONS
    ]
    return {
        "source_operation": source_operation,
        "client_operations": [
            _require_text(item, label=f"{source_operation}.client_operations")
            for item in _require_list(
                operation_spec.get("client_operations"),
                label=f"{source_operation}.client_operations",
            )
        ],
        "baseline": {
            "http_method": baseline_operation.http_method,
            "path": baseline_operation.path,
            "source_operation_id": baseline_operation.operation_id,
            "logical_return_candidate": baseline_response["logical_return"],
        },
        "versions": versions,
    }


def _compare_operation_candidate(
    *,
    baseline_operation: OperationSpec,
    baseline_request: JsonObject,
    baseline_response: JsonObject,
    target: ContractSnapshot,
    resolver: SnapshotTypeResolver,
) -> JsonObject:
    exact = operation_by_source_id(target, baseline_operation.operation_id)
    candidates: tuple[OperationSpec, ...]
    if exact is not None:
        selection = "exact-source-id"
        candidates = (exact,)
        selected = exact
    else:
        candidates = operations_by_exact_route(
            target,
            http_method=baseline_operation.http_method,
            path=baseline_operation.path,
        )
        if len(candidates) == 1:
            selection = "unique-route-candidate"
            selected = candidates[0]
        elif not candidates:
            selection = "candidate-missing"
            selected = None
        else:
            selection = "ambiguous-route-candidate"
            selected = None

    if selected is None:
        same_route: bool | None = None
        same_request: bool | None = None
        same_response: bool | None = None
    else:
        same_route = (
            selected.http_method == baseline_operation.http_method
            and selected.path == baseline_operation.path
        )
        same_request = (
            _request_shape_candidate(selected, target, resolver=resolver)
            == baseline_request
        )
        same_response = (
            effective_response_candidate(selected, target, resolver=resolver)
            == baseline_response
        )

    reasons = _operation_review_reasons(
        selection=selection,
        same_route=same_route,
        same_request=same_request,
        same_response=same_response,
    )
    return {
        "version": target.ds_version,
        "selection": selection,
        "status": _operation_status(
            selection=selection,
            same_route=same_route,
            same_request=same_request,
            same_response=same_response,
        ),
        "candidate_only": True,
        "upstream_absence": "not-established",
        "selected_source_operation": (
            selected.operation_id if selected is not None else None
        ),
        "selected_route": (
            {
                "http_method": selected.http_method,
                "path": selected.path,
            }
            if selected is not None
            else None
        ),
        "candidate_source_operations": [
            operation.operation_id for operation in candidates
        ],
        "same_source_id": exact is not None,
        "same_route": same_route,
        "same_effective_request_candidate": same_request,
        "same_logical_response_candidate": same_response,
        "review_reasons": reasons,
    }


def _request_shape_candidate(
    operation: OperationSpec,
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver,
) -> JsonObject:
    candidate = effective_request_candidate(
        operation,
        snapshot,
        resolver=resolver,
    )
    return {
        key: value
        for key, value in candidate.items()
        if key not in {"http_method", "path"}
    }


def _operation_review_reasons(
    *,
    selection: str,
    same_route: bool | None,
    same_request: bool | None,
    same_response: bool | None,
) -> list[str]:
    reasons: list[str] = []
    if selection == "candidate-missing":
        return ["candidate-missing-needs-review"]
    if selection == "ambiguous-route-candidate":
        return ["multiple-route-candidates"]
    if selection == "unique-route-candidate":
        reasons.append("source-operation-rename-candidate")
    if same_route is False:
        reasons.append("route-drift")
    if same_request is False:
        reasons.append("request-drift")
    if same_response is False:
        reasons.append("response-drift")
    return reasons


def _operation_status(
    *,
    selection: str,
    same_route: bool | None,
    same_request: bool | None,
    same_response: bool | None,
) -> str:
    if selection in {"candidate-missing", "ambiguous-route-candidate"}:
        return selection
    drifts = [
        name
        for name, value in (
            ("route", same_route),
            ("request", same_request),
            ("response", same_response),
        )
        if value is False
    ]
    if drifts:
        return f"{'-and-'.join(drifts)}-drift"
    if selection == "unique-route-candidate":
        return "unique-route-candidate"
    return "exact-source-id-candidate"


def _action_report(
    *,
    baseline_version: str,
    action_payload: Mapping[str, object],
    candidates_by_operation: Mapping[str, JsonObject],
) -> JsonObject:
    action = _require_text(action_payload.get("action"), label="action")
    classification = _require_text(
        action_payload.get("classification"),
        label=f"{action}.classification",
    )
    if classification not in {"local", "diagnostic", "remote"}:
        message = f"invalid action classification for {action!r}"
        raise ValueError(message)
    source_operations = sorted(
        {
            _require_text(
                _require_mapping(item, label="generated operation").get(
                    "source_operation"
                ),
                label="generated operation.source_operation",
            )
            for item in _require_list(
                action_payload.get("generated_operations"),
                label=f"{action}.generated_operations",
            )
        }
    )
    missing_catalog = [
        item for item in source_operations if item not in candidates_by_operation
    ]
    if missing_catalog:
        message = (
            f"action {action!r} references unknown source operations: "
            f"{', '.join(missing_catalog)}"
        )
        raise ValueError(message)
    direct_calls = sorted(
        _require_text(item, label=f"{action}.direct_transport_calls")
        for item in _require_list(
            action_payload.get("direct_transport_calls"),
            label=f"{action}.direct_transport_calls",
        )
    )
    raw_diagnostics = [
        _require_mapping(item, label=f"{action}.diagnostic")
        for item in _require_list(
            action_payload.get("diagnostics"),
            label=f"{action}.diagnostics",
        )
    ]
    non_wire_seams = [
        {
            "code": "adapter-non-wire-method",
            "message": _require_text(
                item.get("message"),
                label=f"{action}.diagnostic.message",
            ),
        }
        for item in raw_diagnostics
        if item.get("code") == "adapter-non-wire-method"
    ]
    unresolved = [
        dict(item)
        for item in raw_diagnostics
        if item.get("code") not in _KNOWN_SEAM_DIAGNOSTICS
    ]
    unavailable = _reviewed_baseline_unavailable(
        action_payload,
        baseline_version=baseline_version,
        source_operations=source_operations,
        direct_calls=direct_calls,
    )
    dependency_relationship, dependency_recipes = _dependency_recipes(
        action_payload,
        action=action,
        source_operations=source_operations,
        direct_calls=direct_calls,
        non_wire_seams=non_wire_seams,
        candidates_by_operation=candidates_by_operation,
    )
    versions = [
        _action_version_candidate(
            classification=classification,
            version=version,
            baseline_version=baseline_version,
            baseline_unavailable=unavailable,
            source_operations=source_operations,
            candidates_by_operation=candidates_by_operation,
            direct_calls=direct_calls,
            non_wire_seams=non_wire_seams,
            unresolved=unresolved,
            dependency_relationship=dependency_relationship,
            dependency_recipes=dependency_recipes,
        )
        for version in REVIEWED_DS_VERSIONS
    ]
    dependency_kinds = []
    if classification == "local":
        dependency_kinds.append("local")
    if classification == "diagnostic":
        dependency_kinds.append("diagnostic")
    if source_operations:
        dependency_kinds.append("generated-wire")
    if direct_calls:
        dependency_kinds.append("direct-transport")
    if non_wire_seams:
        dependency_kinds.append("non-wire-seam")
    if unresolved:
        dependency_kinds.append("unresolved")
    return {
        "action": action,
        "domain": action.partition(".")[0],
        "classification": classification,
        "dependency_kinds": dependency_kinds,
        "dependency_relationship": dependency_relationship,
        "dependency_recipes": dependency_recipes,
        **({"baseline_unavailable": unavailable} if unavailable is not None else {}),
        "source_operations": source_operations,
        "direct_transport_calls": direct_calls,
        "non_wire_seams": non_wire_seams,
        "unresolved_dependency_diagnostics": unresolved,
        "versions": versions,
    }


def _reviewed_baseline_unavailable(
    action: Mapping[str, object],
    *,
    baseline_version: str,
    source_operations: list[str],
    direct_calls: list[str],
) -> JsonObject | None:
    """Retain explicit baseline decisions without inventing wire projections."""
    raw_dependency = action.get("reviewed_semantic_dependency")
    if raw_dependency is None:
        return None
    dependency = _require_mapping(raw_dependency, label="reviewed semantic dependency")
    if dependency.get("baseline_version") != baseline_version:
        message = "reviewed semantic dependency differs from report baseline"
        raise ValueError(message)
    if "unavailable" not in dependency:
        return None
    unavailable = _require_mapping(
        dependency["unavailable"], label="reviewed baseline unavailable decision"
    )
    if (
        source_operations
        or direct_calls
        or dependency.get("source_operations") != []
        or dependency.get("source")
        != "operation-contract-ledger-and-reviewed-unavailable-decision"
        or action.get("classification") == "local"
        or action.get("complete") is not True
        or action.get("diagnostics") != []
    ):
        message = "reviewed unavailable baseline must prove its seam without wire calls"
        raise ValueError(message)
    expected = {"availability", "reason", "constraint", "evidence_sources"}
    if set(unavailable) != expected:
        message = "reviewed unavailable baseline decision fields differ"
        raise ValueError(message)
    availability = _require_text(unavailable["availability"], label="availability")
    reason = _require_text(unavailable["reason"], label="reason")
    expected_reasons = {
        "unsupported": "upstream_capability_absent",
        "limited": "upstream_capability_limited",
    }
    if expected_reasons.get(availability) != reason:
        message = "reviewed unavailable baseline decision is not terminal"
        raise ValueError(message)
    constraint = _require_text(unavailable["constraint"], label="constraint")
    evidence = [
        _require_text(item, label="unavailable evidence source")
        for item in _require_list(
            unavailable["evidence_sources"], label="evidence_sources"
        )
    ]
    if not evidence:
        message = "reviewed unavailable baseline must retain upstream evidence"
        raise ValueError(message)
    return {
        "baseline_version": baseline_version,
        "semantic_operation": _require_text(
            dependency.get("semantic_operation"), label="semantic_operation"
        ),
        "availability": availability,
        "reason": reason,
        "constraint": constraint,
        "evidence_sources": evidence,
    }


def _dependency_recipes(
    action_payload: Mapping[str, object],
    *,
    action: str,
    source_operations: list[str],
    direct_calls: list[str],
    non_wire_seams: list[JsonObject],
    candidates_by_operation: Mapping[str, JsonObject],
) -> tuple[str, list[JsonObject]]:
    raw_recipes = action_payload.get("wire_dependencies")
    if raw_recipes is None:
        if not source_operations and not direct_calls and not non_wire_seams:
            return "none", []
        relationship = (
            "single"
            if len(source_operations) == 1 and not direct_calls and not non_wire_seams
            else "alternatives-or-composition-unresolved"
        )
        return relationship, [
            {
                "port": None,
                "relationship": relationship,
                "source_operations": source_operations,
                "direct_transport_calls": direct_calls,
            }
        ]

    recipes: list[JsonObject] = []
    for raw_recipe in _require_list(
        raw_recipes,
        label=f"{action}.wire_dependencies",
    ):
        recipe = _require_mapping(raw_recipe, label=f"{action}.wire_dependency")
        group = _require_text(
            recipe.get("group"),
            label=f"{action}.wire_dependency.group",
        )
        method = _require_text(
            recipe.get("method"),
            label=f"{action}.wire_dependency.method",
        )
        relationship = _require_text(
            recipe.get("operation_relationship"),
            label=f"{action}.wire_dependency.operation_relationship",
        )
        recipe_operations = sorted(
            {
                _require_text(
                    _require_mapping(
                        item,
                        label=f"{action}.wire_dependency.generated_operation",
                    ).get("source_operation"),
                    label=(
                        f"{action}.wire_dependency.generated_operation.source_operation"
                    ),
                )
                for item in _require_list(
                    recipe.get("generated_operations"),
                    label=f"{action}.wire_dependency.generated_operations",
                )
            }
        )
        missing = [
            item for item in recipe_operations if item not in candidates_by_operation
        ]
        if missing:
            message = (
                f"action {action!r} recipe {group}.{method} references unknown "
                f"source operations: {', '.join(missing)}"
            )
            raise ValueError(message)
        recipe_direct = sorted(
            _require_text(
                item,
                label=f"{action}.wire_dependency.direct_transport_calls",
            )
            for item in _require_list(
                recipe.get("direct_transport_calls"),
                label=f"{action}.wire_dependency.direct_transport_calls",
            )
        )
        recipes.append(
            {
                "port": f"{group}.{method}",
                "relationship": relationship,
                "source_operations": recipe_operations,
                "direct_transport_calls": recipe_direct,
            }
        )
    assigned_direct_calls = {
        _require_text(item, label=f"{action}.recipe.direct_transport_call")
        for recipe in recipes
        for item in _require_list(
            recipe.get("direct_transport_calls"),
            label=f"{action}.recipe.direct_transport_calls",
        )
    }
    unassigned_direct_calls = sorted(set(direct_calls) - assigned_direct_calls)
    if unassigned_direct_calls:
        recipes.append(
            {
                "port": None,
                "relationship": "direct-transport",
                "source_operations": [],
                "direct_transport_calls": unassigned_direct_calls,
            }
        )
    relationship_value = action_payload.get("dependency_relationship")
    if relationship_value is None:
        relationship = (
            "none"
            if not recipes
            else (
                str(recipes[0]["relationship"])
                if len(recipes) == 1
                else "alternatives-or-composition-unresolved"
            )
        )
    else:
        relationship = _require_text(
            relationship_value,
            label=f"{action}.dependency_relationship",
        )
        if relationship == "none" and recipes:
            relationship = (
                str(recipes[0]["relationship"])
                if len(recipes) == 1
                else "alternatives-or-composition-unresolved"
            )
    return relationship, recipes


def _recipe_version_status(
    recipe: Mapping[str, object],
    *,
    operation_statuses_by_source: Mapping[str, JsonObject],
) -> JsonObject:
    source_operations = [
        _require_text(item, label="recipe.source_operations")
        for item in _require_list(
            recipe.get("source_operations"),
            label="recipe.source_operations",
        )
    ]
    statuses = [
        str(operation_statuses_by_source[source_operation]["status"])
        for source_operation in source_operations
    ]
    direct_calls = _require_list(
        recipe.get("direct_transport_calls"),
        label="recipe.direct_transport_calls",
    )
    relationship = _require_text(
        recipe.get("relationship"),
        label="recipe.relationship",
    )
    if direct_calls:
        status = "direct-transport-seam"
    elif relationship == "non-wire":
        status = "non-wire-seam"
    elif not statuses:
        status = "no-wire-dependency"
    elif len(set(statuses)) == 1:
        status = statuses[0]
    elif set(statuses) <= {
        "exact-source-id-candidate",
        "unique-route-candidate",
    }:
        status = "unique-route-candidate"
    else:
        status = "alternatives-unresolved"
    return {
        "port": recipe.get("port"),
        "relationship": relationship,
        "status": status,
        "source_operations": source_operations,
    }


def _action_version_candidate(
    *,
    baseline_version: str,
    baseline_unavailable: Mapping[str, object] | None,
    classification: str,
    version: str,
    source_operations: list[str],
    candidates_by_operation: Mapping[str, JsonObject],
    direct_calls: list[str],
    non_wire_seams: list[JsonObject],
    unresolved: list[JsonObject],
    dependency_relationship: str,
    dependency_recipes: list[JsonObject],
) -> JsonObject:
    if classification == "local":
        return {
            "version": version,
            "status": "not-applicable-local",
            "candidate_only": True,
            "upstream_absence": "not-applicable",
            "requires_manual_review": False,
            "mechanical_blocker": False,
            "review_reasons": [],
            "operation_statuses": [],
            "recipe_statuses": [],
        }
    if baseline_unavailable is not None:
        return {
            "version": version,
            "status": "baseline-unavailable-not-projectable",
            "candidate_only": True,
            "upstream_absence": "not-established",
            "requires_manual_review": True,
            "mechanical_blocker": True,
            "review_reasons": ["selected-baseline-unavailable-no-wire-projection"],
            "operation_statuses": [],
            "recipe_statuses": [],
        }
    operation_statuses = []
    reasons: set[str] = set()
    for source_operation in source_operations:
        operation = candidates_by_operation[source_operation]
        version_candidate = _candidate_for_version(operation, version=version)
        operation_statuses.append(
            {
                "source_operation": source_operation,
                "selection": version_candidate["selection"],
                "status": version_candidate["status"],
                "same_source_id": version_candidate["same_source_id"],
                "same_route": version_candidate["same_route"],
                "same_effective_request_candidate": version_candidate[
                    "same_effective_request_candidate"
                ],
                "same_logical_response_candidate": version_candidate[
                    "same_logical_response_candidate"
                ],
            }
        )
        reasons.update(
            _require_text(item, label="operation candidate.review_reasons")
            for item in _require_list(
                version_candidate.get("review_reasons"),
                label="operation candidate.review_reasons",
            )
        )
    operation_statuses_by_source = {
        str(item["source_operation"]): item for item in operation_statuses
    }
    recipe_statuses = [
        _recipe_version_status(
            recipe,
            operation_statuses_by_source=operation_statuses_by_source,
        )
        for recipe in dependency_recipes
    ]
    if direct_calls:
        reasons.add("direct-transport-unprojected")
    if non_wire_seams:
        reasons.add("non-wire-seam")
    if unresolved:
        reasons.add("dependency-analysis-incomplete")
    if version != baseline_version:
        reasons.add("semantic-support-not-reviewed")
    alternatives_unresolved = (
        dependency_relationship == "alternatives-or-composition-unresolved"
        or any(
            item["relationship"] == "alternatives-or-composition-unresolved"
            for item in recipe_statuses
        )
    )
    has_unresolved_candidate = any(
        item["status"] not in {"exact-source-id-candidate", "unique-route-candidate"}
        for item in operation_statuses
    )
    if alternatives_unresolved and has_unresolved_candidate:
        reasons.add("alternatives-unresolved")
    mechanical_reasons = reasons - {"semantic-support-not-reviewed"}
    return {
        "version": version,
        "status": _action_candidate_status(
            operation_statuses=operation_statuses,
            direct_calls=direct_calls,
            non_wire_seams=non_wire_seams,
            unresolved=unresolved,
            alternatives_unresolved=(
                alternatives_unresolved and has_unresolved_candidate
            ),
        ),
        "candidate_only": True,
        "upstream_absence": "not-established",
        "requires_manual_review": bool(reasons),
        "mechanical_blocker": bool(mechanical_reasons),
        "review_reasons": sorted(reasons),
        "operation_statuses": operation_statuses,
        "recipe_statuses": recipe_statuses,
    }


def _candidate_for_version(
    operation: Mapping[str, object],
    *,
    version: str,
) -> Mapping[str, object]:
    matches = [
        _require_mapping(item, label="operation version candidate")
        for item in _require_list(
            operation.get("versions"),
            label="operation versions",
        )
        if isinstance(item, Mapping) and item.get("version") == version
    ]
    if len(matches) != 1:
        source_operation = operation.get("source_operation")
        message = (
            f"operation {source_operation!r} has {len(matches)} candidates for "
            f"DS {version}"
        )
        raise ValueError(message)
    return matches[0]


def _action_candidate_status(
    *,
    operation_statuses: list[JsonObject],
    direct_calls: list[str],
    non_wire_seams: list[JsonObject],
    unresolved: list[JsonObject],
    alternatives_unresolved: bool,
) -> str:
    if unresolved:
        return "dependency-analysis-incomplete"
    if direct_calls:
        return "direct-transport-seam"
    if non_wire_seams:
        return "non-wire-seam"
    statuses = {str(item["status"]) for item in operation_statuses}
    # A composition label describes how several wire calls cooperate; it must
    # not hide the stronger mechanical fact that none of those calls exists in
    # the exact target source.  This remains a review candidate rather than an
    # upstream-absence claim, but surfaces the right item to reviewers and
    # terminal-decision tooling.
    if statuses == {"candidate-missing"}:
        return "candidate-missing"
    if alternatives_unresolved:
        return "alternatives-unresolved"
    if not statuses:
        return "no-wire-dependency"
    if len(statuses) == 1:
        return statuses.pop()
    if statuses <= {"exact-source-id-candidate", "unique-route-candidate"}:
        return "unique-route-candidate"
    return "mixed-candidates"


def _summary(
    actions: list[JsonObject],
    operation_candidates: list[JsonObject],
) -> JsonObject:
    statuses = Counter(
        str(version["status"]) for action in actions for version in action["versions"]
    )
    classifications = Counter(str(action["classification"]) for action in actions)
    return {
        "action_count": len(actions),
        "version_count": len(REVIEWED_DS_VERSIONS),
        "action_version_count": len(actions) * len(REVIEWED_DS_VERSIONS),
        "unique_baseline_operation_count": len(operation_candidates),
        "classifications": dict(sorted(classifications.items())),
        "candidate_statuses": dict(sorted(statuses.items())),
        "manual_review_action_version_count": sum(
            bool(version["requires_manual_review"])
            for action in actions
            for version in action["versions"]
        ),
        "mechanical_blocker_action_version_count": sum(
            bool(version["mechanical_blocker"])
            for action in actions
            for version in action["versions"]
        ),
    }


def _version_summary(actions: list[JsonObject]) -> list[JsonObject]:
    summary = []
    for version in REVIEWED_DS_VERSIONS:
        rows = [_action_version(action, version=version) for action in actions]
        summary.append(
            {
                "version": version,
                "candidate_statuses": dict(
                    sorted(Counter(str(row["status"]) for row in rows).items())
                ),
                "manual_review_actions": [
                    action["action"]
                    for action, row in zip(actions, rows, strict=True)
                    if row["requires_manual_review"]
                ],
                "mechanical_blocker_actions": [
                    action["action"]
                    for action, row in zip(actions, rows, strict=True)
                    if row["mechanical_blocker"]
                ],
            }
        )
    return summary


def _domain_summary(actions: list[JsonObject]) -> list[JsonObject]:
    grouped: dict[str, list[JsonObject]] = defaultdict(list)
    for action in actions:
        grouped[str(action["domain"])].append(action)
    return [
        {
            "domain": domain,
            "action_count": len(domain_actions),
            "actions": [item["action"] for item in domain_actions],
            "manual_review_action_version_count": sum(
                bool(version["requires_manual_review"])
                for action in domain_actions
                for version in action["versions"]
            ),
            "mechanical_blocker_action_version_count": sum(
                bool(version["mechanical_blocker"])
                for action in domain_actions
                for version in action["versions"]
            ),
        }
        for domain, domain_actions in sorted(grouped.items())
    ]


def _review_queue(actions: list[JsonObject]) -> list[JsonObject]:
    queue = []
    for action in actions:
        versions_by_reason: dict[str, list[str]] = defaultdict(list)
        has_mechanical_blocker = False
        for version in action["versions"]:
            has_mechanical_blocker = has_mechanical_blocker or bool(
                version["mechanical_blocker"]
            )
            for reason in version["review_reasons"]:
                versions_by_reason[str(reason)].append(str(version["version"]))
        if not versions_by_reason:
            continue
        queue.append(
            {
                "domain": action["domain"],
                "action": action["action"],
                "priority": (
                    "mechanical-blocker"
                    if has_mechanical_blocker
                    else "semantic-review"
                ),
                "source_operations": action["source_operations"],
                "reasons": [
                    {
                        "reason": reason,
                        "versions": versions,
                    }
                    for reason, versions in sorted(versions_by_reason.items())
                ],
            }
        )
    return sorted(
        queue,
        key=lambda item: (
            item["priority"] != "mechanical-blocker",
            str(item["domain"]),
            str(item["action"]),
        ),
    )


def _action_version(
    action: Mapping[str, object],
    *,
    version: str,
) -> Mapping[str, object]:
    rows = [
        _require_mapping(item, label="action version candidate")
        for item in _require_list(action.get("versions"), label="action versions")
        if isinstance(item, Mapping) and item.get("version") == version
    ]
    if len(rows) != 1:
        message = (
            f"action {action.get('action')!r} has {len(rows)} rows for DS {version}"
        )
        raise ValueError(message)
    return rows[0]


def _require_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _require_list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        message = f"{label} must be an array"
        raise TypeError(message)
    return value


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{label} must be a non-empty string"
        raise TypeError(message)
    return value


__all__ = [
    "analyze_repository_stable_action_version_matrix",
    "analyze_stable_action_version_matrix",
    "render_stable_action_version_matrix",
]
