"""Bind receipt profiles, contracts and action recipes to independently loaded facts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from live_gate.conformance_evidence.types import (
    _ActionRecipeBinding,
    _Assessment,
    _AssessmentBundle,
    _ContractBinding,
    _CurrentTruth,
    _ProfileBinding,
)
from live_gate.conformance_evidence.values import (
    _EXECUTION_MODES,
    _SUPPORT_LEVELS,
    _VERIFICATIONS,
    _exact_keys,
    _git_object,
    _sha256,
    _text_sequence,
    _validate_fingerprints,
    _validate_flat_contract_source,
    _validate_source,
)
from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_bool as _boolean
from live_gate.evidence_values import required_int as _integer
from live_gate.evidence_values import required_list as _sequence
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text

if TYPE_CHECKING:
    from collections.abc import Mapping

_CURRENT_BUNDLE_MANIFEST_SCHEMA_VERSION = 2


_SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS = frozenset({1, 2})


def _validate_profile(
    value: object,
    *,
    ds_version: str,
    generated_profile: Mapping[str, object],
) -> _ProfileBinding:
    profile = _mapping(value, label="profile")
    _exact_keys(
        profile,
        {
            "ds",
            "selected_ds_version",
            "contract_version",
            "family",
            "support_level",
            "tested",
            "source",
            "fingerprints",
        },
        label="profile",
    )
    for field in ("ds", "selected_ds_version", "contract_version"):
        _constant(
            profile.get(field),
            ds_version,
            label=f"profile {field}",
        )
    _constant(
        profile.get("family"),
        _text(generated_profile.get("family"), label="generated profile family"),
        label="profile family",
    )
    support_level = _text(profile.get("support_level"), label="profile support_level")
    if support_level not in _SUPPORT_LEVELS:
        message = "profile support_level is not recognized"
        raise ValueError(message)
    generated_support_level = _text(
        generated_profile.get("support_level"),
        label="generated profile support_level",
    )
    _constant(
        support_level,
        generated_support_level,
        label="profile support_level",
    )
    tested = _boolean(profile.get("tested"), label="profile tested")
    if tested != (support_level in {"legacy_core", "full"}):
        message = "profile support and tested values are inconsistent"
        raise ValueError(message)
    _constant(
        tested,
        _boolean(generated_profile.get("tested"), label="generated profile tested"),
        label="profile tested",
    )
    source = _validate_source(
        profile.get("source"),
        ds_version=ds_version,
        label="profile source",
    )
    _constant(
        source,
        _validate_source(
            generated_profile.get("source"),
            ds_version=ds_version,
            label="generated profile source",
        ),
        label="profile source",
    )
    _validate_fingerprints(profile.get("fingerprints"), label="profile fingerprints")
    generated_fingerprints = generated_profile.get("fingerprints")
    _validate_fingerprints(
        generated_fingerprints,
        label="generated profile fingerprints",
    )
    _constant(
        profile.get("fingerprints"),
        generated_fingerprints,
        label="profile fingerprints",
    )
    return _ProfileBinding(
        source=source,
        support_level=support_level,
        tested=tested,
    )


def _current_generated_profile(
    ds_version: str,
    *,
    current: _CurrentTruth,
) -> Mapping[str, object]:
    profiles = _mapping(current.profiles, label="generated profiles")
    if tuple(profiles) != current.versions:
        message = "generated profiles do not cover the exact target versions"
        raise ValueError(message)
    profile = _mapping(
        profiles.get(ds_version),
        label=f"generated profile {ds_version}",
    )
    _exact_keys(
        profile,
        {
            "server_version",
            "contract_version",
            "family",
            "support_level",
            "tested",
            "source",
            "fingerprints",
            "actions",
            "build_decisions",
        },
        label=f"generated profile {ds_version}",
    )
    for field in ("server_version", "contract_version"):
        _constant(
            profile.get(field),
            ds_version,
            label=f"generated profile {field}",
        )
    actions = _mapping(
        profile.get("actions"),
        label=f"generated profile {ds_version} actions",
    )
    if tuple(actions) != tuple(sorted(actions)):
        message = f"generated profile {ds_version} actions are not canonical"
        raise ValueError(message)
    decisions = _mapping(
        profile.get("build_decisions"),
        label=f"generated profile {ds_version} build_decisions",
    )
    if not decisions:
        message = f"generated profile {ds_version} has no build decisions"
        raise ValueError(message)
    return profile


def _validate_contract(
    value: object,
    *,
    ds_version: str,
    contracts: Mapping[str, Mapping[str, object]],
) -> tuple[frozenset[str], tuple[str, str, str]]:
    contract = _mapping(value, label="contract")
    generated_contract = _current_generated_contract(
        ds_version,
        contracts=contracts,
    )
    _exact_keys(
        contract,
        {
            "bundle_manifest_schema_version",
            "ds_version",
            "selection",
            "semantic_operations",
            "source_tag",
            "source_commit",
            "source_tree",
            "source_contract_digest",
            "rendered_contract_digest",
            "operation_count",
        },
        label="contract",
    )
    bundle_manifest_schema_version = _integer(
        contract.get("bundle_manifest_schema_version"),
        label="contract bundle_manifest_schema_version",
    )
    if bundle_manifest_schema_version not in _SUPPORTED_BUNDLE_MANIFEST_SCHEMA_VERSIONS:
        message = "contract bundle_manifest_schema_version must be 1 or 2"
        raise ValueError(message)
    _constant(
        bundle_manifest_schema_version,
        generated_contract.schema_version,
        label="contract bundle_manifest_schema_version",
    )
    _constant(
        contract.get("ds_version"),
        generated_contract.ds_version,
        label="contract ds_version",
    )
    _constant(
        contract.get("selection"),
        generated_contract.selection,
        label="contract selection",
    )
    operation_sequence = tuple(
        _text_sequence(
            contract.get("semantic_operations"),
            label="contract semantic_operations",
            allow_empty=True,
        )
    )
    _constant(
        operation_sequence,
        generated_contract.semantic_operations,
        label="contract semantic_operations",
    )
    operations = frozenset(operation_sequence)
    operation_count = _integer(
        contract.get("operation_count"),
        label="contract operation_count",
    )
    _constant(
        operation_count,
        generated_contract.operation_count,
        label="contract operation_count",
    )
    source = _validate_flat_contract_source(contract, ds_version=ds_version)
    _constant(source, generated_contract.source, label="contract source")
    source_contract_digest = _sha256(
        contract.get("source_contract_digest"),
        label="contract source_contract_digest",
    )
    _constant(
        source_contract_digest,
        generated_contract.source_contract_digest,
        label="contract source_contract_digest",
    )
    rendered_contract_digest = _sha256(
        contract.get("rendered_contract_digest"),
        label="contract rendered_contract_digest",
    )
    _constant(
        rendered_contract_digest,
        generated_contract.rendered_contract_digest,
        label="contract rendered_contract_digest",
    )
    return operations, source


def _current_generated_contract(
    ds_version: str,
    *,
    contracts: Mapping[str, Mapping[str, object]],
) -> _ContractBinding:
    manifest = _mapping(
        contracts.get(ds_version),
        label=f"generated contract for DS {ds_version}",
    )
    _exact_keys(
        manifest,
        {
            "bundle_manifest_schema_version",
            "ds_version",
            "selection",
            "semantic_operations",
            "source_tag",
            "source_commit",
            "source_tree",
            "source_contract_digest",
            "rendered_contract_digest",
            "operation_count",
        },
        label=f"generated contract for DS {ds_version}",
    )
    schema_version = _integer(
        manifest.get("bundle_manifest_schema_version"),
        label="generated contract schema_version",
    )
    _constant(
        schema_version,
        _CURRENT_BUNDLE_MANIFEST_SCHEMA_VERSION,
        label="generated contract schema_version",
    )
    generated_version = _text(
        manifest.get("ds_version"),
        label="generated contract DS_VERSION",
    )
    _constant(generated_version, ds_version, label="generated contract DS_VERSION")
    selection = _text(
        manifest.get("selection"),
        label="generated contract selection",
    )
    if selection not in {"full", "runtime-slice"}:
        message = "generated contract selection is not recognized"
        raise ValueError(message)
    semantic_operations = tuple(
        _text_sequence(
            manifest.get("semantic_operations"),
            label="generated contract semantic_operations",
            allow_empty=True,
        )
    )
    if semantic_operations != tuple(sorted(semantic_operations)):
        message = "generated contract semantic_operations are not canonical"
        raise ValueError(message)
    if selection == "full" and semantic_operations:
        message = "generated full contract declares semantic slice roots"
        raise ValueError(message)
    source_tag = _text(
        manifest.get("source_tag"),
        label="generated contract source_tag",
    )
    _constant(source_tag, ds_version, label="generated contract source_tag")
    source = (
        source_tag,
        _git_object(
            manifest.get("source_commit"),
            label="generated contract source_commit",
        ),
        _git_object(
            manifest.get("source_tree"),
            label="generated contract source_tree",
        ),
    )
    operation_count = _integer(
        manifest.get("operation_count"),
        label="generated contract operation_count",
    )
    if operation_count < 0:
        message = "generated contract operation_count must be nonnegative"
        raise ValueError(message)
    if selection == "full" and operation_count == 0:
        message = "generated contract operation_count must be positive"
        raise ValueError(message)
    return _ContractBinding(
        schema_version=schema_version,
        ds_version=generated_version,
        selection=selection,
        semantic_operations=semantic_operations,
        source=source,
        source_contract_digest=_sha256(
            manifest.get("source_contract_digest"),
            label="generated contract source_contract_digest",
        ),
        rendered_contract_digest=_sha256(
            manifest.get("rendered_contract_digest"),
            label="generated contract rendered_contract_digest",
        ),
        operation_count=operation_count,
    )


def _validate_bundle(
    value: object,
    *,
    contract_operations: frozenset[str],
    assessment: _Assessment,
    ds_version: str,
    profile: _ProfileBinding,
    generated_profile: Mapping[str, object],
) -> tuple[str, tuple[str, ...]]:
    bundle = _mapping(value, label="conformance_bundle")
    _exact_keys(
        bundle,
        {
            "catalog_schema_version",
            "assessment_schema_version",
            "catalog_digest",
            "assessment_digest",
            "name",
            "bundle_digest",
            "coordinate_status",
            "extends",
            "inheritance",
            "direct_actions",
            "required_actions",
            "action_recipes",
        },
        label="conformance_bundle",
    )
    _constant(
        bundle.get("catalog_schema_version"),
        assessment.schema_version,
        label="bundle catalog_schema_version",
    )
    _constant(
        bundle.get("assessment_schema_version"),
        assessment.schema_version,
        label="bundle assessment_schema_version",
    )
    _constant(
        bundle.get("catalog_digest"),
        assessment.catalog_digest,
        label="bundle catalog_digest",
    )
    _constant(
        bundle.get("assessment_digest"),
        assessment.assessment_digest,
        label="bundle assessment_digest",
    )
    name = _text(bundle.get("name"), label="bundle name")
    assessed_bundle = assessment.bundles.get(name)
    if assessed_bundle is None:
        message = f"unknown conformance bundle {name!r}"
        raise ValueError(message)
    coordinate = assessed_bundle.coordinates.get(ds_version)
    if coordinate is None:
        message = f"bundle {name!r} has no exact {ds_version} assessment"
        raise ValueError(message)
    _constant(
        bundle.get("coordinate_status"),
        "ready",
        label="bundle coordinate_status",
    )
    if coordinate.status != "ready":
        message = f"bundle {name!r} is not ready for DolphinScheduler {ds_version}"
        raise ValueError(message)
    if (
        coordinate.support_level != profile.support_level
        or coordinate.tested != profile.tested
    ):
        message = "bundle coordinate support/tested values differ from the profile"
        raise ValueError(message)
    extends = tuple(
        _text_sequence(
            bundle.get("extends"),
            label="bundle extends",
            allow_empty=True,
        )
    )
    _constant(extends, assessed_bundle.extends, label="bundle extends")
    required_actions = tuple(
        _text_sequence(
            bundle.get("required_actions"),
            label="bundle required_actions",
        )
    )
    _constant(
        required_actions,
        assessed_bundle.required_actions,
        label="bundle required_actions",
    )
    _validate_bundle_inheritance(
        bundle.get("inheritance"),
        assessed_bundle=assessed_bundle,
        assessment=assessment,
    )
    direct_actions = tuple(
        _text_sequence(
            bundle.get("direct_actions"),
            label="bundle direct_actions",
        )
    )
    _constant(
        direct_actions,
        assessed_bundle.direct_actions,
        label="bundle direct_actions",
    )
    _constant(
        bundle.get("bundle_digest"),
        assessed_bundle.bundle_digest,
        label="bundle bundle_digest",
    )
    _validate_action_recipes(
        bundle.get("action_recipes"),
        required_actions=required_actions,
        contract_operations=contract_operations,
        generated_profile=generated_profile,
    )
    return name, required_actions


def _validate_bundle_inheritance(
    value: object,
    *,
    assessed_bundle: _AssessmentBundle,
    assessment: _Assessment,
) -> None:
    raw_parents = _sequence(value, label="bundle inheritance")
    if len(raw_parents) != len(assessed_bundle.extends):
        message = "bundle inheritance count differs from extends"
        raise ValueError(message)
    for raw_parent, expected_name in zip(
        raw_parents,
        assessed_bundle.extends,
        strict=True,
    ):
        parent = _mapping(raw_parent, label="bundle inheritance parent")
        _exact_keys(
            parent,
            {"name", "bundle_digest", "required_actions"},
            label="bundle inheritance parent",
        )
        _constant(parent.get("name"), expected_name, label="parent bundle name")
        parent_assessment = assessment.bundles[expected_name]
        parent_actions = tuple(
            _text_sequence(
                parent.get("required_actions"),
                label="parent bundle required_actions",
            )
        )
        _constant(
            parent_actions,
            parent_assessment.required_actions,
            label="parent bundle required_actions",
        )
        _constant(
            parent.get("bundle_digest"),
            parent_assessment.bundle_digest,
            label="parent bundle_digest",
        )


def _validate_action_recipes(
    value: object,
    *,
    required_actions: tuple[str, ...],
    contract_operations: frozenset[str],
    generated_profile: Mapping[str, object],
) -> None:
    recipes = _sequence(value, label="bundle action_recipes")
    if len(recipes) != len(required_actions):
        message = "bundle action_recipes count differs from required_actions"
        raise ValueError(message)
    semantic_operations: set[str] = set()
    for raw_recipe, expected_action in zip(recipes, required_actions, strict=True):
        generated_recipe = _generated_action_recipe(
            generated_profile,
            action=expected_action,
        )
        recipe = _mapping(raw_recipe, label="bundle action recipe")
        _exact_keys(
            recipe,
            {
                "action",
                "semantic_operation",
                "availability",
                "execution_mode",
                "verification",
                "build_status",
                "fingerprints",
            },
            label="bundle action recipe",
        )
        _constant(recipe.get("action"), expected_action, label="recipe action")
        semantic_operation = _text(
            recipe.get("semantic_operation"),
            label="recipe semantic_operation",
        )
        _constant(
            semantic_operation,
            generated_recipe.semantic_operation,
            label="recipe semantic_operation",
        )
        semantic_operations.add(semantic_operation)
        _constant(
            recipe.get("availability"),
            generated_recipe.availability,
            label="recipe availability",
        )
        execution_mode = _text(
            recipe.get("execution_mode"),
            label="recipe execution_mode",
        )
        if execution_mode not in _EXECUTION_MODES:
            message = "recipe execution_mode is not executable"
            raise ValueError(message)
        _constant(
            execution_mode,
            generated_recipe.execution_mode,
            label="recipe execution_mode",
        )
        verification = _text(
            recipe.get("verification"),
            label="recipe verification",
        )
        if verification not in _VERIFICATIONS:
            message = "recipe verification is not recognized"
            raise ValueError(message)
        _constant(
            verification,
            generated_recipe.verification,
            label="recipe verification",
        )
        _constant(
            recipe.get("build_status"),
            generated_recipe.build_status,
            label="recipe build_status",
        )
        _validate_fingerprints(
            recipe.get("fingerprints"),
            label=f"recipe {expected_action} fingerprints",
        )
        _constant(
            recipe.get("fingerprints"),
            generated_recipe.fingerprints,
            label=f"recipe {expected_action} fingerprints",
        )
    if not semantic_operations.issubset(contract_operations):
        message = "bundle recipes are absent from the runtime-slice contract"
        raise ValueError(message)


def _generated_action_recipe(
    profile: Mapping[str, object],
    *,
    action: str,
) -> _ActionRecipeBinding:
    actions = _mapping(profile.get("actions"), label="generated profile actions")
    action_fact = _mapping(
        actions.get(action),
        label=f"generated profile action {action}",
    )
    _exact_keys(
        action_fact,
        {"availability", "execution_mode", "verification"},
        label=f"generated profile action {action}",
    )
    availability = _text(
        action_fact.get("availability"),
        label=f"generated profile action {action} availability",
    )
    _constant(
        availability,
        "supported",
        label=f"generated profile action {action} availability",
    )
    execution_mode = _text(
        action_fact.get("execution_mode"),
        label=f"generated profile action {action} execution_mode",
    )
    if execution_mode not in _EXECUTION_MODES:
        message = f"generated profile action {action} is not executable"
        raise ValueError(message)
    verification = _text(
        action_fact.get("verification"),
        label=f"generated profile action {action} verification",
    )
    if verification not in _VERIFICATIONS:
        message = f"generated profile action {action} verification is not recognized"
        raise ValueError(message)
    decisions = _mapping(
        profile.get("build_decisions"),
        label="generated profile build_decisions",
    )
    matching_decisions: list[tuple[str, Mapping[str, object]]] = []
    for operation, raw_decision in decisions.items():
        decision = _mapping(
            raw_decision,
            label=f"generated build decision {operation}",
        )
        if decision.get("stable_action") == action:
            matching_decisions.append((operation, decision))
    if len(matching_decisions) != 1:
        message = f"generated action {action} decision is missing or duplicated"
        raise ValueError(message)
    operation, decision = matching_decisions[0]
    _exact_keys(
        decision,
        {
            "semantic_operation",
            "stable_action",
            "build_status",
            "fingerprints",
        },
        label=f"generated build decision {operation}",
    )
    semantic_operation = _text(
        decision.get("semantic_operation"),
        label=f"generated build decision {operation} semantic_operation",
    )
    _constant(
        semantic_operation,
        operation,
        label=f"generated build decision {operation} semantic_operation",
    )
    _constant(
        decision.get("stable_action"),
        action,
        label=f"generated build decision {operation} stable_action",
    )
    build_status = _text(
        decision.get("build_status"),
        label=f"generated build decision {operation} build_status",
    )
    _constant(
        build_status,
        "accepted",
        label=f"generated build decision {operation} build_status",
    )
    fingerprints = _mapping(
        decision.get("fingerprints"),
        label=f"generated build decision {operation} fingerprints",
    )
    _validate_fingerprints(
        fingerprints,
        label=f"generated build decision {operation} fingerprints",
    )
    return _ActionRecipeBinding(
        semantic_operation=semantic_operation,
        availability=availability,
        execution_mode=execution_mode,
        verification=verification,
        build_status=build_status,
        fingerprints=fingerprints,
    )
