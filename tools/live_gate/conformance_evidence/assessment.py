"""Validate terminal bundle coordinates, inheritance and static assessment digests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from live_gate.conformance_evidence.types import (
    _Assessment,
    _AssessmentBundle,
    _AssessmentCoordinate,
    _CurrentTruth,
)
from live_gate.conformance_evidence.values import (
    _EXECUTION_MODES,
    _SUPPORT_LEVELS,
    _VERIFICATIONS,
    _bundle_digest,
    _canonical_digest,
    _exact_keys,
    _sha256,
    _text_sequence,
)
from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_bool as _boolean
from live_gate.evidence_values import required_list as _sequence
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text

if TYPE_CHECKING:
    from collections.abc import Mapping


def _current_assessment(current: _CurrentTruth) -> _Assessment:
    try:
        return _validate_assessment(
            current.assessment,
            target_versions=current.versions,
            profiles=current.profiles,
        )
    except (KeyError, TypeError, ValueError) as error:
        message = f"current conformance assessment is invalid: {error}"
        raise ValueError(message) from error


def _validate_assessment(
    value: object,
    *,
    target_versions: tuple[str, ...],
    profiles: Mapping[str, object],
) -> _Assessment:
    assessment = _mapping(value, label="conformance assessment")
    _exact_keys(
        assessment,
        {
            "schema_version",
            "kind",
            "claim",
            "catalog_digest",
            "assessment_digest",
            "promotion_claimed",
            "support_level_changes",
            "tested_changes",
            "unassessed_axes",
            "target_versions",
            "summary",
            "bundles",
        },
        label="conformance assessment",
    )
    _constant(assessment.get("schema_version"), 1, label="assessment schema_version")
    _constant(
        assessment.get("kind"),
        "dsctl-conformance-bundle-static-assessment",
        label="assessment kind",
    )
    _constant(
        assessment.get("claim"),
        "static-action-closure-only",
        label="assessment claim",
    )
    for field in ("promotion_claimed", "support_level_changes", "tested_changes"):
        _constant(assessment.get(field), False, label=f"assessment {field}")
    _constant(
        assessment.get("unassessed_axes"),
        ["facets", "scenarios", "freshness", "live_evidence"],
        label="assessment unassessed_axes",
    )
    versions = tuple(
        _text_sequence(
            assessment.get("target_versions"),
            label="assessment target_versions",
        )
    )
    _constant(
        versions,
        target_versions,
        label="assessment target_versions",
    )
    profiles = _mapping(profiles, label="generated profiles")
    raw_bundles = _sequence(assessment.get("bundles"), label="assessment bundles")
    parsed_bundles = tuple(
        _assessment_bundle(
            raw_bundle,
            target_versions=versions,
            profiles=profiles,
        )
        for raw_bundle in raw_bundles
    )
    bundles = {bundle.name: bundle for bundle in parsed_bundles}
    if not bundles or len(bundles) != len(parsed_bundles):
        message = "assessment bundle names must be non-empty and unique"
        raise ValueError(message)
    _validate_assessment_inheritance(bundles)
    _constant(
        tuple(bundle.name for bundle in parsed_bundles),
        _canonical_assessment_bundle_order(bundles),
        label="assessment bundle order",
    )
    catalog_digest = _sha256(
        assessment.get("catalog_digest"),
        label="assessment catalog_digest",
    )
    _constant(
        catalog_digest,
        _assessment_catalog_digest(parsed_bundles),
        label="assessment catalog_digest",
    )
    ready_count = sum(
        coordinate.status == "ready"
        for bundle in parsed_bundles
        for coordinate in bundle.coordinates.values()
    )
    coordinate_count = len(parsed_bundles) * len(versions)
    _constant(
        assessment.get("summary"),
        {
            "bundle_count": len(parsed_bundles),
            "coordinate_count": coordinate_count,
            "ready_coordinate_count": ready_count,
            "blocked_coordinate_count": coordinate_count - ready_count,
        },
        label="assessment summary",
    )
    assessment_digest = _sha256(
        assessment.get("assessment_digest"),
        label="assessment assessment_digest",
    )
    _constant(
        assessment_digest,
        _canonical_digest(
            {
                key: nested
                for key, nested in assessment.items()
                if key != "assessment_digest"
            }
        ),
        label="assessment assessment_digest",
    )
    return _Assessment(
        schema_version=1,
        catalog_digest=catalog_digest,
        assessment_digest=assessment_digest,
        bundles=bundles,
    )


def _assessment_bundle(
    value: object,
    *,
    target_versions: tuple[str, ...],
    profiles: Mapping[str, object],
) -> _AssessmentBundle:
    bundle = _mapping(value, label="assessment bundle")
    _exact_keys(
        bundle,
        {
            "name",
            "bundle_digest",
            "extends",
            "direct_actions",
            "required_actions",
            "status",
            "coordinate_count",
            "ready_coordinate_count",
            "blocked_coordinate_count",
            "versions",
        },
        label="assessment bundle",
    )
    name = _text(bundle.get("name"), label="assessment bundle name")
    extends = tuple(
        _text_sequence(
            bundle.get("extends"),
            label=f"assessment bundle {name} extends",
            allow_empty=True,
        )
    )
    if tuple(sorted(extends)) != extends:
        message = f"assessment bundle {name} extends are not canonical"
        raise ValueError(message)
    direct_actions = tuple(
        _text_sequence(
            bundle.get("direct_actions"),
            label=f"assessment bundle {name} direct_actions",
            allow_empty=True,
        )
    )
    if tuple(sorted(direct_actions)) != direct_actions:
        message = f"assessment bundle {name} direct_actions are not canonical"
        raise ValueError(message)
    required_actions = tuple(
        _text_sequence(
            bundle.get("required_actions"),
            label=f"assessment bundle {name} required_actions",
        )
    )
    if tuple(sorted(required_actions)) != required_actions:
        message = f"assessment bundle {name} required_actions are not canonical"
        raise ValueError(message)
    bundle_digest = _sha256(
        bundle.get("bundle_digest"),
        label=f"assessment bundle {name} bundle_digest",
    )
    _constant(
        bundle_digest,
        _bundle_digest(name, required_actions=required_actions),
        label=f"assessment bundle {name} bundle_digest",
    )
    raw_coordinates = _sequence(
        bundle.get("versions"),
        label=f"assessment bundle {name} versions",
    )
    coordinates = tuple(
        _assessment_coordinate(
            raw_coordinate,
            bundle_name=name,
            required_actions=required_actions,
            profiles=profiles,
        )
        for raw_coordinate in raw_coordinates
    )
    observed_versions = tuple(version for version, _coordinate in coordinates)
    _constant(
        observed_versions,
        target_versions,
        label=f"assessment bundle {name} exact version order",
    )
    coordinate_map = dict(coordinates)
    ready_count = sum(row.status == "ready" for row in coordinate_map.values())
    expected_status = "ready" if ready_count == len(target_versions) else "blocked"
    for field, expected in (
        ("status", expected_status),
        ("coordinate_count", len(target_versions)),
        ("ready_coordinate_count", ready_count),
        ("blocked_coordinate_count", len(target_versions) - ready_count),
    ):
        _constant(
            bundle.get(field),
            expected,
            label=f"assessment bundle {name} {field}",
        )
    return _AssessmentBundle(
        name=name,
        bundle_digest=bundle_digest,
        extends=extends,
        direct_actions=direct_actions,
        required_actions=required_actions,
        coordinates=coordinate_map,
    )


def _assessment_coordinate(
    value: object,
    *,
    bundle_name: str,
    required_actions: tuple[str, ...],
    profiles: Mapping[str, object],
) -> tuple[str, _AssessmentCoordinate]:
    coordinate = _mapping(value, label=f"assessment bundle {bundle_name} coordinate")
    _exact_keys(
        coordinate,
        {"version", "status", "support_level", "tested", "blockers"},
        label=f"assessment bundle {bundle_name} coordinate",
    )
    version = _text(coordinate.get("version"), label="assessment coordinate version")
    profile = _mapping(
        profiles.get(version),
        label=f"generated profile {version}",
    )
    status = _text(coordinate.get("status"), label="assessment coordinate status")
    if status not in {"ready", "blocked"}:
        message = "assessment coordinate status must be ready or blocked"
        raise ValueError(message)
    support_level = _text(
        coordinate.get("support_level"),
        label="assessment coordinate support_level",
    )
    if support_level not in _SUPPORT_LEVELS:
        message = "assessment coordinate support_level is not recognized"
        raise ValueError(message)
    tested = _boolean(coordinate.get("tested"), label="assessment coordinate tested")
    if tested != (support_level in {"legacy_core", "full"}):
        message = "assessment coordinate support/tested values are inconsistent"
        raise ValueError(message)
    _constant(
        support_level,
        profile.get("support_level"),
        label="assessment coordinate support_level",
    )
    _constant(
        tested,
        profile.get("tested"),
        label="assessment coordinate tested",
    )
    blockers = _sequence(
        coordinate.get("blockers"),
        label="assessment coordinate blockers",
    )
    if (status == "ready") != (not blockers):
        message = "assessment coordinate readiness and blockers are inconsistent"
        raise ValueError(message)
    blocker_actions = [
        _validate_assessment_blocker(
            blocker,
            required_actions=frozenset(required_actions),
        )
        for blocker in blockers
    ]
    if len(blocker_actions) != len(set(blocker_actions)):
        message = "assessment coordinate blocker actions must be unique"
        raise ValueError(message)
    if blocker_actions != sorted(blocker_actions):
        message = "assessment coordinate blocker order is not canonical"
        raise ValueError(message)
    _constant(
        blockers,
        _expected_profile_blockers(profile, required_actions=required_actions),
        label="assessment coordinate blockers",
    )
    return version, _AssessmentCoordinate(
        support_level=support_level,
        tested=tested,
        status=status,
    )


def _validate_assessment_blocker(
    value: object,
    *,
    required_actions: frozenset[str],
) -> str:
    blocker = _mapping(value, label="assessment blocker")
    _exact_keys(
        blocker,
        {
            "action",
            "availability",
            "reason",
            "constraint",
            "execution_mode",
            "verification",
        },
        label="assessment blocker",
    )
    action = _text(blocker.get("action"), label="assessment blocker action")
    if action not in required_actions:
        message = "assessment blocker action is outside its bundle"
        raise ValueError(message)
    availability = _text(
        blocker.get("availability"),
        label="assessment blocker availability",
    )
    if availability not in {"limited", "unsupported"}:
        message = "assessment blocker availability is not terminal"
        raise ValueError(message)
    reason = _text(blocker.get("reason"), label="assessment blocker reason")
    _text(blocker.get("constraint"), label="assessment blocker constraint")
    _constant(
        blocker.get("execution_mode"),
        "not_executable",
        label="assessment blocker execution_mode",
    )
    _constant(
        blocker.get("verification"),
        "static",
        label="assessment blocker verification",
    )
    expected_reason = {
        "limited": "upstream_capability_limited",
        "unsupported": "upstream_capability_absent",
    }[availability]
    _constant(reason, expected_reason, label="assessment blocker reason")
    return action


def _validate_assessment_inheritance(
    bundles: Mapping[str, _AssessmentBundle],
) -> None:
    resolved: set[str] = set()
    visiting: set[str] = set()

    def resolve(name: str) -> None:
        if name in resolved:
            return
        if name in visiting:
            message = f"assessment bundle inheritance cycle includes {name!r}"
            raise ValueError(message)
        visiting.add(name)
        bundle = bundles[name]
        inherited_actions: set[str] = set()
        for parent_name in bundle.extends:
            if parent_name not in bundles:
                message = (
                    f"assessment bundle {name!r} has unknown parent {parent_name!r}"
                )
                raise ValueError(message)
            resolve(parent_name)
            inherited_actions.update(bundles[parent_name].required_actions)
        if not set(bundle.required_actions).issuperset(inherited_actions):
            message = f"assessment bundle {name!r} is not a parent-action superset"
            raise ValueError(message)
        expected_direct = tuple(
            action
            for action in bundle.required_actions
            if action not in inherited_actions
        )
        _constant(
            bundle.direct_actions,
            expected_direct,
            label=f"assessment bundle {name} direct_actions",
        )
        visiting.remove(name)
        resolved.add(name)

    for bundle_name in bundles:
        resolve(bundle_name)


def _canonical_assessment_bundle_order(
    bundles: Mapping[str, _AssessmentBundle],
) -> tuple[str, ...]:
    remaining = set(bundles)
    emitted: set[str] = set()
    ordered: list[str] = []
    while remaining:
        ready = sorted(
            name for name in remaining if set(bundles[name].extends).issubset(emitted)
        )
        if not ready:
            message = "assessment bundle inheritance has no topological order"
            raise ValueError(message)
        ordered.extend(ready)
        emitted.update(ready)
        remaining.difference_update(ready)
    return tuple(ordered)


def _expected_profile_blockers(
    profile: Mapping[str, object],
    *,
    required_actions: tuple[str, ...],
) -> list[dict[str, object]]:
    actions = _mapping(profile.get("actions"), label="generated profile actions")
    blockers: list[dict[str, object]] = []
    for action in required_actions:
        capability = _mapping(
            actions.get(action),
            label=f"generated profile action {action}",
        )
        availability = _text(
            capability.get("availability"),
            label=f"generated profile action {action} availability",
        )
        if availability not in {"supported", "limited", "unsupported"}:
            message = (
                f"generated profile action {action} availability is not recognized"
            )
            raise ValueError(message)
        execution_mode = _text(
            capability.get("execution_mode"),
            label=f"generated profile action {action} execution_mode",
        )
        if execution_mode not in {*_EXECUTION_MODES, "not_executable"}:
            message = (
                f"generated profile action {action} execution_mode is not recognized"
            )
            raise ValueError(message)
        verification = _text(
            capability.get("verification"),
            label=f"generated profile action {action} verification",
        )
        if verification not in _VERIFICATIONS:
            message = (
                f"generated profile action {action} verification is not recognized"
            )
            raise ValueError(message)
        if availability == "supported":
            continue
        blockers.append(
            {
                "action": action,
                "availability": availability,
                "reason": _text(
                    capability.get("reason"),
                    label=f"generated profile action {action} reason",
                ),
                "constraint": _text(
                    capability.get("constraint"),
                    label=f"generated profile action {action} constraint",
                ),
                "execution_mode": execution_mode,
                "verification": verification,
            }
        )
    return blockers


def _assessment_catalog_digest(bundles: tuple[_AssessmentBundle, ...]) -> str:
    return _canonical_digest(
        {
            "schema_version": 1,
            "kind": "dsctl-conformance-bundle-catalog",
            "claim": "static-action-closure-only",
            "bundles": [
                {
                    "name": bundle.name,
                    "extends": sorted(bundle.extends),
                    "required_actions": sorted(bundle.required_actions),
                }
                for bundle in sorted(bundles, key=lambda item: item.name)
            ],
        }
    )
