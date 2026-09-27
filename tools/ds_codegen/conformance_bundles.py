"""Assess named conformance bundles against exact generated profiles."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ds_codegen.profile_ledger import exact_versions
from ds_codegen.version_profiles import compile_version_profile_data
from dsctl.cli_surface import stable_leaf_actions

DEFAULT_CONFORMANCE_BUNDLE_CATALOG = Path(__file__).with_name(
    "conformance_bundles.json"
)
GENERATED_CONFORMANCE_BUNDLE_PATH = Path("generated/conformance_bundles.py")
CONFORMANCE_ASSESSMENT_SCHEMA_VERSION = 1
_CATALOG_KIND = "dsctl-conformance-bundle-catalog"
_ASSESSMENT_KIND = "dsctl-conformance-bundle-static-assessment"
_UNASSESSED_AXES = ("facets", "scenarios", "freshness", "live_evidence")
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_BUNDLE_NAMES = frozenset({"legacy_core/v1", "full_core/v1"})
_SUPPORT_LEVELS = frozenset({"full", "legacy_core", "experimental"})
_EXECUTION_MODES = frozenset(
    {
        "not_executable",
        "local",
        "diagnostic_recipe",
        "legacy_adapter",
        "generated_adapter",
        "wire_program",
    }
)
_VERIFICATIONS = frozenset({"static", "contract_tested", "live_smoke", "live_full"})


@dataclass(frozen=True)
class _ValidatedAssessmentBundle:
    raw: Mapping[str, object]
    name: str
    extends: tuple[str, ...]
    required_actions: tuple[str, ...]
    ready_count: int
    blocked_count: int


def load_conformance_bundle_catalog(
    path: Path = DEFAULT_CONFORMANCE_BUNDLE_CATALOG,
) -> dict[str, Any]:
    """Load a mutable copy of the tracked conformance-bundle catalog."""
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = "conformance bundle catalog must contain a JSON object"
        raise TypeError(message)
    copied = copy.deepcopy(loaded)
    specs = _bundle_specs(copied)
    declared_actions = frozenset(
        action
        for spec in specs
        for action in _text_sequence(
            spec.get("actions"),
            label=f"conformance bundle {spec.get('name')}.actions",
        )
    )
    _resolve_bundle_actions(specs, stable_actions=declared_actions)
    return copied


def assess_static_conformance_bundles(
    profile_data: Mapping[str, object],
    *,
    catalog: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Trace named action closures without making a support promotion claim."""
    loaded_catalog = (
        load_conformance_bundle_catalog()
        if catalog is None
        else copy.deepcopy(dict(catalog))
    )
    bundle_specs = _bundle_specs(loaded_catalog)
    target_versions, stable_actions, profiles = _validated_profile_data(profile_data)

    canonical_specs, resolved_actions = _canonical_bundle_plan(
        bundle_specs,
        stable_actions=stable_actions,
    )
    catalog_digest = _catalog_digest(
        canonical_specs,
        resolved_actions=resolved_actions,
    )
    bundles = [
        _assess_bundle(
            spec,
            required_actions=resolved_actions[cast("str", spec["name"])],
            resolved_actions=resolved_actions,
            target_versions=target_versions,
            profiles=profiles,
        )
        for spec in canonical_specs
    ]
    ready_count = sum(
        version["status"] == "ready"
        for bundle in bundles
        for version in bundle["versions"]
    )
    coordinate_count = len(bundles) * len(target_versions)
    assessment: dict[str, Any] = {
        "schema_version": CONFORMANCE_ASSESSMENT_SCHEMA_VERSION,
        "kind": _ASSESSMENT_KIND,
        "claim": "static-action-closure-only",
        "catalog_digest": catalog_digest,
        "promotion_claimed": False,
        "support_level_changes": False,
        "tested_changes": False,
        "unassessed_axes": list(_UNASSESSED_AXES),
        "target_versions": list(target_versions),
        "summary": {
            "bundle_count": len(bundles),
            "coordinate_count": coordinate_count,
            "ready_coordinate_count": ready_count,
            "blocked_coordinate_count": coordinate_count - ready_count,
        },
        "bundles": bundles,
    }
    assessment["assessment_digest"] = _assessment_digest(assessment)
    return assessment


def render_conformance_bundle_data(
    data: Mapping[str, object],
    *,
    profile_data: Mapping[str, object] | None = None,
) -> str:
    """Render deterministic Python containing one static bundle assessment."""
    _validate_assessment_header(data)
    current_profile_data = (
        compile_version_profile_data(stable_actions=stable_leaf_actions())
        if profile_data is None
        else profile_data
    )
    _validate_assessment_against_profiles(data, profile_data=current_profile_data)
    payload = json.dumps(
        data,
        ensure_ascii=True,
        indent=2,
        separators=(",", ": "),
        sort_keys=True,
    )
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "import json as _json",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "_CONFORMANCE_BUNDLE_JSON = r'''",
            payload,
            "'''",
            "CONFORMANCE_BUNDLE_DATA = _json.loads(_CONFORMANCE_BUNDLE_JSON)",
            "CONFORMANCE_BUNDLE_SCHEMA_VERSION = (",
            "    CONFORMANCE_BUNDLE_DATA['schema_version']",
            ")",
            "CONFORMANCE_BUNDLE_CATALOG_DIGEST = (",
            "    CONFORMANCE_BUNDLE_DATA['catalog_digest']",
            ")",
            "CONFORMANCE_ASSESSMENT_DIGEST = (",
            "    CONFORMANCE_BUNDLE_DATA['assessment_digest']",
            ")",
            "",
            "__all__ = [",
            "    'CONFORMANCE_ASSESSMENT_DIGEST',",
            "    'CONFORMANCE_BUNDLE_CATALOG_DIGEST',",
            "    'CONFORMANCE_BUNDLE_DATA',",
            "    'CONFORMANCE_BUNDLE_SCHEMA_VERSION',",
            "]",
            "",
        )
    )


def write_conformance_bundle_data(
    output_root: Path,
    profile_data: Mapping[str, object],
    *,
    catalog: Mapping[str, object] | None = None,
) -> Path:
    """Write the generated static conformance-bundle assessment module."""
    data = assess_static_conformance_bundles(profile_data, catalog=catalog)
    output_path = output_root / GENERATED_CONFORMANCE_BUNDLE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_conformance_bundle_data(data, profile_data=profile_data),
        encoding="utf-8",
    )
    return output_path


def _validate_assessment_header(data: Mapping[str, object]) -> None:
    expected_fields = {
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
    }
    if set(data) != expected_fields:
        message = "conformance assessment fields are not exact"
        raise ValueError(message)
    expected = {
        "schema_version": CONFORMANCE_ASSESSMENT_SCHEMA_VERSION,
        "kind": _ASSESSMENT_KIND,
        "claim": "static-action-closure-only",
        "promotion_claimed": False,
        "support_level_changes": False,
        "tested_changes": False,
        "unassessed_axes": list(_UNASSESSED_AXES),
    }
    for field, expected_value in expected.items():
        if data.get(field) != expected_value:
            message = f"conformance assessment {field} must be {expected_value!r}"
            raise ValueError(message)
    digest = data.get("catalog_digest")
    if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
        message = "conformance assessment catalog_digest must be a sha256 digest"
        raise ValueError(message)
    target_versions = _text_sequence(
        data.get("target_versions"),
        label="conformance assessment.target_versions",
    )
    exact_versions(target_versions, label="conformance assessment target versions")
    raw_bundles = data.get("bundles")
    if not isinstance(raw_bundles, list) or len(raw_bundles) != len(_BUNDLE_NAMES):
        message = "conformance assessment must contain the two named bundles"
        raise TypeError(message)
    bundles = [
        _validated_assessment_bundle(
            item,
            index=index,
            target_versions=target_versions,
        )
        for index, item in enumerate(raw_bundles)
    ]
    if {bundle.name for bundle in bundles} != _BUNDLE_NAMES:
        message = "conformance assessment named bundle set differs"
        raise ValueError(message)
    bundle_specs = [
        {
            "name": bundle.name,
            "extends": list(bundle.extends),
            "actions": list(bundle.required_actions),
        }
        for bundle in bundles
    ]
    stable_actions = frozenset(
        action for bundle in bundles for action in bundle.required_actions
    )
    canonical_specs, resolved_actions = _canonical_bundle_plan(
        bundle_specs,
        stable_actions=stable_actions,
    )
    observed_names = [bundle.name for bundle in bundles]
    canonical_names = [
        _text(spec.get("name"), label="conformance bundle name")
        for spec in canonical_specs
    ]
    if observed_names != canonical_names:
        message = "conformance assessment bundle order is not canonical"
        raise ValueError(message)
    canonical_specs_by_name = {
        _text(spec.get("name"), label="conformance bundle name"): spec
        for spec in canonical_specs
    }
    for bundle in bundles:
        canonical_spec = canonical_specs_by_name[bundle.name]
        canonical_extends = _text_sequence(
            canonical_spec.get("extends"),
            label=f"conformance bundle {bundle.name}.extends",
            allow_empty=True,
        )
        if (
            bundle.extends != canonical_extends
            or bundle.required_actions != resolved_actions[bundle.name]
        ):
            message = (
                f"conformance assessment bundle {bundle.name} ordering is not canonical"
            )
            raise ValueError(message)
        inherited_actions = {
            action for parent in bundle.extends for action in resolved_actions[parent]
        }
        expected_direct = [
            action
            for action in bundle.required_actions
            if action not in inherited_actions
        ]
        direct_actions = _text_sequence(
            bundle.raw.get("direct_actions"),
            label=f"conformance assessment bundle {bundle.name}.direct_actions",
            allow_empty=True,
        )
        if list(direct_actions) != expected_direct:
            message = (
                f"conformance assessment bundle {bundle.name} direct_actions "
                "do not match its resolved inheritance"
            )
            raise ValueError(message)
    expected_catalog_digest = _catalog_digest(
        bundle_specs,
        resolved_actions=resolved_actions,
    )
    if digest != expected_catalog_digest:
        message = "conformance assessment catalog_digest is stale"
        raise ValueError(message)
    ready_count = sum(bundle.ready_count for bundle in bundles)
    blocked_count = sum(bundle.blocked_count for bundle in bundles)
    expected_summary = {
        "bundle_count": len(bundles),
        "coordinate_count": len(bundles) * len(target_versions),
        "ready_coordinate_count": ready_count,
        "blocked_coordinate_count": blocked_count,
    }
    if data.get("summary") != expected_summary:
        message = "conformance assessment coordinate summary is stale"
        raise ValueError(message)
    assessment_digest = data.get("assessment_digest")
    if (
        not isinstance(assessment_digest, str)
        or _SHA256.fullmatch(assessment_digest) is None
    ):
        message = "conformance assessment assessment_digest must be a sha256 digest"
        raise ValueError(message)
    if assessment_digest != _assessment_digest(data):
        message = "conformance assessment assessment_digest is stale"
        raise ValueError(message)


def _validate_assessment_against_profiles(
    data: Mapping[str, object],
    *,
    profile_data: Mapping[str, object],
) -> None:
    target_versions, _stable_actions, profiles = _validated_profile_data(profile_data)
    assessment_versions = _text_sequence(
        data.get("target_versions"),
        label="conformance assessment.target_versions",
    )
    if assessment_versions != target_versions:
        message = "conformance assessment target_versions do not match profile data"
        raise ValueError(message)
    raw_bundles = data.get("bundles")
    if not isinstance(raw_bundles, list):
        message = "conformance assessment.bundles must be a list"
        raise TypeError(message)
    for bundle_value in raw_bundles:
        bundle = _mapping(bundle_value, label="conformance assessment bundle")
        name = _text(bundle.get("name"), label="conformance assessment bundle name")
        required_actions = _text_sequence(
            bundle.get("required_actions"),
            label=f"conformance assessment bundle {name}.required_actions",
        )
        raw_versions = bundle.get("versions")
        if not isinstance(raw_versions, list):
            message = f"conformance assessment bundle {name}.versions must be a list"
            raise TypeError(message)
        coordinates = {
            _text(
                _mapping(
                    coordinate,
                    label=f"conformance assessment bundle {name} coordinate",
                ).get("version"),
                label=f"conformance assessment bundle {name} coordinate version",
            ): _mapping(
                coordinate,
                label=f"conformance assessment bundle {name} coordinate",
            )
            for coordinate in raw_versions
        }
        for version in target_versions:
            expected = _assess_bundle_version(
                version,
                profile=_mapping(
                    profiles[version],
                    label=f"profile_data.profiles.{version}",
                ),
                required_actions=required_actions,
            )
            if coordinates[version] != expected:
                message = (
                    f"conformance assessment {name}/{version} does not match profile"
                )
                raise ValueError(message)


def _validated_assessment_bundle(
    value: object,
    *,
    index: int,
    target_versions: tuple[str, ...],
) -> _ValidatedAssessmentBundle:
    bundle = _mapping(value, label=f"conformance assessment.bundles[{index}]")
    expected_fields = {
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
    }
    if set(bundle) != expected_fields:
        message = f"conformance assessment bundle {index} fields are not exact"
        raise ValueError(message)
    name = _text(bundle.get("name"), label="conformance assessment bundle name")
    extends = _text_sequence(
        bundle.get("extends"),
        label=f"conformance assessment bundle {name}.extends",
        allow_empty=True,
    )
    required_actions = _text_sequence(
        bundle.get("required_actions"),
        label=f"conformance assessment bundle {name}.required_actions",
    )
    expected_digest = _bundle_digest(name, required_actions=required_actions)
    if bundle.get("bundle_digest") != expected_digest:
        message = f"conformance assessment bundle {name} bundle_digest is stale"
        raise ValueError(message)
    versions = bundle.get("versions")
    if not isinstance(versions, list) or len(versions) != len(target_versions):
        message = f"conformance assessment bundle {name} version count is stale"
        raise ValueError(message)
    states = [
        _validated_assessment_coordinate(
            item,
            bundle_name=name,
            required_actions=frozenset(required_actions),
        )
        for item in versions
    ]
    observed_versions = [version for version, _status in states]
    if len(observed_versions) != len(set(observed_versions)) or set(
        observed_versions
    ) != set(target_versions):
        message = f"conformance assessment bundle {name} exact versions differ"
        raise ValueError(message)
    if tuple(observed_versions) != target_versions:
        message = f"conformance assessment bundle {name} version order is not canonical"
        raise ValueError(message)
    ready_count = sum(status == "ready" for _version, status in states)
    blocked_count = len(states) - ready_count
    expected_metadata = {
        "status": "ready" if blocked_count == 0 else "blocked",
        "coordinate_count": len(states),
        "ready_coordinate_count": ready_count,
        "blocked_coordinate_count": blocked_count,
    }
    if any(
        bundle.get(field) != expected for field, expected in expected_metadata.items()
    ):
        message = f"conformance assessment bundle {name} coordinate counts are stale"
        raise ValueError(message)
    return _ValidatedAssessmentBundle(
        raw=bundle,
        name=name,
        extends=extends,
        required_actions=required_actions,
        ready_count=ready_count,
        blocked_count=blocked_count,
    )


def _validated_assessment_coordinate(
    value: object,
    *,
    bundle_name: str,
    required_actions: frozenset[str],
) -> tuple[str, str]:
    coordinate = _mapping(value, label=f"conformance assessment {bundle_name} row")
    expected_fields = {"version", "status", "support_level", "tested", "blockers"}
    if set(coordinate) != expected_fields:
        message = (
            f"conformance assessment {bundle_name} coordinate fields are not exact"
        )
        raise ValueError(message)
    version = _text(
        coordinate.get("version"),
        label=f"conformance assessment {bundle_name} version",
    )
    support_level = _text_choice(
        coordinate.get("support_level"),
        choices=_SUPPORT_LEVELS,
        label=f"conformance assessment {bundle_name}/{version}.support_level",
    )
    tested = coordinate.get("tested")
    if not isinstance(tested, bool):
        message = (
            f"conformance assessment {bundle_name}/{version}.tested must be boolean"
        )
        raise TypeError(message)
    if (support_level in {"full", "legacy_core"}) != tested:
        message = (
            f"conformance assessment {bundle_name}/{version} support/tested "
            "metadata is inconsistent"
        )
        raise ValueError(message)
    blockers = coordinate.get("blockers")
    if not isinstance(blockers, list):
        message = (
            f"conformance assessment {bundle_name}/{version}.blockers must be a list"
        )
        raise TypeError(message)
    blocker_actions = [
        _validated_assessment_blocker(
            blocker,
            bundle_name=bundle_name,
            version=version,
            required_actions=required_actions,
        )
        for blocker in blockers
    ]
    if len(blocker_actions) != len(set(blocker_actions)):
        message = (
            f"conformance assessment {bundle_name}/{version} blockers duplicate actions"
        )
        raise ValueError(message)
    if blocker_actions != sorted(blocker_actions):
        message = (
            f"conformance assessment {bundle_name}/{version} blocker order "
            "is not canonical"
        )
        raise ValueError(message)
    expected_status = "ready" if not blockers else "blocked"
    if coordinate.get("status") != expected_status:
        message = (
            f"conformance assessment {bundle_name}/{version} must be ready exactly "
            "when it has no blockers"
        )
        raise ValueError(message)
    return version, expected_status


def _validated_assessment_blocker(
    value: object,
    *,
    bundle_name: str,
    version: str,
    required_actions: frozenset[str],
) -> str:
    blocker = _mapping(
        value,
        label=f"conformance assessment {bundle_name}/{version} blocker",
    )
    expected_fields = {
        "action",
        "availability",
        "reason",
        "constraint",
        "execution_mode",
        "verification",
    }
    if set(blocker) != expected_fields:
        message = (
            f"conformance assessment {bundle_name}/{version} blocker fields "
            "are not exact"
        )
        raise ValueError(message)
    action = _text(
        blocker.get("action"),
        label=f"conformance assessment {bundle_name}/{version} blocker.action",
    )
    if action not in required_actions:
        message = (
            f"conformance assessment {bundle_name}/{version} blocks an unrelated action"
        )
        raise ValueError(message)
    availability = _text_choice(
        blocker.get("availability"),
        choices=frozenset({"limited", "unsupported"}),
        label=f"conformance assessment {bundle_name}/{version}/{action}.availability",
    )
    reason = _text(
        blocker.get("reason"),
        label=f"conformance assessment {bundle_name}/{version}/{action}.reason",
    )
    _text(
        blocker.get("constraint"),
        label=f"conformance assessment {bundle_name}/{version}/{action}.constraint",
    )
    execution_mode = _text_choice(
        blocker.get("execution_mode"),
        choices=_EXECUTION_MODES,
        label=f"conformance assessment {bundle_name}/{version}/{action}.execution_mode",
    )
    verification = _text_choice(
        blocker.get("verification"),
        choices=_VERIFICATIONS,
        label=f"conformance assessment {bundle_name}/{version}/{action}.verification",
    )
    expected_reason = {
        "limited": "upstream_capability_limited",
        "unsupported": "upstream_capability_absent",
    }[availability]
    if (
        reason != expected_reason
        or execution_mode != "not_executable"
        or verification != "static"
    ):
        message = (
            f"conformance assessment {bundle_name}/{version}/{action} blocker "
            "metadata is inconsistent"
        )
        raise ValueError(message)
    return action


def _bundle_specs(catalog: Mapping[str, object]) -> list[Mapping[str, object]]:
    expected_catalog_fields = {"schema_version", "kind", "bundles"}
    if set(catalog) != expected_catalog_fields:
        message = (
            "conformance bundle catalog fields must be exactly "
            "schema_version, kind, bundles"
        )
        raise ValueError(message)
    if catalog.get("schema_version") != 1:
        message = "conformance bundle catalog schema_version must be 1"
        raise ValueError(message)
    if catalog.get("kind") != _CATALOG_KIND:
        message = f"conformance bundle catalog kind must be {_CATALOG_KIND!r}"
        raise ValueError(message)
    raw_bundles = catalog.get("bundles")
    if not isinstance(raw_bundles, list) or not raw_bundles:
        message = "conformance bundle catalog.bundles must be a non-empty list"
        raise TypeError(message)
    bundles = [
        _mapping(item, label=f"conformance bundle catalog.bundles[{index}]")
        for index, item in enumerate(raw_bundles)
    ]
    expected_bundle_fields = {"name", "extends", "actions"}
    for index, bundle in enumerate(bundles):
        if set(bundle) != expected_bundle_fields:
            message = (
                "conformance bundle fields must be exactly name, extends, actions "
                f"at index {index}"
            )
            raise ValueError(message)
    names = [
        _text(spec.get("name"), label="conformance bundle name") for spec in bundles
    ]
    if len(names) != len(set(names)):
        message = "conformance bundle names must not contain duplicates"
        raise ValueError(message)
    return bundles


def _canonical_bundle_plan(
    bundle_specs: Sequence[Mapping[str, object]],
    *,
    stable_actions: frozenset[str],
) -> tuple[list[Mapping[str, object]], dict[str, tuple[str, ...]]]:
    resolved_actions = _resolve_bundle_actions(
        bundle_specs,
        stable_actions=stable_actions,
    )
    by_name = {
        _text(spec.get("name"), label="conformance bundle name"): spec
        for spec in bundle_specs
    }
    remaining = set(by_name)
    emitted: set[str] = set()
    ordered_names: list[str] = []
    while remaining:
        ready = sorted(
            name
            for name in remaining
            if set(
                _text_sequence(
                    by_name[name].get("extends"),
                    label=f"conformance bundle {name}.extends",
                    allow_empty=True,
                )
            ).issubset(emitted)
        )
        if not ready:
            message = "conformance bundle inheritance has no topological order"
            raise ValueError(message)
        ordered_names.extend(ready)
        emitted.update(ready)
        remaining.difference_update(ready)

    canonical_specs: list[Mapping[str, object]] = []
    canonical_actions: dict[str, tuple[str, ...]] = {}
    for name in ordered_names:
        spec = by_name[name]
        required_actions = tuple(sorted(resolved_actions[name]))
        canonical_actions[name] = required_actions
        canonical_specs.append(
            {
                "name": name,
                "extends": sorted(
                    _text_sequence(
                        spec.get("extends"),
                        label=f"conformance bundle {name}.extends",
                        allow_empty=True,
                    )
                ),
                "actions": list(required_actions),
            }
        )
    return canonical_specs, canonical_actions


def _resolve_bundle_actions(
    bundle_specs: Sequence[Mapping[str, object]],
    *,
    stable_actions: frozenset[str],
) -> dict[str, tuple[str, ...]]:
    by_name = {
        _text(spec.get("name"), label="conformance bundle name"): spec
        for spec in bundle_specs
    }
    resolved: dict[str, tuple[str, ...]] = {}
    visiting: set[str] = set()

    def resolve(name: str) -> tuple[str, ...]:
        existing = resolved.get(name)
        if existing is not None:
            return existing
        if name in visiting:
            message = f"conformance bundle inheritance cycle includes {name!r}"
            raise ValueError(message)
        spec = by_name.get(name)
        if spec is None:
            message = f"unknown conformance bundle parent {name!r}"
            raise ValueError(message)
        visiting.add(name)
        parents = _text_sequence(
            spec.get("extends"),
            label=f"conformance bundle {name}.extends",
            allow_empty=True,
        )
        required_actions = _text_sequence(
            spec.get("actions"),
            label=f"conformance bundle {name}.actions",
        )
        inherited: set[str] = set()
        for parent in parents:
            inherited.update(resolve(parent))
        required = set(required_actions)
        if not required.issuperset(inherited):
            missing = ", ".join(sorted(inherited - required))
            message = (
                f"conformance bundle {name} is not a superset of its parents: {missing}"
            )
            raise ValueError(message)
        unknown_actions = sorted(required - stable_actions)
        if unknown_actions:
            message = (
                f"conformance bundle {name} contains unknown stable actions: "
                f"{', '.join(unknown_actions)}"
            )
            raise ValueError(message)
        visiting.remove(name)
        resolved[name] = required_actions
        return resolved[name]

    for bundle_name in by_name:
        resolve(bundle_name)
    return resolved


def _assess_bundle(
    spec: Mapping[str, object],
    *,
    required_actions: tuple[str, ...],
    resolved_actions: Mapping[str, tuple[str, ...]],
    target_versions: Sequence[str],
    profiles: Mapping[str, object],
) -> dict[str, Any]:
    name = _text(spec.get("name"), label="conformance bundle name")
    extends = tuple(
        sorted(
            _text_sequence(
                spec.get("extends"),
                label=f"conformance bundle {name}.extends",
                allow_empty=True,
            )
        )
    )
    inherited_actions = {
        action for parent in extends for action in resolved_actions[parent]
    }
    direct_actions = tuple(
        sorted(action for action in required_actions if action not in inherited_actions)
    )
    versions = [
        _assess_bundle_version(
            version,
            profile=_mapping(
                profiles[version],
                label=f"profile_data.profiles.{version}",
            ),
            required_actions=required_actions,
        )
        for version in target_versions
    ]
    ready_count = sum(version["status"] == "ready" for version in versions)
    return {
        "name": name,
        "bundle_digest": _bundle_digest(name, required_actions=required_actions),
        "extends": list(extends),
        "direct_actions": list(direct_actions),
        "required_actions": list(required_actions),
        "status": "ready" if ready_count == len(versions) else "blocked",
        "coordinate_count": len(versions),
        "ready_coordinate_count": ready_count,
        "blocked_coordinate_count": len(versions) - ready_count,
        "versions": versions,
    }


def _catalog_digest(
    bundle_specs: Sequence[Mapping[str, object]],
    *,
    resolved_actions: Mapping[str, tuple[str, ...]],
) -> str:
    normalized_bundles: list[dict[str, object]] = []
    for spec in bundle_specs:
        name = _text(spec.get("name"), label="conformance bundle name")
        normalized_bundles.append(
            {
                "name": name,
                "extends": sorted(
                    _text_sequence(
                        spec.get("extends"),
                        label=f"conformance bundle {name}.extends",
                        allow_empty=True,
                    )
                ),
                "required_actions": sorted(resolved_actions[name]),
            }
        )
    return _canonical_digest(
        {
            "schema_version": 1,
            "kind": _CATALOG_KIND,
            "claim": "static-action-closure-only",
            "bundles": sorted(
                normalized_bundles,
                key=lambda bundle: cast("str", bundle["name"]),
            ),
        }
    )


def _bundle_digest(name: str, *, required_actions: Sequence[str]) -> str:
    return _canonical_digest(
        {
            "claim": "static-action-closure-only",
            "name": name,
            "required_actions": sorted(required_actions),
        }
    )


def _canonical_digest(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _assessment_digest(data: Mapping[str, object]) -> str:
    return _canonical_digest(
        {
            key: copy.deepcopy(value)
            for key, value in data.items()
            if key != "assessment_digest"
        }
    )


def _validated_profile_data(
    profile_data: Mapping[str, object],
) -> tuple[tuple[str, ...], frozenset[str], Mapping[str, object]]:
    if profile_data.get("schema_version") != 1:
        message = "profile_data.schema_version must be 1"
        raise ValueError(message)
    target_versions = _text_sequence(
        profile_data.get("target_versions"),
        label="profile_data.target_versions",
    )
    exact_versions(target_versions, label="profile_data target versions")
    stable_actions = frozenset(
        _text_sequence(
            profile_data.get("stable_actions"),
            label="profile_data.stable_actions",
        )
    )
    profiles = _mapping(profile_data.get("profiles"), label="profile_data.profiles")
    if set(profiles) != set(target_versions):
        message = "profile_data.profiles must cover every exact target version"
        raise ValueError(message)
    for version in target_versions:
        profile = _mapping(
            profiles[version],
            label=f"profile_data.profiles.{version}",
        )
        if (
            profile.get("server_version") != version
            or profile.get("contract_version") != version
        ):
            message = f"profile {version} must retain its exact version identity"
            raise ValueError(message)
        actions = _mapping(
            profile.get("actions"),
            label=f"profile_data.profiles.{version}.actions",
        )
        if set(actions) != stable_actions:
            message = f"profile {version} actions do not match the stable action set"
            raise ValueError(message)
    return target_versions, stable_actions, profiles


def _assess_bundle_version(
    version: str,
    *,
    profile: Mapping[str, object],
    required_actions: Sequence[str],
) -> dict[str, Any]:
    support_level = _text_choice(
        profile.get("support_level"),
        choices=_SUPPORT_LEVELS,
        label=f"profile {version}.support_level",
    )
    tested = profile.get("tested")
    if not isinstance(tested, bool):
        message = f"profile {version}.tested must be boolean"
        raise TypeError(message)
    if (support_level in {"full", "legacy_core"}) != tested:
        message = f"profile {version} support_level/tested metadata is inconsistent"
        raise ValueError(message)
    actions = _mapping(profile.get("actions"), label=f"profile {version}.actions")
    blockers: list[dict[str, str]] = []
    for action in required_actions:
        if action not in actions:
            message = f"profile {version} is missing conformance action {action!r}"
            raise ValueError(message)
        capability = _mapping(
            actions[action],
            label=f"profile {version}.actions.{action}",
        )
        availability = _text_choice(
            capability.get("availability"),
            choices=frozenset({"supported", "limited", "unsupported"}),
            label=f"profile {version}.actions.{action}.availability",
        )
        execution_mode = _text_choice(
            capability.get("execution_mode"),
            choices=_EXECUTION_MODES,
            label=f"profile {version}.actions.{action}.execution_mode",
        )
        verification = _text_choice(
            capability.get("verification"),
            choices=_VERIFICATIONS,
            label=f"profile {version}.actions.{action}.verification",
        )
        if availability == "supported":
            continue
        blockers.append(
            {
                "action": action,
                "availability": availability,
                "reason": _text(
                    capability.get("reason"),
                    label=f"profile {version}.actions.{action}.reason",
                ),
                "constraint": _text(
                    capability.get("constraint"),
                    label=f"profile {version}.actions.{action}.constraint",
                ),
                "execution_mode": execution_mode,
                "verification": verification,
            }
        )
    blockers.sort(key=lambda blocker: blocker["action"])
    return {
        "version": version,
        "status": "ready" if not blockers else "blocked",
        "support_level": support_level,
        "tested": tested,
        "blockers": blockers,
    }


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise TypeError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"{label} keys must be text"
        raise TypeError(message)
    return cast("Mapping[str, object]", value)


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _text_choice(
    value: object,
    *,
    choices: frozenset[str],
    label: str,
) -> str:
    text = _text(value, label=label)
    if text not in choices:
        message = f"{label} must be one of {', '.join(sorted(choices))}"
        raise ValueError(message)
    return text


def _text_sequence(
    value: object,
    *,
    label: str,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list | tuple) or not all(
        isinstance(item, str) and item for item in value
    ):
        message = f"{label} must be a text sequence"
        raise TypeError(message)
    if not value and not allow_empty:
        message = f"{label} must not be empty"
        raise ValueError(message)
    if len(value) != len(set(value)):
        message = f"{label} must not contain duplicates"
        raise ValueError(message)
    return cast("tuple[str, ...]", tuple(value))


__all__ = [
    "CONFORMANCE_ASSESSMENT_SCHEMA_VERSION",
    "DEFAULT_CONFORMANCE_BUNDLE_CATALOG",
    "GENERATED_CONFORMANCE_BUNDLE_PATH",
    "assess_static_conformance_bundles",
    "load_conformance_bundle_catalog",
    "render_conformance_bundle_data",
    "write_conformance_bundle_data",
]
