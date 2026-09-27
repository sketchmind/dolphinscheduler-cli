"""Measure terminal support decisions across every exact version/action pair."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


_TERMINAL_LIMITED_REASON = "upstream_capability_limited"
_WIRE_PRESENT_RUNTIME_LIMITED_REASON = "upstream_runtime_limited"
_TERMINAL_UNSUPPORTED_REASON = "upstream_capability_absent"
_STATES = (
    "supported",
    "upstream_limited",
    "upstream_absent",
    "pending",
)


def analyze_support_coverage(profile_data: Mapping[str, object]) -> JsonObject:
    """Return a fail-closed summary of the materialized support decision space."""
    target_versions = _text_sequence(
        profile_data.get("target_versions"),
        label="profile_data.target_versions",
    )
    stable_actions = _text_sequence(
        profile_data.get("stable_actions"),
        label="profile_data.stable_actions",
    )
    profiles = _mapping(profile_data.get("profiles"), label="profile_data.profiles")
    if set(profiles) != set(target_versions):
        message = "profile_data.profiles must cover every exact target version"
        raise ValueError(message)

    counts: Counter[str] = Counter()
    version_rows: list[JsonObject] = []
    domain_counts: dict[str, Counter[str]] = defaultdict(Counter)
    pending: list[JsonObject] = []
    for version in target_versions:
        profile = _mapping(profiles[version], label=f"profile_data.profiles.{version}")
        actions = _mapping(
            profile.get("actions"),
            label=f"profile_data.profiles.{version}.actions",
        )
        if set(actions) != set(stable_actions):
            message = f"profile {version} actions do not match the stable action set"
            raise ValueError(message)
        version_counts: Counter[str] = Counter()
        version_pending: list[str] = []
        for action in stable_actions:
            capability = _mapping(
                actions[action],
                label=f"profile_data.profiles.{version}.actions.{action}",
            )
            state = _decision_state(capability, version=version, action=action)
            counts[state] += 1
            version_counts[state] += 1
            domain_counts[action.partition(".")[0]][state] += 1
            if state == "pending":
                version_pending.append(action)
                pending.append(
                    cast(
                        "JsonObject",
                        {
                            "version": version,
                            "action": action,
                            "availability": capability["availability"],
                            "reason": capability.get("reason"),
                        },
                    )
                )
        version_rows.append(
            {
                "version": version,
                "counts": _complete_counts(version_counts),
                "complete": not version_pending,
                "pending_actions": version_pending,
            }
        )

    coordinate_count = len(target_versions) * len(stable_actions)
    return {
        "schema_version": 1,
        "kind": "dsctl-exact-version-support-coverage",
        "complete": not pending,
        "target_version_count": len(target_versions),
        "stable_action_count": len(stable_actions),
        "coordinate_count": coordinate_count,
        "terminal_coordinate_count": coordinate_count - len(pending),
        "counts": _complete_counts(counts),
        "versions": version_rows,
        "domains": [
            {
                "domain": domain,
                "counts": _complete_counts(domain_counts[domain]),
                "complete": domain_counts[domain]["pending"] == 0,
            }
            for domain in sorted(domain_counts)
        ],
        "pending": pending,
    }


def render_support_coverage(report: Mapping[str, object]) -> str:
    """Serialize one support-coverage report deterministically."""
    return json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _decision_state(
    capability: Mapping[str, object],
    *,
    version: str,
    action: str,
) -> str:
    availability = capability.get("availability")
    reason = capability.get("reason")
    if availability == "supported":
        if reason is not None:
            message = f"supported capability {version}:{action} must not have a reason"
            raise ValueError(message)
        return "supported"
    if availability == "limited" and reason in {
        _TERMINAL_LIMITED_REASON,
        _WIRE_PRESENT_RUNTIME_LIMITED_REASON,
    }:
        _require_terminal_evidence(capability, version=version, action=action)
        return "upstream_limited"
    if availability == "unsupported" and reason == _TERMINAL_UNSUPPORTED_REASON:
        _require_terminal_evidence(capability, version=version, action=action)
        return "upstream_absent"
    if availability not in {"limited", "unsupported"}:
        message = f"invalid availability for {version}:{action}: {availability!r}"
        raise ValueError(message)
    return "pending"


def _require_terminal_evidence(
    capability: Mapping[str, object],
    *,
    version: str,
    action: str,
) -> None:
    constraint = capability.get("constraint")
    if not isinstance(constraint, str) or not constraint.strip():
        message = f"terminal capability {version}:{action} requires a constraint"
        raise ValueError(message)
    sources = capability.get("evidence_sources")
    if (
        not isinstance(sources, list)
        or not sources
        or not all(isinstance(source, str) and source.strip() for source in sources)
    ):
        message = f"terminal capability {version}:{action} requires evidence_sources"
        raise ValueError(message)


def _complete_counts(counts: Counter[str]) -> JsonObject:
    return cast("JsonObject", {state: counts[state] for state in _STATES})


def _mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise TypeError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"{label} keys must be text"
        raise TypeError(message)
    return cast("Mapping[str, object]", value)


def _text_sequence(value: object, *, label: str) -> Sequence[str]:
    if (
        not isinstance(value, list | tuple)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        message = f"{label} must be a non-empty-text sequence"
        raise TypeError(message)
    if len(value) != len(set(value)):
        message = f"{label} must not contain duplicates"
        raise ValueError(message)
    return cast("Sequence[str]", value)


__all__ = ["analyze_support_coverage", "render_support_coverage"]
