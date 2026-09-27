"""Emit the machine-readable static conformance-bundle assessment."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ds_codegen.conformance_bundles import assess_static_conformance_bundles
from dsctl.generated.conformance_bundles import CONFORMANCE_BUNDLE_DATA
from dsctl.generated.version_profiles import PROFILE_DATA

_EXPECTED_BUNDLE_NAMES = frozenset({"legacy_core/v1", "full_core/v1"})
_EXPECTED_COORDINATE_COUNT = len(PROFILE_DATA["target_versions"]) * len(
    _EXPECTED_BUNDLE_NAMES
)
_TERMINAL_FAILURE = (
    "static conformance assessment does not cover exactly "
    f"{_EXPECTED_COORDINATE_COUNT} terminal coordinates"
)


@dataclass(frozen=True)
class _TerminalBundleCounts:
    name: str
    ready: int
    blocked: int


class _IncompleteAssessmentError(ValueError):
    """Raised when a static coordinate has no terminal assessment."""


def build_parser() -> argparse.ArgumentParser:
    """Build the static conformance assessment parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Trace named support-tier action closures over every exact generated "
            "profile without making a promotion claim."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write deterministic JSON here instead of stdout.",
    )
    parser.add_argument(
        "--check-generated",
        action="store_true",
        help="Fail if the packaged generated assessment is not current.",
    )
    parser.add_argument(
        "--require-assessed",
        action="store_true",
        help=(
            f"Require all {_EXPECTED_COORDINATE_COUNT} named bundle/version "
            "coordinates to terminate as "
            "ready or blocked."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Render the current static action-closure assessment."""
    args = build_parser().parse_args(argv)
    try:
        report = assess_static_conformance_bundles(PROFILE_DATA)
        if args.check_generated and _load_generated_assessment() != report:
            sys.stderr.write(
                "error: generated conformance bundle data has diverged; "
                "regenerate runtime bundles\n"
            )
            return 1
        if args.require_assessed:
            terminal_error = _terminal_assessment_error(report)
            if terminal_error is not None:
                sys.stderr.write(f"error: {terminal_error}\n")
                return 1
        rendered = (
            json.dumps(
                report,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        if args.output is None:
            sys.stdout.write(rendered)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
    except (ImportError, OSError, TypeError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    return 0


def _load_generated_assessment() -> Mapping[str, object]:
    if not isinstance(CONFORMANCE_BUNDLE_DATA, Mapping):
        message = "generated conformance bundle data must be an object"
        raise TypeError(message)
    return cast("Mapping[str, object]", CONFORMANCE_BUNDLE_DATA)


def _terminal_assessment_error(report: Mapping[str, object]) -> str | None:
    try:
        _require_terminal_assessment(report)
    except _IncompleteAssessmentError as exc:
        return f"{_TERMINAL_FAILURE}: {exc}"
    return None


def _require_terminal_assessment(report: Mapping[str, object]) -> None:
    target_versions = report.get("target_versions")
    expected_versions = PROFILE_DATA["target_versions"]
    if target_versions != expected_versions or not isinstance(target_versions, list):
        message = "exact target versions differ"
        raise _IncompleteAssessmentError(message)
    bundles = report.get("bundles")
    if not isinstance(bundles, list) or len(bundles) != 2:
        message = "expected two named bundles"
        raise _IncompleteAssessmentError(message)
    counts = [
        _terminal_bundle_counts(raw_bundle, target_versions=target_versions)
        for raw_bundle in bundles
    ]
    if {item.name for item in counts} != _EXPECTED_BUNDLE_NAMES:
        message = "named bundle set differs"
        raise _IncompleteAssessmentError(message)
    ready_count = sum(item.ready for item in counts)
    blocked_count = sum(item.blocked for item in counts)
    summary = report.get("summary")
    expected_summary = {
        "bundle_count": 2,
        "coordinate_count": _EXPECTED_COORDINATE_COUNT,
        "ready_coordinate_count": ready_count,
        "blocked_coordinate_count": blocked_count,
    }
    if (
        summary != expected_summary
        or ready_count + blocked_count != _EXPECTED_COORDINATE_COUNT
    ):
        message = "coordinate summary is inconsistent"
        raise _IncompleteAssessmentError(message)


def _terminal_bundle_counts(
    raw_bundle: object,
    *,
    target_versions: list[object],
) -> _TerminalBundleCounts:
    if not isinstance(raw_bundle, Mapping):
        message = "bundle row is not an object"
        raise _IncompleteAssessmentError(message)
    name = raw_bundle.get("name")
    if not isinstance(name, str):
        message = "bundle name is missing"
        raise _IncompleteAssessmentError(message)
    versions = raw_bundle.get("versions")
    if not isinstance(versions, list) or len(versions) != len(target_versions):
        message = f"{name} does not cover every exact version"
        raise _IncompleteAssessmentError(message)
    states = [
        _terminal_coordinate_state(raw_version, bundle_name=name)
        for raw_version in versions
    ]
    observed_versions = [version for version, _state in states]
    if len(observed_versions) != len(set(observed_versions)):
        message = f"{name} versions are duplicated"
        raise _IncompleteAssessmentError(message)
    if set(observed_versions) != set(target_versions):
        message = f"{name} exact version set differs"
        raise _IncompleteAssessmentError(message)
    return _TerminalBundleCounts(
        name=name,
        ready=sum(state == "ready" for _version, state in states),
        blocked=sum(state == "blocked" for _version, state in states),
    )


def _terminal_coordinate_state(
    raw_version: object,
    *,
    bundle_name: str,
) -> tuple[str, str]:
    if not isinstance(raw_version, Mapping):
        message = f"{bundle_name} coordinate is not an object"
        raise _IncompleteAssessmentError(message)
    version = raw_version.get("version")
    status = raw_version.get("status")
    blockers = raw_version.get("blockers")
    if not isinstance(version, str):
        message = f"{bundle_name} coordinate version is missing"
        raise _IncompleteAssessmentError(message)
    if not isinstance(blockers, list):
        message = f"{bundle_name}/{version} blockers are not a list"
        raise _IncompleteAssessmentError(message)
    if (status == "ready" and not blockers) or (status == "blocked" and blockers):
        return version, status
    message = f"{bundle_name}/{version} is not terminal"
    raise _IncompleteAssessmentError(message)


if __name__ == "__main__":
    raise SystemExit(main())
