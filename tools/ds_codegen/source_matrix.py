"""Load the exact upstream source matrix used by mechanical analyzers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from runtime_bundle_manifest import natural_version_key

DEFAULT_EXACT_SOURCE_MANIFEST = Path(__file__).with_name("exact_sources.json")
_VERSION_LABEL = re.compile(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?\Z")


@dataclass(frozen=True)
class ExactSourceSpec:
    """One exact DS release and its repository-local source worktree."""

    version: str
    source_root: Path


def load_exact_source_specs(
    repo_root: Path,
    manifest_path: Path | None = None,
) -> tuple[ExactSourceSpec, ...]:
    """Load the complete source plan for compatibility fact extraction."""
    path = manifest_path or DEFAULT_EXACT_SOURCE_MANIFEST
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = "exact source manifest must contain a JSON object"
        raise TypeError(message)
    if loaded.get("schema_version") != 1:
        message = "exact source manifest schema_version must be 1"
        raise ValueError(message)
    if loaded.get("kind") != "dolphinscheduler-exact-source-matrix":
        message = "invalid exact source manifest kind"
        raise ValueError(message)
    raw_targets = loaded.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        message = "exact source manifest requires at least one target"
        raise ValueError(message)

    specs = tuple(
        _parse_exact_source_spec(repo_root, index=index, raw_target=raw_target)
        for index, raw_target in enumerate(raw_targets)
    )
    versions = tuple(spec.version for spec in specs)
    duplicates = sorted(
        {version for version in versions if versions.count(version) > 1}
    )
    if duplicates:
        message = f"duplicate exact source versions: {', '.join(duplicates)}"
        raise ValueError(message)
    roots = tuple(spec.source_root for spec in specs)
    duplicate_roots = sorted({str(root) for root in roots if roots.count(root) > 1})
    if duplicate_roots:
        message = f"duplicate exact source roots: {', '.join(duplicate_roots)}"
        raise ValueError(message)

    return tuple(sorted(specs, key=lambda spec: natural_version_key(spec.version)))


def _parse_exact_source_spec(
    repo_root: Path,
    *,
    index: int,
    raw_target: object,
) -> ExactSourceSpec:
    if not isinstance(raw_target, dict):
        message = f"exact source target {index} must be a JSON object"
        raise TypeError(message)
    version = raw_target.get("version")
    if not isinstance(version, str) or _VERSION_LABEL.fullmatch(version) is None:
        message = f"exact source target {index} version must be an exact DS version"
        raise ValueError(message)
    raw_source_root = raw_target.get("source_root")
    if not isinstance(raw_source_root, str) or not raw_source_root:
        message = f"exact source target {version} source_root must be a path"
        raise ValueError(message)
    relative_source_root = Path(raw_source_root)
    if (
        relative_source_root.is_absolute()
        or ".." in relative_source_root.parts
        or relative_source_root == Path()
    ):
        message = (
            f"exact source target {version} source_root must be repository-relative"
        )
        raise ValueError(message)
    return ExactSourceSpec(
        version=version,
        source_root=repo_root / relative_source_root,
    )


__all__ = [
    "DEFAULT_EXACT_SOURCE_MANIFEST",
    "ExactSourceSpec",
    "load_exact_source_specs",
]
