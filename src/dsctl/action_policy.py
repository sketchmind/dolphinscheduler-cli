from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.services.version_resolution import (
    CompatibilityResolution,
    resolve_runtime_selection,
    resolve_target,
    resolve_version,
    set_invocation_action,
)
from dsctl.upstream import get_version_support, is_action_preflight_exempt
from dsctl.upstream.read_compatibility import available_read_actions
from dsctl.upstream.registry import is_local_action

if TYPE_CHECKING:
    from pathlib import Path


def preflight_selected_action(
    action: str,
    env_file: Path | None,
) -> frozenset[str] | None:
    """Apply the exact-version CLI action policy before command execution."""
    set_invocation_action(action)
    if is_action_preflight_exempt(action) or action in {"enum.names", "enum.list"}:
        return None
    resolution = resolve_target(
        env_file, mode="local" if is_local_action(action) else "runtime"
    )
    if isinstance(resolution, CompatibilityResolution):
        if is_local_action(action):
            resolve_version(env_file, mode="local")
        resolve_runtime_selection(env_file, action=action)
        return available_read_actions(
            resolution.candidate_versions,
            compatible_operations=resolution.compatible_operations,
        )
    support = get_version_support(resolution.version)
    support.catalog.preflight(action)
    return support.catalog.available_actions()


__all__ = ["preflight_selected_action"]
