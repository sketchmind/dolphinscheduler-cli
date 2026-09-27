from dsctl import __version__
from dsctl.output import CommandResult
from dsctl.services.version_resolution import (
    CompatibilityResolution,
    compatibility_details,
    resolve_settings,
    resolve_target,
    selection_details,
)
from dsctl.upstream import SUPPORTED_VERSIONS, get_version_support
from dsctl.upstream.read_compatibility import available_read_actions


def get_version_result(*, env_file: str | None = None) -> CommandResult:
    """Return CLI and selectable DolphinScheduler version metadata."""
    resolution = resolve_target(env_file, mode="local")
    if isinstance(resolution, CompatibilityResolution):
        return CommandResult(
            data={
                "cli": __version__,
                "ds": None,
                "selected_ds_version": None,
                **compatibility_details(resolution),
                "automatic_read_actions": sorted(
                    available_read_actions(
                        resolution.candidate_versions,
                        compatible_operations=resolution.compatible_operations,
                    )
                ),
                "supported_ds_versions": list(SUPPORTED_VERSIONS),
            }
        )
    support = get_version_support(resolution.version)
    return CommandResult(
        data={
            "cli": __version__,
            "ds": support.server_version,
            "selected_ds_version": support.server_version,
            "version_source": resolution.source,
            "version_checked_at": resolution.checked_at,
            "version_evidence": resolution.evidence,
            "contract_version": support.contract_version,
            "family": support.family,
            "support_level": support.support_level,
            "supported_ds_versions": list(SUPPORTED_VERSIONS),
        }
    )


def get_context_result(*, env_file: str | None = None) -> CommandResult:
    """Return the locally resolved target used by subsequent commands."""
    settings = resolve_settings(env_file)
    return CommandResult(
        data={
            "context": settings.context_name,
            "api_url": settings.api_url,
            "ds_version": settings.requested_version or "auto",
            "project": settings.project,
        },
        resolved={
            "selection": selection_details(settings),
            "remote_validation": "not_performed",
        },
    )
