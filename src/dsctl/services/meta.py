from dsctl import __version__
from dsctl.config import load_profile, load_selected_ds_version
from dsctl.context import load_context
from dsctl.output import CommandResult
from dsctl.upstream import SUPPORTED_VERSIONS, get_version_support


def get_version_result(*, env_file: str | None = None) -> CommandResult:
    """Return CLI and supported DolphinScheduler version metadata."""
    selected_version = load_selected_ds_version(env_file)
    support = get_version_support(selected_version)
    return CommandResult(
        data={
            "cli": __version__,
            "ds": support.server_version,
            "selected_ds_version": support.server_version,
            "contract_version": support.contract_version,
            "family": support.family,
            "support_level": support.support_level,
            "supported_ds_versions": list(SUPPORTED_VERSIONS),
        }
    )


def get_context_result(*, env_file: str | None = None) -> CommandResult:
    """Return the effective config profile and stored session context."""
    profile = load_profile(env_file)
    session = load_context()
    return CommandResult(
        data={
            "api_url": profile.api_url,
            "ds_version": profile.ds_version,
            "project": session.project,
            "workflow": session.workflow,
        }
    )
