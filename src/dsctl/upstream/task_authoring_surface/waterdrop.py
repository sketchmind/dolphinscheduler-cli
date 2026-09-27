from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

WaterdropScriptEncoding = Literal["platform-default"]


@dataclass(frozen=True, slots=True)
class WaterdropAuthoringSurface:
    """Legacy Waterdrop Shell alias, staging, and lifecycle facts."""

    available: bool
    exclusion_reason: str | None
    channel_registered: bool
    script_encoding: WaterdropScriptEncoding | None
    parameter_substitution: bool
    task_params_logged: bool
    command_logged: bool
    result_output_supported: bool
    cancel_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_WATERDROP_ABSENT = WaterdropAuthoringSurface(
    available=False,
    exclusion_reason="upstream-absent",
    channel_registered=False,
    script_encoding=None,
    parameter_substitution=False,
    task_params_logged=False,
    command_logged=False,
    result_output_supported=False,
    cancel_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_WATERDROP_UNREGISTERED = replace(
    _WATERDROP_ABSENT,
    exclusion_reason="waterdrop-channel-not-registered-to-shell",
    script_encoding="platform-default",
    parameter_substitution=True,
)
_WATERDROP_SHELL_ALIAS = WaterdropAuthoringSurface(
    available=True,
    exclusion_reason=None,
    channel_registered=True,
    script_encoding="platform-default",
    parameter_substitution=True,
    task_params_logged=True,
    command_logged=True,
    result_output_supported=False,
    cancel_supported=True,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)


def _waterdrop_surface(version: str) -> WaterdropAuthoringSurface:
    if version == "2.0.0":
        return _WATERDROP_UNREGISTERED
    if version in {
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        return _WATERDROP_SHELL_ALIAS
    if version in TARGET_DS_VERSIONS:
        return _WATERDROP_ABSENT
    message = f"No exact WATERDROP authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
