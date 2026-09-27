from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface.shared import (
        CommandTaskCancelMode,
        CommandTaskLineSeparator,
    )

SeatunnelWireEpoch = Literal[
    "legacy-raw-shell",
    "engine-custom-config",
    "engine-explicit-local-master",
    "startup-script-custom-config",
]
SeatunnelExecutionEpoch = Literal[
    "legacy-shell-file",
    "shell-interceptor",
    "task-request",
]
SeatunnelWorkflowParameterForwarding = Literal[
    "none",
    "unsafe-values",
    "quoted-values",
]
SeatunnelApplicationIdObservation = Literal[
    "post-exit-log-discovery-nondurable",
    "none",
]


@dataclass(frozen=True, slots=True)
class SeatunnelAuthoringSurface:
    """SeaTunnel literal-config wire, command, disclosure, and lifecycle facts."""

    available: bool
    wire_epoch: SeatunnelWireEpoch | None
    execution_epoch: SeatunnelExecutionEpoch | None
    line_separator: CommandTaskLineSeparator | None
    script_encoding: Literal["utf-8", "platform-default"] | None
    parameter_substitution: bool
    workflow_parameter_forwarding: SeatunnelWorkflowParameterForwarding
    startup_script_guarded: bool
    task_params_logged: bool
    config_logged: bool
    command_logged: bool
    application_id_observation: SeatunnelApplicationIdObservation
    result_output_supported: bool
    cancel_mode: CommandTaskCancelMode | None
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_SEATUNNEL_ABSENT = SeatunnelAuthoringSurface(
    available=False,
    wire_epoch=None,
    execution_epoch=None,
    line_separator=None,
    script_encoding=None,
    parameter_substitution=False,
    workflow_parameter_forwarding="none",
    startup_script_guarded=False,
    task_params_logged=False,
    config_logged=False,
    command_logged=False,
    application_id_observation="none",
    result_output_supported=False,
    cancel_mode=None,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_SEATUNNEL_LEGACY_RAW_SHELL = replace(
    _SEATUNNEL_ABSENT,
    available=True,
    wire_epoch="legacy-raw-shell",
    execution_epoch="legacy-shell-file",
    line_separator="lf",
    script_encoding="platform-default",
    parameter_substitution=True,
    task_params_logged=True,
    config_logged=True,
    command_logged=True,
    application_id_observation="post-exit-log-discovery-nondurable",
    cancel_mode="wrapper-kill",
    retry_reexecutes=True,
)
_SEATUNNEL_ENGINE_CUSTOM_CONFIG = replace(
    _SEATUNNEL_LEGACY_RAW_SHELL,
    wire_epoch="engine-custom-config",
    script_encoding="utf-8",
)
_SEATUNNEL_STARTUP_CUSTOM_CONFIG = replace(
    _SEATUNNEL_ENGINE_CUSTOM_CONFIG,
    wire_epoch="startup-script-custom-config",
    application_id_observation="none",
)
_SEATUNNEL_SHELL_INTERCEPTOR = replace(
    _SEATUNNEL_STARTUP_CUSTOM_CONFIG,
    execution_epoch="shell-interceptor",
    line_separator="system",
    cancel_mode="direct-process-destroy",
)
_SEATUNNEL_UNSAFE_FORWARDING = replace(
    _SEATUNNEL_SHELL_INTERCEPTOR,
    workflow_parameter_forwarding="unsafe-values",
    cancel_mode="process-tree-and-application",
)
_SEATUNNEL_TASK_REQUEST_UNSAFE_FORWARDING = replace(
    _SEATUNNEL_UNSAFE_FORWARDING,
    execution_epoch="task-request",
)
_SEATUNNEL_TASK_REQUEST_QUOTED_FORWARDING = replace(
    _SEATUNNEL_TASK_REQUEST_UNSAFE_FORWARDING,
    workflow_parameter_forwarding="quoted-values",
    startup_script_guarded=True,
)


def _seatunnel_surface(version: str) -> SeatunnelAuthoringSurface:
    if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _SEATUNNEL_LEGACY_RAW_SHELL
    if version in {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }:
        return _seatunnel_31_surface(version)
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _SEATUNNEL_SHELL_INTERCEPTOR
    if version in {"3.3.1", "3.3.2"}:
        return _SEATUNNEL_UNSAFE_FORWARDING
    if version == "3.4.0":
        return _SEATUNNEL_TASK_REQUEST_UNSAFE_FORWARDING
    if version in {"3.4.1", "3.4.2", "3.4.3"}:
        return _SEATUNNEL_TASK_REQUEST_QUOTED_FORWARDING
    if version in TARGET_DS_VERSIONS:
        return _SEATUNNEL_ABSENT
    message = f"No exact SEATUNNEL authoring surface for DolphinScheduler {version}"
    raise ValueError(message)


def _seatunnel_31_surface(version: str) -> SeatunnelAuthoringSurface:
    """Select the exact engine and remote-adapter transitions within 3.1.x."""
    if version == "3.1.6":
        return replace(
            _SEATUNNEL_ENGINE_CUSTOM_CONFIG,
            wire_epoch="engine-explicit-local-master",
            application_id_observation="none",
        )
    if version in {"3.1.1", "3.1.2", "3.1.3", "3.1.4", "3.1.5"}:
        return replace(
            _SEATUNNEL_ENGINE_CUSTOM_CONFIG,
            application_id_observation="none",
        )
    if version == "3.1.0":
        return _SEATUNNEL_ENGINE_CUSTOM_CONFIG
    if version in {"3.1.7", "3.1.8", "3.1.9"}:
        return _SEATUNNEL_STARTUP_CUSTOM_CONFIG
    message = f"No exact SeaTunnel 3.1.x engine surface for {version}"
    raise ValueError(message)
