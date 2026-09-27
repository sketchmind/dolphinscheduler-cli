from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface.shared import (
        CommandTaskCancelMode,
        CommandTaskLineSeparator,
    )

ChunJunExecutionEpoch = Literal[
    "legacy-shell-file",
    "shell-interceptor",
    "task-request",
]
ChunJunApplicationIdObservation = Literal[
    "post-exit-log-discovery-nondurable",
    "none",
]


@dataclass(frozen=True, slots=True)
class ChunJunAuthoringSurface:
    """ChunJun custom-JSON wire, launcher, disclosure, and recovery facts."""

    available: bool
    execution_epoch: ChunJunExecutionEpoch | None
    application_id_observation: ChunJunApplicationIdObservation | None
    json_encoding: Literal["utf-8"] | None
    line_separator: CommandTaskLineSeparator | None
    parameter_substitution: bool
    ui_custom_config_default: bool | None
    builtin_mode_runnable: bool
    local_params_supported: bool
    resource_files_supported: bool
    task_params_logged: bool
    json_logged: bool
    command_logged: bool
    secret_storage_supported: bool
    result_output_supported: bool
    cancel_mode: CommandTaskCancelMode | None
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_CHUNJUN_ABSENT = ChunJunAuthoringSurface(
    available=False,
    execution_epoch=None,
    application_id_observation=None,
    json_encoding=None,
    line_separator=None,
    parameter_substitution=False,
    ui_custom_config_default=None,
    builtin_mode_runnable=False,
    local_params_supported=False,
    resource_files_supported=False,
    task_params_logged=False,
    json_logged=False,
    command_logged=False,
    secret_storage_supported=False,
    result_output_supported=False,
    cancel_mode=None,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_CHUNJUN_LEGACY_LOG_DISCOVERY = ChunJunAuthoringSurface(
    available=True,
    execution_epoch="legacy-shell-file",
    application_id_observation="post-exit-log-discovery-nondurable",
    json_encoding="utf-8",
    line_separator="lf",
    parameter_substitution=True,
    ui_custom_config_default=False,
    builtin_mode_runnable=False,
    local_params_supported=True,
    resource_files_supported=False,
    task_params_logged=True,
    json_logged=True,
    command_logged=True,
    secret_storage_supported=False,
    result_output_supported=False,
    cancel_mode="wrapper-kill",
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_CHUNJUN_LEGACY_EMPTY_APPLICATION_IDS = replace(
    _CHUNJUN_LEGACY_LOG_DISCOVERY,
    application_id_observation="none",
    ui_custom_config_default=True,
)
_CHUNJUN_SHELL_INTERCEPTOR_DIRECT_CANCEL = replace(
    _CHUNJUN_LEGACY_EMPTY_APPLICATION_IDS,
    execution_epoch="shell-interceptor",
    cancel_mode="direct-process-destroy",
)
_CHUNJUN_SHELL_INTERCEPTOR_PROCESS_TREE_CANCEL = replace(
    _CHUNJUN_SHELL_INTERCEPTOR_DIRECT_CANCEL,
    cancel_mode="process-tree-and-application",
)
_CHUNJUN_TASK_REQUEST = replace(
    _CHUNJUN_SHELL_INTERCEPTOR_PROCESS_TREE_CANCEL,
    execution_epoch="task-request",
)


def _chunjun_surface(version: str) -> ChunJunAuthoringSurface:
    if version in {"3.1.1", "3.1.2", "3.1.3", "3.1.4", "3.1.5", "3.1.6"}:
        return replace(
            _CHUNJUN_LEGACY_EMPTY_APPLICATION_IDS,
            ui_custom_config_default=False,
        )
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return _CHUNJUN_ABSENT
    if version == "3.1.0":
        return _CHUNJUN_LEGACY_LOG_DISCOVERY
    if version in {
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
        return _CHUNJUN_LEGACY_EMPTY_APPLICATION_IDS
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _CHUNJUN_SHELL_INTERCEPTOR_DIRECT_CANCEL
    if version in {"3.3.1", "3.3.2"}:
        return _CHUNJUN_SHELL_INTERCEPTOR_PROCESS_TREE_CANCEL
    if version in {"3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _CHUNJUN_TASK_REQUEST
    message = f"No exact CHUNJUN authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
