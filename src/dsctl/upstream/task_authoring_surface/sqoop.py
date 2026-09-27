from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

SqoopLineSeparator = Literal["lf", "system"]
SqoopScriptEncoding = Literal["utf-8", "platform-default"]
SqoopCompletionEpoch = Literal[
    "launcher-plus-yarn-final-state",
    "launcher-exit-only",
]
SqoopApplicationIdObservation = Literal[
    "post-exit-log-discovery-nondurable",
    "post-exit-log-or-app-info-final-result",
    "post-exit-context-transport-hole",
]
SqoopCancelEpoch = Literal[
    "wrapper-kill-and-yarn-discovery",
    "direct-process-and-outer-yarn-cancel",
    "process-tree-and-generic-yarn-cancel",
]


@dataclass(frozen=True, slots=True)
class SqoopAuthoringSurface:
    """SQOOP command rendering, logging, cancellation, and recovery facts."""

    available: bool
    line_separator: SqoopLineSeparator
    script_encoding: SqoopScriptEncoding
    completion_epoch: SqoopCompletionEpoch
    application_id_observation: SqoopApplicationIdObservation
    cancel_epoch: SqoopCancelEpoch
    parameter_substitution: bool
    late_password_mask: bool
    task_params_logged: bool
    command_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_SQOOP_LEGACY_YARN_FINAL = SqoopAuthoringSurface(
    available=True,
    line_separator="lf",
    script_encoding="utf-8",
    completion_epoch="launcher-plus-yarn-final-state",
    application_id_observation="post-exit-log-discovery-nondurable",
    cancel_epoch="wrapper-kill-and-yarn-discovery",
    parameter_substitution=True,
    late_password_mask=False,
    task_params_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_SQOOP_LAUNCHER_LOG = replace(
    _SQOOP_LEGACY_YARN_FINAL,
    completion_epoch="launcher-exit-only",
)
_SQOOP_PLATFORM_DEFAULT_LOG = replace(
    _SQOOP_LAUNCHER_LOG,
    script_encoding="platform-default",
)
_SQOOP_APP_INFO = replace(
    _SQOOP_PLATFORM_DEFAULT_LOG,
    application_id_observation="post-exit-log-or-app-info-final-result",
)
_SQOOP_SYSTEM_LINE_ENDING = replace(
    _SQOOP_APP_INFO,
    line_separator="system",
    cancel_epoch="direct-process-and-outer-yarn-cancel",
    late_password_mask=True,
)
_SQOOP_CONTEXT_TRANSPORT_HOLE = replace(
    _SQOOP_SYSTEM_LINE_ENDING,
    application_id_observation="post-exit-context-transport-hole",
    cancel_epoch="process-tree-and-generic-yarn-cancel",
)


def _sqoop_surface(version: str) -> SqoopAuthoringSurface:
    if version == "1.3.9":
        return _SQOOP_LEGACY_YARN_FINAL
    if version in {
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
        "3.1.0",
    }:
        return _SQOOP_LAUNCHER_LOG
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
        return _SQOOP_PLATFORM_DEFAULT_LOG
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _SQOOP_SYSTEM_LINE_ENDING
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _SQOOP_CONTEXT_TRANSPORT_HOLE
    message = f"No exact SQOOP authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
