from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

PytorchWireEpoch = Literal[
    "positive-resource-id-python-home",
    "resource-name-python-launcher",
]
PytorchOutputProtocol = Literal["legacy-var-pool", "task-output-unparsed"]
PytorchStopMode = Literal["outer-process-best-effort", "plugin-cancel-noop"]
PytorchTimeoutMode = Literal[
    "process-tree-best-effort",
    "direct-process-best-effort",
    "output-future-before-timeout-check",
    "output-future-and-live-process-exit-value-hole",
]


@dataclass(frozen=True, slots=True)
class PytorchAuthoringSurface:
    """PYTORCH resource-script wire, command, output, and recovery semantics."""

    available: bool
    wire_epoch: PytorchWireEpoch | None
    script_encoding: Literal["utf-8", "platform-default"] | None
    parameter_substitution: bool
    resource_files_supported: bool
    git_projects_supported: bool
    environment_creation_supported: bool
    output_protocol: PytorchOutputProtocol | None
    task_params_logged: bool
    command_logged: bool
    result_output_supported: bool
    cancel_supported: bool
    stop_mode: PytorchStopMode | None
    timeout_mode: PytorchTimeoutMode | None
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_PYTORCH_ABSENT = PytorchAuthoringSurface(
    available=False,
    wire_epoch=None,
    script_encoding=None,
    parameter_substitution=False,
    resource_files_supported=False,
    git_projects_supported=False,
    environment_creation_supported=False,
    output_protocol=None,
    task_params_logged=False,
    command_logged=False,
    result_output_supported=False,
    cancel_supported=False,
    stop_mode=None,
    timeout_mode=None,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_PYTORCH_POSITIVE_RESOURCE_ID_UTF8 = PytorchAuthoringSurface(
    available=True,
    wire_epoch="positive-resource-id-python-home",
    script_encoding="utf-8",
    parameter_substitution=True,
    resource_files_supported=True,
    git_projects_supported=True,
    environment_creation_supported=True,
    output_protocol="legacy-var-pool",
    task_params_logged=True,
    command_logged=True,
    result_output_supported=False,
    cancel_supported=False,
    stop_mode="outer-process-best-effort",
    timeout_mode="process-tree-best-effort",
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_PYTORCH_POSITIVE_RESOURCE_ID_PLATFORM_DEFAULT = replace(
    _PYTORCH_POSITIVE_RESOURCE_ID_UTF8,
    script_encoding="platform-default",
)
_PYTORCH_RESOURCE_NAME = replace(
    _PYTORCH_POSITIVE_RESOURCE_ID_PLATFORM_DEFAULT,
    wire_epoch="resource-name-python-launcher",
    timeout_mode="direct-process-best-effort",
)
_PYTORCH_TASK_OUTPUT_UNPARSED = replace(
    _PYTORCH_RESOURCE_NAME,
    output_protocol="task-output-unparsed",
)
_PYTORCH_PLUGIN_CANCEL_NOOP = replace(
    _PYTORCH_TASK_OUTPUT_UNPARSED,
    stop_mode="plugin-cancel-noop",
    timeout_mode="output-future-before-timeout-check",
)
_PYTORCH_TIMEOUT_HOLE = replace(
    _PYTORCH_PLUGIN_CANCEL_NOOP,
    timeout_mode="output-future-and-live-process-exit-value-hole",
)


def _pytorch_surface(version: str) -> PytorchAuthoringSurface:
    if version == "3.1.0":
        return _PYTORCH_POSITIVE_RESOURCE_ID_UTF8
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
        return _PYTORCH_POSITIVE_RESOURCE_ID_PLATFORM_DEFAULT
    if version == "3.2.0":
        return _PYTORCH_RESOURCE_NAME
    if version in {"3.2.1", "3.2.2"}:
        return _PYTORCH_TASK_OUTPUT_UNPARSED
    if version == "3.3.1":
        return _PYTORCH_PLUGIN_CANCEL_NOOP
    if version == "3.3.2":
        return _PYTORCH_TIMEOUT_HOLE
    if version in TARGET_DS_VERSIONS:
        return _PYTORCH_ABSENT
    message = f"No exact PYTORCH authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
