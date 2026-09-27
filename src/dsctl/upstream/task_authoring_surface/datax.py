from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.upstream.task_authoring_surface.shared import (
    CommandTaskCancelMode,
    CommandTaskLineSeparator,
)

DataxWireEpoch = Literal["legacy-custom-json", "custom-json-jvm-memory"]
# Compatibility aliases retained for downstream imports from the older DataX surface.
DataxLineSeparator = CommandTaskLineSeparator
DataxCancelMode = CommandTaskCancelMode


@dataclass(frozen=True, slots=True)
class DataxAuthoringSurface:
    """DataX custom-JSON wire, launcher, disclosure, and recovery facts."""

    registered: bool
    typed_custom_json_available: bool
    wire_epoch: DataxWireEpoch
    exclusion_reason: str | None
    python_launcher: str
    datax_launcher: str
    line_separator: CommandTaskLineSeparator
    parameter_substitution: bool
    prepared_params_forwarded: bool
    resource_files_supported: bool
    task_params_logged: bool
    command_logged: bool
    secret_storage_supported: bool
    result_output_supported: bool
    cancel_mode: CommandTaskCancelMode
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool
    empty_json_object_is_absent: bool = False


_DATAX_LEGACY_CUSTOM_JSON = DataxAuthoringSurface(
    registered=True,
    typed_custom_json_available=True,
    wire_epoch="legacy-custom-json",
    exclusion_reason=None,
    python_launcher="PYTHON_HOME-or-python2.7",
    datax_launcher="DATAX_HOME/bin/datax.py",
    line_separator="lf",
    parameter_substitution=True,
    prepared_params_forwarded=False,
    resource_files_supported=False,
    task_params_logged=True,
    command_logged=True,
    secret_storage_supported=False,
    result_output_supported=False,
    cancel_mode="wrapper-kill",
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_DATAX_CUSTOM_JSON_JVM_MEMORY = replace(
    _DATAX_LEGACY_CUSTOM_JSON,
    wire_epoch="custom-json-jvm-memory",
)
_DATAX_BROKEN_EMPTY_PREPARED_PARAMS = replace(
    _DATAX_CUSTOM_JSON_JVM_MEMORY,
    typed_custom_json_available=False,
    exclusion_reason="null-empty-prepare-params-map-breaks-custom-command",
    prepared_params_forwarded=True,
    resource_files_supported=True,
)
_DATAX_RESOURCE_PARAMETERS = replace(
    _DATAX_CUSTOM_JSON_JVM_MEMORY,
    prepared_params_forwarded=True,
    resource_files_supported=True,
)
_DATAX_MODERN_LAUNCHERS = replace(
    _DATAX_RESOURCE_PARAMETERS,
    python_launcher="PYTHON_LAUNCHER",
    datax_launcher="DATAX_LAUNCHER",
    line_separator="system",
    cancel_mode="direct-process-destroy",
)
_DATAX_PROCESS_TREE_CANCELLATION = replace(
    _DATAX_MODERN_LAUNCHERS,
    cancel_mode="process-tree-and-application",
)
_DATAX_RESOURCE_FILE_FALLBACK = replace(
    _DATAX_PROCESS_TREE_CANCELLATION,
    empty_json_object_is_absent=True,
)


def _datax_surface(version: str) -> DataxAuthoringSurface:
    if version == "3.4.3":
        # DataxParameters.isInlineJsonAbsent treats {} as a missing definition;
        # native resource fallback does not expand the literal typed facet.
        return _DATAX_RESOURCE_FILE_FALLBACK
    if version == "1.3.9":
        return _DATAX_LEGACY_CUSTOM_JSON
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
    }:
        return _DATAX_CUSTOM_JSON_JVM_MEMORY
    if version == "3.1.0":
        return _DATAX_BROKEN_EMPTY_PREPARED_PARAMS
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
        return _DATAX_RESOURCE_PARAMETERS
    if version in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
    }:
        return _DATAX_MODERN_LAUNCHERS
    if version in {
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    }:
        return _DATAX_PROCESS_TREE_CANCELLATION
    message = f"No exact DATAX authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
