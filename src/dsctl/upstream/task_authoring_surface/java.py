from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

JavaWireEpoch = Literal["source-and-jar", "fat-and-normal-jar"]
JavaRuntimeEpoch = Literal[
    "legacy-resource-map",
    "broken-double-prefixed-resource-context",
    "fixed-resource-context",
    "fat-normal-legacy-argument-order",
    "fat-normal-parameterized",
]
JavaFatJarRunType = Literal["JAR", "FAT_JAR"]
JavaCancelMode = Literal["direct-process", "process-tree-and-generic-app"]


@dataclass(frozen=True, slots=True)
class JavaAuthoringSurface:
    """JAVA fat-JAR wire, resource staging, command, and recovery semantics."""

    available: bool
    typed_fat_jar_supported: bool
    exclusion_reason: str | None
    wire_epoch: JavaWireEpoch | None
    runtime_epoch: JavaRuntimeEpoch | None
    fat_jar_run_type: JavaFatJarRunType | None
    source_mode_supported: bool
    normal_jar_supported: bool
    jvm_args_before_target: bool
    parameter_substitution: bool
    cancel_mode: JavaCancelMode | None
    task_params_logged: bool
    command_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool


_JAVA_ABSENT = JavaAuthoringSurface(
    available=False,
    typed_fat_jar_supported=False,
    exclusion_reason="task-type-absent",
    wire_epoch=None,
    runtime_epoch=None,
    fat_jar_run_type=None,
    source_mode_supported=False,
    normal_jar_supported=False,
    jvm_args_before_target=False,
    parameter_substitution=False,
    cancel_mode=None,
    task_params_logged=False,
    command_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_JAVA_LEGACY_RESOURCE_MAP = JavaAuthoringSurface(
    available=True,
    typed_fat_jar_supported=True,
    exclusion_reason=None,
    wire_epoch="source-and-jar",
    runtime_epoch="legacy-resource-map",
    fat_jar_run_type="JAR",
    source_mode_supported=True,
    normal_jar_supported=False,
    jvm_args_before_target=False,
    parameter_substitution=False,
    cancel_mode="direct-process",
    task_params_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_JAVA_BROKEN_RESOURCE_CONTEXT = replace(
    _JAVA_LEGACY_RESOURCE_MAP,
    typed_fat_jar_supported=False,
    exclusion_reason="main-jar-absolute-path-is-double-prefixed",
    runtime_epoch="broken-double-prefixed-resource-context",
)
_JAVA_FIXED_RESOURCE_CONTEXT = replace(
    _JAVA_LEGACY_RESOURCE_MAP,
    runtime_epoch="fixed-resource-context",
)
_JAVA_FAT_NORMAL_LEGACY_ARGUMENT_ORDER = replace(
    _JAVA_FIXED_RESOURCE_CONTEXT,
    wire_epoch="fat-and-normal-jar",
    runtime_epoch="fat-normal-legacy-argument-order",
    fat_jar_run_type="FAT_JAR",
    source_mode_supported=False,
    normal_jar_supported=True,
    cancel_mode="process-tree-and-generic-app",
)
_JAVA_FAT_NORMAL_PARAMETERIZED = replace(
    _JAVA_FAT_NORMAL_LEGACY_ARGUMENT_ORDER,
    runtime_epoch="fat-normal-parameterized",
    jvm_args_before_target=True,
    parameter_substitution=True,
)


def _java_surface(version: str) -> JavaAuthoringSurface:
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
        return _JAVA_ABSENT
    if version == "3.2.0":
        return _JAVA_LEGACY_RESOURCE_MAP
    if version == "3.2.1":
        return _JAVA_BROKEN_RESOURCE_CONTEXT
    if version == "3.2.2":
        return _JAVA_FIXED_RESOURCE_CONTEXT
    if version in {"3.3.1", "3.3.2", "3.4.0"}:
        return _JAVA_FAT_NORMAL_LEGACY_ARGUMENT_ORDER
    if version in {"3.4.1", "3.4.2", "3.4.3"}:
        return _JAVA_FAT_NORMAL_PARAMETERIZED
    message = f"No exact JAVA authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
