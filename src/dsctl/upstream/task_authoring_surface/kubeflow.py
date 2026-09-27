from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

KubeflowWireEpoch = Literal["legacy-namespace-tfjob-yaml"]
KubeflowRetryBehavior = Literal[
    "reapply-same-manifest",
    "reobserve-same-manifest",
]


@dataclass(frozen=True, slots=True)
class KubeflowAuthoringSurface:
    """Reviewed TFJob manifest wire, watcher, disclosure, and recovery facts."""

    available: bool
    wire_epoch: KubeflowWireEpoch | None
    script_encoding: Literal["platform-default"] | None
    parameter_substitution: bool
    namespace_context_enforced: bool
    task_params_logged: bool
    manifest_logged: bool
    terminal_resource_logged: bool
    kubeconfig_logged: bool
    poll_interval_seconds: int | None
    success_statuses: tuple[str, ...]
    failure_statuses: tuple[str, ...]
    empty_conditions_runtime_failure: bool
    result_output_supported: bool
    cancel_supported: bool
    durable_application_id: bool
    durable_submission_marker: bool
    failover_tracking_supported: bool
    callback_persistence_gap: bool
    retry_supported: bool
    native_retry_behavior: KubeflowRetryBehavior | None


_KUBEFLOW_ABSENT = KubeflowAuthoringSurface(
    available=False,
    wire_epoch=None,
    script_encoding=None,
    parameter_substitution=False,
    namespace_context_enforced=False,
    task_params_logged=False,
    manifest_logged=False,
    terminal_resource_logged=False,
    kubeconfig_logged=False,
    poll_interval_seconds=None,
    success_statuses=(),
    failure_statuses=(),
    empty_conditions_runtime_failure=False,
    result_output_supported=False,
    cancel_supported=False,
    durable_application_id=False,
    durable_submission_marker=False,
    failover_tracking_supported=False,
    callback_persistence_gap=False,
    retry_supported=False,
    native_retry_behavior=None,
)
_KUBEFLOW_TFJOB_REOBSERVE_RETRY = KubeflowAuthoringSurface(
    available=True,
    wire_epoch="legacy-namespace-tfjob-yaml",
    script_encoding="platform-default",
    parameter_substitution=True,
    namespace_context_enforced=False,
    task_params_logged=True,
    manifest_logged=True,
    terminal_resource_logged=True,
    kubeconfig_logged=True,
    poll_interval_seconds=3,
    success_statuses=("Succeeded", "Available", "Bound"),
    failure_statuses=("Failed",),
    empty_conditions_runtime_failure=True,
    result_output_supported=False,
    cancel_supported=True,
    durable_application_id=False,
    durable_submission_marker=True,
    failover_tracking_supported=True,
    callback_persistence_gap=True,
    retry_supported=False,
    native_retry_behavior="reobserve-same-manifest",
)
_KUBEFLOW_TFJOB_REAPPLY_RETRY = replace(
    _KUBEFLOW_TFJOB_REOBSERVE_RETRY,
    native_retry_behavior="reapply-same-manifest",
)


def _kubeflow_surface(version: str) -> KubeflowAuthoringSurface:
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _KUBEFLOW_TFJOB_REAPPLY_RETRY
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _KUBEFLOW_TFJOB_REOBSERVE_RETRY
    if version in TARGET_DS_VERSIONS:
        return _KUBEFLOW_ABSENT
    message = f"No exact KUBEFLOW authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
