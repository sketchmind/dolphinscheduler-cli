from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

MrWireEpoch = Literal[
    "legacy-task-node-positive-id",
    "task-definition-positive-id-runtime-enriched",
    "resource-name",
]
MrQueueEpoch = Literal["runtime-overwritten", "yarn-default-empty"]
MrCompletionEpoch = Literal[
    "launcher-plus-yarn-final-state",
    "launcher-exit-only",
]
MrApplicationIdObservation = Literal[
    "post-exit-log-discovery-nondurable",
    "post-exit-log-or-app-info-final-result",
    "post-exit-context-transport-hole",
]
MrLocalCancelEpoch = Literal[
    "legacy-process-tree-and-yarn-log-scan",
    "soft-root-with-worker-tree-yarn-fallback",
    "soft-root-hard-tree-and-yarn-fallback",
    "direct-destroy-with-outer-tree-yarn-cancel",
    "tree-plus-generic-yarn-cancel",
]
MrCancelMode = Literal["best-effort-observed-application-id"]
MrFailoverEpoch = Literal["fresh-rerun-no-reattach"]


@dataclass(frozen=True, slots=True)
class MrAuthoringSurface:
    """MR wire, queue, launcher, logging, cancellation, and recovery facts."""

    available: bool
    wire_epoch: MrWireEpoch
    queue_epoch: MrQueueEpoch
    completion_epoch: MrCompletionEpoch
    application_id_observation: MrApplicationIdObservation
    application_id_callback_supported: bool
    durable_application_id: bool
    local_cancel_epoch: MrLocalCancelEpoch
    cancel_mode: MrCancelMode
    failover_epoch: MrFailoverEpoch
    failover_supported: bool
    failover_yarn_kill_configurable: bool
    retry_reexecutes: bool
    retry_may_duplicate_effects: bool
    task_params_logged: bool
    command_logged: bool
    child_output_logged: bool
    result_output_supported: bool


_MR_LEGACY_TASK_NODE = MrAuthoringSurface(
    available=True,
    wire_epoch="legacy-task-node-positive-id",
    queue_epoch="runtime-overwritten",
    completion_epoch="launcher-plus-yarn-final-state",
    application_id_observation="post-exit-log-discovery-nondurable",
    application_id_callback_supported=False,
    durable_application_id=False,
    local_cancel_epoch="legacy-process-tree-and-yarn-log-scan",
    cancel_mode="best-effort-observed-application-id",
    failover_epoch="fresh-rerun-no-reattach",
    failover_supported=False,
    failover_yarn_kill_configurable=False,
    retry_reexecutes=True,
    retry_may_duplicate_effects=True,
    task_params_logged=True,
    command_logged=True,
    child_output_logged=True,
    result_output_supported=False,
)
_MR_TASK_DEFINITION_POSITIVE_ID = replace(
    _MR_LEGACY_TASK_NODE,
    wire_epoch="task-definition-positive-id-runtime-enriched",
    completion_epoch="launcher-exit-only",
)
_MR_RESOURCE_NAME = replace(
    _MR_TASK_DEFINITION_POSITIVE_ID,
    wire_epoch="resource-name",
    queue_epoch="yarn-default-empty",
    application_id_observation="post-exit-log-or-app-info-final-result",
    local_cancel_epoch="direct-destroy-with-outer-tree-yarn-cancel",
)


def _mr_surface(version: str) -> MrAuthoringSurface:
    if version == "3.1.1":
        return replace(
            _MR_TASK_DEFINITION_POSITIVE_ID,
            application_id_observation="post-exit-log-or-app-info-final-result",
            local_cancel_epoch="soft-root-with-worker-tree-yarn-fallback",
        )
    if version == "1.3.9":
        return _MR_LEGACY_TASK_NODE
    if version in {"2.0.0", "2.0.1"}:
        return _MR_TASK_DEFINITION_POSITIVE_ID
    if version in {
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        return replace(
            _MR_TASK_DEFINITION_POSITIVE_ID,
            failover_yarn_kill_configurable=True,
        )
    if version in {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
    }:
        return replace(
            _MR_TASK_DEFINITION_POSITIVE_ID,
            local_cancel_epoch="soft-root-with-worker-tree-yarn-fallback",
        )
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
        return replace(
            _MR_TASK_DEFINITION_POSITIVE_ID,
            application_id_observation=("post-exit-log-or-app-info-final-result"),
            local_cancel_epoch="soft-root-hard-tree-and-yarn-fallback",
        )
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _MR_RESOURCE_NAME
    if version in {
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }:
        return replace(
            _MR_RESOURCE_NAME,
            application_id_observation="post-exit-context-transport-hole",
            local_cancel_epoch="tree-plus-generic-yarn-cancel",
        )
    message = f"No exact MR authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
