from __future__ import annotations

import pytest

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface


@pytest.mark.parametrize(
    ("version", "wire_epoch", "queue_epoch", "completion_epoch"),
    [
        (
            "1.3.9",
            "legacy-task-node-positive-id",
            "runtime-overwritten",
            "launcher-plus-yarn-final-state",
        ),
        *[
            (
                version,
                "task-definition-positive-id-runtime-enriched",
                "runtime-overwritten",
                "launcher-exit-only",
            )
            for version in (
                "2.0.0",
                "2.0.9",
                "3.0.0",
                "3.0.6",
                "3.1.0",
                "3.1.9",
            )
        ],
        *[
            (
                version,
                "resource-name",
                "yarn-default-empty",
                "launcher-exit-only",
            )
            for version in (
                "3.2.0",
                "3.2.1",
                "3.2.2",
                "3.3.1",
                "3.3.2",
                "3.4.0",
                "3.4.1",
                "3.4.2",
            )
        ],
    ],
)
def test_mr_surface_tracks_exact_wire_queue_and_completion_epochs(
    version: str,
    wire_epoch: str,
    queue_epoch: str,
    completion_epoch: str,
) -> None:
    surface = get_task_authoring_surface(version).mr

    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.queue_epoch == queue_epoch
    assert surface.completion_epoch == completion_epoch


@pytest.mark.parametrize(
    ("version", "application_id_observation", "local_cancel_epoch"),
    [
        *[
            (
                version,
                "post-exit-log-discovery-nondurable",
                "legacy-process-tree-and-yarn-log-scan",
            )
            for version in ("1.3.9", "2.0.0", "2.0.9")
        ],
        *[
            (
                version,
                "post-exit-log-discovery-nondurable",
                "soft-root-with-worker-tree-yarn-fallback",
            )
            for version in ("3.0.0", "3.0.6", "3.1.0")
        ],
        (
            "3.1.9",
            "post-exit-log-or-app-info-final-result",
            "soft-root-hard-tree-and-yarn-fallback",
        ),
        *[
            (
                version,
                "post-exit-log-or-app-info-final-result",
                "direct-destroy-with-outer-tree-yarn-cancel",
            )
            for version in ("3.2.0", "3.2.1", "3.2.2")
        ],
        *[
            (
                version,
                "post-exit-context-transport-hole",
                "tree-plus-generic-yarn-cancel",
            )
            for version in ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2")
        ],
    ],
)
def test_mr_surface_tracks_exact_application_id_and_cancel_epochs(
    version: str,
    application_id_observation: str,
    local_cancel_epoch: str,
) -> None:
    surface = get_task_authoring_surface(version).mr

    assert surface.application_id_observation == application_id_observation
    assert surface.local_cancel_epoch == local_cancel_epoch
    assert surface.cancel_mode == "best-effort-observed-application-id"


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_mr_surface_discloses_non_resumable_reexecution_and_info_logging(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).mr

    assert surface.application_id_callback_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_epoch == "fresh-rerun-no-reattach"
    assert surface.failover_supported is False
    assert surface.failover_yarn_kill_configurable is (
        version
        in {"2.0.2", "2.0.3", "2.0.4", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9"}
    )
    assert surface.retry_reexecutes is True
    assert surface.retry_may_duplicate_effects is True
    assert surface.task_params_logged is True
    assert surface.command_logged is True
    assert surface.child_output_logged is True
    assert surface.result_output_supported is False
