from __future__ import annotations

from typing import TYPE_CHECKING

from tests.live.support import (
    DsctlCommandResult,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    wait_for_result,
)

if TYPE_CHECKING:
    from pathlib import Path


def _execution_instance_id(
    data: dict[str, object],
    *,
    repo_root: Path,
    env_file: Path,
    project: str,
    fresh_workflow: str,
    label: str,
) -> int:
    """Resolve one accepted run without treating a trigger as an instance id."""
    assert data.get("accepted") is True, f"{label} was not accepted"
    identities = require_list(data.get("workflowInstanceIds"), label=label)
    if data.get("instanceResolution") == "resolved":
        return _single_instance_id(identities, label=label)
    assert identities == [], f"{label} mixed unresolved receipt and instance ids"
    if data.get("instanceResolution") == "unavailable":
        # These scenarios create an exclusive workflow and submit it exactly once.
        # Older executors acknowledge the submission without returning an identity.
        assert data.get("triggerCode") is None
        result = wait_for_result(
            repo_root,
            [
                "workflow-instance",
                "list",
                "--project",
                project,
                "--workflow",
                fresh_workflow,
                "--all",
            ],
            env_file=env_file,
            accept=lambda current: _fresh_workflow_instance_id(current) is not None,
            timeout_seconds=60.0,
            interval_seconds=2.0,
            command_timeout_seconds=30.0,
        )
        identity = _fresh_workflow_instance_id(result)
        assert identity is not None, f"{label} accepted run never materialized"
        return identity
    assert data.get("instanceResolution") == "pending", (
        f"{label} has unavailable instance identity; no instance can be watched"
    )
    trigger_code = _single_instance_id([data.get("triggerCode")], label=label)

    def trigger_ready(result: DsctlCommandResult) -> bool:
        return _trigger_instance_id(result, trigger_code=trigger_code) is not None

    result = wait_for_result(
        repo_root,
        [
            "workflow-instance",
            "list",
            "--project",
            project,
            "--trigger-code",
            str(trigger_code),
        ],
        env_file=env_file,
        accept=trigger_ready,
        timeout_seconds=60.0,
        interval_seconds=2.0,
        command_timeout_seconds=30.0,
    )
    identity = _trigger_instance_id(result, trigger_code=trigger_code)
    assert identity is not None, (
        f"{label} trigger {trigger_code} stayed pending; execution was accepted "
        "but instance identity was not observed before the deadline"
    )
    return identity


def _fresh_workflow_instance_id(result: DsctlCommandResult) -> int | None:
    payload = require_ok_payload(
        result,
        expected_action="workflow-instance.list",
        label="fresh workflow instance query",
    )
    data = require_mapping(payload["data"], label="fresh workflow instance data")
    coverage = require_mapping(data.get("coverage"), label="instance coverage")
    assert coverage.get("scope_complete") is True, "instance listing is incomplete"
    rows = require_list(data.get("totalList"), label="fresh workflow instances")
    assert data.get("total") == len(rows), "instance listing total differs"
    if not rows:
        return None
    identities = [
        require_mapping(row, label="fresh workflow instance").get("id") for row in rows
    ]
    return _single_instance_id(identities, label="fresh workflow instance")


def _single_instance_id(values: list[object], *, label: str) -> int:
    assert len(values) == 1, f"{label} requires exactly one instance identity"
    identity = require_int_value(values[0], label=label)
    assert not isinstance(identity, bool), (
        f"{label} requires a positive integer identity"
    )
    assert identity > 0, f"{label} requires a positive integer identity"
    return identity


def _trigger_instance_id(
    result: DsctlCommandResult, *, trigger_code: int
) -> int | None:
    payload = require_ok_payload(
        result,
        expected_action="workflow-instance.list",
        label="workflow trigger instance query",
    )
    data = require_mapping(payload["data"], label="workflow trigger data")
    assert data.get("triggerCode") == trigger_code, "trigger query changed identity"
    rows = require_list(data.get("totalList"), label="workflow trigger instances")
    if data.get("instanceResolution") == "pending":
        assert rows == [], "pending trigger query returned instance rows"
        return None
    assert data.get("instanceResolution") == "resolved", (
        "trigger query omitted its instance resolution"
    )
    identities = [
        require_mapping(row, label="workflow trigger instance").get("id")
        for row in rows
    ]
    return _single_instance_id(identities, label="workflow trigger instances")
