from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.commands import workflow as workflow_commands
from dsctl.output import CommandResult, success_payload
from dsctl.result_navigation import (
    ActionIndexData,
    navigation_for,
    next_actions_for,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from dsctl.support.json_types import JsonObject, JsonValue


def _targets_for(action_index: ActionIndexData, action: str) -> str | list[int | str]:
    for group in action_index["groups"]:
        categories = (
            group.get("read", []),
            group.get("read_needs_input", []),
            group.get("mutate", []),
            group.get("mutate_needs_input", []),
        )
        for actions in categories:
            if action in actions:
                return group["targets"]
    message = f"action not indexed: {action}"
    raise AssertionError(message)


def test_workflow_instance_list_discovers_actions_from_known_row_states() -> None:
    navigation = navigation_for(
        "workflow-instance.list",
        resolved={},
        data={
            "totalList": [
                {"id": 263, "state": "SUCCESS"},
                {"id": 264, "state": "RUNNING_EXECUTION"},
                {"id": 265, "state": "FAILURE"},
            ]
        },
    )

    assert navigation == {
        "action_index": {
            "scope": "data.totalList",
            "target": {"resource": "workflow-instance", "field": "id"},
            "authorization": "not_evaluated",
            "eligibility": "row_facts_only",
            "groups": [
                {
                    "targets": "all",
                    "read": [
                        "workflow-instance.get",
                        "workflow-instance.digest",
                        "workflow-instance.watch",
                        "workflow-instance.export",
                    ],
                },
                {"targets": [264], "mutate": ["workflow-instance.stop"]},
                {
                    "targets": [263, 265],
                    "mutate": ["workflow-instance.rerun"],
                    "mutate_needs_input": [
                        "workflow-instance.edit",
                        "workflow-instance.execute-task",
                    ],
                },
                {
                    "targets": [265],
                    "mutate": ["workflow-instance.recover-failed"],
                },
            ],
            "schema_command": "dsctl schema --command ACTION",
            "group_command": "dsctl schema --group workflow-instance",
            "target_count": 3,
            "indexed_target_count": 3,
            "truncated": False,
        }
    }


def test_schedule_list_discovers_state_eligible_actions_without_requests() -> None:
    navigation = navigation_for(
        "schedule.list",
        resolved={"project": {"code": 7}},
        data={
            "totalList": [
                {"id": 41, "releaseState": "OFFLINE"},
                {"id": 42, "releaseState": "ONLINE"},
                {"id": 43, "releaseState": None},
            ]
        },
    )

    assert navigation["action_index"] == {
        "scope": "data.totalList",
        "target": {"resource": "schedule", "field": "id"},
        "authorization": "not_evaluated",
        "eligibility": "row_facts_only",
        "groups": [
            {
                "targets": "all",
                "read": ["schedule.get", "schedule.preview"],
                "read_needs_input": ["schedule.explain"],
                "mutate_needs_input": ["schedule.update"],
            },
            {
                "targets": [41],
                "mutate": ["schedule.online"],
                "mutate_needs_input": ["schedule.delete"],
            },
            {"targets": [42], "mutate": ["schedule.offline"]},
        ],
        "schema_command": "dsctl schema --command ACTION",
        "group_command": "dsctl schema --group schedule",
        "target_count": 3,
        "indexed_target_count": 3,
        "truncated": False,
    }


def test_workflow_list_discovers_reads_and_known_lifecycle_actions() -> None:
    navigation = navigation_for(
        "workflow.list",
        resolved={"project": {"code": 7}},
        data={
            "totalList": [
                {
                    "code": 101,
                    "releaseState": "ONLINE",
                    "scheduleReleaseState": "OFFLINE",
                },
                {
                    "code": 102,
                    "releaseState": "OFFLINE",
                    "scheduleReleaseState": None,
                },
                {
                    "code": 103,
                    "releaseState": "OFFLINE",
                    "scheduleReleaseState": "ONLINE",
                },
                {
                    "code": 104,
                    "releaseState": None,
                    "scheduleReleaseState": None,
                },
            ]
        },
    )

    assert navigation["action_index"] == {
        "scope": "data.totalList",
        "target": {"resource": "workflow", "field": "code"},
        "authorization": "not_evaluated",
        "eligibility": "row_facts_only",
        "groups": [
            {
                "targets": "all",
                "read": [
                    "workflow.get",
                    "workflow.digest",
                    "workflow.describe",
                    "workflow.export",
                    "task.list",
                    "schedule.list",
                    "workflow-instance.list",
                    "workflow.lineage.get",
                    "workflow.lineage.dependent-tasks",
                ],
            },
            {
                "targets": [101],
                "mutate": ["workflow.run", "workflow.offline"],
                "mutate_needs_input": [
                    "workflow.run-task",
                    "workflow.backfill",
                ],
            },
            {
                "targets": [102, 103],
                "mutate": ["workflow.online"],
                "mutate_needs_input": ["workflow.edit"],
            },
            {
                "targets": [102],
                "mutate_needs_input": ["workflow.delete"],
            },
        ],
        "schema_command": "dsctl schema --command ACTION",
        "group_command": "dsctl schema --group workflow",
        "target_count": 4,
        "indexed_target_count": 4,
        "truncated": False,
    }


def test_task_instance_list_discovers_actions_only_from_complete_row_facts() -> None:
    navigation = navigation_for(
        "task-instance.list",
        resolved={"workflow_instance": 263},
        data={
            "totalList": [
                {
                    "id": 483,
                    "state": "SUCCESS",
                    "taskType": "SHELL",
                    "taskExecuteType": "BATCH",
                    "workflowInstanceId": 263,
                    "host": "worker:1234",
                    "logPath": "/logs/483.log",
                },
                {
                    "id": 484,
                    "state": "RUNNING_EXECUTION",
                    "taskType": "SHELL",
                    "taskExecuteType": "BATCH",
                    "workflowInstanceId": 263,
                    "host": "worker:1234",
                    "logPath": "",
                },
                {
                    "id": 485,
                    "state": "RUNNING_EXECUTION",
                    "taskType": "SUB_WORKFLOW",
                    "taskExecuteType": "BATCH",
                    "workflowInstanceId": 263,
                    "host": "worker:1234",
                    "logPath": "/logs/485.log",
                },
                {
                    "id": 486,
                    "state": "FAILURE",
                    "taskType": "SHELL",
                    "taskExecuteType": "BATCH",
                    "workflowInstanceId": 263,
                    "host": "worker:1234",
                    "logPath": None,
                },
                {
                    "id": 487,
                    "state": "RUNNING_EXECUTION",
                    "taskType": "SHELL",
                    "taskExecuteType": "STREAM",
                    "workflowInstanceId": 263,
                    "host": "worker:1234",
                    "logPath": None,
                },
            ]
        },
    )

    action_index = navigation["action_index"]
    assert action_index == {
        "scope": "data.totalList",
        "target": {"resource": "task-instance", "field": "id"},
        "authorization": "not_evaluated",
        "eligibility": "row_facts_only",
        "groups": [
            {
                "targets": "all",
                "read": ["task-instance.get", "task-instance.watch"],
            },
            {"targets": [483, 485], "read": ["task-instance.log"]},
            {"targets": [485], "read": ["task-instance.sub-workflow"]},
            {"targets": [486], "mutate": ["task-instance.force-success"]},
            {
                "targets": [487],
                "mutate": ["task-instance.savepoint", "task-instance.stop"],
            },
        ],
        "schema_command": "dsctl schema --command ACTION",
        "group_command": "dsctl schema --group task-instance",
        "target_count": 5,
        "indexed_target_count": 5,
        "truncated": False,
    }


def test_action_index_caps_unique_valid_targets_in_stable_row_order() -> None:
    rows: list[JsonValue] = [
        {"id": target, "releaseState": "OFFLINE"} for target in range(1, 102)
    ]
    rows.extend(
        [
            {"id": 0, "releaseState": "OFFLINE"},
            {"id": "invalid", "releaseState": "OFFLINE"},
        ]
    )

    navigation = navigation_for(
        "schedule.list",
        resolved={"project": {"code": 7}},
        data={"totalList": rows},
    )

    action_index = navigation["action_index"]
    assert _targets_for(action_index, "schedule.online") == "all"
    assert action_index["target_count"] == 103
    assert action_index["indexed_target_count"] == 100
    assert action_index["truncated"] is True


def test_action_index_interns_target_sets_to_keep_large_pages_bounded() -> None:
    rows: list[JsonValue] = [
        {
            "code": 10_000_000_000 + target,
            "releaseState": "ONLINE" if target % 2 == 0 else "OFFLINE",
            "scheduleReleaseState": None,
            "scheduleId": None,
        }
        for target in range(1, 101)
    ]

    action_index = navigation_for(
        "workflow.list",
        resolved={"project": {"code": 7}},
        data={"totalList": rows},
    )["action_index"]
    encoded = json.dumps(action_index, separators=(",", ":")).encode("utf-8")

    assert len(action_index["groups"]) == 3
    assert len(encoded) < 2560


def test_action_index_ignores_malformed_and_duplicate_targets_fail_closed() -> None:
    navigation = navigation_for(
        "schedule.list",
        resolved={"project": {"code": 7}},
        data={
            "totalList": [
                {"id": 2, "releaseState": "OFFLINE"},
                {"id": 2, "releaseState": "ONLINE"},
                {"id": 0, "releaseState": "OFFLINE"},
                {"id": True, "releaseState": "OFFLINE"},
                {"id": "3", "releaseState": "OFFLINE"},
                {"id": 3, "releaseState": "UNKNOWN"},
                {"id": 4, "releaseState": "ONLINE"},
                {"id": 5, "releaseState": "OFFLINE"},
            ]
        },
    )

    action_index = navigation["action_index"]
    assert _targets_for(action_index, "schedule.online") == [5]
    assert _targets_for(action_index, "schedule.offline") == [4]
    assert _targets_for(action_index, "schedule.delete") == [5]
    assert action_index["target_count"] == 8
    assert action_index["indexed_target_count"] == 3
    assert action_index["truncated"] is False


def test_every_list_action_index_uses_the_same_target_bound() -> None:
    cases: tuple[tuple[str, JsonObject, JsonValue, str | None], ...] = (
        (
            "workflow.list",
            {"project": {"code": 7}},
            {
                "totalList": [
                    {"code": target, "releaseState": "ONLINE"}
                    for target in range(1, 102)
                ]
            },
            None,
        ),
        (
            "workflow-instance.list",
            {},
            {
                "totalList": [
                    {"id": target, "state": "SUCCESS"} for target in range(1, 102)
                ]
            },
            "workflow-instance.rerun",
        ),
        (
            "task-instance.list",
            {},
            {
                "totalList": [
                    {
                        "id": target,
                        "state": "RUNNING_EXECUTION",
                        "workflowInstanceId": 263,
                        "logPath": f"/logs/{target}.log",
                    }
                    for target in range(1, 102)
                ]
            },
            "task-instance.log",
        ),
    )

    for action, resolved, data, conditional_action in cases:
        action_index = navigation_for(
            action,
            resolved=resolved,
            data=data,
        )["action_index"]

        assert action_index["indexed_target_count"] == 100
        assert action_index["truncated"] is True
        if conditional_action is not None:
            assert _targets_for(action_index, conditional_action) == "all"


def test_workflow_create_offline_suggests_complete_numeric_online_command() -> None:
    actions = next_actions_for(
        "workflow.create",
        resolved={
            "project": {"code": 7, "name": "project with spaces"},
            "workflow": {"code": 101, "name": "$(unsafe)"},
        },
        data={"code": 101, "releaseState": "OFFLINE"},
    )

    assert actions == [
        {
            "action": "workflow.online",
            "command": (
                "dsctl --compact --columns code,name,releaseState "
                "workflow online 101 --project 7"
            ),
            "mutates": True,
        }
    ]
    assert shlex.split(actions[0]["command"]) == [
        "dsctl",
        "--compact",
        "--columns",
        "code,name,releaseState",
        "workflow",
        "online",
        "101",
        "--project",
        "7",
    ]
    assert "unsafe" not in actions[0]["command"]


def test_workflow_create_online_and_online_result_suggest_run() -> None:
    expected = [
        {
            "action": "workflow.run",
            "command": "dsctl --compact workflow run 101 --project 7",
            "mutates": True,
        }
    ]

    for action in ("workflow.create", "workflow.online"):
        assert (
            next_actions_for(
                action,
                resolved={
                    "project": {"code": 7},
                    "workflow": {"code": 101},
                },
                data={"code": 101, "releaseState": "ONLINE"},
            )
            == expected
        )


def test_workflow_create_dry_run_suggests_complete_apply_command() -> None:
    actions = next_actions_for(
        "workflow.create",
        resolved={
            "file": " /workflows/workflow specs/luna.yaml ",
            "project": {"code": 7},
            "workflow": {"name": "luna"},
        },
        data={"dry_run": True},
    )

    assert actions == [
        {
            "action": "workflow.create",
            "command": (
                "dsctl --compact --columns code,name,releaseState workflow create "
                "--file ' /workflows/workflow specs/luna.yaml ' --project 7"
            ),
            "mutates": True,
        }
    ]
    assert shlex.split(actions[0]["command"]) == [
        "dsctl",
        "--compact",
        "--columns",
        "code,name,releaseState",
        "workflow",
        "create",
        "--file",
        " /workflows/workflow specs/luna.yaml ",
        "--project",
        "7",
    ]


def test_workflow_create_dry_run_preserves_required_confirmation_token() -> None:
    actions = next_actions_for(
        "workflow.create",
        resolved={
            "file": "/workflows/luna.yaml",
            "project": {"code": 7},
        },
        data={
            "dry_run": True,
            "schedule_preview": {"count": 5},
            "schedule_confirmation": {
                "required": True,
                "token": " risk token ",
            },
        },
    )

    assert shlex.split(actions[0]["command"])[-2:] == [
        "--confirm-risk",
        " risk token ",
    ]


def test_workflow_create_next_action_replays_through_the_real_parser(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "cluster $(opaque).env"
    workflow_file = tmp_path / "workflow it's $(opaque).yaml"
    env_file.write_text("", encoding="utf-8")
    workflow_file.write_text("name: replayed\n", encoding="utf-8")
    confirmation_token = " risk\n'$(opaque)' "
    captured: dict[str, object] = {}

    def fake_create_workflow_result(
        *,
        file: Path,
        project: str | None = None,
        dry_run: bool = False,
        confirm_risk: str | None = None,
        env_file: str | None = None,
    ) -> CommandResult:
        captured.update(
            {
                "file": file,
                "project": project,
                "dry_run": dry_run,
                "confirm_risk": confirm_risk,
                "env_file": env_file,
            }
        )
        return CommandResult(
            data={
                "code": 101,
                "name": "replayed",
                "releaseState": "OFFLINE",
            }
        )

    monkeypatch.setattr(
        workflow_commands,
        "create_workflow_result",
        fake_create_workflow_result,
    )
    actions = next_actions_for(
        "workflow.create",
        resolved={
            "file": str(workflow_file),
            "project": {"code": 7},
        },
        data={
            "dry_run": True,
            "schedule_preview": {"count": 5},
            "schedule_confirmation": {
                "required": True,
                "token": confirmation_token,
            },
        },
        env_file=str(env_file),
    )

    replay = CliRunner().invoke(app, shlex.split(actions[0]["command"])[1:])

    assert replay.exit_code == 0, replay.output
    assert captured == {
        "file": workflow_file.resolve(),
        "project": "7",
        "dry_run": False,
        "confirm_risk": confirmation_token,
        "env_file": str(env_file.resolve()),
    }


def test_workflow_run_suggests_watch_only_for_one_instance() -> None:
    expected = [
        {
            "action": "workflow-instance.watch",
            "command": (
                "dsctl --compact --columns id,name,state,startTime,endTime,duration "
                "workflow-instance watch 242"
            ),
            "mutates": False,
        }
    ]

    assert (
        next_actions_for(
            "workflow.run",
            resolved={},
            data={"workflowInstanceIds": [242]},
        )
        == expected
    )
    assert (
        next_actions_for(
            "workflow.run",
            resolved={},
            data={"workflowInstanceIds": []},
        )
        == []
    )
    assert (
        next_actions_for(
            "workflow.run",
            resolved={},
            data={"workflowInstanceIds": [242, 243]},
        )
        == []
    )


def test_workflow_watch_success_suggests_bounded_task_list() -> None:
    assert next_actions_for(
        "workflow-instance.watch",
        resolved={"workflow_instance": {"id": 242}},
        data={"id": 242, "state": "SUCCESS"},
    ) == [
        {
            "action": "task-instance.list",
            "command": (
                "dsctl --compact --columns "
                "id,name,state,taskType,endTime,logPath task-instance list "
                "--workflow-instance 242 --page-size 20"
            ),
            "mutates": False,
        }
    ]


def test_workflow_watch_failure_suggests_bounded_complete_digest() -> None:
    assert next_actions_for(
        "workflow-instance.watch",
        resolved={},
        data={"id": 242, "state": "FAILURE"},
    ) == [
        {
            "action": "workflow-instance.digest",
            "command": (
                "dsctl --compact --columns "
                "taskCount,taskStateCounts,progress,failedTasks "
                "workflow-instance digest 242"
            ),
            "mutates": False,
        },
    ]


def test_workflow_digest_suggests_logs_for_all_failed_state_buckets() -> None:
    assert next_actions_for(
        "workflow-instance.digest",
        resolved={"workflowInstance": {"id": 242}},
        data={
            "failedTasks": [
                {
                    "id": 421,
                    "state": "NEED_FAULT_TOLERANCE",
                    "logAvailable": True,
                },
                {"id": 422, "state": "KILL", "logAvailable": True},
                {"id": 423, "state": "FAILURE", "logAvailable": False},
            ]
        },
    ) == [
        {
            "action": "task-instance.log",
            "command": "dsctl task-instance log 422 --tail 80 --raw",
            "mutates": False,
        },
        {
            "action": "task-instance.log",
            "command": "dsctl task-instance log 421 --tail 80 --raw",
            "mutates": False,
        },
    ]


def test_task_list_prioritizes_failed_logs_and_is_bounded() -> None:
    actions = next_actions_for(
        "task-instance.list",
        resolved={"workflow_instance": 242},
        data={
            "totalList": [
                {
                    "id": 421,
                    "state": "SUCCESS",
                    "endTime": "2026-07-11 22:00:01",
                    "logPath": "/logs/421.log",
                },
                {
                    "id": 422,
                    "state": "FAILURE",
                    "endTime": "2026-07-11 22:00:03",
                    "logPath": "/logs/422.log",
                },
                {
                    "id": 423,
                    "state": "FAILURE",
                    "endTime": "2026-07-11 22:00:04",
                    "logPath": "/logs/423.log",
                },
                {
                    "id": 424,
                    "state": "FAILURE",
                    "endTime": "2026-07-11 22:00:02",
                    "logPath": "/logs/424.log",
                },
            ]
        },
    )

    assert actions == [
        {
            "action": "task-instance.log",
            "command": "dsctl task-instance log 423 --tail 80 --raw",
            "mutates": False,
        },
        {
            "action": "task-instance.log",
            "command": "dsctl task-instance log 422 --tail 80 --raw",
            "mutates": False,
        },
    ]


def test_task_list_all_success_suggests_only_latest_available_log() -> None:
    assert next_actions_for(
        "task-instance.list",
        resolved={},
        data={
            "totalList": [
                {
                    "id": 423,
                    "state": "SUCCESS",
                    "endTime": "2026-07-11 22:00:03",
                    "logPath": "",
                },
                {
                    "id": 424,
                    "state": "SUCCESS",
                    "endTime": "2026-07-11 22:00:04",
                    "logPath": "/logs/424.log",
                },
            ]
        },
    ) == [
        {
            "action": "task-instance.log",
            "command": "dsctl task-instance log 424 --tail 30 --raw",
            "mutates": False,
        }
    ]


def test_navigation_is_fail_closed_for_dry_run_unknown_or_malformed_facts() -> None:
    cases: tuple[tuple[str, JsonObject, JsonValue], ...] = (
        ("workflow.create", {}, {"dry_run": True}),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {
                "dry_run": True,
                "schedule_preview": {"count": 5},
                "schedule_confirmation": {"required": True, "token": None},
            },
        ),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {"dry_run": True, "schedule_preview": {"count": 5}},
        ),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {
                "dry_run": True,
                "schedule_confirmation": {"required": False, "token": None},
            },
        ),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {
                "dry_run": True,
                "schedule_preview": {"count": 5},
                "schedule_confirmation": {},
            },
        ),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {
                "dry_run": True,
                "schedule_preview": {"count": 5},
                "schedule_confirmation": {"required": 0, "token": None},
            },
        ),
        (
            "workflow.create",
            {"file": "/workflows/luna.yaml", "project": {"code": 7}},
            {
                "dry_run": True,
                "schedule_preview": {"count": 5},
                "schedule_confirmation": "invalid",
            },
        ),
        (
            "workflow.create",
            {
                "file": "/workflows/luna\0.yaml",
                "project": {"code": 7},
            },
            {"dry_run": True},
        ),
        (
            "workflow.online",
            {"project": {"code": 7}, "workflow": {"code": 101}},
            {"dry_run": True, "releaseState": "ONLINE"},
        ),
        ("workflow.create", {"project": {"code": 7}}, {"releaseState": "OFFLINE"}),
        ("workflow.run", {}, {"workflowInstanceIds": [True]}),
        ("workflow-instance.watch", {}, {"id": "242", "state": "SUCCESS"}),
        ("unrelated.action", {}, {"id": 242}),
    )

    for action, resolved, data in cases:
        assert next_actions_for(action, resolved=resolved, data=data) == []

    assert (
        next_actions_for(
            "workflow.run",
            resolved={},
            data={"workflowInstanceIds": [242]},
            env_file="cluster\0.env",
        )
        == []
    )


def test_success_payload_adds_navigation_only_when_applicable() -> None:
    navigable = success_payload(
        "workflow.run",
        CommandResult(data={"workflowInstanceIds": [242]}),
    )
    terminal = success_payload(
        "task-instance.log",
        CommandResult(data={"text": "done"}),
    )

    assert navigable["next_actions"] == [
        {
            "action": "workflow-instance.watch",
            "command": (
                "dsctl --compact --columns id,name,state,startTime,endTime,duration "
                "workflow-instance watch 242"
            ),
            "mutates": False,
        }
    ]
    assert "next_actions" not in terminal
    assert (
        len(
            json.dumps(navigable["next_actions"], separators=(",", ":")).encode("utf-8")
        )
        < 768
    )


def test_success_payload_adds_list_action_index_through_unified_navigation() -> None:
    payload = success_payload(
        "workflow-instance.list",
        CommandResult(
            data={"totalList": [{"id": 263, "state": "SUCCESS"}]},
        ),
    )

    action_index = payload["action_index"]
    assert isinstance(action_index, dict)
    target = action_index["target"]
    assert isinstance(target, dict)
    assert target == {
        "resource": "workflow-instance",
        "field": "id",
    }
    groups = action_index["groups"]
    assert isinstance(groups, list)
    assert isinstance(groups[0], dict)
    assert groups[0] == {
        "targets": "all",
        "read": [
            "workflow-instance.get",
            "workflow-instance.digest",
            "workflow-instance.watch",
            "workflow-instance.export",
        ],
        "mutate": ["workflow-instance.rerun"],
        "mutate_needs_input": [
            "workflow-instance.edit",
            "workflow-instance.execute-task",
        ],
    }
