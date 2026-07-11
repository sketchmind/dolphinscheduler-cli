from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from dsctl.output import CommandResult, success_payload
from dsctl.result_navigation import next_actions_for

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue


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
