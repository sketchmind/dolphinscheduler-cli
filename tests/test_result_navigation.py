from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.commands import workflow as workflow_commands
from dsctl.output import CommandResult, result_payload
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


def test_list_discovery_commands_keep_selected_context_or_file() -> None:
    cases: list[tuple[JsonObject, list[str]]] = [
        (
            {"context": "production", "env_file": "/config/prod.env"},
            ["dsctl", "--context", "production"],
        ),
        (
            {"context": None, "env_file": "/config/team ' one.env"},
            ["dsctl", "--env-file", "/config/team ' one.env"],
        ),
    ]
    for selection, expected in cases:
        result = navigation_for(
            "workflow-instance.list",
            resolved={"selection": selection},
            data={"totalList": [{"id": 263, "state": "SUCCESS"}]},
        )
        index = result["action_index"]
        assert shlex.split(index["schema_command_pattern"]) == [
            *expected,
            "schema",
            "--command",
            "ACTION",
        ]
        assert "schema_command" not in index
        assert shlex.split(index["group_command"]) == [
            *expected,
            "schema",
            "--group",
            "workflow-instance",
        ]


def test_lifecycle_followup_keeps_named_context_over_its_file() -> None:
    result = navigation_for(
        "workflow.run",
        resolved={
            "project": {"code": 7},
            "selection": {"context": "production", "env_file": "/config/prod.env"},
        },
        data={"workflowInstanceIds": [242]},
    )
    command = shlex.split(result["next_actions"][0]["command"])
    assert command[:3] == ["dsctl", "--context", "production"]
    assert command[-5:] == ["workflow-instance", "watch", "242", "--project", "7"]
    assert "--env-file" not in command


def test_pending_trigger_navigates_to_identity_resolution_not_watch(
    tmp_path: Path,
) -> None:
    env_file = str(tmp_path / "project ' one.env")
    actions = next_actions_for(
        "workflow.run",
        resolved={"project": {"code": 7}},
        data={
            "workflowInstanceIds": [],
            "triggerCode": 900,
            "instanceResolution": "pending",
        },
        env_file=env_file,
    )
    assert len(actions) == 1
    assert actions[0]["action"] == "workflow-instance.list"
    argv = shlex.split(actions[0]["command"])
    assert argv[argv.index("--trigger-code") + 1] == "900"
    assert argv[argv.index("--env-file") + 1] == env_file
    assert "watch" not in argv


def test_replay_navigation_requires_a_verified_baseline() -> None:
    resolved: JsonObject = {"project": {"code": 7}, "workflowInstance": {"id": 12}}
    assert (
        next_actions_for(
            "workflow-instance.rerun", resolved=resolved, data={"state": "SUCCESS"}
        )
        == []
    )
    resolved["execution_baseline"] = {"run_times": 2}
    actions = next_actions_for(
        "workflow-instance.rerun", resolved=resolved, data={"state": "SUCCESS"}
    )
    assert len(actions) == 1
    argv = shlex.split(actions[0]["command"])
    assert argv[argv.index("--after-run-times") + 1] == "2"
    assert argv[argv.index("watch") + 1] == "12"


def test_legacy_execution_navigation_uses_the_actual_project_name() -> None:
    resolved: JsonObject = {
        "project": {"id": 7, "name": "legacy project's"},
        "workflowInstance": {"id": 12},
        "execution_baseline": {"run_times": 2},
    }
    cases: tuple[tuple[str, JsonObject], ...] = (
        ("workflow-instance.rerun", {"state": "SUCCESS"}),
        ("workflow.run", {"workflowInstanceIds": [12]}),
        ("workflow-instance.watch", {"id": 12, "state": "SUCCESS"}),
    )
    for action, data in cases:
        actions = next_actions_for(action, resolved=resolved, data=data)
        assert len(actions) == 1
        argv = shlex.split(actions[0]["command"])
        assert argv[argv.index("--project") + 1] == "legacy project's"
        if action == "workflow-instance.rerun":
            assert argv[argv.index("--after-run-times") + 1] == "2"
    resolved["project"] = {"id": 7}
    assert next_actions_for("workflow-instance.rerun", resolved=resolved, data={}) == []


def test_log_window_navigation_preserves_location_and_scope() -> None:
    actions = next_actions_for(
        "task-instance.log",
        resolved={"taskInstance": {"id": 8}},
        data={"window": {"next_start_line": 201, "requested_limit": 200}},
    )
    assert len(actions) == 1
    argv = shlex.split(actions[0]["command"])
    assert argv[argv.index("log") + 1] == "8"
    assert argv[argv.index("--start-line") + 1] == "201"
    assert "--tail" not in argv
    assert (
        next_actions_for(
            "task-instance.log",
            resolved={"taskInstance": {"id": 8}},
            data={"window": {"next_start_line": None, "requested_limit": 200}},
        )
        == []
    )


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
            "schema_command_pattern": "dsctl schema --command ACTION",
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
            },
            {
                "targets": [41],
                "mutate": ["schedule.online"],
                "mutate_needs_input": ["schedule.update", "schedule.delete"],
            },
            {"targets": [42], "mutate": ["schedule.offline"]},
        ],
        "schema_command_pattern": "dsctl schema --command ACTION",
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
        "schema_command_pattern": "dsctl schema --command ACTION",
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
        "schema_command_pattern": "dsctl schema --command ACTION",
        "group_command": "dsctl schema --group task-instance",
        "target_count": 5,
        "indexed_target_count": 5,
        "truncated": False,
    }


def test_standalone_stream_discovers_project_scoped_actions() -> None:
    navigation = navigation_for(
        "task-instance.list",
        resolved={"project": {"code": 7}},
        data={
            "totalList": [
                {
                    "id": 487,
                    "state": "RUNNING_EXECUTION",
                    "taskType": "SHELL",
                    "taskExecuteType": "STREAM",
                    "workflowInstanceId": 0,
                    "host": "worker:1234",
                }
            ]
        },
    )
    index = navigation["action_index"]
    for action in ("get", "watch", "savepoint", "stop"):
        assert _targets_for(index, f"task-instance.{action}") == "all"
    serialized = json.dumps(index)
    assert "task-instance.force-success" not in serialized
    assert "task-instance.sub-workflow" not in serialized


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
    assert _targets_for(action_index, "schedule.online") == list(range(1, 101))
    assert _targets_for(action_index, "schedule.get") == list(range(1, 101))
    assert len(action_index["groups"]) == 1
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
    assert _targets_for(action_index, "workflow.get") == "all"
    assert action_index["target_count"] == action_index["indexed_target_count"] == 100
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
                None,
            ]
        },
    )

    action_index = navigation["action_index"]
    assert _targets_for(action_index, "schedule.online") == [5]
    assert _targets_for(action_index, "schedule.offline") == [4]
    assert _targets_for(action_index, "schedule.delete") == [5]
    assert _targets_for(action_index, "schedule.get") == [3, 4, 5]
    assert action_index["target_count"] == 9
    assert action_index["indexed_target_count"] == 3
    assert action_index["truncated"] is False


def test_action_index_excludes_duplicates_beyond_the_indexed_page_prefix() -> None:
    rows: list[JsonValue] = [
        {"id": target, "releaseState": "OFFLINE"} for target in range(1, 102)
    ]
    rows.append({"id": 1, "releaseState": "ONLINE"})
    index = navigation_for("schedule.list", resolved={}, data={"totalList": rows})[
        "action_index"
    ]

    assert _targets_for(index, "schedule.get") == list(range(2, 102))
    assert _targets_for(index, "schedule.online") == list(range(2, 102))
    assert index["target_count"] == 102
    assert index["indexed_target_count"] == 100
    assert index["truncated"] is False
    assert len(index["groups"]) == 1


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
        assert all(group["targets"] != "all" for group in action_index["groups"])
        if conditional_action is not None:
            assert _targets_for(action_index, conditional_action) == list(range(1, 101))


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
                "dsctl --format json-compact --columns code,name,releaseState "
                "workflow online 101 --project 7"
            ),
            "mutates": True,
        }
    ]
    assert shlex.split(actions[0]["command"]) == [
        "dsctl",
        "--format",
        "json-compact",
        "--columns",
        "code,name,releaseState",
        "workflow",
        "online",
        "101",
        "--project",
        "7",
    ]
    assert "unsafe" not in actions[0]["command"]


def test_online_workflow_offers_manual_run_and_schedule_inspection() -> None:
    expected = [
        {
            "action": "workflow.run",
            "command": "dsctl --format json-compact workflow run 101 --project 7",
            "mutates": True,
        },
        {
            "action": "schedule.list",
            "command": (
                "dsctl --format json-compact schedule list --project 7 --workflow 101"
            ),
            "mutates": False,
        },
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
                "dsctl --format json-compact --columns code,name,releaseState "
                "workflow create "
                "--file ' /workflows/workflow specs/luna.yaml ' --project 7"
            ),
            "mutates": True,
        }
    ]
    assert shlex.split(actions[0]["command"]) == [
        "dsctl",
        "--format",
        "json-compact",
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
                "dsctl --format json-compact --columns "
                "id,name,state,startTime,endTime,duration "
                "workflow-instance watch 242 --project 7"
            ),
            "mutates": False,
        }
    ]

    assert (
        next_actions_for(
            "workflow.run",
            resolved={"project": {"code": 7}},
            data={"workflowInstanceIds": [242]},
        )
        == expected
    )
    assert (
        next_actions_for(
            "workflow.run",
            resolved={"project": {"code": 7}},
            data={"workflowInstanceIds": []},
        )
        == []
    )
    assert (
        next_actions_for(
            "workflow.run",
            resolved={"project": {"code": 7}},
            data={"workflowInstanceIds": [242, 243]},
        )
        == []
    )


def test_workflow_watch_success_suggests_bounded_task_list() -> None:
    assert next_actions_for(
        "workflow-instance.watch",
        resolved={
            "project": {"code": 7},
            "workflow_instance": {"id": 242},
        },
        data={"id": 242, "state": "SUCCESS"},
    ) == [
        {
            "action": "task-instance.list",
            "command": (
                "dsctl --format json-compact --columns "
                "id,name,state,taskType,endTime,logPath task-instance list "
                "--workflow-instance 242 --project 7 --page-size 20"
            ),
            "mutates": False,
        }
    ]


def test_workflow_watch_failure_suggests_bounded_complete_digest() -> None:
    assert next_actions_for(
        "workflow-instance.watch",
        resolved={"project": {"code": 7}},
        data={"id": 242, "state": "FAILURE"},
    ) == [
        {
            "action": "workflow-instance.digest",
            "command": (
                "dsctl --format json-compact --columns "
                "taskCount,taskStateCounts,progress,failedTasks "
                "workflow-instance digest 242 --project 7"
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
        ("workflow.run", {}, {"workflowInstanceIds": [242]}),
        ("workflow-instance.watch", {}, {"id": "242", "state": "SUCCESS"}),
        ("workflow-instance.watch", {}, {"id": 242, "state": "SUCCESS"}),
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


def test_result_payload_adds_navigation_only_when_applicable() -> None:
    navigable = result_payload(
        "workflow.run",
        CommandResult(
            data={"workflowInstanceIds": [242]},
            resolved={"project": {"code": 7}},
        ),
    )
    terminal = result_payload(
        "task-instance.log",
        CommandResult(data={"text": "done"}),
    )

    assert navigable["next_actions"] == [
        {
            "action": "workflow-instance.watch",
            "command": (
                "dsctl --format json-compact --columns "
                "id,name,state,startTime,endTime,duration "
                "workflow-instance watch 242 --project 7"
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


def test_result_payload_adds_list_action_index_through_unified_navigation() -> None:
    payload = result_payload(
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


def test_result_payload_filters_navigation_through_exact_version_capabilities() -> None:
    payload = result_payload(
        "workflow.list",
        CommandResult(
            data={
                "totalList": [
                    {
                        "code": 101,
                        "name": "daily-etl",
                        "releaseState": "OFFLINE",
                    }
                ]
            },
            resolved={"project": {"code": 7}},
        ),
        available_actions=frozenset({"workflow.get"}),
    )

    action_index = payload["action_index"]
    assert isinstance(action_index, dict)
    assert action_index["groups"] == [{"targets": "all", "read": ["workflow.get"]}]

    terminal = result_payload(
        "workflow.run",
        CommandResult(data={"workflowInstanceIds": [242]}),
        available_actions=frozenset(),
    )
    assert "next_actions" not in terminal


def _relationship_cases() -> list[tuple[str, JsonObject, JsonObject, list[str]]]:
    project: JsonObject = {"code": 7, "name": "team's $(project)"}
    workflow: JsonObject = {"code": 101}
    workflow_changes: list[tuple[str, JsonObject, JsonObject, list[str]]] = [
        (
            action,
            {"project": project, "workflow": workflow},
            {"code": 101, "releaseState": "OFFLINE", "schedule": {"id": 41}},
            ["workflow.describe", "schedule.get"],
        )
        for action in ("workflow.edit", "workflow.offline")
    ]
    schedule_changes: list[tuple[str, JsonObject, JsonObject, list[str]]] = [
        (
            action,
            {"schedule": {"id": 41}},
            {
                "id": 41,
                "releaseState": "OFFLINE",
                "projectName": "team's $(project)",
            },
            ["schedule.preview"],
        )
        for action in ("schedule.create", "schedule.update", "schedule.offline")
    ]
    group_changes: list[tuple[str, JsonObject, JsonObject, list[str]]] = [
        (action, {"taskGroup": {"id": 5}}, {"id": 5}, ["task-group.queue.list"])
        for action in (
            "task-group.get",
            "task-group.create",
            "task-group.update",
            "task-group.start",
        )
    ]
    return [
        (
            "workflow.online",
            {"project": project, "workflow": workflow},
            {"code": 101, "releaseState": "ONLINE", "schedule": {"id": 41}},
            ["workflow.run", "schedule.get"],
        ),
        *workflow_changes,
        *schedule_changes,
        (
            "schedule.online",
            {"schedule": {"id": 41}},
            {
                "id": 41,
                "releaseState": "ONLINE",
                "projectName": "team's $(project)",
                "workflowDefinitionName": "daily's $(workflow)",
                "workflowDefinitionCode": 101,
            },
            ["workflow-instance.list"],
        ),
        *group_changes,
        (
            "task-group.queue.list",
            {"taskGroup": {"id": 5}},
            {
                "totalList": [
                    {
                        "id": 9,
                        "groupId": 5,
                        "taskId": 202,
                        "workflowInstanceId": 200,
                        "projectName": "team's $(project)",
                    }
                ]
            },
            ["task-instance.get"],
        ),
    ]


def test_relationship_navigation_preserves_scope_and_parses_without_io() -> None:
    root = get_command(app)
    for source, resolved, data, expected in _relationship_cases():
        resolved["selection"] = {
            "context": "prod's $(context)",
            "env_file": "/unused.env",
        }
        actions = next_actions_for(source, resolved=resolved, data=data)
        assert [item["action"] for item in actions] == expected, source
        for item in actions:
            argv = shlex.split(item["command"])
            assert argv[:3] == ["dsctl", "--context", "prod's $(context)"]
            assert "--env-file" not in argv
            assert "--compact" not in argv
            contract = COMMAND_CATALOG.command(item["action"])
            assert item["mutates"] is (contract.effects.remote == "write")
            # Parse each generated invocation with the real leaf parser, without
            # running a callback or opening a connection.
            command = root
            for segment in contract.route:
                assert isinstance(command, TyperGroup)
                command = command.commands[segment]
            route_start = argv.index(contract.route[0], 3)
            with command.make_context(
                contract.route[-1], argv[route_start + len(contract.route) :]
            ) as context:
                if item["action"] == "task-instance.get":
                    assert context.params["task_instance"] == 202
                    assert context.params["workflow_instance"] == 200
                    assert context.params["project"] == "team's $(project)"
                elif source == "schedule.online":
                    assert context.params["workflow"] == "daily's $(workflow)"


def test_relationship_navigation_respects_exact_availability() -> None:
    for source, resolved, data, expected in _relationship_cases():
        navigation = navigation_for(
            source, resolved=resolved, data=data, available_actions={expected[-1]}
        )
        assert [item["action"] for item in navigation["next_actions"]] == [expected[-1]]
        assert "next_actions" not in navigation_for(
            source, resolved=resolved, data=data, available_actions=set()
        )


def test_parent_child_navigation_does_not_reuse_source_project_for_target() -> None:
    # DS relations may cross projects. These responses verify the target ID,
    # but resolved.project describes the source and cannot scope target reads.
    cases: list[tuple[str, JsonObject, JsonObject]] = [
        (
            "workflow-instance.parent",
            {
                "project": {"code": 8, "name": "child project"},
                "subWorkflowInstance": {"id": 201},
            },
            {"parentWorkflowInstance": 200},
        ),
        (
            "task-instance.sub-workflow",
            {
                "project": {"code": 7, "name": "parent project"},
                "workflowInstance": {"id": 200},
                "taskInstance": {"id": 202},
            },
            {"subWorkflowInstanceId": 201},
        ),
    ]
    for action, resolved, data in cases:
        resolved["selection"] = {"context": "production"}
        assert navigation_for(action, resolved=resolved, data=data) == {}


def test_relationship_navigation_rejects_missing_and_conflicting_identities() -> None:
    cases: list[tuple[str, JsonObject, JsonValue]] = [
        ("workflow.offline", {"project": {"code": 7}}, {"code": 101}),
        (
            "workflow.edit",
            {"project": {"code": 7}, "workflow": {"code": 101}},
            {"code": 102},
        ),
        (
            "workflow.online",
            {"project": {"code": 7}, "workflow": {"code": 101}},
            {"code": 102, "releaseState": "ONLINE"},
        ),
        ("schedule.create", {}, {"id": 41}),
        ("schedule.update", {"schedule": {"id": 42}}, {"id": 41, "projectName": "p"}),
        ("schedule.offline", {}, {"id": True, "projectName": "p"}),
        ("schedule.online", {}, {"id": 41, "projectName": "p", "dry_run": True}),
        ("task-group.get", {"taskGroup": {"id": 5}}, {"id": 6}),
        ("task-group.queue.force-start", {}, {"queueId": 9, "accepted": True}),
        ("task-group.queue.set-priority", {}, {"queueId": 9, "priority": 2}),
    ]
    queue: JsonObject = {
        "id": 9,
        "groupId": 5,
        "taskId": 202,
        "workflowInstanceId": 200,
        "projectName": "p",
    }
    cases.extend(
        ("task-group.queue.list", {"taskGroup": {"id": 5}}, {"totalList": rows})
        for rows in (
            [queue, queue],
            [{**queue, "groupId": 6}],
            [{**queue, "workflowInstanceId": None}],
            [{**queue, "projectName": None}],
        )
    )
    for source, resolved, data in cases:
        assert next_actions_for(source, resolved=resolved, data=data) == [], source


def test_queue_without_task_id_links_to_scoped_task_discovery() -> None:
    actions = next_actions_for(
        "task-group.queue.list",
        resolved={"taskGroup": {"id": 5}, "selection": {"context": "test cluster"}},
        data={
            "totalList": [
                {
                    "id": 9,
                    "groupId": 5,
                    "taskId": None,
                    "workflowInstanceId": 200,
                    "projectName": "owned project",
                }
            ]
        },
    )
    assert len(actions) == 1
    assert actions[0]["action"] == "task-instance.list"
    argv = shlex.split(actions[0]["command"])
    assert argv[:3] == ["dsctl", "--context", "test cluster"]
    command_start = argv.index("task-instance")
    assert argv[command_start : command_start + 2] == ["task-instance", "list"]
    assert argv[argv.index("--project") + 1] == "owned project"
    assert argv[argv.index("--workflow-instance") + 1] == "200"
    assert actions[0]["mutates"] is False


def test_workflow_edit_uses_renamed_legacy_workflow_without_relabeling_native_id() -> (
    None
):
    actions = next_actions_for(
        "workflow.edit",
        resolved={
            "project": {"id": 7, "name": "legacy"},
            "workflow": {"id": 101, "name": "old"},
        },
        data={"id": 101, "name": "new name", "releaseState": "OFFLINE"},
    )
    assert len(actions) == 1
    assert shlex.split(actions[0]["command"])[-5:] == [
        "workflow",
        "describe",
        "new name",
        "--project",
        "legacy",
    ]


def test_schedule_update_index_omits_online_unknown_and_duplicate_rows() -> None:
    index = navigation_for(
        "schedule.list",
        resolved={},
        data={
            "totalList": [
                {"id": 1, "releaseState": "OFFLINE"},
                {"id": 2, "releaseState": "ONLINE"},
                {"id": 3},
                {"id": 4, "releaseState": "FUTURE"},
                {"id": 5, "releaseState": "OFFLINE"},
                {"id": 5, "releaseState": "ONLINE"},
            ]
        },
    )["action_index"]
    assert _targets_for(index, "schedule.update") == [1]
