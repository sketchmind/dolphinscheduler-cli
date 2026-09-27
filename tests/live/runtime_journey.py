"""Installed-wheel SHELL success, real failure, repair and recovery journey."""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

import yaml

from tests.live.journey_support import Journey, positive_id
from tests.live.support import require_list, require_mapping, require_text_value

if TYPE_CHECKING:
    from pathlib import Path


DEFINITION_SYNC_EDIT_VERSIONS = frozenset({"2.0.0", "2.0.1", "2.0.2"})


def _tasks_by_name(
    document: dict[str, object], *, label: str
) -> dict[str, dict[str, object]]:
    tasks: dict[str, dict[str, object]] = {}
    for value in require_list(document.get("tasks"), label=f"{label} tasks"):
        task = require_mapping(value, label=f"{label} task")
        name = require_text_value(task.get("name"), label=f"{label} task name")
        assert name not in tasks, f"{label} contains duplicate task names"
        tasks[name] = task
    return tasks


def _task_command(task: dict[str, object], *, label: str) -> str:
    command = task.get("command")
    if command is None:
        command = require_mapping(
            task.get("task_params"), label=f"{label} task params"
        ).get("rawScript")
    return require_text_value(command, label=f"{label} task command")


def _export_definition(journey: Journey) -> dict[str, object]:
    # Reuse the same installed-CLI export path as Journey's cleanup guard.
    return journey._export("workflow", journey.workflow)


def _assert_original_definition(journey: Journey) -> None:
    tasks = _tasks_by_name(_export_definition(journey), label="current definition")
    assert set(tasks) == {"extract", "load"}, "current definition task set changed"
    assert _task_command(tasks["extract"], label="extract") == journey.script("extract")
    assert _task_command(tasks["load"], label="load") == journey.script("load")
    assert tasks["extract"].get("depends_on", []) == []
    assert tasks["load"].get("depends_on", []) == ["extract"]


def _repair(
    journey: Journey,
    instance_id: int,
    command: str,
    *,
    label: str,
    sync_definition: bool,
) -> None:
    patch = journey.workspace / f"{label}.yaml"
    patch.write_text(
        yaml.safe_dump(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {"match": {"name": "load"}, "set": {"command": command}},
                        ]
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    edit = [
        "workflow-instance",
        "edit",
        str(instance_id),
        "--project",
        journey.project,
        "--patch",
        str(patch),
    ]
    if sync_definition:
        edit.append("--sync-definition")
    journey.ok(edit)
    exported = journey.export_instance(instance_id)
    tasks = _tasks_by_name(exported, label="exported instance")
    assert set(tasks) == {"extract", "load"}, "repair changed instance task set"
    assert _task_command(tasks["load"], label="instance load") == command, (
        "instance repair readback mismatch"
    )
    if sync_definition:
        definition = _tasks_by_name(
            _export_definition(journey), label="synchronized definition"
        )
        assert set(definition) == {"extract", "load"}, (
            "definition sync changed task set"
        )
        assert _task_command(definition["load"], label="definition load") == command, (
            "definition sync readback mismatch"
        )


def _task_logs(
    journey: Journey,
    instance_id: int,
    *,
    expected: dict[str, tuple[str, str]],
) -> dict[str, int]:
    deadline = time.monotonic() + 45
    while True:
        rows = journey.rows(
            [
                "task-instance",
                "list",
                "--project",
                journey.project,
                "--workflow-instance",
                str(instance_id),
                "--all",
            ]
        )
        # Some exact list routes retain prior attempts; choose latest matching state.
        selected: dict[str, dict[str, object]] = {}
        for row in rows:
            name = row.get("name")
            if (
                isinstance(name, str)
                and name in expected
                and row.get("state") == expected[name][0]
            ) and (
                name not in selected
                or positive_id(row.get("id")) > positive_id(selected[name].get("id"))
            ):
                selected[name] = row
        if set(selected) == set(expected):
            break
        assert time.monotonic() < deadline, "expected task terminal states not observed"
        time.sleep(2)
    evidence: dict[str, int] = {}
    for name, row in selected.items():
        identity = positive_id(row.get("id"))
        marker = expected[name][1]
        while True:
            result = journey.call(
                ["task-instance", "log", str(identity), "--tail", "500"]
            )
            if result.exit_code == 0 and result.payload.get("ok") is True:
                log = require_mapping(result.payload.get("data"), label="task log")
                text = require_text_value(log.get("text"), label="task log text")
                # Match emitted output, not the marker inside the logged script.
                if emitted_marker(text, marker):
                    break
            else:
                error = require_mapping(result.payload.get("error"), label="log error")
                assert error.get("type") == "task_not_dispatched", "task log failed"
            assert time.monotonic() < deadline, "task output marker never observed"
            time.sleep(2)
        evidence[name] = identity
    return evidence


def emitted_marker(text: str, marker: str) -> bool:
    """Recognize emitted lines with DS worker prefixes, never quoted scripts."""
    prefix = r"(?:\s*|(?:\d{4}-\d{2}-\d{2}[^\n]*?\bINFO\b|\[INFO\])[^\n]*?->\s*)"
    return re.search(rf"(?m)^{prefix}{re.escape(marker)}\s*$", text) is not None


def run_runtime_journey(
    *,
    repo_root: Path,
    executable: Path,
    env_file: Path,
    ds_version: str,
    workspace: Path,
    prefix: str,
) -> dict[str, object]:
    """Exercise only this installed executable and always clean its owned fixture.

    Definition scripts retain the existing wheel's narrow cleanup lineage.
    Exact 2.0.0-2.0.2 synchronize both edits because their native non-sync DAG
    edit is ineffective; the second edit restores the original definition.
    Missing exact actions and worker prerequisites remain failing evidence.
    """
    with Journey(
        repo_root=repo_root,
        executable=executable,
        env_file=env_file,
        ds_version=ds_version,
        workspace=workspace,
        prefix=prefix,
    ) as journey:
        for action in (
            "workflow-instance.edit",
            "workflow-instance.export",
            "workflow-instance.rerun",
            "task-instance.list",
            "task-instance.log",
        ):
            assert journey.capability(action), f"runtime journey requires {action}"
        recovery = (
            "recover-failed"
            if journey.capability("workflow-instance.recover-failed")
            else "rerun"
        )
        sync_definition = ds_version in DEFINITION_SYNC_EDIT_VERSIONS
        journey.create_workflow()
        journey.ok(
            ["workflow", "online", journey.workflow, "--project", journey.project]
        )
        run = journey.data(
            ["workflow", "run", journey.workflow, "--project", journey.project]
        )
        instance_id = journey.resolve_run(run)
        initial = journey.watch(instance_id)
        assert initial.get("state") == "SUCCESS", "initial SHELL DAG did not succeed"
        initial_tasks = _task_logs(
            journey,
            instance_id,
            expected={
                task: ("SUCCESS", f"{journey.run_id}-{task}")
                for task in ("extract", "load")
            },
        )
        initial_round = positive_id(initial.get("runTimes"))
        failure_marker = f"{journey.run_id}-intentional-exit23"
        failure_script = f'printf "%s\\n" "{failure_marker}"\nexit 23\n'
        _repair(
            journey,
            instance_id,
            failure_script,
            label="inject-failure",
            sync_definition=sync_definition,
        )
        journey.ok(
            [
                "workflow-instance",
                "rerun",
                str(instance_id),
                "--project",
                journey.project,
            ]
        )
        failed = journey.watch(instance_id, after_run_times=initial_round)
        assert failed.get("state") == "FAILURE", "exit 23 did not fail the workflow"
        failed_tasks = _task_logs(
            journey, instance_id, expected={"load": ("FAILURE", failure_marker)}
        )
        negative = journey.call(
            [
                "workflow-instance",
                "watch",
                str(instance_id),
                "--project",
                journey.project,
                "--exit-status",
                "--interval-seconds",
                "1",
                "--timeout-seconds",
                "5",
            ]
        )
        error = require_mapping(
            negative.payload.get("error"), label="failed watch error"
        )
        assert negative.exit_code == 1
        assert error.get("type") == "execution_failed"
        assert (
            require_mapping(
                negative.payload.get("data"), label="failed watch instance"
            ).get("state")
            == "FAILURE"
        )
        failed_round = positive_id(failed.get("runTimes"))
        _repair(
            journey,
            instance_id,
            journey.script("load"),
            label="repair-failure",
            sync_definition=sync_definition,
        )
        journey.ok(
            [
                "workflow-instance",
                recovery,
                str(instance_id),
                "--project",
                journey.project,
            ]
        )
        recovered = journey.watch(instance_id, after_run_times=failed_round)
        assert recovered.get("state") == "SUCCESS", "repaired workflow did not succeed"
        assert positive_id(recovered.get("runTimes")) > failed_round
        recovered_tasks = _task_logs(
            journey,
            instance_id,
            expected={"load": ("SUCCESS", f"{journey.run_id}-load")},
        )
        readback = journey.data(
            ["workflow-instance", "get", str(instance_id), "--project", journey.project]
        )
        assert readback.get("state") == "SUCCESS"
        _assert_original_definition(journey)
        evidence: dict[str, object] = {
            "ds_version": ds_version,
            "status": "passed",
            "identity_kind": journey.identity_key,
            "project_identity": journey.project_identity,
            "workflow_identity": journey.workflow_identity,
            "workflow_instance_id": instance_id,
            "states": ["SUCCESS", "FAILURE", "SUCCESS"],
            "intentional_exit_code": 23,
            "failure_error_type": "execution_failed",
            "recovery_action": recovery,
            "definition_sync_required": sync_definition,
            "final_definition_original": True,
            "run_times": [initial_round, failed_round, recovered.get("runTimes")],
            "task_instances": {
                "initial": initial_tasks,
                "failed": failed_tasks,
                "recovered": recovered_tasks,
            },
        }
    evidence.update(cleanup=journey.cleanup_evidence, trace=journey.trace)
    return evidence
