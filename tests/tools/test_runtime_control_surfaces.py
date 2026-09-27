from __future__ import annotations

import ast
import json
from pathlib import Path

import httpx
import pytest
from tests.live import test_runtime_control_surfaces as controls
from tests.live.support import DsctlCommandResult
from tests.live.test_runtime_control_surfaces import (
    EXECUTE_TASK_API_VERSIONS,
    EXECUTE_TASK_NO_MASTER_HANDLER_VERSIONS,
    EXECUTE_TASK_RUN_TIMES_VERSIONS,
    EXECUTE_TASK_VERSIONS,
    TASK_CONTROL_VERSIONS,
    TASK_STOP_SIGINT_FAILURE_VERSIONS,
    TASK_STOP_TERM_FAILURE_VERSIONS,
    TASK_STOP_VERIFIED_FAILURE_VERSIONS,
    TASK_STOP_VERIFIED_STOP_VERSIONS,
    _assert_action_warning,
    _assert_replay_round,
    _assert_task_identity,
    _expected_stopped_task_states,
    _expected_workflow_instance_stop_state,
    _expected_workflow_stop_state,
    _observe_execute_task_round_terminal,
    _require_run_identity_before_cleanup,
    _require_signal_task_stop_log,
    _wait_for_cleanup_terminal,
)
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS


def _result(
    action: str, *, data: dict[str, object], resolved: dict[str, object] | None = None
) -> DsctlCommandResult:
    return DsctlCommandResult(
        argv=(action,),
        exit_code=0,
        stdout="",
        stderr="",
        payload={
            "ok": True,
            "action": action,
            "data": data,
            "resolved": resolved or {},
        },
    )


def test_runtime_control_version_memberships_are_independent() -> None:
    assert len(TARGET_DS_VERSIONS) == 37
    assert len(TASK_CONTROL_VERSIONS) == 19
    assert len(EXECUTE_TASK_API_VERSIONS) == 9
    assert len(EXECUTE_TASK_VERSIONS) == 5
    assert len(EXECUTE_TASK_RUN_TIMES_VERSIONS) == 3
    assert {
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
    } == EXECUTE_TASK_NO_MASTER_HANDLER_VERSIONS
    assert {"1.3.9", "2.0.9", "3.0.6"}.isdisjoint(TASK_CONTROL_VERSIONS)
    assert "3.1.0" in TASK_CONTROL_VERSIONS
    assert "3.1.9" not in EXECUTE_TASK_VERSIONS
    assert "3.2.0" in EXECUTE_TASK_VERSIONS
    assert "3.4.3" in EXECUTE_TASK_VERSIONS
    assert TASK_STOP_VERIFIED_FAILURE_VERSIONS.isdisjoint(
        TASK_STOP_VERIFIED_STOP_VERSIONS
    )
    assert TASK_CONTROL_VERSIONS == (
        TASK_STOP_VERIFIED_FAILURE_VERSIONS | TASK_STOP_VERIFIED_STOP_VERSIONS
    )
    assert {
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    } == TASK_STOP_SIGINT_FAILURE_VERSIONS
    assert TASK_STOP_TERM_FAILURE_VERSIONS == TASK_STOP_VERIFIED_FAILURE_VERSIONS
    assert TASK_STOP_SIGINT_FAILURE_VERSIONS <= TASK_STOP_VERIFIED_STOP_VERSIONS
    assert len(TASK_STOP_VERIFIED_FAILURE_VERSIONS) == 13
    assert len(TASK_STOP_VERIFIED_STOP_VERSIONS) == 6


@pytest.mark.parametrize(
    ("version", "task_state", "workflow_state"),
    [
        ("3.1.0", "KILL", "FAILURE"),
        ("3.1.0", "FAILURE", "FAILURE"),
        ("3.2.0", "KILL", "FAILURE"),
        ("3.3.1", "KILL", "STOP"),
        ("3.3.1", "FAILURE", "FAILURE"),
        ("3.4.2", "KILL", "STOP"),
        ("3.4.2", "FAILURE", "FAILURE"),
        ("3.4.3", "KILL", "STOP"),
    ],
)
def test_workflow_stop_state_follows_observed_task_terminal_state(
    version: str, task_state: str, workflow_state: str
) -> None:
    assert _expected_workflow_stop_state(version, task_state) == workflow_state


def test_unproven_task_failure_cannot_choose_workflow_failure() -> None:
    with pytest.raises(AssertionError):
        _expected_workflow_stop_state("3.4.3", "FAILURE")


@pytest.mark.parametrize(
    ("version", "task_state", "workflow_state"),
    [
        ("3.1.0", "KILL", "STOP"),
        ("3.1.0", "FAILURE", "STOP"),
        ("3.2.0", "KILL", "STOP"),
        ("3.2.2", "KILL", "STOP"),
        ("3.3.1", "KILL", "STOP"),
        ("3.3.1", "FAILURE", "FAILURE"),
        ("3.4.2", "KILL", "STOP"),
        ("3.4.2", "FAILURE", "FAILURE"),
        ("3.4.3", "KILL", "STOP"),
    ],
)
def test_workflow_instance_stop_uses_its_own_ready_stop_rule(
    version: str, task_state: str, workflow_state: str
) -> None:
    assert _expected_workflow_instance_stop_state(version, task_state) == workflow_state


def test_workflow_instance_stop_failure_requires_reviewed_signal_path() -> None:
    with pytest.raises(AssertionError):
        _expected_workflow_instance_stop_state("3.4.3", "FAILURE")
    assert _expected_stopped_task_states("3.1.0") == frozenset({"KILL", "FAILURE"})
    assert _expected_stopped_task_states("3.3.1") == frozenset({"KILL", "FAILURE"})
    assert _expected_stopped_task_states("3.2.0") == frozenset({"KILL", "FAILURE"})
    assert _expected_stopped_task_states("3.4.3") == "KILL"


def test_runtime_harness_literal_instance_commands_select_a_project() -> None:
    source_path = Path(controls.__file__ or "")
    tree = ast.parse(source_path.read_text())
    found = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.List) or len(node.elts) < 2:
            continue
        first, second = node.elts[:2]
        if not (
            isinstance(first, ast.Constant)
            and isinstance(first.value, str)
            and first.value in {"workflow-instance", "task-instance"}
            and isinstance(second, ast.Constant)
            and isinstance(second.value, str)
        ):
            continue
        found += 1
        if second.value == "log":
            continue  # The logger API selects by task id without a project.
        literal_options = {
            item.value
            for item in node.elts
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        }
        assert "--project" in literal_options, (
            f"{first.value} {second.value} at line {node.lineno} lacks --project"
        )
    assert found >= 15


@pytest.mark.parametrize("exit_code", [130, 143])
def test_signal_stop_log_requires_selected_task_and_exact_native_exit_line(
    exit_code: int,
) -> None:
    line = (
        "process has exited. execute path:/tmp/task, processId:42 "
        f",exitStatusCode:{exit_code} ,processWaitForStatus:true "
        f",processExitValue:{exit_code}"
    )
    good_data: dict[str, object] = {
        "text": f"runtime-stop\n{line}",
        "window": {"mode": "tail", "scope_complete": True},
    }
    selected: dict[str, object] = {"taskInstance": {"id": 41}}
    _require_signal_task_stop_log(
        _result("task-instance.log", data=good_data, resolved=selected),
        task_id=41,
        exit_code=exit_code,
    )
    if exit_code == 143:
        good_data["text"] = str(good_data["text"]).replace(
            "process has exited.", "process has exited,"
        )
        _require_signal_task_stop_log(
            _result("task-instance.log", data=good_data, resolved=selected),
            task_id=41,
            exit_code=exit_code,
        )

    for text in (
        "runtime-stop\nprocess exited with 0; unrelated task FAILURE",
        line.replace(f"processExitValue:{exit_code}", "processExitValue:0"),
        line.replace(f"exitStatusCode:{exit_code}", "exitStatusCode:1"),
        line.replace(f"processExitValue:{exit_code}", f"processExitValue:{exit_code}0"),
    ):
        bad_data = {**good_data, "text": text}
        with pytest.raises(AssertionError, match="lacks a native exit"):
            _require_signal_task_stop_log(
                _result("task-instance.log", data=bad_data, resolved=selected),
                task_id=41,
                exit_code=exit_code,
            )
    with pytest.raises(AssertionError):
        _require_signal_task_stop_log(
            _result(
                "task-instance.log",
                data=good_data,
                resolved={"taskInstance": {"id": 42}},
            ),
            task_id=41,
            exit_code=exit_code,
        )
    with pytest.raises(AssertionError):
        _require_signal_task_stop_log(
            _result(
                "task-instance.log",
                data={**good_data, "window": {"mode": "tail", "scope_complete": False}},
                resolved=selected,
            ),
            task_id=41,
            exit_code=exit_code,
        )


@pytest.mark.parametrize(
    ("action", "argv"),
    [
        ("workflow-instance.list", ["workflow-instance", "list"]),
        ("workflow-instance.get", ["workflow-instance", "get", "91"]),
        ("workflow-instance.watch", ["workflow-instance", "watch", "91"]),
        ("workflow-instance.stop", ["workflow-instance", "stop", "91"]),
        ("workflow-instance.rerun", ["workflow-instance", "rerun", "91"]),
        (
            "workflow-instance.recover-failed",
            ["workflow-instance", "recover-failed", "91"],
        ),
        (
            "workflow-instance.execute-task",
            ["workflow-instance", "execute-task", "91", "--task", "echo-task"],
        ),
        (
            "task-instance.list",
            ["task-instance", "list", "--workflow-instance", "91"],
        ),
        (
            "task-instance.get",
            ["task-instance", "get", "41", "--workflow-instance", "91"],
        ),
        (
            "task-instance.savepoint",
            ["task-instance", "savepoint", "41", "--workflow-instance", "91"],
        ),
        (
            "task-instance.stop",
            ["task-instance", "stop", "41", "--workflow-instance", "91"],
        ),
        (
            "task-instance.force-success",
            ["task-instance", "force-success", "41", "--workflow-instance", "91"],
        ),
    ],
)
def test_real_instance_cli_rejects_missing_project_before_transport(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    action: str,
    argv: list[str],
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("DSCTL_CONTEXT", raising=False)
    monkeypatch.delenv("DSCTL_ENV_FILE", raising=False)
    profile = tmp_path / "scope.env"
    profile.write_text(
        "DS_VERSION=3.4.3\nDS_API_URL=http://127.0.0.1:9\nDS_API_TOKEN=fixture-token\n"
    )

    def reject_transport(*_args: object, **_kwargs: object) -> None:
        pytest.fail("missing project must fail before any HTTP request")

    monkeypatch.setattr(httpx.Client, "send", reject_transport)
    result = CliRunner().invoke(app, ["--env-file", str(profile), *argv])
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stderr)
    assert payload["action"] == action
    assert payload["error"]["type"] == "user_input_error"
    assert "Project is required" in payload["error"]["message"]


def test_runtime_poll_fails_immediately_on_user_input_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    polls = 0

    def one_poll(
        _repo_root: Path,
        _argv: list[str],
        *,
        accept: object,
        **_kwargs: object,
    ) -> DsctlCommandResult:
        nonlocal polls
        polls += 1
        result = DsctlCommandResult(
            argv=("task-instance", "list"),
            exit_code=1,
            stdout="",
            stderr="",
            payload={
                "ok": False,
                "action": "task-instance.list",
                "error": {
                    "type": "user_input_error",
                    "message": "Project is required",
                },
            },
        )
        assert callable(accept)
        accept(result)
        return result

    monkeypatch.setattr(controls, "wait_for_result", one_poll)
    with pytest.raises(AssertionError, match="Project is required"):
        controls._poll_runtime(
            tmp_path,
            ["task-instance", "list"],
            env_file=tmp_path / "fixture.env",
            accept=lambda _result: False,
            timeout_seconds=30.0,
        )
    assert polls == 1


def test_task_identity_checks_both_task_and_owner() -> None:
    task = {"id": 41, "workflowInstanceId": 91, "name": "sleep-task"}
    _assert_task_identity(task, task_id=41, workflow_instance_id=91, name="sleep-task")
    with pytest.raises(AssertionError):
        _assert_task_identity(
            task, task_id=42, workflow_instance_id=91, name="sleep-task"
        )
    with pytest.raises(AssertionError):
        _assert_task_identity(
            task, task_id=41, workflow_instance_id=92, name="sleep-task"
        )


def test_stop_warning_is_optional_but_must_describe_the_actual_readback() -> None:
    _assert_action_warning(
        {"data": {"state": "STOP"}}, action="stop", target_state="STOP"
    )
    with pytest.raises(AssertionError):
        _assert_action_warning(
            {"data": {"state": "READY_STOP"}}, action="stop", target_state="STOP"
        )
    warning: dict[str, object] = {
        "code": "workflow_instance_action_state_after_request",
        "action": "stop",
        "current_state": "READY_STOP",
        "target_state": "STOP",
        "expect_non_final": False,
        "message": ("stop requested; current workflow instance state is READY_STOP"),
    }
    readback = {"state": "READY_STOP"}
    valid: dict[str, object] = {"data": readback, "warnings": [warning]}
    _assert_action_warning(valid, action="stop", target_state="STOP")
    readback["state"] = "RUNNING_EXECUTION"
    with pytest.raises(AssertionError):
        _assert_action_warning(valid, action="stop", target_state="STOP")
    readback["state"] = "READY_STOP"
    warning["target_state"] = "SUCCESS"
    with pytest.raises(AssertionError):
        _assert_action_warning(valid, action="stop", target_state="STOP")


@pytest.mark.parametrize(
    ("action", "command_type"),
    [
        ("rerun", "REPEAT_RUNNING"),
        ("recover-failed", "START_FAILURE_TASK_PROCESS"),
    ],
)
def test_replay_requires_new_round_and_expected_final_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    action: str,
    command_type: str,
) -> None:
    calls: list[list[str]] = []
    final = {"id": 91, "runTimes": 2, "state": "FAILURE", "commandType": command_type}

    def fake_run(
        _repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
        timeout_seconds: float = 60.0,
    ) -> DsctlCommandResult:
        calls.append(argv)
        if argv[:2] == ["workflow-instance", "watch"]:
            return _result("workflow-instance.watch", data=final)
        return _result(
            f"workflow-instance.{action}",
            data={"id": 91, "state": "SUBMITTED_SUCCESS"},
            resolved={"execution_baseline": {"run_times": 1}},
        )

    monkeypatch.setattr("tests.live.test_runtime_control_surfaces.run_dsctl", fake_run)
    _assert_replay_round(
        tmp_path,
        tmp_path / "unused.env",
        project="fixture-project",
        workflow_instance_id=91,
        previous_run_times=1,
        action=action,
    )
    assert calls[0] == [
        "workflow-instance",
        action,
        "91",
        "--project",
        "fixture-project",
    ]
    assert calls[1][0:3] == ["workflow-instance", "watch", "91"]
    assert calls[1][calls[1].index("--after-run-times") + 1] == "1"

    final["runTimes"] = 1
    with pytest.raises(AssertionError):
        _assert_replay_round(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            previous_run_times=1,
            action=action,
        )
    final["runTimes"] = 2
    final["state"] = "SUCCESS"
    with pytest.raises(AssertionError):
        _assert_replay_round(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            previous_run_times=1,
            action=action,
        )


def test_replay_cleanup_requires_a_later_execution(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[list[str]] = []
    final = {"id": 91, "state": "FAILURE", "runTimes": 1}

    def fake_run(
        _repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
        timeout_seconds: float,
    ) -> DsctlCommandResult:
        calls.append(argv)
        return _result("workflow-instance.watch", data=final)

    monkeypatch.setattr("tests.live.test_runtime_control_surfaces.run_dsctl", fake_run)
    with pytest.raises(AssertionError):
        _wait_for_cleanup_terminal(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            after_run_times=1,
        )
    assert calls[0][calls[0].index("--after-run-times") + 1] == "1"
    final["runTimes"] = 2
    _wait_for_cleanup_terminal(
        tmp_path,
        tmp_path / "unused.env",
        project="fixture-project",
        workflow_instance_id=91,
        after_run_times=1,
    )


def test_unresolved_run_identity_retains_owned_fixture() -> None:
    with pytest.raises(AssertionError, match="fixture retained"):
        _require_run_identity_before_cleanup(
            attempt={"sent": True}, workflow_instance_id=None
        )
    _require_run_identity_before_cleanup(
        attempt={"sent": False}, workflow_instance_id=None
    )


def test_replay_marks_dispatch_before_response_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attempted = False

    def mark_attempted() -> None:
        nonlocal attempted
        attempted = True

    def malformed_run(
        _repo_root: Path, _argv: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        return _result("wrong.action", data={})

    monkeypatch.setattr(
        "tests.live.test_runtime_control_surfaces.run_dsctl", malformed_run
    )
    with pytest.raises(AssertionError):
        _assert_replay_round(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            previous_run_times=1,
            action="rerun",
            on_dispatch_attempted=mark_attempted,
        )
    assert attempted


@pytest.mark.parametrize(
    ("version", "run_times"),
    [("3.2.2", 2), ("3.4.2", 1), ("3.4.3", 1)],
)
def test_execute_task_observation_separates_quiescence_from_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, version: str, run_times: int
) -> None:
    module = "tests.live.test_runtime_control_surfaces"
    created = {
        "id": 13,
        "name": "echo-task",
        "workflowInstanceId": 91,
        "state": "SUCCESS",
    }
    workflow = {
        "id": 91,
        "runTimes": run_times,
        "state": "SUCCESS",
        "commandType": "EXECUTE_TASK",
    }
    calls: list[list[str]] = []

    def poll(
        _repo_root: Path, argv: list[str], **_kwargs: object
    ) -> DsctlCommandResult:
        calls.append(argv)
        if argv[:2] == ["task-instance", "list"]:
            return _result("task-instance.list", data={"totalList": [created]})
        if argv[:2] == ["workflow-instance", "get"]:
            return _result("workflow-instance.get", data=workflow)
        return _result("task-instance.get", data=created)

    monkeypatch.setattr(f"{module}.wait_for_result", poll)
    task, final = _observe_execute_task_round_terminal(
        tmp_path,
        tmp_path / "unused.env",
        project="fixture-project",
        workflow_instance_id=91,
        prior_run_times=1,
        prior_task_id=12,
        prior_task_start_time=None,
        prior_task_end_time=None,
        version=version,
    )
    assert task["id"] == 13
    assert final["commandType"] == "EXECUTE_TASK"
    assert [argv[:2] for argv in calls] == [
        ["task-instance", "list"],
        ["workflow-instance", "get"],
        ["task-instance", "get"],
    ]
    assert all("--after-run-times" not in argv for argv in calls)

    created["state"] = "FAILURE"
    workflow["state"] = "FAILURE"
    task, final = _observe_execute_task_round_terminal(
        tmp_path,
        tmp_path / "unused.env",
        project="fixture-project",
        workflow_instance_id=91,
        prior_run_times=1,
        prior_task_id=12,
        prior_task_start_time=None,
        prior_task_end_time=None,
        version=version,
    )
    assert task["state"] == final["state"] == "FAILURE"

    workflow["commandType"] = "START_PROCESS"
    with pytest.raises(AssertionError):
        _observe_execute_task_round_terminal(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            prior_run_times=1,
            prior_task_id=12,
            prior_task_start_time=None,
            prior_task_end_time=None,
            version=version,
        )
    workflow["commandType"] = "EXECUTE_TASK"
    if version == "3.2.2":
        workflow["runTimes"] = 1
        with pytest.raises(AssertionError):
            _observe_execute_task_round_terminal(
                tmp_path,
                tmp_path / "unused.env",
                project="fixture-project",
                workflow_instance_id=91,
                prior_run_times=1,
                prior_task_id=12,
                prior_task_start_time=None,
                prior_task_end_time=None,
                version=version,
            )
        workflow["runTimes"] = 2

    created["id"] = 12
    with pytest.raises(AssertionError, match="one new task"):
        _observe_execute_task_round_terminal(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            prior_run_times=1,
            prior_task_id=12,
            prior_task_start_time=None,
            prior_task_end_time=None,
            version=version,
        )


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1"])
def test_execute_task_reused_id_requires_new_round_and_task_times(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, version: str
) -> None:
    previous_start = "2026-09-24 10:00:00"
    previous_end = "2026-09-24 10:00:01"
    task = {
        "id": 12,
        "name": "echo-task",
        "workflowInstanceId": 91,
        "state": "SUCCESS",
        "startTime": "2026-09-24 10:01:00",
        "endTime": "2026-09-24 10:01:01",
    }
    workflow = {
        "id": 91,
        "runTimes": 2,
        "state": "SUCCESS",
        "commandType": "EXECUTE_TASK",
    }

    def poll(
        _repo_root: Path, argv: list[str], **_kwargs: object
    ) -> DsctlCommandResult:
        if argv[:2] == ["task-instance", "list"]:
            return _result("task-instance.list", data={"totalList": [task]})
        if argv[:2] == ["workflow-instance", "get"]:
            return _result("workflow-instance.get", data=workflow)
        return _result("task-instance.get", data=task)

    monkeypatch.setattr(
        "tests.live.test_runtime_control_surfaces.wait_for_result", poll
    )

    def observe() -> tuple[dict[str, object], dict[str, object]]:
        return _observe_execute_task_round_terminal(
            tmp_path,
            tmp_path / "unused.env",
            project="fixture-project",
            workflow_instance_id=91,
            prior_run_times=1,
            prior_task_id=12,
            prior_task_start_time=previous_start,
            prior_task_end_time=previous_end,
            version=version,
        )

    observed_task, observed_workflow = observe()
    assert observed_task["id"] == 12
    assert observed_workflow["runTimes"] == 2

    task["startTime"] = previous_start
    task["endTime"] = previous_end
    with pytest.raises(AssertionError):
        observe()  # Old SUCCESS is not evidence of another task execution.

    task["startTime"] = "2026-09-24 10:01:00"
    task["endTime"] = None
    with pytest.raises(AssertionError):
        observe()

    task["endTime"] = "2026-09-24 10:01:01"
    workflow["runTimes"] = 1
    with pytest.raises(AssertionError):
        observe()


def test_running_cleanup_attempts_other_workflow_after_one_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    identities = iter((91, 92))
    watched: list[int] = []
    deleted: list[str] = []

    def start(
        _repo_root: Path,
        _env_file: Path,
        *,
        project: str,
        workflow: str,
        attempt: dict[str, bool],
    ) -> int:
        attempt["sent"] = True
        return next(identities)

    def command(
        _repo_root: Path, argv: list[str], **_kwargs: object
    ) -> DsctlCommandResult:
        action = ".".join(argv[:2])
        if argv[0] == "task-instance":
            data: dict[str, object] = {
                "requested": True,
                "taskInstance": {
                    "id": 41,
                    "workflowInstanceId": 91,
                    "name": "sleep-task",
                },
            }
        else:
            data = {"id": 92, "state": "STOP"}
        return _result(action, data=data)

    def task_rows(
        _repo_root: Path,
        _env_file: Path,
        *,
        project: str,
        workflow_instance_id: int,
    ) -> list[dict[str, object]]:
        assert project == "runtime-control-project"
        return [
            {
                "id": workflow_instance_id - 50,
                "workflowInstanceId": workflow_instance_id,
                "name": (
                    "sleep-task" if workflow_instance_id == 91 else "workflow-stop-task"
                ),
            }
        ]

    def watch(
        _repo_root: Path,
        _env_file: Path,
        *,
        project: str,
        workflow_instance_id: int,
    ) -> None:
        assert project == "runtime-control-project"
        watched.append(workflow_instance_id)
        if workflow_instance_id == 91:
            message = "first workflow has not become quiescent"
            raise AssertionError(message)

    monkeypatch.setattr(controls, "_run_workflow", start)
    monkeypatch.setattr(controls, "run_dsctl", command)
    monkeypatch.setattr(controls, "_wait_for_task_rows", task_rows)
    monkeypatch.setattr(
        controls, "_wait_for_task_state", lambda *args, **kwargs: {"state": "KILL"}
    )
    monkeypatch.setattr(
        controls, "_wait_for_workflow_state", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(controls, "_wait_for_cleanup_terminal", watch)
    monkeypatch.setattr(
        controls,
        "delete_workflow_eventually",
        lambda *args, **kwargs: deleted.append(kwargs["workflow"]),
    )
    monkeypatch.setattr(
        controls,
        "delete_project_eventually",
        lambda *args, **kwargs: deleted.append("project"),
    )
    with pytest.raises(ExceptionGroup) as caught:
        controls.test_etl_running_runtime_control_surfaces_round_trip(
            tmp_path, "3.4.3", tmp_path / "unused.env", lambda stem: stem, tmp_path
        )
    assert len(caught.value.exceptions) == 1
    assert "first workflow" in str(caught.value.exceptions[0])
    assert watched == [91, 92]
    assert deleted == ["runtime-stop-workflow"]


@pytest.mark.parametrize("version", sorted(TASK_STOP_TERM_FAILURE_VERSIONS))
def test_reviewed_term_exit_maps_task_and_workflow_stop_separately(
    version: str,
) -> None:
    assert _expected_stopped_task_states(version) == frozenset({"KILL", "FAILURE"})
    assert _expected_workflow_stop_state(version, "FAILURE") == "FAILURE"
    assert _expected_workflow_instance_stop_state(version, "FAILURE") == "STOP"
