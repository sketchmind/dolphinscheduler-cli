from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Literal

import pytest
import yaml
from tests.live import journey_support
from tests.live.runtime_journey import emitted_marker, run_runtime_journey
from tests.live.support import (
    DsctlCommandResult,
    TaskDefinitionCleanupInvocation,
    require_list,
    require_mapping,
    require_text_value,
)


@pytest.mark.parametrize(
    "line",
    [
        "\tMARKER\n",
        "2026-09-20 22:31:44.918 INFO  -  -> MARKER\n",
        "[INFO] 2026-09-20 - [taskAppId=x] -> MARKER\n",
    ],
)
def test_emitted_marker_supports_worker_log_prefixes(line: str) -> None:
    assert emitted_marker(line, "MARKER")
    assert not emitted_marker('printf "%s\\n" "MARKER"\n', "MARKER")
    assert not emitted_marker(
        '2026-09-20 INFO - Final script: printf "MARKER"\n', "MARKER"
    )


@pytest.mark.parametrize(
    ("ds_version", "failed_state", "expected_sync"),
    [
        ("2.0.0", "FAILURE", True),
        ("2.0.1", "FAILURE", True),
        ("2.0.2", "FAILURE", True),
        ("3.4.1", "FAILURE", False),
        ("3.4.1", "SUCCESS", False),
    ],
)
def test_runtime_journey_requires_real_failure_and_always_cleans(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    ds_version: str,
    failed_state: str,
    expected_sync: object,
) -> None:
    """A fake server catches stale success and records installed-only invocations."""
    assert type(expected_sync) is bool
    executable = tmp_path / "wheel" / "bin" / "dsctl"
    env_file = tmp_path / "private.env"
    project: dict[str, object] = {}
    workflow: dict[str, object] = {}
    definition_document: dict[str, object] = {}
    instance_document: dict[str, object] = {}
    definition_exports: list[dict[str, object]] = []
    round_number = 1
    state = "SUCCESS"
    calls: list[list[str]] = []

    def result(
        argv: list[str], data: object, *, error: bool = False
    ) -> DsctlCommandResult:
        action = argv[0] if argv[0] == "capabilities" else ".".join(argv[:2])
        payload: dict[str, object] = {
            "action": action,
            "ok": not error,
            "data": data,
            "resolved": {"selection": {"api_token": "never-persist-this"}},
        }
        if error:
            payload["error"] = {"type": "execution_failed"}
        return DsctlCommandResult(tuple(argv), int(error), "", "", payload)

    def invoke(_root: Path, argv: list[str], **kwargs: object) -> DsctlCommandResult:  # noqa: C901
        nonlocal definition_document, instance_document, state, round_number
        assert kwargs["executable"] == executable
        assert kwargs["env_file"] == env_file
        calls.append(argv)
        family, action = argv[:2]
        if family == "capabilities":
            return result(
                argv,
                {
                    "ds": {"selected_version": ds_version},
                    "capability": {"action": argv[2], "availability": "supported"},
                },
            )
        if family == "template":
            template = (
                {"workflow": {}, "tasks": []}
                if action == "workflow"
                else {"type": "SHELL", "retry": {"times": 0, "interval": 0}}
            )
            return result(argv, {"yaml": yaml.safe_dump(template)})
        if family == "lint":
            return result(argv, {})
        if family == "project":
            if action == "create":
                project.update(
                    name=argv[argv.index("--name") + 1],
                    description=argv[argv.index("--description") + 1],
                    code=7,
                )
                return result(argv, dict(project))
            if action == "delete":
                project.clear()
            return result(argv, [dict(project)] if project else [])
        if family == "workflow":
            if action == "create":
                definition_document = yaml.safe_load(
                    (tmp_path / "workflow.yaml").read_text()
                )
                workflow.update(
                    name=require_mapping(
                        definition_document["workflow"], label="workflow"
                    )["name"],
                    code=8,
                    releaseState="OFFLINE",
                )
            if action in {"online", "offline"}:
                workflow["releaseState"] = action.upper()
            if action == "list":
                return result(argv, [dict(workflow)] if workflow else [])
            if action == "run":
                instance_document = deepcopy(definition_document)
                return result(
                    argv,
                    {
                        "accepted": True,
                        "instanceResolution": "resolved",
                        "workflowInstanceIds": [9],
                    },
                )
            if action == "delete":
                workflow.clear()
            return result(argv, {})
        if family == "schedule":
            return result(argv, [])
        if family == "workflow-instance":
            if action == "edit":
                patch = yaml.safe_load(
                    Path(argv[argv.index("--patch") + 1]).read_text()
                )
                task = require_mapping(
                    require_list(instance_document["tasks"], label="tasks")[1],
                    label="task",
                )
                task["command"] = patch["patch"]["tasks"]["update"][0]["set"]["command"]
                if "--sync-definition" in argv:
                    definition_document = deepcopy(instance_document)
            if action in {"rerun", "recover-failed"}:
                round_number += 1
                state = failed_state if action == "rerun" else "SUCCESS"
            data = {"id": 9, "state": state, "runTimes": round_number}
            if action == "watch" and "--after-run-times" in argv:
                assert (
                    int(argv[argv.index("--after-run-times") + 1]) == round_number - 1
                )
            return result(
                argv,
                [data] if action == "list" else data,
                error="--exit-status" in argv,
            )
        if family == "task-instance":
            if action == "list":
                return result(
                    argv,
                    [
                        {"id": 10, "name": "extract", "state": "SUCCESS"},
                        {"id": 11, "name": "load", "state": state},
                    ],
                )
            task = require_mapping(
                require_list(instance_document["tasks"], label="tasks")[
                    0 if argv[2] == "10" else 1
                ],
                label="task",
            )
            marker = require_text_value(task["command"], label="command").split('"')[3]
            return result(argv, {"text": f"worker output:\n\t{marker}\n"})
        pytest.fail(f"unexpected command: {family}.{action}")

    def export(_root: Path, argv: list[str], **kwargs: object) -> DsctlCommandResult:
        assert kwargs["executable"] == executable
        document = definition_document if argv[0] == "workflow" else instance_document
        if argv[0] == "workflow":
            definition_exports.append(deepcopy(document))
        body = yaml.safe_dump(document)
        return DsctlCommandResult(tuple(argv), 0, body, "", {})

    def private_cleanup(
        _root: Path, **kwargs: object
    ) -> TaskDefinitionCleanupInvocation:
        operation: Literal["prove", "cleanup"]
        if kwargs["operation"] == "cleanup":
            operation = "cleanup"
        else:
            assert kwargs["operation"] == "prove"
            operation = "prove"
        cleanup = operation == "cleanup"
        return TaskDefinitionCleanupInvocation(
            operation=operation,
            ds_version=require_text_value(kwargs["ds_version"], label="version"),
            observed=2,
            released=2 if cleanup else 0,
            deleted=2 if cleanup else 0,
            remaining=0 if cleanup else 2,
            remote_mutations=4 if cleanup else 0,
        )

    monkeypatch.setattr(journey_support, "run_dsctl", invoke)
    monkeypatch.setattr(journey_support, "run_dsctl_raw", export)
    monkeypatch.setattr(journey_support, "run_task_definition_cleanup", private_cleanup)

    def run() -> dict[str, object]:
        return run_runtime_journey(
            repo_root=tmp_path,
            executable=executable,
            env_file=env_file,
            ds_version=ds_version,
            workspace=tmp_path,
            prefix="unique-test",
        )

    if failed_state == "SUCCESS":
        with pytest.raises(AssertionError, match="exit 23 did not fail"):
            run()
    else:
        evidence = run()
        assert evidence["states"] == ["SUCCESS", "FAILURE", "SUCCESS"]
        assert evidence["run_times"] == [1, 2, 3]
        assert evidence["definition_sync_required"] is expected_sync
        assert evidence["final_definition_original"] is True
    edit_calls = [argv for argv in calls if argv[:2] == ["workflow-instance", "edit"]]
    assert len(edit_calls) == (1 if failed_state == "SUCCESS" else 2)
    assert all(("--sync-definition" in argv) is expected_sync for argv in edit_calls)
    assert not project
    assert not workflow
    assert any(argv[:2] == ["workflow", "offline"] for argv in calls)
    original = yaml.safe_load((tmp_path / "workflow.yaml").read_text())
    assert definition_exports
    assert definition_exports[-1] == original
    if expected_sync:
        injected_tasks = require_list(
            definition_exports[0]["tasks"], label="injected definition tasks"
        )
        injected_load = require_mapping(injected_tasks[1], label="injected load")
        assert "intentional-exit23" in require_text_value(
            injected_load["command"], label="injected command"
        )
    else:
        assert all(document == original for document in definition_exports)
    saved = (tmp_path / "journey-evidence.json").read_text()
    assert "never-persist-this" not in saved
    assert json.loads(saved)["cleanup"]["projects_remaining"] == 0
