"""Installed-wheel, same-process task.update stale-plan acceptance runner.

The exact-profile pytest wrapper validates ownership and wheel identity before
invoking this file with the wheel's isolated Python interpreter.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from copy import deepcopy
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, cast
from unittest.mock import patch

import dsctl
from dsctl.errors import ConflictError
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.services.runtime import open_task_definition_service_runtime
from dsctl.upstream.task_definitions import (
    TaskDefinitions,
    TaskSelector,
    TaskUpdateIntent,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def _fail(message: str) -> NoReturn:
    raise AssertionError(message)


def _command(task: Mapping[str, object]) -> str:
    params = task.get("taskParams")
    command = params.get("rawScript") if isinstance(params, dict) else None
    if not isinstance(command, str):
        message = "Owned SHELL task has no text rawScript"
        raise TypeError(message)
    return command


def _non_command_state(task: dict[str, object]) -> dict[str, object]:
    state = deepcopy(task)
    for key in ("version", "updateTime", "modifyBy"):
        state.pop(key, None)
    params = state.get("taskParams")
    if isinstance(params, dict):
        params.pop("rawScript", None)
    return state


def _require_cleanup_ownership(
    current: dict[str, object],
    *,
    task_code: int,
    task_name: str,
    baseline_non_command_state: dict[str, object],
) -> None:
    if (
        current.get("code") != task_code
        or current.get("name") != task_name
        or current.get("taskType") != "SHELL"
        or _non_command_state(current) != baseline_non_command_state
    ):
        _fail("Refusing cleanup after an unrelated task change")


def _restore_after_interference(
    definitions: TaskDefinitions,
    selector: TaskSelector,
    *,
    task_code: int,
    task_name: str,
    original: str,
    interfering: str,
    original_version: int,
    marker_version: int | None,
    baseline_non_command_state: dict[str, object],
) -> bool:
    current = cast("dict[str, object]", definitions.get(selector).view.to_data())
    _require_cleanup_ownership(
        current,
        task_code=task_code,
        task_name=task_name,
        baseline_non_command_state=baseline_non_command_state,
    )
    current_version = current.get("version")
    if current_version == original_version and _command(current) == original:
        return True
    if (
        type(current_version) is not int
        or current_version <= original_version
        or _command(current) != interfering
        or (marker_version is not None and current_version != marker_version)
    ):
        _fail("Refusing cleanup after an unrelated task change")
    applied_version = current_version
    restore = definitions.prepare_update(_intent(selector, original))
    outcome = definitions.apply(restore)
    readback = cast("dict[str, object]", definitions.get(selector).view.to_data())
    restored_version = readback.get("version")
    if type(restored_version) is not int:
        _fail("Owned task command restoration failed")
    if (
        not outcome.mutation_applied
        or readback.get("code") != task_code
        or readback.get("name") != task_name
        or readback.get("taskType") != "SHELL"
        or _command(readback) != original
        or _non_command_state(readback) != baseline_non_command_state
        or restored_version <= applied_version
    ):
        _fail("Owned task command restoration failed")
    return True


def _intent(selector: TaskSelector, command: str) -> TaskUpdateIntent:
    return TaskUpdateIntent(
        selector=selector,
        patch=WorkflowPatchTaskSetSpec.model_validate({"command": command}),
        requested_fields=("command",),
    )


def run(args: argparse.Namespace) -> dict[str, object]:  # noqa: C901
    module = Path(dsctl.__file__).resolve()
    venv = Path(sys.prefix).resolve()
    if not module.is_relative_to(venv):
        _fail("dsctl was not imported from the installed wheel venv")
    selector = TaskSelector(args.project, args.workflow, str(args.task_code))
    evidence: dict[str, object] = {
        "scope": "exact 3.4.2 owned SHELL task; installed wheel; live REST",
        "dsctl_distribution": version("dolphinscheduler-cli"),
        "module_from_wheel_venv": True,
        "prepare_object_reused": False,
        "interference_applied": False,
        "stale_apply_wire_calls": None,
        "stale_conflict": False,
        "wrong_write_absent": False,
        "restored": False,
    }
    with open_task_definition_service_runtime(env_file=str(args.env_file)) as runtime:
        if runtime.profile.ds_version != "3.4.2":
            _fail("Runner accepts exact DS 3.4.2 only")
        definitions = cast("TaskDefinitions", runtime.definitions)
        baseline = cast("dict[str, object]", definitions.get(selector).view.to_data())
        if (
            baseline.get("code") != args.task_code
            or baseline.get("name") != args.task_name
            or baseline.get("taskType") != "SHELL"
        ):
            _fail("Owned fixture identity or task type changed")
        original = _command(baseline)
        original_other_state = _non_command_state(baseline)
        if not original.strip():
            _fail("Owned fixture command is blank")
        original_version = baseline.get("version")
        if type(original_version) is not int:
            _fail("Owned fixture task version is invalid")
        intended = f'printf "%s\\n" "{args.marker}-stale-intended"\n'
        interfering = f'printf "%s\\n" "{args.marker}-interference"\n'
        stale = definitions.prepare_update(_intent(selector, intended))
        evidence["prepare_object_reused"] = True
        if stale.no_change or stale.request.method != "PUT":
            _fail("Stale candidate did not prepare a PUT")
        if (
            not stale.request.path.endswith("/with-upstream")
            or stale._wire_call is None
            or stale._whole_workflow_call is not None
        ):
            _fail("Exact 3.4.2 standalone native update route was not selected")
        marker_version: int | None = None
        interference_attempted = False
        try:
            concurrent = definitions.prepare_update(_intent(selector, interfering))
            interference_attempted = True
            outcome = definitions.apply(concurrent)
            if not outcome.mutation_applied:
                _fail("Legal interference was not applied")
            evidence["interference_applied"] = True
            changed = cast(
                "dict[str, object]", definitions.get(selector).view.to_data()
            )
            observed_version = changed.get("version")
            if (
                type(observed_version) is not int
                or observed_version <= original_version
                or _command(changed) != interfering
                or _non_command_state(changed) != original_other_state
            ):
                _fail("Interference readback did not advance exactly")
            marker_version = observed_version
            wire_type = type(definitions.wire)
            no_put_message = "stale PUT sent"
            with patch.object(
                wire_type, "apply_update", side_effect=AssertionError(no_put_message)
            ) as apply_wire:
                try:
                    definitions.apply(stale)
                except ConflictError as error:
                    if (
                        error.details.get("phase") != "stale_check"
                        or error.details.get("mutation_applied") is not False
                    ):
                        raise
                    evidence["stale_conflict"] = True
                else:
                    _fail("The stale prepared object was accepted")
                evidence["stale_apply_wire_calls"] = apply_wire.call_count
            after = cast("dict[str, object]", definitions.get(selector).view.to_data())
            evidence["wrong_write_absent"] = (
                after["version"] == marker_version
                and _command(after) == interfering
                and _non_command_state(after) == original_other_state
            )
            if not evidence["wrong_write_absent"]:
                _fail("Stale apply changed the owned task")
        finally:
            if interference_attempted:
                evidence["restored"] = _restore_after_interference(
                    definitions,
                    selector,
                    task_code=args.task_code,
                    task_name=args.task_name,
                    original=original,
                    interfering=interfering,
                    original_version=original_version,
                    marker_version=marker_version,
                    baseline_non_command_state=original_other_state,
                )
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--task-code", type=int, required=True)
    parser.add_argument("--task-name", required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    args.output.parent.chmod(0o700)
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"status": "passed", "evidence": str(args.output)}))


if __name__ == "__main__":
    main()
