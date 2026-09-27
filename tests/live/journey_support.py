"""Small installed-wheel journey helpers; all business calls use the public CLI."""

from __future__ import annotations

import hashlib
import json
import time
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Literal

import yaml

from dsctl.generated.task_definition_cleanup_profiles import (
    FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS,
)
from tests.live.support import (
    DsctlCommandResult,
    require_list,
    require_mapping,
    require_text_value,
    run_dsctl,
    run_dsctl_raw,
    run_task_definition_cleanup,
)

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType


def positive_id(value: object) -> int:
    """Keep native ids and codes distinct and reject coercion."""
    assert type(value) is int, "missing integer native identity"
    assert value > 0, "non-positive native identity"
    return value


@dataclass
class Journey:
    repo_root: Path
    executable: Path
    env_file: Path
    ds_version: str
    workspace: Path
    prefix: str
    schedule_ids: set[int] = field(default_factory=set)
    trace: list[dict[str, object]] = field(default_factory=list)
    cleanup_evidence: dict[str, object] = field(default_factory=dict)
    project_identity: int | None = None
    workflow_identity: int | None = None
    _project_attempted: bool = False
    _workflow_attempted: bool = False
    _workflow_run_unresolved: bool = False
    _workflow_run_instances: set[int] = field(default_factory=set)
    _capabilities: dict[str, bool] = field(default_factory=dict)

    @property
    def run_id(self) -> str:
        return hashlib.sha256(self.prefix.encode()).hexdigest()[:16]

    @property
    def project(self) -> str:
        return f"dsctl-journey-{self.ds_version.replace('.', '-')}-{self.run_id}"

    @property
    def workflow(self) -> str:
        # The wheel's private cleanup owns this exact narrow fixture lineage.
        return f"dsctl-full-{self.ds_version.replace('.', '-')}-{self.run_id}"

    @property
    def identity_key(self) -> str:
        return "id" if self.ds_version == "1.3.9" else "code"

    def __enter__(self) -> Journey:
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.workspace.chmod(0o700)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.cleanup()
        except Exception as cleanup_error:
            self.cleanup_evidence["error_type"] = type(cleanup_error).__name__
            if exc is not None:
                msg = "journey failed and owned cleanup failed"
                raise ExceptionGroup(
                    msg,
                    [exc, cleanup_error]
                    if isinstance(exc, Exception)
                    else [cleanup_error],
                ) from None
            raise
        finally:
            (self.workspace / "journey-evidence.json").write_text(
                json.dumps(
                    {
                        "ds_version": self.ds_version,
                        "trace": self.trace,
                        "cleanup": self.cleanup_evidence,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

    def call(
        self, argv: list[str], *, timeout_seconds: float = 60.0
    ) -> DsctlCommandResult:
        workflow_run = argv[:2] == ["workflow", "run"]
        if workflow_run:
            # The subprocess can time out after the server accepted the command.
            # Keep cleanup fail-closed until an owned instance is observable.
            self._workflow_run_unresolved = True
        result = run_dsctl(
            self.repo_root,
            argv,
            env_file=self.env_file,
            executable=self.executable,
            timeout_seconds=timeout_seconds,
        )
        if workflow_run:
            self._record_workflow_run_result(result)
        # Deliberately exclude argv, selection, raw output and server messages.
        error = result.payload.get("error")
        self.trace.append(
            {
                "action": result.payload.get("action", ".".join(argv[:2])),
                "exit_code": result.exit_code,
                "ok": result.payload.get("ok") is True,
                "error_type": error.get("type") if isinstance(error, dict) else None,
            }
        )
        commands = self.workspace / "commands.jsonl"
        commands.touch(mode=0o600, exist_ok=True)
        commands.chmod(0o600)
        with commands.open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps({"command": argv, "payload": result.payload}) + "\n"
            )
        trace_path = self.workspace / "trace.json.tmp"
        trace_path.write_text(json.dumps(self.trace, indent=2) + "\n", encoding="utf-8")
        trace_path.replace(self.workspace / "trace.json")
        if result.exit_code != 0:
            diagnostic = self.workspace / "private-command-failure.json"
            diagnostic.touch(mode=0o600, exist_ok=True)
            diagnostic.chmod(0o600)
            diagnostic.write_text(
                json.dumps({"command": argv, "payload": result.payload}, indent=2)
                + "\n",
                encoding="utf-8",
            )
        return result

    def ok(
        self, argv: list[str], *, timeout_seconds: float = 60.0
    ) -> dict[str, object]:
        result = self.call(argv, timeout_seconds=timeout_seconds)
        error = result.payload.get("error")
        error_type = error.get("type") if isinstance(error, dict) else None
        assert result.payload.get("ok") is True, (
            f"{'.'.join(argv[:2])} failed: exit={result.exit_code}, type={error_type}"
        )
        assert result.exit_code == 0
        expected = (
            argv[0] if argv[0] in {"capabilities", "schema"} else ".".join(argv[:2])
        )
        assert result.payload.get("action") == expected, "CLI action mismatch"
        return result.payload

    def data(
        self, argv: list[str], *, timeout_seconds: float = 60.0
    ) -> dict[str, object]:
        return require_mapping(
            self.ok(argv, timeout_seconds=timeout_seconds).get("data"),
            label="journey data",
        )

    def export_instance(self, instance_id: int) -> dict[str, object]:
        return self._export("workflow-instance", str(instance_id))

    def _export(self, family: str, selector: str) -> dict[str, object]:
        result = run_dsctl_raw(
            self.repo_root,
            [
                family,
                "export",
                selector,
                "--project",
                self.project,
            ],
            env_file=self.env_file,
            executable=self.executable,
        )
        self.trace.append(
            {
                "action": f"{family}.export",
                "exit_code": result.exit_code,
                "ok": result.exit_code == 0,
            }
        )
        assert result.exit_code == 0, "workflow export failed"
        return require_mapping(yaml.safe_load(result.stdout), label="workflow export")

    def rows(self, argv: list[str]) -> list[dict[str, object]]:
        data = self.ok(argv).get("data")
        if isinstance(data, dict):
            values = require_list(data.get("totalList"), label="journey page")
            assert data.get("total") == len(values), "incomplete journey inventory"
        else:
            values = require_list(data, label="journey rows")
        return [require_mapping(row, label="journey row") for row in values]

    def capability(self, action: str) -> bool:
        if action not in self._capabilities:
            data = self.data(["capabilities", "--action", action])
            ds = require_mapping(data.get("ds"), label="exact capability profile")
            assert ds.get("selected_version") == self.ds_version, (
                "profile version mismatch"
            )
            capability = require_mapping(data.get("capability"), label="capability")
            assert capability.get("action") == action
            status = capability.get("availability")
            assert status in {"supported", "unsupported"}, "unresolved exact capability"
            self._capabilities[action] = status == "supported"
        return self._capabilities[action]

    def script(self, task: str) -> str:
        assert task in {"extract", "load"}
        return f'printf "%s\\n" "{self.run_id}-{task}"\n'

    def create_workflow(self) -> dict[str, object]:
        """Author from this wheel's exact templates, retaining cleanup ownership."""
        for action in (
            "project.create",
            "workflow.create",
            "workflow.run",
            "workflow-instance.watch",
            "workflow-instance.stop",
        ):
            assert self.capability(action), f"required action absent: {action}"
        workflow_template = self.data(["template", "workflow"])
        task_template = self.data(["template", "task", "SHELL"])
        document = require_mapping(
            yaml.safe_load(
                require_text_value(
                    workflow_template.get("yaml"), label="workflow template"
                )
            ),
            label="workflow template",
        )
        task = require_mapping(
            yaml.safe_load(
                require_text_value(task_template.get("yaml"), label="task template")
            ),
            label="task template",
        )
        header = require_mapping(document.get("workflow"), label="workflow header")
        header.update(name=self.workflow, project=self.project, release_state="OFFLINE")
        header.pop("global_params", None)
        tasks = []
        for name in ("extract", "load"):
            node = deepcopy(task)
            node.update(
                name=name,
                command=self.script(name),
                depends_on=[] if name == "extract" else ["extract"],
                description=f"dsctl-conformance-owner:{self.run_id};resource=task;name={name}",
            )
            tasks.append(node)
        document["tasks"] = tasks
        spec = self.workspace / "workflow.yaml"
        spec.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        self.ok(["lint", "workflow", str(spec)])
        assert not [
            row
            for row in self.rows(["project", "list", "--search", self.project, "--all"])
            if row.get("name") == self.project
        ], "owned project name collision"
        self._project_attempted = True
        project = self.data(
            [
                "project",
                "create",
                "--name",
                self.project,
                "--description",
                f"dsctl-journey-owner:{self.run_id}",
            ]
        )
        self.project_identity = positive_id(project.get(self.identity_key))
        self._workflow_attempted = True
        self.ok(["workflow", "create", "--file", str(spec), "--project", self.project])
        workflows = self.rows(["workflow", "list", "--project", self.project, "--all"])
        assert len(workflows) == 1
        assert workflows[0].get("name") == self.workflow
        self.workflow_identity = positive_id(workflows[0].get(self.identity_key))
        if self.ds_version in FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS:
            self.cleanup_evidence["initial_definition_proof"] = self._private_cleanup(
                "prove"
            )
        return workflows[0]

    def resolve_run(self, data: dict[str, object]) -> int:
        assert data.get("accepted") is True, "run was not accepted"
        identities = require_list(
            data.get("workflowInstanceIds"), label="run instances"
        )
        if data.get("instanceResolution") == "resolved":
            assert len(identities) == 1
            instance_id = positive_id(identities[0])
            self._resolve_workflow_run(instance_id)
            return instance_id
        assert not identities
        assert data.get("instanceResolution") in {
            "pending",
            "unavailable",
        }
        trigger = data.get("triggerCode")
        deadline = time.monotonic() + 90
        while True:
            args = ["workflow-instance", "list", "--project", self.project]
            if trigger is not None:
                trigger_code = positive_id(trigger)
                args.extend(["--trigger-code", str(trigger_code)])
                rows = self._trigger_resolution_rows(args, trigger_code=trigger_code)
            else:
                args.extend(["--workflow", self.workflow, "--all"])
                rows = self.rows(args)
            if rows:
                # Exclusive, freshly created workflow has exactly one submission.
                assert len(rows) == 1, "ambiguous accepted run identity"
                instance_id = positive_id(rows[0].get("id"))
                self._resolve_workflow_run(instance_id)
                return instance_id
            assert time.monotonic() < deadline, "accepted run never materialized"
            time.sleep(2)

    def _trigger_resolution_rows(
        self,
        argv: list[str],
        *,
        trigger_code: int,
    ) -> list[dict[str, object]]:
        """Validate the dedicated trigger lookup envelope without page fields."""
        data = self.data(argv)
        assert data.get("triggerCode") == trigger_code, "trigger query changed identity"
        resolution = data.get("instanceResolution")
        assert resolution in {"pending", "resolved"}, (
            "trigger query omitted its instance resolution"
        )
        values = require_list(data.get("totalList"), label="workflow trigger instances")
        rows = [
            require_mapping(row, label="workflow trigger instance") for row in values
        ]
        if resolution == "pending":
            assert not rows, "pending trigger query returned instance rows"
        else:
            assert len(rows) == 1, (
                "resolved trigger query requires exactly one instance"
            )
        return rows

    def watch(
        self, instance_id: int, *, after_run_times: int | None = None
    ) -> dict[str, object]:
        args = [
            "workflow-instance",
            "watch",
            str(instance_id),
            "--project",
            self.project,
            "--interval-seconds",
            "2",
            "--timeout-seconds",
            "120",
        ]
        if after_run_times is not None:
            args.extend(["--after-run-times", str(after_run_times)])
        return self.data(args, timeout_seconds=140)

    def cleanup(self) -> None:
        """Stop exclusive instances before removing owned schedules and definitions."""
        if not self._project_attempted:
            return
        projects = [
            row
            for row in self.rows(["project", "list", "--search", self.project, "--all"])
            if row.get("name") == self.project
        ]
        if not projects:
            self.cleanup_evidence.update(projects_remaining=0)
            return
        assert len(projects) == 1, "project cleanup ownership mismatch"
        assert projects[0].get("description") == f"dsctl-journey-owner:{self.run_id}", (
            "project cleanup ownership mismatch"
        )
        identity = positive_id(projects[0].get(self.identity_key))
        assert self.project_identity in {None, identity}
        self.project_identity = identity
        workflows = self.rows(["workflow", "list", "--project", self.project, "--all"])
        assert all(row.get("name") == self.workflow for row in workflows), (
            "foreign workflow in owned project"
        )
        self._cleanup_schedules(has_workflow=bool(workflows))
        self._stop_instances()
        cleanup_profile = (
            self.ds_version in FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
        )
        for workflow in workflows:
            identity = positive_id(workflow.get(self.identity_key))
            assert self.workflow_identity in {None, identity}, (
                "workflow identity changed"
            )
            self.workflow_identity = identity
            if workflow.get("releaseState") == "ONLINE":
                self.ok(
                    ["workflow", "offline", self.workflow, "--project", self.project]
                )
            self._prove_current_definition()
            self.ok(
                [
                    "workflow",
                    "delete",
                    self.workflow,
                    "--project",
                    self.project,
                    "--force",
                ]
            )
        assert not self.rows(
            ["workflow", "list", "--project", self.project, "--all"]
        ), "workflow residue"
        if cleanup_profile:
            self.cleanup_evidence["harness_private_task_cleanup"] = (
                self._private_cleanup("cleanup")
            )
        self.ok(["project", "delete", self.project, "--force"])
        assert not [
            row
            for row in self.rows(["project", "list", "--search", self.project, "--all"])
            if row.get("name") == self.project
        ], "project residue"
        self.cleanup_evidence.update(
            projects_remaining=0,
            workflows_remaining=0,
            active_instances_remaining=0,
            schedules_remaining=0,
        )

    def _prove_current_definition(self) -> None:
        # Instance edits legitimately add log/history versions even without sync.
        # The current definition must still be the exact original owned graph.
        document = self._export("workflow", self.workflow)
        tasks = [
            require_mapping(row, label="owned task")
            for row in require_list(
                document.get("tasks"), label="owned definition tasks"
            )
        ]
        assert len(tasks) == 2
        assert {task.get("name") for task in tasks} == {"extract", "load"}
        for task in tasks:
            name = require_text_value(task.get("name"), label="owned task name")
            command = task.get("command")
            if command is None:
                command = require_mapping(
                    task.get("task_params"), label="owned task params"
                ).get("rawScript")
            assert task.get("type") == "SHELL"
            assert command == self.script(name), "current definition script changed"
            assert (
                task.get("description")
                == f"dsctl-conformance-owner:{self.run_id};resource=task;name={name}"
            )
            assert task.get("depends_on", []) == (
                [] if name == "extract" else ["extract"]
            ), "current definition topology changed"
        self.cleanup_evidence["current_definition_original"] = True

    def _cleanup_schedules(self, *, has_workflow: bool) -> None:
        # Discover schedules even when create timed out before receipt registration.
        if has_workflow and self.capability("schedule.list"):
            current_ids: set[int] = set()
            for row in self.rows(
                [
                    "schedule",
                    "list",
                    "--project",
                    self.project,
                    "--workflow",
                    self.workflow,
                    "--all",
                ]
            ):
                current_ids.add(positive_id(row.get("id")))
            # A delete may have committed before its response was lost.
            self.schedule_ids = current_ids
        for schedule_id in sorted(self.schedule_ids):
            current = self.data(
                ["schedule", "get", str(schedule_id), "--project", self.project]
            )
            if current.get("releaseState") == "ONLINE":
                self.ok(
                    ["schedule", "offline", str(schedule_id), "--project", self.project]
                )
            self.ok(
                [
                    "schedule",
                    "delete",
                    str(schedule_id),
                    "--project",
                    self.project,
                    "--force",
                ]
            )

    def _stop_instances(self) -> None:
        self._reconcile_uncertain_workflow_run()
        deadline = time.monotonic() + 120
        while True:
            instances = self.rows(
                ["workflow-instance", "list", "--project", self.project, "--all"]
            )
            pending = [
                row
                for row in instances
                if row.get("state") not in {"SUCCESS", "FAILURE", "STOP", "PAUSE"}
            ]
            if not pending:
                break
            for row in pending:
                if row.get("state") in {
                    "RUNNING_EXECUTION",
                    "READY_PAUSE",
                    "READY_STOP",
                    "SERIAL_WAIT",
                }:
                    result = self.call(
                        [
                            "workflow-instance",
                            "stop",
                            str(positive_id(row.get("id"))),
                            "--project",
                            self.project,
                        ]
                    )
                    error = result.payload.get("error")
                    assert result.payload.get("ok") is True or (
                        isinstance(error, dict) and error.get("type") == "invalid_state"
                    ), "owned instance stop failed"
            assert time.monotonic() < deadline, (
                "owned runtime did not quiesce; refusing definition deletion"
            )
            time.sleep(2)

    def _record_workflow_run_result(self, result: DsctlCommandResult) -> None:
        if result.exit_code != 0 or result.payload.get("ok") is not True:
            return
        data = result.payload.get("data")
        if not isinstance(data, dict) or data.get("accepted") is not True:
            return
        identities = data.get("workflowInstanceIds")
        if not isinstance(identities, list):
            return
        resolved = [positive_id(value) for value in identities]
        if len(resolved) == 1:
            self._resolve_workflow_run(resolved[0])

    def _resolve_workflow_run(self, instance_id: int) -> None:
        self._workflow_run_instances.add(instance_id)
        self._workflow_run_unresolved = False

    def _reconcile_uncertain_workflow_run(self) -> None:
        if not self._workflow_run_unresolved and not self._workflow_run_instances:
            return
        deadline = time.monotonic() + 120
        while True:
            instances = self.rows(
                ["workflow-instance", "list", "--project", self.project, "--all"]
            )
            observed = {positive_id(row.get("id")) for row in instances}
            if self._workflow_run_unresolved and observed:
                self._workflow_run_instances.update(observed)
                self._workflow_run_unresolved = False
            if (
                not self._workflow_run_unresolved
                and self._workflow_run_instances <= observed
            ):
                self.cleanup_evidence["uncertain_workflow_run_reconciled"] = True
                return
            if time.monotonic() >= deadline:
                self.cleanup_evidence["uncertain_workflow_run_reconciled"] = False
                message = (
                    "workflow run outcome is still unresolved; refusing workflow "
                    "and project deletion"
                )
                raise AssertionError(message)
            time.sleep(2)

    def _private_cleanup(
        self, operation: Literal["prove", "cleanup"]
    ) -> dict[str, object]:
        assert self.project_identity is not None
        result = run_task_definition_cleanup(
            self.repo_root,
            python=self.executable.parent / "python",
            env_file=self.env_file,
            ds_version=self.ds_version,
            operation=operation,
            project_code=self.project_identity,
            workflow_code=self.workflow_identity,
            run_id=self.run_id,
        )
        if operation == "cleanup":
            assert result.remaining == 0, "task-definition residue"
        return asdict(result)
