from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
import yaml

from tests.live.exact_profile_gate import (
    CleanupAttestation,
    ExactProfileFixture,
    ExactProfileGateConfig,
    ExternalTaskCleanupAttestation,
    OperationTraceEntry,
    identity_hmac,
    inspect_installed_exact_profile,
    load_exact_profile_gate_config,
    write_exact_profile_evidence,
)
from tests.live.support import (
    DsctlCommandResult,
    create_live_run_prefix,
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
    run_dsctl_raw,
)
from tests.request_assertions import first_dry_run_request

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = [
    pytest.mark.live,
    pytest.mark.live_developer,
    pytest.mark.live_exact_profile,
    pytest.mark.destructive,
]

_CONFIRMED_CREATE_REJECTION_TYPES = frozenset(
    {
        "config_error",
        "confirmation_required",
        "conflict",
        "invalid_state",
        "not_found",
        "permission_denied",
        "resolution_error",
        "unsupported_feature",
        "user_input_error",
    }
)
_RETRYABLE_CLEANUP_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_TASK_READBACK_VOLATILE_FIELDS = frozenset({"modifyBy", "updateTime", "version"})
_DOCTOR_READINESS_ATTEMPTS = 3
_DOCTOR_READINESS_INTERVAL_SECONDS = 10.0


@dataclass(frozen=True)
class _TaskGateSnapshot:
    command: str
    version: int
    non_owned_state: str
    topology_state: str


def _doctor_checks_ready(result: DsctlCommandResult) -> bool:
    """Return whether one read-only doctor result proves API and identity readiness."""
    if result.exit_code != 0 or result.payload.get("ok") is not True:
        return False
    data = result.payload.get("data")
    if not isinstance(data, dict):
        return False
    raw_checks = data.get("checks")
    if not isinstance(raw_checks, list):
        return False
    statuses = {
        check.get("name"): check.get("status")
        for check in raw_checks
        if isinstance(check, dict)
    }
    return statuses.get("api") == "ok" and statuses.get("current_user") == "ok"


def _wait_for_doctor(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    max_attempts: int = _DOCTOR_READINESS_ATTEMPTS,
    interval_seconds: float = _DOCTOR_READINESS_INTERVAL_SECONDS,
) -> tuple[DsctlCommandResult, int]:
    """Bound transient ingress readiness with read-only doctor invocations."""
    if max_attempts < 1:
        message = "doctor readiness requires at least one attempt"
        raise ValueError(message)
    if interval_seconds < 0:
        message = "doctor readiness interval must be non-negative"
        raise ValueError(message)

    result: DsctlCommandResult | None = None
    for attempt in range(1, max_attempts + 1):
        result = invoke(["doctor"])
        if _doctor_checks_ready(result) or attempt == max_attempts:
            return result, attempt
        if interval_seconds:
            time.sleep(interval_seconds)

    message = "unreachable doctor readiness loop"
    raise AssertionError(message)


@pytest.fixture(scope="session")
def exact_profile_gate_config() -> ExactProfileGateConfig:
    if not os.environ.get("DS_LIVE_EXACT_ENV_FILE", "").strip():
        pytest.skip(
            "Set the DS_LIVE_EXACT_* inputs documented in live-testing.md to run "
            "the exact DolphinScheduler profile gate."
        )
    return load_exact_profile_gate_config(os.environ)


def test_exact_profile_installed_wheel_gate(
    live_repo_root: Path,
    exact_profile_gate_config: ExactProfileGateConfig,
) -> None:
    config = exact_profile_gate_config
    operation_trace: list[OperationTraceEntry] = []
    installation = inspect_installed_exact_profile(config)

    def invoke(argv: list[str]) -> DsctlCommandResult:
        return run_dsctl(
            live_repo_root,
            argv,
            env_file=config.env_file,
            executable=config.executable,
        )

    def invoke_raw(argv: list[str]) -> DsctlCommandResult:
        return run_dsctl_raw(
            live_repo_root,
            argv,
            env_file=config.env_file,
            executable=config.executable,
        )

    version_payload = require_ok_payload(
        invoke(["version"]),
        expected_action="version",
        label="exact profile version preflight",
    )
    version_data = require_mapping(
        version_payload["data"],
        label="exact profile version data",
    )
    assert version_data["cli"] == installation.distribution_version
    assert version_data["ds"] == config.policy.ds_version
    assert version_data["selected_ds_version"] == config.policy.ds_version
    assert version_data["contract_version"] == config.policy.ds_version
    assert version_data["family"] == "workflow-3.3-plus"
    assert version_data["support_level"] == "experimental"
    assert installation.distribution_version == version_data["cli"]
    _record_success(
        operation_trace,
        "version",
        argv_shape="version",
        assertions=("selected-contract-and-family-matched",),
    )

    for action in config.policy.actions:
        capability_payload = require_ok_payload(
            invoke(["capabilities", "--action", action]),
            expected_action="capabilities",
            label=f"profile capability {action}",
        )
        capability_data = require_mapping(
            capability_payload["data"],
            label=f"profile capability {action} data",
        )
        capability = require_mapping(
            capability_data["capability"],
            label=f"profile capability {action} fact",
        )
        assert capability == {
            "action": action,
            "availability": "supported",
            "verification": "live_smoke",
        }
        _record_success(
            operation_trace,
            "capabilities",
            argv_shape="capabilities --action <action>",
            assertions=(f"{action}-supported-live-smoke",),
        )

    doctor_result, doctor_attempts = _wait_for_doctor(invoke)
    doctor_payload = require_ok_payload(
        doctor_result,
        expected_action="doctor",
        label="exact profile doctor",
    )
    doctor_data = require_mapping(
        doctor_payload["data"],
        label="exact profile doctor data",
    )
    checks = {
        str(check["name"]): check
        for value in require_list(doctor_data["checks"], label="doctor checks")
        for check in [require_mapping(value, label="doctor check")]
    }
    assert checks["api"]["status"] == "ok"
    assert checks["current_user"]["status"] == "ok"
    current_user_details = require_mapping(
        checks["current_user"]["details"],
        label="exact profile current-user details",
    )
    current_user_name = current_user_details.get("userName")
    assert isinstance(current_user_name, str)
    assert current_user_name
    assert (
        identity_hmac(
            current_user_name,
            key=config.attestation_key,
        )
        == config.cluster.principal_hmac_sha256
    )
    assert current_user_details.get("userType") == "GENERAL_USER"
    _record_success(
        operation_trace,
        "doctor",
        argv_shape="doctor",
        assertions=(
            "api-health-ok",
            "current-user-ok",
            "general-user-confirmed",
            "principal-hmac-matched",
            f"readiness-attempts-{doctor_attempts}",
        ),
    )

    _verify_workflow_reads(
        invoke,
        invoke_raw=invoke_raw,
        config=config,
        operation_trace=operation_trace,
    )
    task_cleanup = _verify_task_definition_round_trip(
        invoke,
        config=config,
        operation_trace=operation_trace,
    )
    cleanup = _verify_project_lifecycle(
        invoke,
        external_task=task_cleanup,
        operation_trace=operation_trace,
    )

    write_exact_profile_evidence(
        config,
        version_data=version_data,
        installation=installation,
        operation_trace=operation_trace,
        cleanup=cleanup,
    )


def _verify_workflow_reads(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    invoke_raw: Callable[[list[str]], DsctlCommandResult],
    config: ExactProfileGateConfig,
    operation_trace: list[OperationTraceEntry],
) -> None:
    list_payload = require_ok_payload(
        invoke(
            [
                "workflow",
                "list",
                "--project",
                config.fixture.project_name,
                "--search",
                config.fixture.workflow_name,
                "--page-size",
                "20",
            ]
        ),
        expected_action="workflow.list",
        label="exact profile workflow list",
    )
    list_data = require_mapping(
        list_payload["data"],
        label="exact profile workflow list data",
    )
    rows = [
        require_mapping(item, label="exact profile workflow row")
        for item in require_list(
            list_data["totalList"],
            label="exact profile workflow rows",
        )
    ]
    matching = [
        row
        for row in rows
        if row.get("name") == config.fixture.workflow_name
        and row.get("code") == config.fixture.workflow_code
    ]
    assert len(matching) == 1
    workflow_code = matching[0].get("code")
    assert isinstance(workflow_code, int)
    assert not isinstance(workflow_code, bool)
    assert workflow_code == config.fixture.workflow_code
    _record_success(
        operation_trace,
        "workflow.list",
        argv_shape="workflow list --project <project-name> --search <workflow-name>",
        selector_kind="project-name+workflow-search",
        assertions=("native-workflow-code-matched", "pagination-search-matched"),
    )

    for selector, selector_kind in (
        (config.fixture.workflow_name, "workflow-name"),
        (str(workflow_code), "workflow-code"),
    ):
        get_payload = require_ok_payload(
            invoke(
                [
                    "workflow",
                    "get",
                    selector,
                    "--project",
                    config.fixture.project_name,
                ]
            ),
            expected_action="workflow.get",
            label=f"exact profile workflow get by {selector_kind}",
        )
        get_data = require_mapping(
            get_payload["data"],
            label="exact profile workflow get data",
        )
        assert get_data["code"] == workflow_code
        assert get_data["name"] == config.fixture.workflow_name
        assert get_data["releaseState"] == config.fixture.workflow_release_state
        schedule = require_mapping(
            get_data["schedule"],
            label="exact profile attached schedule",
        )
        schedule_id = schedule.get("id")
        assert isinstance(schedule_id, int)
        assert not isinstance(schedule_id, bool)
        assert schedule_id == config.fixture.schedule_id
        _record_success(
            operation_trace,
            "workflow.get",
            argv_shape="workflow get <workflow> --project <project-name>",
            selector_kind=selector_kind,
            assertions=(
                "native-workflow-code-matched",
                "schedule-hydrated",
                "offline-fixture-confirmed",
            ),
        )

    _verify_workflow_inspection(
        invoke,
        invoke_raw=invoke_raw,
        config=config,
        workflow_code=workflow_code,
        operation_trace=operation_trace,
    )

    missing_code = config.fixture.workflow_code + 1_000_000_000_000
    missing_result = invoke(
        [
            "workflow",
            "get",
            str(missing_code),
            "--project",
            config.fixture.project_name,
        ]
    )
    require_error_payload(
        missing_result,
        expected_action="workflow.get",
        expected_type="not_found",
        label="exact profile missing workflow",
    )
    _record_error(
        operation_trace,
        "workflow.get",
        argv_shape="workflow get <missing-workflow-code> --project <project-name>",
        error_type="not_found",
        exit_code=missing_result.exit_code,
        selector_kind="workflow-code",
        assertions=("stable-not-found",),
    )


def _verify_workflow_inspection(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    invoke_raw: Callable[[list[str]], DsctlCommandResult],
    config: ExactProfileGateConfig,
    workflow_code: int,
    operation_trace: list[OperationTraceEntry],
) -> None:
    selector = [str(workflow_code), "--project", config.fixture.project_name]
    describe_payload = require_ok_payload(
        invoke(["workflow", "describe", *selector]),
        expected_action="workflow.describe",
        label="exact profile workflow describe",
    )
    description = require_mapping(
        describe_payload["data"],
        label="exact profile workflow describe data",
    )
    workflow = require_mapping(
        description["workflow"],
        label="exact profile described workflow",
    )
    tasks = [
        require_mapping(item, label="exact profile described task")
        for item in require_list(
            description["tasks"],
            label="exact profile described tasks",
        )
    ]
    relations = [
        require_mapping(item, label="exact profile described relation")
        for item in require_list(
            description["relations"],
            label="exact profile described relations",
        )
    ]
    assert workflow["code"] == workflow_code
    assert workflow["name"] == config.fixture.workflow_name
    task_codes = {
        _required_trace_int(task.get("code"), label="task code") for task in tasks
    }
    assert task_codes
    assert len(task_codes) == len(tasks)
    assert all(
        _required_trace_int(relation.get("postTaskCode"), label="post task code")
        in task_codes
        and (
            _required_trace_int(relation.get("preTaskCode"), label="pre task code")
            in task_codes
            or relation.get("preTaskCode") == 0
        )
        for relation in relations
    )
    schedule = require_mapping(
        workflow["schedule"],
        label="exact profile described schedule",
    )
    assert schedule["id"] == config.fixture.schedule_id
    _record_success(
        operation_trace,
        "workflow.describe",
        argv_shape="workflow describe <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=(
            "dag-task-set-nonempty",
            "dag-relations-valid",
            "schedule-hydrated",
        ),
    )

    digest_payload = require_ok_payload(
        invoke(["workflow", "digest", *selector]),
        expected_action="workflow.digest",
        label="exact profile workflow digest",
    )
    digest = require_mapping(
        digest_payload["data"],
        label="exact profile workflow digest data",
    )
    digest_tasks = [
        require_mapping(item, label="exact profile digest task")
        for item in require_list(digest["tasks"], label="exact profile digest tasks")
    ]
    expected_edges = {
        (
            _required_trace_int(relation.get("preTaskCode"), label="pre task code"),
            _required_trace_int(relation.get("postTaskCode"), label="post task code"),
        )
        for relation in relations
        if relation.get("preTaskCode") in task_codes
        and relation.get("postTaskCode") in task_codes
    }
    digest_edges = {
        (
            _required_trace_int(task.get("code"), label="digest task code"),
            _required_trace_int(
                downstream.get("code"),
                label="digest downstream task code",
            ),
        )
        for task in digest_tasks
        for downstream in [
            require_mapping(item, label="exact profile downstream task")
            for item in require_list(
                task["downstreamTasks"],
                label="exact profile downstream tasks",
            )
        ]
    }
    assert digest["taskCount"] == len(tasks)
    assert digest["relationCount"] == len(expected_edges)
    assert digest_edges == expected_edges
    assert {
        _required_trace_int(task.get("code"), label="digest task code")
        for task in digest_tasks
    } == task_codes
    _record_success(
        operation_trace,
        "workflow.digest",
        argv_shape="workflow digest <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=(
            "digest-counts-match-describe",
            "digest-topology-match-describe",
        ),
    )

    export_result = invoke_raw(["workflow", "export", *selector])
    assert export_result.exit_code == 0, export_result.stderr
    export_document = require_mapping(
        yaml.safe_load(export_result.stdout),
        label="exact profile workflow export document",
    )
    exported_workflow = require_mapping(
        export_document["workflow"],
        label="exact profile exported workflow",
    )
    exported_tasks = [
        require_mapping(item, label="exact profile exported task")
        for item in require_list(
            export_document["tasks"],
            label="exact profile exported tasks",
        )
    ]
    described_names = {
        _required_trace_text(task.get("name"), label="described task name")
        for task in tasks
    }
    task_name_by_code = {
        _required_trace_int(
            task.get("code"), label="described task code"
        ): _required_trace_text(task.get("name"), label="described task name")
        for task in tasks
    }
    expected_name_edges = {
        (task_name_by_code[upstream], task_name_by_code[downstream])
        for upstream, downstream in expected_edges
    }
    exported_name_edges = {
        (
            _required_trace_text(upstream, label="exported dependency name"),
            _required_trace_text(task.get("name"), label="exported task name"),
        )
        for task in exported_tasks
        for upstream in require_list(
            task.get("depends_on", []),
            label="exact profile exported dependencies",
        )
    }
    assert exported_workflow["name"] == workflow["name"]
    assert {
        _required_trace_text(task.get("name"), label="exported task name")
        for task in exported_tasks
    } == described_names
    assert exported_name_edges == expected_name_edges
    exported_schedule = require_mapping(
        export_document["schedule"],
        label="exact profile exported schedule",
    )
    assert exported_schedule == {
        "cron": schedule["crontab"],
        "timezone": schedule["timezoneId"],
        "start": schedule["startTime"],
        "end": schedule["endTime"],
        "failure_strategy": schedule["failureStrategy"],
        "priority": schedule["workflowInstancePriority"],
        "release_state": schedule["releaseState"],
    }
    _record_success(
        operation_trace,
        "workflow.export",
        argv_shape="workflow export <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=("yaml-dag-matches-describe", "yaml-schedule-matches-live"),
    )

    schedule_payload = require_ok_payload(
        invoke(
            [
                "schedule",
                "list",
                "--project",
                config.fixture.project_name,
                "--workflow",
                str(workflow_code),
                "--page-size",
                "20",
            ]
        ),
        expected_action="schedule.list",
        label="exact profile schedule list",
    )
    schedule_data = require_mapping(
        schedule_payload["data"],
        label="exact profile schedule list data",
    )
    schedule_rows = [
        require_mapping(item, label="exact profile schedule row")
        for item in require_list(
            schedule_data["totalList"],
            label="exact profile schedule rows",
        )
    ]
    matching_schedules = [
        row
        for row in schedule_rows
        if row.get("id") == config.fixture.schedule_id
        and row.get("workflowDefinitionCode") == workflow_code
    ]
    assert len(matching_schedules) == 1
    assert all(
        row.get("workflowDefinitionCode") == workflow_code for row in schedule_rows
    )
    _record_success(
        operation_trace,
        "schedule.list",
        argv_shape=(
            "schedule list --project <project-name> --workflow <workflow-code>"
        ),
        selector_kind="project-name+workflow-code",
        assertions=("fixture-schedule-id-matched", "workflow-filter-matched"),
    )


def _verify_task_definition_round_trip(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    config: ExactProfileGateConfig,
    operation_trace: list[OperationTraceEntry],
) -> ExternalTaskCleanupAttestation:
    """Mutate one exact profile task and restore its command in all exit paths."""
    fixture = config.fixture
    scope = [
        "--project",
        fixture.project_name,
        "--workflow",
        fixture.workflow_name,
    ]
    baseline = _read_task_gate_snapshot(
        invoke,
        scope=scope,
        fixture=fixture,
        label="exact profile initial task state",
    )
    _record_success(
        operation_trace,
        "task.list",
        argv_shape=(
            "task list --project <project-name> --workflow <workflow-name> "
            "--search <task-name>"
        ),
        selector_kind="project-name+workflow-name+task-search",
        assertions=("editable-shell-task-matched", "dag-version-matched-detail"),
    )
    original_command = baseline.command
    if not original_command.strip():
        message = "Exact profile editable SHELL fixture command must not be blank"
        raise AssertionError(message)
    _record_success(
        operation_trace,
        "task.get",
        argv_shape=(
            "task get <task-name> --project <project-name> --workflow <workflow-name>"
        ),
        selector_kind="task-name",
        assertions=("task-get-matched-list-version",),
    )
    _record_success(
        operation_trace,
        "workflow.digest",
        argv_shape="workflow digest <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=("task-update-baseline-topology-captured",),
    )

    replacement_command = (
        f'printf "%s\\n" "{create_live_run_prefix()}-exact-task-update"\n'
    )
    update_selector = str(fixture.task_code)
    update_argv = [
        "task",
        "update",
        update_selector,
        *scope,
        "--set",
        f"command={replacement_command}",
    ]
    cleanup_required = False
    mutation_completed = False
    updated_version: int | None = None
    restoration: tuple[bool, bool, bool, bool, bool, bool] | None = None
    try:
        cleanup_required = True
        dry_run_payload = require_ok_payload(
            invoke([*update_argv, "--dry-run", "--columns", "*"]),
            expected_action="task.update",
            label="exact profile task update dry run",
        )
        _verify_task_update_dry_run(
            dry_run_payload,
            fixture=fixture,
            replacement_command=replacement_command,
        )
        dry_run_snapshot = _read_task_gate_snapshot(
            invoke,
            scope=scope,
            fixture=fixture,
            label="exact profile state after task update dry run",
        )
        assert dry_run_snapshot == baseline
        _record_success(
            operation_trace,
            "task.update",
            argv_shape=(
                "task update <task-code> --project <project-name> "
                "--workflow <workflow-name> --set command=<command> --dry-run"
            ),
            selector_kind="task-code",
            assertions=(
                "exact-update-request-compiled",
                "dry-run-no-request-sent",
                "dry-run-state-unchanged",
                "dry-run-dag-unchanged",
            ),
        )

        update_payload = require_ok_payload(
            invoke(update_argv),
            expected_action="task.update",
            label="exact profile task update",
        )
        mutation_completed = True
        update_data = require_mapping(
            update_payload["data"],
            label="exact profile task update data",
        )
        assert (
            _task_command(
                update_data,
                label="exact profile updated task command",
            )
            == replacement_command
        )
        updated_version = _required_trace_int(
            update_data.get("version"),
            label="updated task version",
        )
        assert updated_version > baseline.version
        assert _task_non_owned_state(update_data) == baseline.non_owned_state
        _record_success(
            operation_trace,
            "task.update",
            argv_shape=(
                "task update <task-code> --project <project-name> "
                "--workflow <workflow-name> --set command=<command>"
            ),
            selector_kind="task-code",
            assertions=("command-updated-exactly", "task-version-advanced"),
        )

        updated_snapshot = _read_task_gate_snapshot(
            invoke,
            scope=scope,
            fixture=fixture,
            label="exact profile updated task state",
        )
        assert updated_snapshot.command == replacement_command
        assert updated_snapshot.version == updated_version
        assert updated_snapshot.non_owned_state == baseline.non_owned_state
        assert updated_snapshot.topology_state == baseline.topology_state
        _record_success(
            operation_trace,
            "task.get",
            argv_shape=(
                "task get <task-code> --project <project-name> "
                "--workflow <workflow-name>"
            ),
            selector_kind="task-code",
            assertions=("update-readback-exact", "non-owned-fields-preserved"),
        )
        _record_success(
            operation_trace,
            "task.list",
            argv_shape=(
                "task list --project <project-name> --workflow <workflow-name> "
                "--search <task-name>"
            ),
            selector_kind="project-name+workflow-name+task-search",
            assertions=("dag-task-version-matched-readback",),
        )
        _record_success(
            operation_trace,
            "workflow.digest",
            argv_shape="workflow digest <workflow-code> --project <project-name>",
            selector_kind="workflow-code",
            assertions=("task-update-topology-preserved",),
        )
    finally:
        if cleanup_required:
            restoration = _restore_task_fixture(
                invoke,
                scope=scope,
                fixture=fixture,
                baseline=baseline,
                gate_command=replacement_command,
                updated_version=updated_version,
                operation_trace=operation_trace,
            )

    assert mutation_completed
    assert restoration is not None
    (
        restore_succeeded,
        exact_command_restored,
        dag_version_matched,
        non_owned_fields_restored,
        topology_preserved,
        state_preserved,
    ) = restoration
    return ExternalTaskCleanupAttestation(
        mutated=True,
        restored=restore_succeeded,
        exact_command_restored=exact_command_restored,
        restored_dag_version_consistent=dag_version_matched,
        non_owned_fields_restored=non_owned_fields_restored,
        dag_topology_preserved=topology_preserved,
        workflow_release_state_preserved=state_preserved,
    )


def _restore_task_fixture(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    scope: list[str],
    fixture: ExactProfileFixture,
    baseline: _TaskGateSnapshot,
    gate_command: str,
    updated_version: int | None,
    operation_trace: list[OperationTraceEntry],
) -> tuple[bool, bool, bool, bool, bool, bool]:
    current = _read_task_gate_snapshot(
        invoke,
        scope=scope,
        fixture=fixture,
        label="exact profile task cleanup preflight",
    )
    _record_success(
        operation_trace,
        "task.get",
        argv_shape=(
            "task get <task-code> --project <project-name> --workflow <workflow-name>"
        ),
        selector_kind="task-code",
        assertions=("cleanup-state-inspected",),
    )
    if current.command not in {baseline.command, gate_command}:
        message = "Refusing to overwrite a concurrent task command during cleanup"
        raise AssertionError(message)
    if (
        current.command == gate_command
        and updated_version is not None
        and current.version != updated_version
    ):
        message = "Refusing task cleanup after a concurrent task version change"
        raise AssertionError(message)

    restore_succeeded = False
    restored = current
    if current.command == gate_command:
        restore_succeeded, restored = _restore_task_command(
            invoke,
            scope=scope,
            fixture=fixture,
            baseline=baseline,
            gate_command=gate_command,
            marker_version=current.version,
            operation_trace=operation_trace,
        )

    exact_command_restored = restored.command == baseline.command
    assert exact_command_restored
    non_owned_fields_restored = restored.non_owned_state == baseline.non_owned_state
    assert non_owned_fields_restored
    topology_preserved = restored.topology_state == baseline.topology_state
    assert topology_preserved
    _record_success(
        operation_trace,
        "task.get",
        argv_shape=(
            "task get <task-code> --project <project-name> --workflow <workflow-name>"
        ),
        selector_kind="task-code",
        assertions=("original-command-readback-exact", "restored-non-owned-fields"),
    )
    _record_success(
        operation_trace,
        "task.list",
        argv_shape=(
            "task list --project <project-name> --workflow <workflow-name> "
            "--search <task-name>"
        ),
        selector_kind="project-name+workflow-name+task-search",
        assertions=("restored-dag-version-consistent",),
    )
    _record_success(
        operation_trace,
        "workflow.digest",
        argv_shape="workflow digest <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=("restored-dag-topology-preserved",),
    )

    workflow_payload = require_ok_payload(
        invoke(["workflow", "get", str(fixture.workflow_code), *scope[:2]]),
        expected_action="workflow.get",
        label="exact profile workflow state after task restore",
    )
    workflow_data = require_mapping(
        workflow_payload["data"],
        label="exact profile workflow state after task restore data",
    )
    state_preserved = (
        workflow_data.get("releaseState") == fixture.workflow_release_state
    )
    assert state_preserved
    _record_success(
        operation_trace,
        "workflow.get",
        argv_shape="workflow get <workflow-code> --project <project-name>",
        selector_kind="workflow-code",
        assertions=("workflow-release-state-preserved",),
    )
    return (
        restore_succeeded,
        exact_command_restored,
        True,
        non_owned_fields_restored,
        topology_preserved,
        state_preserved,
    )


def _restore_task_command(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    scope: list[str],
    fixture: ExactProfileFixture,
    baseline: _TaskGateSnapshot,
    gate_command: str,
    marker_version: int,
    operation_trace: list[OperationTraceEntry],
) -> tuple[bool, _TaskGateSnapshot]:
    restore_argv = [
        "task",
        "update",
        str(fixture.task_code),
        *scope,
        "--set",
        f"command={baseline.command}",
    ]
    succeeded, restore_error = _invoke_task_restore(
        invoke,
        argv=restore_argv,
        baseline=baseline,
        marker_version=marker_version,
        operation_trace=operation_trace,
    )
    observed = _read_task_gate_snapshot(
        invoke,
        scope=scope,
        fixture=fixture,
        label="exact profile restored task reconciliation",
    )
    if observed.command == baseline.command:
        if restore_error is not None:
            raise restore_error
        return succeeded, observed
    if observed.command != gate_command or observed.version != marker_version:
        message = "Refusing to overwrite a concurrent task mutation during cleanup"
        raise AssertionError(message)
    if restore_error is not None:
        raise restore_error
    message = "Task restore response was not reflected by exact readback"
    raise AssertionError(message)


def _invoke_task_restore(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    argv: list[str],
    baseline: _TaskGateSnapshot,
    marker_version: int,
    operation_trace: list[OperationTraceEntry],
) -> tuple[bool, AssertionError | None]:
    try:
        result = invoke(argv)
    except (OSError, subprocess.SubprocessError) as exc:
        _record_error(
            operation_trace,
            "task.update",
            argv_shape=(
                "task update <task-code> --project <project-name> "
                "--workflow <workflow-name> --set command=<original-command>"
            ),
            error_type="api_transport_error",
            exit_code=1,
            selector_kind="task-code",
            assertions=("restore-response-ambiguous", "cleanup-state-inspected"),
        )
        return False, AssertionError("Task restore transport failed", exc)
    if result.exit_code != 0 or result.payload.get("ok") is not True:
        error = require_mapping(result.payload.get("error"), label="task restore error")
        error_type = _required_trace_text(
            error.get("type"),
            label="task restore error type",
        )
        _record_error(
            operation_trace,
            "task.update",
            argv_shape=(
                "task update <task-code> --project <project-name> "
                "--workflow <workflow-name> --set command=<original-command>"
            ),
            error_type=error_type,
            exit_code=result.exit_code,
            selector_kind="task-code",
            assertions=("restore-attempt-failed", "cleanup-state-inspected"),
        )
        return False, AssertionError("Task restore command failed")

    restore_payload = require_ok_payload(
        result,
        expected_action="task.update",
        label="exact profile task restore",
    )
    restore_data = require_mapping(
        restore_payload["data"],
        label="exact profile task restore data",
    )
    assert _task_command(restore_data, label="restored task") == baseline.command
    restored_version = _required_trace_int(
        restore_data.get("version"),
        label="restored task version",
    )
    assert restored_version > marker_version
    _record_success(
        operation_trace,
        "task.update",
        argv_shape=(
            "task update <task-code> --project <project-name> "
            "--workflow <workflow-name> --set command=<original-command>"
        ),
        selector_kind="task-code",
        assertions=("original-command-restored-exactly",),
    )
    return True, None


def _verify_task_update_dry_run(
    payload: dict[str, object],
    *,
    fixture: ExactProfileFixture,
    replacement_command: str,
) -> None:
    data = require_mapping(payload["data"], label="task update dry-run data")
    assert data["dry_run"] is True
    request = require_mapping(
        first_dry_run_request(data), label="task update dry-run request"
    )
    assert request["method"] == "PUT"
    assert request["path"] == (
        f"/projects/{fixture.project_code}/task-definition/"
        f"{fixture.task_code}/with-upstream"
    )
    request_form = require_mapping(request["form"], label="task update dry-run form")
    compiled_task = require_mapping(
        json.loads(
            _required_trace_text(
                request_form.get("taskDefinitionJsonObj"),
                label="taskDefinitionJsonObj",
            )
        ),
        label="compiled task definition",
    )
    compiled_params = require_mapping(
        json.loads(
            _required_trace_text(
                compiled_task.get("taskParams"),
                label="compiled taskParams",
            )
        ),
        label="compiled task parameters",
    )
    assert {
        "code",
        "id",
        "version",
        "projectCode",
        "createTime",
        "updateTime",
    }.isdisjoint(compiled_task)
    assert compiled_task["taskType"] == fixture.task_type
    assert compiled_params["rawScript"] == replacement_command
    warning_details = [
        require_mapping(item, label="task update dry-run warning detail")
        for item in require_list(
            payload["warnings"],
            label="task update dry-run warning details",
        )
    ]
    assert any(item.get("mutation_sent") is False for item in warning_details)


def _read_task_gate_snapshot(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    scope: list[str],
    fixture: ExactProfileFixture,
    label: str,
) -> _TaskGateSnapshot:
    task = _read_fixture_task(
        invoke,
        selector=str(fixture.task_code),
        scope=scope,
        fixture=fixture,
        label=f"{label} detail",
    )
    row = _read_fixture_task_row(
        invoke,
        scope=scope,
        fixture=fixture,
        label=f"{label} DAG row",
    )
    version = _required_trace_int(task.get("version"), label=f"{label} version")
    assert (
        _required_trace_int(
            row.get("version"),
            label=f"{label} DAG version",
        )
        == version
    )
    return _TaskGateSnapshot(
        command=_task_command(task, label=f"{label} command"),
        version=version,
        non_owned_state=_task_non_owned_state(task),
        topology_state=_read_fixture_topology(
            invoke,
            fixture=fixture,
            label=f"{label} topology",
        ),
    )


def _task_non_owned_state(task: dict[str, object]) -> str:
    state = {
        key: value
        for key, value in task.items()
        if key not in _TASK_READBACK_VOLATILE_FIELDS
    }
    task_params = dict(
        require_mapping(task["taskParams"], label="task preservation taskParams")
    )
    task_params.pop("rawScript", None)
    state["taskParams"] = task_params
    return json.dumps(state, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _read_fixture_topology(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    fixture: ExactProfileFixture,
    label: str,
) -> str:
    payload = require_ok_payload(
        invoke(
            [
                "workflow",
                "digest",
                str(fixture.workflow_code),
                "--project",
                fixture.project_name,
            ]
        ),
        expected_action="workflow.digest",
        label=label,
    )
    data = require_mapping(payload["data"], label=f"{label} data")
    workflow = require_mapping(data["workflow"], label=f"{label} workflow")
    assert workflow["code"] == fixture.workflow_code
    assert workflow["releaseState"] == fixture.workflow_release_state
    normalized = dict(data)
    normalized_workflow = dict(workflow)
    normalized_workflow.pop("version", None)
    normalized["workflow"] = normalized_workflow
    return json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _read_fixture_task_row(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    scope: list[str],
    fixture: ExactProfileFixture,
    label: str,
) -> dict[str, object]:
    payload = require_ok_payload(
        invoke(["task", "list", *scope, "--search", fixture.task_name]),
        expected_action="task.list",
        label=label,
    )
    rows = [
        require_mapping(item, label=f"{label} row")
        for item in require_list(payload["data"], label=f"{label} data")
    ]
    matching = [
        row
        for row in rows
        if row.get("code") == fixture.task_code and row.get("name") == fixture.task_name
    ]
    assert len(matching) == 1
    return matching[0]


def _read_fixture_task(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    selector: str,
    scope: list[str],
    fixture: ExactProfileFixture,
    label: str,
) -> dict[str, object]:
    payload = require_ok_payload(
        invoke(["task", "get", selector, *scope]),
        expected_action="task.get",
        label=label,
    )
    task = require_mapping(payload["data"], label=f"{label} data")
    assert task["code"] == fixture.task_code
    assert task["name"] == fixture.task_name
    assert task["projectCode"] == fixture.project_code
    assert task["taskType"] == fixture.task_type
    return task


def _task_command(task: dict[str, object], *, label: str) -> str:
    task_params = require_mapping(task["taskParams"], label=f"{label} taskParams")
    command = task_params.get("rawScript")
    if not isinstance(command, str):
        message = f"{label} rawScript must be text"
        raise TypeError(message)
    return command


def _required_trace_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"{label} must be an integer"
        raise TypeError(message)
    return value


def _required_trace_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _verify_project_lifecycle(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    external_task: ExternalTaskCleanupAttestation,
    operation_trace: list[OperationTraceEntry],
) -> CleanupAttestation:
    run_prefix = create_live_run_prefix()
    initial_name = f"{run_prefix}-project"
    updated_name = f"{run_prefix}-project-updated"
    ownership_marker = f"exact-342-owner={run_prefix}"
    initial_description = f"{ownership_marker};state=initial"
    updated_description = f"{ownership_marker};state=updated"
    project_code: int | None = None
    create_may_have_committed = True
    deleted = False
    cleanup_confirmed = False

    try:
        create_result = invoke(
            [
                "project",
                "create",
                "--name",
                initial_name,
                "--description",
                initial_description,
            ]
        )
        create_may_have_committed = not _is_confirmed_create_rejection(create_result)
        create_payload = require_ok_payload(
            create_result,
            expected_action="project.create",
            label="exact profile project create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="exact profile project create data",
        )
        code = create_data.get("code")
        assert isinstance(code, int)
        assert not isinstance(code, bool)
        project_code = code
        _record_success(
            operation_trace,
            "project.create",
            argv_shape="project create --name <project-name>",
            assertions=("native-project-code-returned",),
        )

        conflict_result = invoke(["project", "create", "--name", initial_name])
        conflict = require_error_payload(
            conflict_result,
            expected_action="project.create",
            expected_type="conflict",
            label="exact profile duplicate project",
        )
        assert conflict["suggestion"]
        _record_error(
            operation_trace,
            "project.create",
            argv_shape="project create --name <duplicate-project-name>",
            error_type="conflict",
            exit_code=conflict_result.exit_code,
            assertions=("stable-conflict", "actionable-suggestion"),
        )

        list_payload = require_ok_payload(
            invoke(
                [
                    "project",
                    "list",
                    "--search",
                    initial_name,
                    "--page-size",
                    "20",
                ]
            ),
            expected_action="project.list",
            label="exact profile project list",
        )
        list_data = require_mapping(
            list_payload["data"],
            label="exact profile project list data",
        )
        project_rows = [
            require_mapping(item, label="exact profile project row")
            for item in require_list(
                list_data["totalList"],
                label="exact profile project rows",
            )
        ]
        assert any(row.get("code") == project_code for row in project_rows)
        _record_success(
            operation_trace,
            "project.list",
            argv_shape="project list --search <project-name>",
            selector_kind="search",
            assertions=("native-project-code-matched", "pagination-search-matched"),
        )

        _assert_project_get(
            invoke,
            selector=initial_name,
            selector_kind="project-name",
            expected_code=project_code,
            expected_name=initial_name,
            operation_trace=operation_trace,
        )

        update_payload = require_ok_payload(
            invoke(
                [
                    "project",
                    "update",
                    initial_name,
                    "--name",
                    updated_name,
                    "--description",
                    updated_description,
                ]
            ),
            expected_action="project.update",
            label="exact profile project update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="exact profile project update data",
        )
        assert update_data["code"] == project_code
        assert update_data["name"] == updated_name
        assert update_data["description"] == updated_description
        _record_success(
            operation_trace,
            "project.update",
            argv_shape="project update <project-name> --name <new-project-name>",
            selector_kind="project-name",
            assertions=("name-and-description-updated", "native-code-preserved"),
        )

        _assert_project_get(
            invoke,
            selector=str(project_code),
            selector_kind="project-code",
            expected_code=project_code,
            expected_name=updated_name,
            operation_trace=operation_trace,
        )

        delete_payload = require_ok_payload(
            invoke(["project", "delete", str(project_code), "--force"]),
            expected_action="project.delete",
            label="exact profile project delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="exact profile project delete data",
        )
        assert delete_data["deleted"] is True
        deleted = True
        _record_success(
            operation_trace,
            "project.delete",
            argv_shape="project delete <project-code> --force",
            selector_kind="project-code",
            assertions=("deleted-true", "cleanup-applied"),
        )

        not_found_result = invoke(["project", "get", str(project_code)])
        require_error_payload(
            not_found_result,
            expected_action="project.get",
            expected_type="not_found",
            label="exact profile project get after delete",
        )
        _record_error(
            operation_trace,
            "project.get",
            argv_shape="project get <deleted-project-code>",
            error_type="not_found",
            exit_code=not_found_result.exit_code,
            selector_kind="project-code",
            assertions=("stable-not-found-after-delete",),
        )

        absent_payload = require_ok_payload(
            invoke(["project", "list", "--search", updated_name]),
            expected_action="project.list",
            label="exact profile project list after delete",
        )
        absent_data = require_mapping(
            absent_payload["data"],
            label="exact profile project list after delete data",
        )
        absent_rows = require_list(
            absent_data["totalList"],
            label="exact profile project list after delete rows",
        )
        assert not absent_rows
        _record_success(
            operation_trace,
            "project.list",
            argv_shape="project list --search <deleted-project-name>",
            selector_kind="search",
            assertions=("cleanup-leftovers-zero",),
        )
        cleanup_confirmed = True
        return CleanupAttestation(
            confirmed=True,
            project_leftovers=0,
            external_task=external_task,
        )
    finally:
        if not cleanup_confirmed:
            _cleanup_project_after_failure(
                invoke,
                project_code=project_code,
                candidate_names=(initial_name, updated_name),
                ownership_descriptions=(initial_description, updated_description),
                create_may_have_committed=create_may_have_committed,
                already_deleted=deleted,
            )


def _cleanup_project_after_failure(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    project_code: int | None,
    candidate_names: tuple[str, ...],
    ownership_descriptions: tuple[str, ...],
    create_may_have_committed: bool,
    already_deleted: bool,
) -> None:
    """Recover possibly committed creates, but delete only gate-owned projects."""
    if project_code is None and not create_may_have_committed:
        return

    resolved_code = project_code
    if resolved_code is None and create_may_have_committed and not already_deleted:
        resolved_code = _resolve_failed_project_code(invoke, candidate_names)

    if resolved_code is not None and not already_deleted:
        resolved_code = _delete_owned_failed_project(
            invoke,
            project_code=resolved_code,
            ownership_descriptions=ownership_descriptions,
        )

    _verify_failed_project_absent(
        invoke,
        candidate_names=candidate_names,
        project_code=resolved_code,
    )


def _is_confirmed_create_rejection(result: DsctlCommandResult) -> bool:
    if result.exit_code == 0:
        return False
    return _result_error_type(result) in _CONFIRMED_CREATE_REJECTION_TYPES


def _resolve_failed_project_code(
    invoke: Callable[[list[str]], DsctlCommandResult],
    candidate_names: tuple[str, ...],
) -> int | None:
    for attempt in range(5):
        for name in candidate_names:
            result = invoke(["project", "get", name])
            if result.exit_code == 0:
                payload = require_ok_payload(
                    result,
                    expected_action="project.get",
                    label="exact profile failed-run project lookup",
                )
                data = require_mapping(
                    payload["data"],
                    label="exact profile failed-run project lookup data",
                )
                code = data.get("code")
                assert isinstance(code, int)
                assert not isinstance(code, bool)
                return code
            if _is_retryable_cleanup_lookup_error(result):
                continue
            require_error_payload(
                result,
                expected_action="project.get",
                expected_type="not_found",
                label="exact profile failed-run project lookup",
            )
        if attempt < 4:
            time.sleep(1.0)
    return None


def _is_retryable_cleanup_lookup_error(result: DsctlCommandResult) -> bool:
    error_type = _result_error_type(result)
    if error_type in {"api_transport_error", "timeout"}:
        return True
    if error_type != "api_http_error":
        return False
    error = result.payload.get("error")
    if not isinstance(error, dict):
        return False
    for container_name in ("details", "source"):
        container = error.get(container_name)
        if not isinstance(container, dict):
            continue
        status_code = container.get("status_code")
        if isinstance(status_code, int) and not isinstance(status_code, bool):
            return status_code in _RETRYABLE_CLEANUP_HTTP_STATUSES
    return False


def _result_error_type(result: DsctlCommandResult) -> str | None:
    error = result.payload.get("error")
    if not isinstance(error, dict):
        return None
    error_type = error.get("type")
    return error_type if isinstance(error_type, str) else None


def _delete_owned_failed_project(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    project_code: int,
    ownership_descriptions: tuple[str, ...],
) -> int | None:
    ownership_result = invoke(["project", "get", str(project_code)])
    if ownership_result.exit_code != 0:
        require_error_payload(
            ownership_result,
            expected_action="project.get",
            expected_type="not_found",
            label="exact profile failed-run ownership lookup",
        )
        return None
    ownership_payload = require_ok_payload(
        ownership_result,
        expected_action="project.get",
        label="exact profile failed-run ownership lookup",
    )
    ownership_data = require_mapping(
        ownership_payload["data"],
        label="exact profile failed-run ownership data",
    )
    if ownership_data.get("description") not in ownership_descriptions:
        message = "Refusing cleanup because project ownership marker differs"
        raise AssertionError(message)
    cleanup_payload = require_ok_payload(
        invoke(["project", "delete", str(project_code), "--force"]),
        expected_action="project.delete",
        label="exact profile project cleanup",
    )
    cleanup_data = require_mapping(
        cleanup_payload["data"],
        label="exact profile project cleanup data",
    )
    assert cleanup_data["deleted"] is True
    return project_code


def _verify_failed_project_absent(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    candidate_names: tuple[str, ...],
    project_code: int | None,
) -> None:

    for name in candidate_names:
        absent_payload = require_ok_payload(
            invoke(["project", "list", "--search", name]),
            expected_action="project.list",
            label="exact profile failed-run cleanup verification",
        )
        absent_data = require_mapping(
            absent_payload["data"],
            label="exact profile failed-run cleanup verification data",
        )
        rows = [
            require_mapping(item, label="exact profile cleanup row")
            for item in require_list(
                absent_data["totalList"],
                label="exact profile failed-run cleanup rows",
            )
        ]
        assert not any(
            row.get("name") in candidate_names
            or (project_code is not None and row.get("code") == project_code)
            for row in rows
        )


def _assert_project_get(
    invoke: Callable[[list[str]], DsctlCommandResult],
    *,
    selector: str,
    selector_kind: str,
    expected_code: int,
    expected_name: str,
    operation_trace: list[OperationTraceEntry],
) -> None:
    payload = require_ok_payload(
        invoke(["project", "get", selector]),
        expected_action="project.get",
        label=f"exact profile project get by {selector_kind}",
    )
    data = require_mapping(payload["data"], label="exact profile project get data")
    assert data["code"] == expected_code
    assert data["name"] == expected_name
    _record_success(
        operation_trace,
        "project.get",
        argv_shape="project get <project>",
        selector_kind=selector_kind,
        assertions=("native-project-code-matched", "project-name-matched"),
    )


def _record_success(
    trace: list[OperationTraceEntry],
    action: str,
    *,
    argv_shape: str,
    assertions: tuple[str, ...],
    selector_kind: str | None = None,
) -> None:
    trace.append(
        OperationTraceEntry(
            sequence=len(trace) + 1,
            argv_shape=argv_shape,
            action=action,
            exit_code=0,
            ok=True,
            assertions=assertions,
            selector_kind=selector_kind,
        )
    )


def _record_error(
    trace: list[OperationTraceEntry],
    action: str,
    *,
    argv_shape: str,
    error_type: str,
    exit_code: int,
    assertions: tuple[str, ...],
    selector_kind: str | None = None,
) -> None:
    trace.append(
        OperationTraceEntry(
            sequence=len(trace) + 1,
            argv_shape=argv_shape,
            action=action,
            exit_code=exit_code,
            ok=False,
            assertions=assertions,
            selector_kind=selector_kind,
            error_type=error_type,
        )
    )
