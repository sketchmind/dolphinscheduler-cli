from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from tests.live.journey_support import Journey

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from tests.live.support import DsctlCommandResult


_SCHEDULE_TIMEZONE = "Asia/Shanghai"
_SCHEDULE_CRON = "0 * * * * ?"


def run_schedule_journey(
    *,
    repo_root: Path,
    executable: Path,
    env_file: Path,
    ds_version: str,
    workspace: Path,
    prefix: str,
) -> dict[str, object]:
    """Prove one exact profile's schedule lifecycle and real cron execution."""
    evidence: dict[str, object]
    with Journey(
        repo_root=repo_root,
        executable=executable,
        env_file=env_file,
        ds_version=ds_version,
        workspace=workspace,
        prefix=prefix,
    ) as journey:
        journey.create_workflow()
        option_flags = _schedule_create_option_flags(journey)
        selected_tenant = "default" if "--tenant-code" in option_flags else None
        schedule_start, schedule_end = _schedule_window(
            include_offset="--timezone" not in option_flags
        )
        timing_fields = _schedule_fields(
            option_flags=option_flags,
            start=schedule_start,
            end=schedule_end,
            include_create_options=False,
        )
        schedule_fields = _schedule_fields(
            option_flags=option_flags,
            start=schedule_start,
            end=schedule_end,
        )

        project_get = journey.data(["project", "get", journey.project])
        workflow_get = journey.data(
            [
                "workflow",
                "get",
                journey.workflow,
                "--project",
                journey.project,
            ]
        )
        project_identity = _native_identity(
            project_get,
            fields=(journey.identity_key,),
            label="project",
        )
        workflow_identity = _native_identity(
            workflow_get,
            fields=(journey.identity_key,),
            label="workflow",
        )

        preview_data = journey.data(
            [
                "schedule",
                "preview",
                "--project",
                journey.project,
                *timing_fields,
            ]
        )
        preview_times = _mapping_list(preview_data, "times", label="preview times")
        if not preview_times:
            message = "schedule preview returned no future trigger times"
            raise AssertionError(message)

        online_workflow = journey.data(
            [
                "workflow",
                "online",
                journey.workflow,
                "--project",
                journey.project,
            ]
        )
        _require_value(
            online_workflow,
            "releaseState",
            "ONLINE",
            label="workflow online",
        )

        instance_argv = [
            "workflow-instance",
            "list",
            "--project",
            journey.project,
            "--workflow",
            journey.workflow,
            "--page-size",
            "100",
        ]
        baseline_ids = {
            _positive_int(row.get("id"), label="baseline workflow-instance id")
            for row in journey.rows(instance_argv)
        }

        create_argv = [
            "schedule",
            "create",
            "--project",
            journey.project,
            "--workflow",
            journey.workflow,
            *schedule_fields,
        ]
        _expect_error(
            journey.call(create_argv),
            action="schedule.create",
            error_type="confirmation_required",
        )
        schedule_create = _successful_schedule_data(
            _confirmed_schedule_call(
                journey,
                explain_argv=["schedule", "explain", *create_argv[2:]],
                mutation_argv=create_argv,
            ),
            action="schedule.create",
        )
        schedule_id = _positive_int(
            schedule_create.get("id"),
            label="created schedule id",
        )
        journey.schedule_ids.add(schedule_id)
        _require_value(
            schedule_create,
            "releaseState",
            "OFFLINE",
            label="created schedule",
        )
        _require_selected_tenant(
            schedule_create,
            selected_tenant=selected_tenant,
            label="created schedule",
        )

        schedule_scope = ["--project", journey.project]
        stored_schedule = journey.data(
            ["schedule", "get", str(schedule_id), *schedule_scope]
        )
        _require_value(
            stored_schedule,
            "id",
            schedule_id,
            label="stored schedule",
        )
        _require_selected_tenant(
            stored_schedule,
            selected_tenant=selected_tenant,
            label="stored schedule",
            required=True,
        )
        existing_preview = journey.data(
            ["schedule", "preview", str(schedule_id), *schedule_scope]
        )
        existing_times = _mapping_list(
            existing_preview,
            "times",
            label="stored schedule preview times",
        )
        if not existing_times:
            message = "stored schedule preview returned no trigger times"
            raise AssertionError(message)

        update_fields = [str(schedule_id), *schedule_scope, "--warning-type", "SUCCESS"]
        updated_schedule = _successful_schedule_data(
            _confirmed_schedule_call(
                journey,
                explain_argv=["schedule", "explain", *update_fields],
                mutation_argv=["schedule", "update", *update_fields],
            ),
            action="schedule.update",
        )
        _require_value(
            updated_schedule,
            "warningType",
            "SUCCESS",
            label="updated schedule",
        )
        _require_selected_tenant(
            updated_schedule,
            selected_tenant=selected_tenant,
            label="updated schedule",
            required=True,
        )

        online_schedule = journey.data(
            ["schedule", "online", str(schedule_id), *schedule_scope]
        )
        _require_value(
            online_schedule,
            "releaseState",
            "ONLINE",
            label="online schedule",
        )
        _expect_online_update_invalid_state(
            journey,
            explain_argv=[
                "schedule",
                "explain",
                str(schedule_id),
                *schedule_scope,
                "--warning-type",
                "FAILURE",
            ],
            update_argv=[
                "schedule",
                "update",
                str(schedule_id),
                *schedule_scope,
                "--warning-type",
                "FAILURE",
            ],
        )

        scheduled_instance = _wait_for_scheduled_instance(
            journey,
            instance_argv=instance_argv,
            baseline_ids=baseline_ids,
        )
        _require_selected_tenant(
            scheduled_instance,
            selected_tenant=selected_tenant,
            label="scheduled workflow instance",
        )
        instance_id = _positive_int(
            scheduled_instance.get("id"),
            label="scheduled workflow-instance id",
        )
        terminal_instance = journey.watch(instance_id)
        if terminal_instance.get("state") != "SUCCESS":
            _capture_failed_task_diagnostics(journey, instance_id=instance_id)
        _require_value(
            terminal_instance,
            "state",
            "SUCCESS",
            label="scheduled workflow instance",
        )

        offline_schedule = journey.data(
            ["schedule", "offline", str(schedule_id), *schedule_scope]
        )
        _require_value(
            offline_schedule,
            "releaseState",
            "OFFLINE",
            label="offline schedule",
        )
        refreshed_schedule = journey.data(
            ["schedule", "get", str(schedule_id), *schedule_scope]
        )
        _require_value(
            refreshed_schedule,
            "releaseState",
            "OFFLINE",
            label="refreshed offline schedule",
        )
        _require_selected_tenant(
            refreshed_schedule,
            selected_tenant=selected_tenant,
            label="refreshed offline schedule",
            required=True,
        )
        deleted_schedule = journey.data(
            [
                "schedule",
                "delete",
                str(schedule_id),
                *schedule_scope,
                "--force",
            ]
        )
        _require_value(
            deleted_schedule,
            "deleted",
            True,
            label="deleted schedule",
        )
        journey.schedule_ids.discard(schedule_id)

        definition_identity = _native_identity(
            scheduled_instance,
            fields=("workflowDefinitionCode", "workflowDefinitionId"),
            label="scheduled workflow definition",
        )
        instance_project_identity = _native_identity(
            scheduled_instance,
            fields=("projectCode", "projectId"),
            label="scheduled workflow project",
        )
        evidence = {
            "kind": "schedule_journey",
            "ds_version": ds_version,
            "resources": {
                "project": journey.project,
                "workflow": journey.workflow,
                "project_identity": project_identity,
                "workflow_identity": workflow_identity,
                "schedule_id": schedule_id,
            },
            "exact_contract": {
                "schedule_create_options": {
                    "timezone": "--timezone" in option_flags,
                    "tenant_code": "--tenant-code" in option_flags,
                    "environment_code": "--environment-code" in option_flags,
                    "missed_fire_policy": ("--missed-fire-policy" in option_flags),
                },
                "definition_identity": definition_identity,
                "instance_project_identity": instance_project_identity,
                "environment": _native_optional_field(
                    stored_schedule,
                    field="environmentCode",
                ),
                "missed_fire_policy": _native_optional_field(
                    stored_schedule,
                    field="missedFirePolicy",
                ),
                "tenant": _native_optional_field(
                    stored_schedule,
                    field="tenantCode",
                ),
            },
            "schedule": {
                "cron": _SCHEDULE_CRON,
                "preview_count": len(preview_times),
                "stored_preview_count": len(existing_times),
                "updated_warning_type": updated_schedule["warningType"],
                "lifecycle": ["OFFLINE", "ONLINE", "OFFLINE", "DELETED"],
            },
            "trigger": {
                "workflow_instance_id": instance_id,
                "command_type": scheduled_instance["commandType"],
                "schedule_time": scheduled_instance["scheduleTime"],
                "tenant": _native_optional_field(
                    scheduled_instance,
                    field="tenantCode",
                ),
                "state": terminal_instance["state"],
                "run_times": terminal_instance.get(
                    "runTimes", scheduled_instance.get("runTimes")
                ),
            },
            "negative_checks": [
                "schedule.create:confirmation_required",
                "schedule.update:invalid_state_while_online",
            ],
        }

    evidence["cleanup"] = dict(journey.cleanup_evidence)
    return evidence


def _schedule_create_option_flags(journey: Journey) -> set[str]:
    schema = journey.data(["schema", "--command", "schedule.create"])
    capability = _as_mapping(
        schema.get("capability"),
        label="schedule.create schema capability",
    )
    if capability.get("availability") != "supported":
        message = "schedule.create is unavailable on the selected exact profile"
        raise AssertionError(message)
    command = _as_mapping(schema.get("command"), label="schedule.create schema command")
    options = command.get("options")
    if not isinstance(options, list):
        message = "schedule.create schema options is not a list"
        raise TypeError(message)
    flags: set[str] = set()
    for index, option in enumerate(options):
        item = _as_mapping(option, label=f"schedule.create option {index}")
        flag = item.get("flag")
        if isinstance(flag, str):
            flags.add(flag)
    return flags


def _schedule_fields(
    *,
    option_flags: set[str],
    start: str,
    end: str,
    include_create_options: bool = True,
) -> list[str]:
    fields = ["--cron", _SCHEDULE_CRON, "--start", start, "--end", end]
    if "--timezone" in option_flags:
        fields.extend(["--timezone", _SCHEDULE_TIMEZONE])
    if include_create_options and "--tenant-code" in option_flags:
        fields.extend(["--tenant-code", "default"])
    if include_create_options and "--environment-code" in option_flags:
        fields.extend(["--environment-code", "0"])
    if include_create_options and "--missed-fire-policy" in option_flags:
        fields.extend(["--missed-fire-policy", "SKIP_MISSED"])
    return fields


def _schedule_window(*, include_offset: bool = False) -> tuple[str, str]:
    now = datetime.now(tz=ZoneInfo(_SCHEDULE_TIMEZONE)).replace(
        second=0,
        microsecond=0,
    )
    start = now + timedelta(minutes=2)
    end = start + timedelta(minutes=16)
    if include_offset:
        # Make the window verifiable without assuming the server-local timezone.
        return start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def _confirmation_token(data: Mapping[str, object], *, label: str) -> str:
    confirmation = _as_mapping(
        data.get("confirmation"),
        label=f"{label} confirmation",
    )
    if confirmation.get("required") is not True:
        message = f"{label} did not require high-frequency confirmation"
        raise AssertionError(message)
    token = confirmation.get("token")
    if not isinstance(token, str) or not token:
        message = f"{label} confirmation token is missing"
        raise AssertionError(message)
    return token


def _expect_error(
    result: DsctlCommandResult,
    *,
    action: str,
    error_type: str,
) -> None:
    payload = result.payload
    if result.exit_code == 0 or payload.get("ok") is not False:
        message = f"{action} unexpectedly succeeded"
        raise AssertionError(message)
    if payload.get("action") != action:
        message = f"expected error action {action!r}, got {payload.get('action')!r}"
        raise AssertionError(message)
    error = _as_mapping(payload.get("error"), label=f"{action} error")
    if error.get("type") != error_type:
        message = f"expected {action} error {error_type!r}, got {error.get('type')!r}"
        raise AssertionError(message)


def _expect_online_update_invalid_state(
    journey: Journey,
    *,
    explain_argv: Sequence[str],
    update_argv: Sequence[str],
) -> None:
    _expect_error(
        _confirmed_schedule_call(
            journey, explain_argv=explain_argv, mutation_argv=update_argv
        ),
        action="schedule.update",
        error_type="invalid_state",
    )


def _successful_schedule_data(
    result: DsctlCommandResult, *, action: str
) -> Mapping[str, object]:
    if result.exit_code != 0 or result.payload.get("ok") is not True:
        error_type, _ = _failed_error(result, action=action)
        message = f"{action} failed: {error_type}"
        raise AssertionError(message)
    if result.payload.get("action") != action:
        message = "schedule mutation action mismatch"
        raise AssertionError(message)
    return _as_mapping(result.payload.get("data"), label=action)


def _confirmed_schedule_call(
    journey: Journey,
    *,
    explain_argv: Sequence[str],
    mutation_argv: Sequence[str],
) -> DsctlCommandResult:
    """Retry once only after an explicit no-write confirmation-token rollover."""
    action = ".".join(mutation_argv[:2])
    if action not in {"schedule.create", "schedule.update"}:
        message = "confirmation refresh only supports schedule create/update"
        raise AssertionError(message)
    token = _confirmation_token(journey.data(list(explain_argv)), label=action)
    result = journey.call([*mutation_argv, "--confirm-risk", token])
    if result.exit_code == 0 or result.payload.get("ok") is not False:
        return result
    error_type, error = _failed_error(result, action=action)
    if error_type != "confirmation_required":
        return result
    details = _as_mapping(error.get("details"), label=f"{action} confirmation details")
    _require_value(
        details, "risk_type", "high_frequency_schedule", label=f"{action} confirmation"
    )
    current_token = details.get("confirmation_token")
    if (
        not isinstance(current_token, str)
        or not current_token
        or current_token == token
    ):
        message = f"{action} did not prove a changed confirmation token"
        raise AssertionError(message)
    # Both services check confirmation before sending their remote mutation.
    refreshed = _confirmation_token(journey.data(list(explain_argv)), label=action)
    if refreshed == token:
        message = f"{action} explain returned an unchanged confirmation token"
        raise AssertionError(message)
    return journey.call([*mutation_argv, "--confirm-risk", refreshed])


def _failed_error(
    result: DsctlCommandResult,
    *,
    action: str,
) -> tuple[object, Mapping[str, object]]:
    payload = result.payload
    if result.exit_code == 0 or payload.get("ok") is not False:
        message = f"{action} unexpectedly succeeded"
        raise AssertionError(message)
    if payload.get("action") != action:
        message = f"expected error action {action!r}, got {payload.get('action')!r}"
        raise AssertionError(message)
    error = _as_mapping(payload.get("error"), label=f"{action} error")
    return error.get("type"), error


def _wait_for_scheduled_instance(
    journey: Journey,
    *,
    instance_argv: Sequence[str],
    baseline_ids: set[int],
    timeout_seconds: float = 190.0,
    interval_seconds: float = 5.0,
) -> Mapping[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_rows: list[dict[str, object]] = []
    while True:
        last_rows = journey.rows(list(instance_argv))
        for row in last_rows:
            row_id = row.get("id")
            if (
                isinstance(row_id, int)
                and not isinstance(row_id, bool)
                and row_id > 0
                and row_id not in baseline_ids
                and row.get("commandType") == "SCHEDULER"
                and isinstance(row.get("scheduleTime"), str)
                and bool(row["scheduleTime"])
            ):
                return row
        if time.monotonic() >= deadline:
            observed = [
                {
                    "id": row.get("id"),
                    "commandType": row.get("commandType"),
                    "scheduleTime": row.get("scheduleTime"),
                    "state": row.get("state"),
                }
                for row in last_rows
            ]
            message = (
                "no new scheduler-originated workflow instance appeared before "
                f"timeout; observed={observed!r}"
            )
            raise AssertionError(message)
        time.sleep(interval_seconds)


def _capture_failed_task_diagnostics(journey: Journey, *, instance_id: int) -> None:
    """Persist bounded CLI reads without replacing the journey's original failure."""
    try:
        _read_failed_task_diagnostics(journey, instance_id=instance_id)
    except (subprocess.TimeoutExpired, AssertionError, OSError) as error:
        path = journey.workspace / "failed-task-diagnostic-error.json"
        try:
            path.touch(mode=0o600, exist_ok=True)
            path.chmod(0o600)
            path.write_text(
                json.dumps({"error_type": type(error).__name__}) + "\n",
                encoding="utf-8",
            )
        except OSError:
            return


def _read_failed_task_diagnostics(journey: Journey, *, instance_id: int) -> None:
    if not journey.capability("task-instance.list"):
        return
    result = journey.call(
        [
            "task-instance",
            "list",
            "--project",
            journey.project,
            "--workflow-instance",
            str(instance_id),
            "--page-size",
            "20",
        ]
    )
    if result.exit_code != 0 or result.payload.get("ok") is not True:
        return
    data = result.payload.get("data")
    if not isinstance(data, dict):
        return
    rows = data.get("totalList")
    if not isinstance(rows, list):
        return
    if not journey.capability("task-instance.log"):
        return
    failed_ids: set[int] = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or row.get("workflowInstanceId") != instance_id
            or row.get("state") != "FAILURE"
        ):
            continue
        task_id = row.get("id")
        if isinstance(task_id, int) and not isinstance(task_id, bool) and task_id > 0:
            failed_ids.add(task_id)
    for task_id in sorted(failed_ids)[:2]:
        journey.call(["task-instance", "log", str(task_id), "--tail", "100"])


def _native_identity(
    data: Mapping[str, object],
    *,
    fields: Sequence[str],
    label: str,
) -> dict[str, object]:
    field, value = _first_native_identity(data, fields=fields, label=label)
    return {"field": field, "value": value}


def _first_native_identity(
    data: Mapping[str, object],
    *,
    fields: Sequence[str],
    label: str,
) -> tuple[str, int]:
    for field in fields:
        value = data.get(field)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return field, value
    joined = ", ".join(fields)
    message = f"{label} has no positive native identity in {joined}"
    raise AssertionError(message)


def _native_optional_field(
    data: Mapping[str, object],
    *,
    field: str,
) -> dict[str, object]:
    return {"present": field in data, "value": data.get(field)}


def _require_selected_tenant(
    data: Mapping[str, object],
    *,
    selected_tenant: str | None,
    label: str,
    required: bool = False,
) -> None:
    if selected_tenant is None:
        return
    if not required and "tenantCode" not in data:
        return
    _require_value(data, "tenantCode", selected_tenant, label=label)


def _mapping_list(
    data: Mapping[str, object],
    field: str,
    *,
    label: str,
) -> list[object]:
    value = data.get(field)
    if not isinstance(value, list):
        message = f"{label} is not a list"
        raise TypeError(message)
    return value


def _as_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        message = f"{label} is not an object"
        raise TypeError(message)
    return value


def _positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} is not a positive integer"
        raise AssertionError(message)
    return value


def _require_value(
    data: Mapping[str, object],
    field: str,
    expected: object,
    *,
    label: str,
) -> None:
    actual = data.get(field)
    if actual != expected:
        message = f"{label} field {field!r}: expected {expected!r}, got {actual!r}"
        raise AssertionError(message)
