from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest
import yaml

from tests.live.execution_support import _execution_instance_id
from tests.live.support import (
    DsctlCommandResult,
    require_error_payload,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    require_text_value,
    result_error_code,
    run_dsctl,
    run_dsctl_raw,
    wait_for_result,
)
from tests.live.workflow_support import (
    delete_project_eventually,
    delete_workflow_eventually,
    write_shell_workflow_spec,
    write_single_shell_workflow_spec,
    write_sub_workflow_parent_spec,
    write_workflow_patch,
)
from tests.request_assertions import first_dry_run_request

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]

# DS 3.3.1 checks processDefinitionCode although its task model writes
# workflowDefinitionCode; 3.3.2 fixes the guard's constant.
_SUB_WORKFLOW_ONLINE_GUARD_VERSIONS = frozenset(
    {
        "3.2.1",
        "3.2.2",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_SUB_WORKFLOW_NATIVE_TASK_TYPE_VERSIONS = frozenset(
    {
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
# TaskInstanceController first accepts taskExecuteType in DS 3.1.0.
_TASK_EXECUTE_TYPE_FILTER_VERSIONS = frozenset(
    {
        *(f"3.1.{patch}" for patch in range(10)),
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_INSTANCE_DAG_EDIT_REQUIRES_SYNC_VERSIONS = frozenset({"2.0.0", "2.0.1", "2.0.2"})


def _task_log_contains(
    result: DsctlCommandResult,
    *,
    marker: str,
) -> bool:
    data = _ok_payload_data(result, action="task-instance.log")
    if data is None:
        return False
    text = data.get("text")
    line_count = data.get("lineCount")
    return (
        isinstance(text, str)
        and marker in text
        and isinstance(line_count, int)
        and line_count > 0
    )


def _workflow_instance_digest_has_tasks(
    result: DsctlCommandResult,
    *,
    count: int,
) -> bool:
    data = _ok_payload_data(result, action="workflow-instance.digest")
    if data is None:
        return False
    task_count = data.get("taskCount")
    return isinstance(task_count, int) and task_count >= count


def _task_instance_list_has_rows(
    result: DsctlCommandResult,
    *,
    count: int,
) -> bool:
    data = _ok_payload_data(result, action="task-instance.list")
    if not isinstance(data, dict):
        return False
    rows = data.get("totalList")
    return isinstance(rows, list) and len(rows) >= count


def _task_instance_sub_workflow_ready(
    result: DsctlCommandResult,
) -> bool:
    data = _ok_payload_data(result, action="task-instance.sub-workflow")
    if not isinstance(data, dict):
        return False
    sub_workflow_instance_id = data.get("subWorkflowInstanceId")
    return isinstance(sub_workflow_instance_id, int) and sub_workflow_instance_id > 0


def _ok_payload_data(
    result: DsctlCommandResult,
    *,
    action: str,
) -> dict[str, object] | None:
    if result.exit_code != 0:
        return None
    payload = result.payload
    if payload.get("ok") is not True or payload.get("action") != action:
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return data


def _task_rows_by_name(task_rows: list[object]) -> dict[str, dict[str, object]]:
    rows_by_name: dict[str, dict[str, object]] = {}
    for item in task_rows:
        row = require_mapping(item, label="task row")
        name = require_text_value(row.get("name"), label="task row name")
        rows_by_name[name] = row
    return rows_by_name


def _exported_instance_task_names(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    instance_id: int,
) -> set[str]:
    result = run_dsctl_raw(
        repo_root,
        ["workflow-instance", "export", str(instance_id), "--project", project],
        env_file=env_file,
    )
    assert result.exit_code == 0, result.stderr
    document = require_mapping(
        yaml.safe_load(result.stdout), label="workflow-instance export document"
    )
    tasks = require_list(document.get("tasks"), label="workflow-instance export tasks")
    return {
        require_text_value(
            require_mapping(task, label="workflow-instance export task").get("name"),
            label="workflow-instance export task name",
        )
        for task in tasks
    }


def _workflow_definition_version(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow: str,
) -> int:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["workflow", "get", workflow, "--project", project],
            env_file=env_file,
        ),
        expected_action="workflow.get",
        label="workflow definition version read",
    )
    data = require_mapping(payload["data"], label="workflow definition version data")
    return require_int_value(data.get("version"), label="workflow definition version")


def _assert_workflow_version_after_update(
    *,
    ds_version: str | None,
    before: int,
    after: int,
    change: Literal["task_update", "workflow_edit", "instance_sync"],
) -> None:
    # 2.0.0-2.0.2 compare definition metadata (including locations) but not
    # task/relation content when deciding whether to save a new version.
    # 2.0.3 also considers changed tasks and its instance edit always saves.
    if ds_version == "1.3.9" or (
        change in {"task_update", "instance_sync"}
        and ds_version in _INSTANCE_DAG_EDIT_REQUIRES_SYNC_VERSIONS
    ):
        assert after == before
    else:
        assert after > before


def _native_definition_identity(
    data: dict[str, object],
    *,
    ds_version: str | None,
    label: str,
) -> int:
    field = "id" if ds_version == "1.3.9" else "code"
    return require_int_value(data.get(field), label=f"{label} {field}")


def _project_identity_field(ds_version: str | None) -> str:
    return "projectId" if ds_version == "1.3.9" else "projectCode"


def test_etl_workflow_definition_and_runtime_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_etl_ds_version: str | None,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("workflow-project")
    workflow_name = live_name_factory("workflow")
    extract_marker = f"{workflow_name}-extract-marker"
    load_marker = f"{workflow_name}-load-marker"
    workflow_spec = write_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        extract_marker=extract_marker,
        load_marker=load_marker,
    )

    project_created = False
    workflow_created = False
    workflow_deleted = False
    workflow_instance_id: int | None = None

    try:
        project_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "workflow runtime live test project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_create_data = require_mapping(
            project_create_payload["data"],
            label="project create data",
        )
        project_identity = _native_definition_identity(
            project_create_data,
            ds_version=live_etl_ds_version,
            label="project",
        )
        project_created = True

        workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "create",
                    "--file",
                    str(workflow_spec),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="workflow create",
        )
        workflow_create_data = require_mapping(
            workflow_create_payload["data"],
            label="workflow create data",
        )
        workflow_identity = _native_definition_identity(
            workflow_create_data,
            ds_version=live_etl_ds_version,
            label="workflow",
        )
        assert workflow_create_data["name"] == workflow_name
        assert (
            workflow_create_data[_project_identity_field(live_etl_ds_version)]
            == project_identity
        )
        assert workflow_create_data["releaseState"] == "OFFLINE"
        workflow_created = True

        workflow_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "get",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.get",
            label="workflow get",
        )
        workflow_get_data = require_mapping(
            workflow_get_payload["data"],
            label="workflow get data",
        )
        assert (
            _native_definition_identity(
                workflow_get_data,
                ds_version=live_etl_ds_version,
                label="workflow get",
            )
            == workflow_identity
        )
        assert workflow_get_data["releaseState"] == "OFFLINE"
        initial_workflow_version = require_int_value(
            workflow_get_data.get("version"),
            label="initial workflow version",
        )

        workflow_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "list",
                    "--project",
                    project_name,
                    "--search",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.list",
            label="workflow list",
        )
        workflow_list_data = require_mapping(
            workflow_list_payload["data"],
            label="workflow list page",
        )
        workflow_rows = require_list(
            workflow_list_data["totalList"],
            label="workflow list rows",
        )
        assert any(
            _native_definition_identity(
                require_mapping(item, label="workflow row"),
                ds_version=live_etl_ds_version,
                label="workflow row",
            )
            == workflow_identity
            for item in workflow_rows
        )

        workflow_describe_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "describe",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.describe",
            label="workflow describe",
        )
        workflow_describe_data = require_mapping(
            workflow_describe_payload["data"],
            label="workflow describe data",
        )
        described_tasks = require_list(
            workflow_describe_data["tasks"],
            label="workflow describe tasks",
        )
        described_relations = require_list(
            workflow_describe_data["relations"],
            label="workflow describe relations",
        )
        assert len(described_tasks) == 2
        if live_etl_ds_version == "1.3.9":
            described_tasks_by_name = _task_rows_by_name(described_tasks)
            assert len(described_relations) == 1
            relation = require_mapping(
                described_relations[0],
                label="legacy workflow describe relation",
            )
            assert relation["preTaskName"] == "extract"
            assert relation["postTaskName"] == "load"
            assert relation["preTaskId"] == described_tasks_by_name["extract"]["id"]
            assert relation["postTaskId"] == described_tasks_by_name["load"]["id"]
            assert "preTaskCode" not in relation
            assert "postTaskCode" not in relation
        else:
            root_relations = [
                require_mapping(item, label="workflow describe root relation")
                for item in described_relations
                if require_mapping(
                    item,
                    label="workflow describe relation",
                ).get("preTaskCode")
                == 0
            ]
            explicit_relations = [
                require_mapping(item, label="workflow describe explicit relation")
                for item in described_relations
                if require_mapping(
                    item,
                    label="workflow describe relation",
                ).get("preTaskCode")
                != 0
            ]
            assert len(root_relations) == 1
            assert len(explicit_relations) == 1

        workflow_digest_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "digest",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.digest",
            label="workflow digest",
        )
        workflow_digest_data = require_mapping(
            workflow_digest_payload["data"],
            label="workflow digest data",
        )
        assert workflow_digest_data["taskCount"] == 2
        assert workflow_digest_data["relationCount"] == 1
        task_type_counts = require_mapping(
            workflow_digest_data["taskTypeCounts"],
            label="workflow digest taskTypeCounts",
        )
        assert task_type_counts["SHELL"] == 2
        root_tasks = require_list(
            workflow_digest_data["rootTasks"],
            label="workflow digest rootTasks",
        )
        leaf_tasks = require_list(
            workflow_digest_data["leafTasks"],
            label="workflow digest leafTasks",
        )
        assert (
            require_mapping(root_tasks[0], label="workflow digest root task")["name"]
            == "extract"
        )
        assert (
            require_mapping(leaf_tasks[0], label="workflow digest leaf task")["name"]
            == "load"
        )

        task_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.list",
            label="task list",
        )
        task_rows = require_list(task_list_payload["data"], label="task list data")
        task_rows_by_name = _task_rows_by_name(task_rows)
        assert set(task_rows_by_name) == {"extract", "load"}
        if live_etl_ds_version == "1.3.9":
            extract_task_id = require_text_value(
                task_rows_by_name["extract"].get("id"),
                label="extract task id",
            )
            load_task_id = require_text_value(
                task_rows_by_name["load"].get("id"),
                label="load task id",
            )
            assert "code" not in task_rows_by_name["extract"]
            assert "version" not in task_rows_by_name["extract"]
            assert "code" not in task_rows_by_name["load"]
            assert "version" not in task_rows_by_name["load"]
        else:
            extract_task_code = require_int_value(
                task_rows_by_name["extract"].get("code"),
                label="extract task code",
            )
            extract_task_version = require_int_value(
                task_rows_by_name["extract"].get("version"),
                label="extract task version",
            )
            load_task_code = require_int_value(
                task_rows_by_name["load"].get("code"),
                label="load task code",
            )
            load_task_version = require_int_value(
                task_rows_by_name["load"].get("version"),
                label="load task version",
            )
            assert extract_task_version >= 1
            assert load_task_version >= 1

        task_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "get",
                    "extract",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.get",
            label="task get",
        )
        task_get_data = require_mapping(task_get_payload["data"], label="task get data")
        task_params = require_mapping(
            task_get_data["taskParams"],
            label="task get taskParams",
        )
        raw_script = require_text_value(
            task_params.get("rawScript"),
            label="task rawScript",
        )
        assert extract_marker in raw_script

        updated_load_marker = f"{workflow_name}-load-marker-updated"
        task_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "update",
                    "load",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                    "--set",
                    f"command=echo {updated_load_marker}",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.update",
            label="task update",
        )
        task_update_data = require_mapping(
            task_update_payload["data"],
            label="task update data",
        )
        updated_task_params = require_mapping(
            task_update_data["taskParams"],
            label="task update taskParams",
        )
        assert updated_load_marker in require_text_value(
            updated_task_params.get("rawScript"),
            label="task update rawScript",
        )
        if live_etl_ds_version == "1.3.9":
            assert "code" not in task_update_data
            assert "version" not in task_update_data
        else:
            updated_load_version = require_int_value(
                task_update_data.get("version"),
                label="task update version",
            )
            assert updated_load_version > load_task_version

        workflow_get_after_task_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "get",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.get",
            label="workflow get after task update",
        )
        workflow_get_after_task_update_data = require_mapping(
            workflow_get_after_task_update_payload["data"],
            label="workflow get after task update data",
        )
        workflow_version_after_task_update = require_int_value(
            workflow_get_after_task_update_data.get("version"),
            label="workflow version after task update",
        )
        _assert_workflow_version_after_update(
            ds_version=live_etl_ds_version,
            before=initial_workflow_version,
            after=workflow_version_after_task_update,
            change="task_update",
        )

        workflow_patch = write_workflow_patch(
            tmp_path / f"{workflow_name}.patch.yaml",
            workflow_description="live workflow/runtime round trip edited",
            renames=[("load", "load-stage")],
        )
        workflow_edit_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "edit",
                    workflow_name,
                    "--project",
                    project_name,
                    "--patch",
                    str(workflow_patch),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.edit",
            label="workflow edit",
        )
        workflow_edit_data = require_mapping(
            workflow_edit_payload["data"],
            label="workflow edit data",
        )
        assert (
            workflow_edit_data["description"]
            == "live workflow/runtime round trip edited"
        )
        workflow_version_after_edit = require_int_value(
            workflow_edit_data.get("version"),
            label="workflow version after edit",
        )
        _assert_workflow_version_after_update(
            ds_version=live_etl_ds_version,
            before=workflow_version_after_task_update,
            after=workflow_version_after_edit,
            change="workflow_edit",
        )

        renamed_task_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.list",
            label="task list after workflow edit",
        )
        renamed_task_rows = require_list(
            renamed_task_list_payload["data"],
            label="task list after workflow edit data",
        )
        renamed_task_rows_by_name = _task_rows_by_name(renamed_task_rows)
        assert set(renamed_task_rows_by_name) == {"extract", "load-stage"}
        if live_etl_ds_version == "1.3.9":
            assert (
                require_text_value(
                    renamed_task_rows_by_name["extract"].get("id"),
                    label="extract task id after workflow edit",
                )
                == extract_task_id
            )
            assert (
                require_text_value(
                    renamed_task_rows_by_name["load-stage"].get("id"),
                    label="renamed load task id",
                )
                == load_task_id
            )
            assert "code" not in renamed_task_rows_by_name["extract"]
            assert "version" not in renamed_task_rows_by_name["extract"]
            assert "code" not in renamed_task_rows_by_name["load-stage"]
            assert "version" not in renamed_task_rows_by_name["load-stage"]
        else:
            assert (
                require_int_value(
                    renamed_task_rows_by_name["extract"].get("code"),
                    label="extract task code after workflow edit",
                )
                == extract_task_code
            )
            assert (
                require_int_value(
                    renamed_task_rows_by_name["extract"].get("version"),
                    label="extract task version after workflow edit",
                )
                == extract_task_version
            )
            assert (
                require_int_value(
                    renamed_task_rows_by_name["load-stage"].get("code"),
                    label="renamed load task code",
                )
                == load_task_code
            )
            renamed_load_version = require_int_value(
                renamed_task_rows_by_name["load-stage"].get("version"),
                label="renamed load task version",
            )
            assert renamed_load_version > updated_load_version

        renamed_task_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "get",
                    "load-stage",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.get",
            label="renamed task get",
        )
        renamed_task_get_data = require_mapping(
            renamed_task_get_payload["data"],
            label="renamed task get data",
        )
        renamed_task_params = require_mapping(
            renamed_task_get_data["taskParams"],
            label="renamed task taskParams",
        )
        assert updated_load_marker in require_text_value(
            renamed_task_params.get("rawScript"),
            label="renamed task rawScript",
        )

        workflow_yaml_result = run_dsctl_raw(
            live_repo_root,
            [
                "workflow",
                "export",
                workflow_name,
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
        )
        assert workflow_yaml_result.exit_code == 0, workflow_yaml_result.stderr
        workflow_yaml_text = workflow_yaml_result.stdout
        workflow_yaml_document = yaml.safe_load(workflow_yaml_text)
        workflow_yaml_tasks = workflow_yaml_document["tasks"]
        assert all("code" not in task for task in workflow_yaml_tasks)
        assert all("version" not in task for task in workflow_yaml_tasks)
        assert {task["name"] for task in workflow_yaml_tasks} == {
            "extract",
            "load-stage",
        }

        workflow_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="workflow online",
        )
        workflow_online_data = require_mapping(
            workflow_online_payload["data"],
            label="workflow online data",
        )
        assert workflow_online_data["releaseState"] == "ONLINE"

        workflow_online_edit_patch = write_workflow_patch(
            tmp_path / f"{workflow_name}.online.patch.yaml",
            workflow_description="live workflow/runtime online edit should fail",
        )
        workflow_online_edit_result = run_dsctl(
            live_repo_root,
            [
                "workflow",
                "edit",
                workflow_name,
                "--project",
                project_name,
                "--patch",
                str(workflow_online_edit_patch),
            ],
            env_file=live_etl_env_file,
        )
        workflow_online_edit_error = require_error_payload(
            workflow_online_edit_result,
            expected_action="workflow.edit",
            expected_type="invalid_state",
            label="workflow edit while online",
        )
        assert result_error_code(workflow_online_edit_result) is None
        assert "must be offline" in require_text_value(
            workflow_online_edit_error.get("message"),
            label="workflow edit while online message",
        )
        workflow_online_edit_details = require_mapping(
            workflow_online_edit_error.get("details"),
            label="workflow edit while online details",
        )
        workflow_constraint = require_mapping(
            workflow_online_edit_details.get("constraint_detail"),
            label="workflow edit while online constraint detail",
        )
        assert workflow_constraint["code"] == "workflow_must_be_offline"
        assert workflow_constraint["blocking"] is True
        schedule_impact_raw = workflow_online_edit_details.get("schedule_impact_detail")
        if schedule_impact_raw is not None:
            schedule_impact = require_mapping(
                schedule_impact_raw,
                label="workflow edit while online schedule impact detail",
            )
            assert schedule_impact["code"] == "offline_also_offlines_attached_schedule"

        workflow_run_task_dry_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "run-task",
                    workflow_name,
                    "--project",
                    project_name,
                    "--task",
                    "extract",
                    "--dry-run",
                    "--columns",
                    "*",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run-task",
            label="workflow run-task dry-run",
        )
        workflow_run_task_dry_run_data = require_mapping(
            workflow_run_task_dry_run_payload["data"],
            label="workflow run-task dry-run data",
        )
        workflow_run_task_dry_run_request = require_mapping(
            first_dry_run_request(workflow_run_task_dry_run_data),
            label="workflow run-task dry-run request",
        )
        workflow_run_task_dry_run_form = require_mapping(
            workflow_run_task_dry_run_request["form"],
            label="workflow run-task dry-run form",
        )
        assert workflow_run_task_dry_run_form["taskDependType"] == "TASK_ONLY"

        workflow_backfill_dry_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "backfill",
                    workflow_name,
                    "--project",
                    project_name,
                    "--task",
                    "extract",
                    "--start",
                    "2026-04-01 00:00:00",
                    "--end",
                    "2026-04-02 00:00:00",
                    "--dry-run",
                    "--columns",
                    "*",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.backfill",
            label="workflow backfill dry-run",
        )
        workflow_backfill_dry_run_data = require_mapping(
            workflow_backfill_dry_run_payload["data"],
            label="workflow backfill dry-run data",
        )
        workflow_backfill_dry_run_request = require_mapping(
            first_dry_run_request(workflow_backfill_dry_run_data),
            label="workflow backfill dry-run request",
        )
        workflow_backfill_dry_run_form = require_mapping(
            workflow_backfill_dry_run_request["form"],
            label="workflow backfill dry-run form",
        )
        assert workflow_backfill_dry_run_form["execType"] == "COMPLEMENT_DATA"
        assert workflow_backfill_dry_run_form["taskDependType"] == "TASK_ONLY"

        workflow_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "run",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="workflow run",
        )
        workflow_run_data = require_mapping(
            workflow_run_payload["data"],
            label="workflow run data",
        )
        workflow_instance_id = _execution_instance_id(
            workflow_run_data,
            repo_root=live_repo_root,
            env_file=live_etl_env_file,
            project=project_name,
            fresh_workflow=workflow_name,
            label="workflow run",
        )

        workflow_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.watch",
            label="workflow-instance watch",
        )
        workflow_watch_data = require_mapping(
            workflow_watch_payload["data"],
            label="workflow-instance watch data",
        )
        assert workflow_watch_data["id"] == workflow_instance_id
        assert workflow_watch_data["state"] == "SUCCESS"

        workflow_instance_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "get",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.get",
            label="workflow-instance get",
        )
        workflow_instance_get_data = require_mapping(
            workflow_instance_get_payload["data"],
            label="workflow-instance get data",
        )
        instance_definition_field = (
            "workflowDefinitionId"
            if live_etl_ds_version == "1.3.9"
            else "workflowDefinitionCode"
        )
        assert (
            workflow_instance_get_data[instance_definition_field] == workflow_identity
        )
        assert workflow_instance_get_data["state"] == "SUCCESS"

        workflow_instance_yaml_result = run_dsctl_raw(
            live_repo_root,
            [
                "workflow-instance",
                "export",
                str(workflow_instance_id),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
        )
        assert workflow_instance_yaml_result.exit_code == 0
        assert workflow_instance_yaml_result.stdout.startswith("workflow:\n")
        assert "tasks:" in workflow_instance_yaml_result.stdout

        workflow_instance_digest_result = wait_for_result(
            live_repo_root,
            [
                "workflow-instance",
                "digest",
                str(workflow_instance_id),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda result: _workflow_instance_digest_has_tasks(
                result,
                count=2,
            ),
        )
        workflow_instance_digest_payload = require_ok_payload(
            workflow_instance_digest_result,
            expected_action="workflow-instance.digest",
            label="workflow-instance digest",
        )
        workflow_instance_digest_data = require_mapping(
            workflow_instance_digest_payload["data"],
            label="workflow-instance digest data",
        )
        assert workflow_instance_digest_data["taskCount"] == 2
        progress = require_mapping(
            workflow_instance_digest_data["progress"],
            label="workflow-instance digest progress",
        )
        assert progress["success"] == 2
        assert progress["failed"] == 0

        workflow_instance_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.list",
            label="workflow-instance list",
        )
        workflow_instance_list_data = require_mapping(
            workflow_instance_list_payload["data"],
            label="workflow-instance list data",
        )
        workflow_instance_rows = require_list(
            workflow_instance_list_data["totalList"],
            label="workflow-instance rows",
        )
        assert any(
            require_mapping(item, label="workflow-instance row").get("id")
            == workflow_instance_id
            for item in workflow_instance_rows
        )

        task_instance_list_result = wait_for_result(
            live_repo_root,
            [
                "task-instance",
                "list",
                "--workflow-instance",
                str(workflow_instance_id),
                "--page-size",
                "20",
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda result: _task_instance_list_has_rows(
                result,
                count=2,
            ),
        )
        task_instance_list_payload = require_ok_payload(
            task_instance_list_result,
            expected_action="task-instance.list",
            label="task-instance list",
        )
        task_instance_list_data = require_mapping(
            task_instance_list_payload["data"],
            label="task-instance list data",
        )
        task_instance_rows = require_list(
            task_instance_list_data["totalList"],
            label="task-instance rows",
        )
        assert len(task_instance_rows) == 2
        task_instances_by_name = {
            require_text_value(
                require_mapping(item, label="task-instance row").get("name"),
                label="task-instance name",
            ): require_mapping(item, label="task-instance row")
            for item in task_instance_rows
        }
        extract_task_instance_id = require_int_value(
            task_instances_by_name["extract"].get("id"),
            label="extract task-instance id",
        )

        project_task_instance_list_result = wait_for_result(
            live_repo_root,
            [
                "task-instance",
                "list",
                "--project",
                project_name,
                "--task",
                "extract",
                "--state",
                "SUCCESS",
                *(
                    ["--execute-type", "BATCH"]
                    if live_etl_ds_version in _TASK_EXECUTE_TYPE_FILTER_VERSIONS
                    else []
                ),
                "--start",
                "2020-01-01 00:00:00",
                "--end",
                "2099-01-01 00:00:00",
                "--page-size",
                "20",
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda result: _task_instance_list_has_rows(
                result,
                count=1,
            ),
        )
        project_task_instance_list_payload = require_ok_payload(
            project_task_instance_list_result,
            expected_action="task-instance.list",
            label="project-scoped task-instance list",
        )
        project_task_instance_list_data = require_mapping(
            project_task_instance_list_payload["data"],
            label="project-scoped task-instance list data",
        )
        project_task_instance_rows = require_list(
            project_task_instance_list_data["totalList"],
            label="project-scoped task-instance rows",
        )
        assert any(
            require_mapping(item, label="project-scoped task-instance row").get("id")
            == extract_task_instance_id
            for item in project_task_instance_rows
        )

        task_instance_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "watch",
                    str(extract_task_instance_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "60",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=70.0,
            ),
            expected_action="task-instance.watch",
            label="task-instance watch",
        )
        task_instance_watch_data = require_mapping(
            task_instance_watch_payload["data"],
            label="task-instance watch data",
        )
        assert task_instance_watch_data["id"] == extract_task_instance_id
        assert task_instance_watch_data["state"] == "SUCCESS"

        task_instance_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "get",
                    str(extract_task_instance_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.get",
            label="task-instance get",
        )
        task_instance_get_data = require_mapping(
            task_instance_get_payload["data"],
            label="task-instance get data",
        )
        assert task_instance_get_data["id"] == extract_task_instance_id
        assert task_instance_get_data["state"] == "SUCCESS"
        assert task_instance_get_data["workflowInstanceId"] == workflow_instance_id

        task_log_result = wait_for_result(
            live_repo_root,
            [
                "task-instance",
                "log",
                str(extract_task_instance_id),
                "--tail",
                "50",
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda result: _task_log_contains(
                result,
                marker=extract_marker,
            ),
        )
        task_log_payload = require_ok_payload(
            task_log_result,
            expected_action="task-instance.log",
            label="task-instance log",
        )
        task_log_data = require_mapping(
            task_log_payload["data"],
            label="task-instance log data",
        )
        assert extract_marker in require_text_value(
            task_log_data.get("text"),
            label="task-instance log text",
        )
        assert (
            require_int_value(
                task_log_data.get("lineCount"),
                label="task-instance log lineCount",
            )
            > 0
        )

        workflow_offline_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "offline",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.offline",
            label="workflow offline",
        )
        workflow_offline_data = require_mapping(
            workflow_offline_payload["data"],
            label="workflow offline data",
        )
        assert workflow_offline_data["releaseState"] == "OFFLINE"

        workflow_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    workflow_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.delete",
            label="workflow delete",
        )
        workflow_delete_data = require_mapping(
            workflow_delete_payload["data"],
            label="workflow delete data",
        )
        assert workflow_delete_data["deleted"] is True
        workflow_deleted = True
    finally:
        if workflow_instance_id is not None and not workflow_deleted:
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            )
        if workflow_created and not workflow_deleted:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )


def test_workflow_instance_edit_respects_sync_definition_flag(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_etl_ds_version: str | None,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("instance-update-project")
    workflow_name = live_name_factory("instance-update-workflow")
    extract_marker = f"{workflow_name}-extract"
    load_marker = f"{workflow_name}-load"
    workflow_spec = write_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        extract_marker=extract_marker,
        load_marker=load_marker,
    )
    no_sync_patch = write_workflow_patch(
        tmp_path / f"{workflow_name}-instance-no-sync.patch.yaml",
        renames=[("extract", "extract-instance-only")],
    )
    sync_patch = write_workflow_patch(
        tmp_path / f"{workflow_name}-instance-sync.patch.yaml",
        renames=[
            (
                "extract"
                if live_etl_ds_version in _INSTANCE_DAG_EDIT_REQUIRES_SYNC_VERSIONS
                else "extract-instance-only",
                "extract-synced",
            )
        ],
    )

    project_created = False
    workflow_created = False
    workflow_deleted = False

    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "live workflow-instance edit project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="workflow create",
        )
        workflow_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="workflow online before run",
        )

        initial_task_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.list",
            label="initial task list",
        )
        initial_task_rows = require_list(
            initial_task_list_payload["data"],
            label="initial task list data",
        )
        assert set(_task_rows_by_name(initial_task_rows)) == {"extract", "load"}

        workflow_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "run",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="workflow run",
        )
        workflow_run_data = require_mapping(
            workflow_run_payload["data"],
            label="workflow run data",
        )
        workflow_instance_id = _execution_instance_id(
            workflow_run_data,
            repo_root=live_repo_root,
            env_file=live_etl_env_file,
            project=project_name,
            fresh_workflow=workflow_name,
            label="workflow run",
        )

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.watch",
            label="workflow-instance watch",
        )

        initial_instance_tasks = _exported_instance_task_names(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            instance_id=workflow_instance_id,
        )
        assert initial_instance_tasks == {"extract", "load"}
        initial_instance_version: int | None = None
        initial_definition_version: int | None = None
        if live_etl_ds_version in _INSTANCE_DAG_EDIT_REQUIRES_SYNC_VERSIONS:
            initial_definition_version = _workflow_definition_version(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
            initial_instance_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    [
                        "workflow-instance",
                        "get",
                        str(workflow_instance_id),
                        "--project",
                        project_name,
                    ],
                    env_file=live_etl_env_file,
                ),
                expected_action="workflow-instance.get",
                label="workflow-instance get before rejected edit",
            )
            initial_instance_data = require_mapping(
                initial_instance_payload["data"],
                label="workflow-instance before rejected edit data",
            )
            initial_instance_version = require_int_value(
                initial_instance_data.get("workflowDefinitionVersion"),
                label="workflow-instance version before rejected edit",
            )
        no_sync_update_result = run_dsctl(
            live_repo_root,
            [
                "workflow-instance",
                "edit",
                str(workflow_instance_id),
                "--patch",
                str(no_sync_patch),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
        )
        if live_etl_ds_version in _INSTANCE_DAG_EDIT_REQUIRES_SYNC_VERSIONS:
            no_sync_error = require_error_payload(
                no_sync_update_result,
                expected_action="workflow-instance.edit",
                expected_type="user_input_error",
                label="workflow-instance DAG edit without required sync-definition",
            )
            no_sync_details = require_mapping(
                no_sync_error.get("details"), label="no-sync edit error details"
            )
            assert no_sync_details["required_option"] == "--sync-definition"
            assert no_sync_details["ds_version"] == live_etl_ds_version
            assert no_sync_details["workflow_instance_id"] == workflow_instance_id
            assert (
                _exported_instance_task_names(
                    live_repo_root,
                    live_etl_env_file,
                    project=project_name,
                    instance_id=workflow_instance_id,
                )
                == initial_instance_tasks
            )
            rejected_instance_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    [
                        "workflow-instance",
                        "get",
                        str(workflow_instance_id),
                        "--project",
                        project_name,
                    ],
                    env_file=live_etl_env_file,
                ),
                expected_action="workflow-instance.get",
                label="workflow-instance get after rejected edit",
            )
            rejected_instance_data = require_mapping(
                rejected_instance_payload["data"],
                label="workflow-instance after rejected edit data",
            )
            no_sync_version = require_int_value(
                rejected_instance_data.get("workflowDefinitionVersion"),
                label="workflow-instance version after rejected edit",
            )
            assert no_sync_version == initial_instance_version
            assert (
                _workflow_definition_version(
                    live_repo_root,
                    live_etl_env_file,
                    project=project_name,
                    workflow=workflow_name,
                )
                == initial_definition_version
            )
        else:
            no_sync_update_payload = require_ok_payload(
                no_sync_update_result,
                expected_action="workflow-instance.edit",
                label="workflow-instance edit without sync-definition",
            )
            no_sync_update_resolved = require_mapping(
                no_sync_update_payload["resolved"],
                label="workflow-instance edit resolved",
            )
            no_sync_update_data = require_mapping(
                no_sync_update_payload["data"],
                label="workflow-instance edit data",
            )
            assert no_sync_update_resolved["syncDefine"] is False
            assert no_sync_update_data["id"] == workflow_instance_id
            no_sync_version = require_int_value(
                no_sync_update_data.get("workflowDefinitionVersion"),
                label="workflow-instance version after no-sync edit",
            )
            assert _exported_instance_task_names(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                instance_id=workflow_instance_id,
            ) == {"extract-instance-only", "load"}

        task_list_after_no_sync_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.list",
            label="task list after no-sync workflow-instance edit",
        )
        task_rows_after_no_sync = require_list(
            task_list_after_no_sync_payload["data"],
            label="task list after no-sync workflow-instance edit data",
        )
        assert set(_task_rows_by_name(task_rows_after_no_sync)) == {"extract", "load"}

        sync_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "edit",
                    str(workflow_instance_id),
                    "--patch",
                    str(sync_patch),
                    "--sync-definition",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.edit",
            label="workflow-instance edit with sync-definition",
        )
        sync_update_resolved = require_mapping(
            sync_update_payload["resolved"],
            label="workflow-instance sync edit resolved",
        )
        sync_update_data = require_mapping(
            sync_update_payload["data"],
            label="workflow-instance sync edit data",
        )
        assert sync_update_resolved["syncDefine"] is True
        assert sync_update_data["id"] == workflow_instance_id
        sync_version = require_int_value(
            sync_update_data.get("workflowDefinitionVersion"),
            label="workflow-instance version after sync-definition edit",
        )
        _assert_workflow_version_after_update(
            ds_version=live_etl_ds_version,
            before=no_sync_version,
            after=sync_version,
            change="instance_sync",
        )

        task_list_after_sync_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task.list",
            label="task list after sync-definition workflow-instance edit",
        )
        task_rows_after_sync = require_list(
            task_list_after_sync_payload["data"],
            label="task list after sync-definition workflow-instance edit data",
        )
        assert set(_task_rows_by_name(task_rows_after_sync)) == {
            "extract-synced",
            "load",
        }
        assert _exported_instance_task_names(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            instance_id=workflow_instance_id,
        ) == {"extract-synced", "load"}
    finally:
        if workflow_created and not workflow_deleted:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
            workflow_deleted = True
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )


def test_etl_sub_workflow_runtime_observes_online_guard_and_runs_child_instance(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_etl_ds_version: str | None,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("subworkflow-project")
    child_workflow_name = live_name_factory("child-workflow")
    parent_workflow_name = live_name_factory("parent-workflow")
    child_marker = f"{child_workflow_name}-marker"
    trailing_marker = f"{parent_workflow_name}-after-child"
    child_spec = write_single_shell_workflow_spec(
        tmp_path / f"{child_workflow_name}.yaml",
        project_name=project_name,
        workflow_name=child_workflow_name,
        task_name="child-shell",
        command=f'echo "{child_marker}"',
        description="live child workflow for sub-workflow runtime test",
    )

    project_created = False
    child_workflow_created = False
    parent_workflow_created = False
    child_workflow_deleted = False
    parent_workflow_deleted = False
    parent_instance_id: int | None = None
    child_instance_id: int | None = None

    try:
        project_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "live sub-workflow runtime project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_create_data = require_mapping(
            project_create_payload["data"],
            label="project create data",
        )
        project_identity = _native_definition_identity(
            project_create_data,
            ds_version=live_etl_ds_version,
            label="project",
        )
        project_created = True

        child_workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(child_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="child workflow create",
        )
        child_workflow_create_data = require_mapping(
            child_workflow_create_payload["data"],
            label="child workflow create data",
        )
        child_workflow_identity = _native_definition_identity(
            child_workflow_create_data,
            ds_version=live_etl_ds_version,
            label="child workflow",
        )
        assert (
            child_workflow_create_data[_project_identity_field(live_etl_ds_version)]
            == project_identity
        )
        child_workflow_created = True

        parent_spec = write_sub_workflow_parent_spec(
            tmp_path / f"{parent_workflow_name}.yaml",
            project_name=project_name,
            workflow_name=parent_workflow_name,
            child_workflow_name=(
                child_workflow_name if live_etl_ds_version == "1.3.9" else None
            ),
            child_workflow_code=(
                None if live_etl_ds_version == "1.3.9" else child_workflow_identity
            ),
            trailing_marker=trailing_marker,
        )
        parent_workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(parent_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="parent workflow create",
        )
        parent_workflow_create_data = require_mapping(
            parent_workflow_create_payload["data"],
            label="parent workflow create data",
        )
        assert (
            parent_workflow_create_data[_project_identity_field(live_etl_ds_version)]
            == project_identity
        )
        parent_workflow_created = True

        parent_online_result = run_dsctl(
            live_repo_root,
            ["workflow", "online", parent_workflow_name, "--project", project_name],
            env_file=live_etl_env_file,
        )
        if live_etl_ds_version not in _SUB_WORKFLOW_ONLINE_GUARD_VERSIONS:
            unguarded_parent_online_payload = require_ok_payload(
                parent_online_result,
                expected_action="workflow.online",
                label="unguarded parent workflow online before child",
            )
            unguarded_parent_online_data = require_mapping(
                unguarded_parent_online_payload["data"],
                label="unguarded parent workflow online data",
            )
            assert unguarded_parent_online_data["releaseState"] == "ONLINE"
        else:
            parent_online_error = require_error_payload(
                parent_online_result,
                expected_action="workflow.online",
                label="parent workflow online before child",
            )
            assert result_error_code(parent_online_result) == 10000
            assert parent_online_error["type"] == "invalid_state"
            assert "sub-workflows are already online" in require_text_value(
                parent_online_error.get("message"),
                label="parent workflow online error message",
            )

        child_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", child_workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="child workflow online",
        )
        child_online_data = require_mapping(
            child_online_payload["data"],
            label="child workflow online data",
        )
        assert child_online_data["releaseState"] == "ONLINE"

        parent_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", parent_workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="parent workflow online",
        )
        parent_online_data = require_mapping(
            parent_online_payload["data"],
            label="parent workflow online data",
        )
        assert parent_online_data["releaseState"] == "ONLINE"

        parent_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "run", parent_workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="parent workflow run",
        )
        parent_run_data = require_mapping(
            parent_run_payload["data"],
            label="parent workflow run data",
        )
        parent_instance_id = _execution_instance_id(
            parent_run_data,
            repo_root=live_repo_root,
            env_file=live_etl_env_file,
            project=project_name,
            fresh_workflow=parent_workflow_name,
            label="parent workflow run",
        )

        parent_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(parent_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            ),
            expected_action="workflow-instance.watch",
            label="parent workflow watch",
        )
        parent_watch_data = require_mapping(
            parent_watch_payload["data"],
            label="parent workflow watch data",
        )
        assert parent_watch_data["state"] == "SUCCESS"

        parent_task_list_result = wait_for_result(
            live_repo_root,
            [
                "task-instance",
                "list",
                "--workflow-instance",
                str(parent_instance_id),
                "--page-size",
                "100",
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            accept=lambda current: _task_instance_list_has_rows(current, count=2),
            timeout_seconds=60.0,
            interval_seconds=2.0,
        )
        parent_task_list_payload = require_ok_payload(
            parent_task_list_result,
            expected_action="task-instance.list",
            label="parent task-instance list",
        )
        parent_task_list_data = require_mapping(
            parent_task_list_payload["data"],
            label="parent task-instance list data",
        )
        parent_task_rows = require_list(
            parent_task_list_data["totalList"],
            label="parent task-instance rows",
        )
        sub_workflow_task_instance_id: int | None = None
        expected_sub_workflow_task_type = (
            "SUB_WORKFLOW"
            if live_etl_ds_version in _SUB_WORKFLOW_NATIVE_TASK_TYPE_VERSIONS
            else "SUB_PROCESS"
        )
        for row in parent_task_rows:
            row_data = require_mapping(row, label="parent task-instance row")
            if (
                row_data.get("taskType") == expected_sub_workflow_task_type
                and row_data.get("name") == "run-child"
            ):
                sub_workflow_task_instance_id = require_int_value(
                    row_data.get("id"),
                    label="sub-workflow task-instance id",
                )
                break
        if sub_workflow_task_instance_id is None:
            message = "parent workflow task list did not include run-child"
            raise AssertionError(message)

        child_relation_result = wait_for_result(
            live_repo_root,
            [
                "task-instance",
                "sub-workflow",
                str(sub_workflow_task_instance_id),
                "--workflow-instance",
                str(parent_instance_id),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            accept=_task_instance_sub_workflow_ready,
            timeout_seconds=60.0,
            interval_seconds=2.0,
        )
        child_relation_payload = require_ok_payload(
            child_relation_result,
            expected_action="task-instance.sub-workflow",
            label="task-instance sub-workflow",
        )
        child_relation_data = require_mapping(
            child_relation_payload["data"],
            label="task-instance sub-workflow data",
        )
        child_instance_id = require_int_value(
            child_relation_data.get("subWorkflowInstanceId"),
            label="child workflow-instance id",
        )

        parent_relation_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "parent",
                    str(child_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.parent",
            label="workflow-instance parent",
        )
        parent_relation_data = require_mapping(
            parent_relation_payload["data"],
            label="workflow-instance parent data",
        )
        assert parent_relation_data["parentWorkflowInstance"] == parent_instance_id

        child_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(child_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            ),
            expected_action="workflow-instance.watch",
            label="child workflow watch",
        )
        child_watch_data = require_mapping(
            child_watch_payload["data"],
            label="child workflow watch data",
        )
        assert child_watch_data["state"] == "SUCCESS"
    finally:
        if child_instance_id is not None:
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(child_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            )
        if parent_instance_id is not None:
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(parent_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "180",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            )
        if parent_workflow_created and not parent_workflow_deleted:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=parent_workflow_name,
            )
        if child_workflow_created and not child_workflow_deleted:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=child_workflow_name,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )
