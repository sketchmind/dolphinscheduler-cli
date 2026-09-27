"""Full-document reconciliation, task versions, no-op edits, and schedule snapshots."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ConfirmationRequiredError,
    ConflictError,
)
from dsctl.services import workflow as workflow_service


def test_edit_workflow_result_allocates_codes_only_for_new_tasks(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    offline_workflow = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    workflow_adapter = replace(
        fake_workflow_adapter,
        workflows=[offline_workflow, *fake_workflow_adapter.workflows[1:]],
        dags={
            **fake_workflow_adapter.dags,
            101: replace(
                fake_workflow_adapter.dags[101],
                workflow_definition_value=offline_workflow,
            ),
        },
    )
    fake_task_adapter.generated_codes = [8_101]
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    task_definitions = json.loads(
        str(workflow_adapter.update_calls[0]["task_definition_json"])
    )

    assert fake_task_adapter.generate_code_calls == [{"project_code": 7, "count": 1}]
    assert [(task["name"], task["code"]) for task in task_definitions] == [
        ("extract", 201),
        ("load", 202),
        ("verify", 8_101),
    ]


def test_edit_workflow_result_full_file_dry_run_reconciles_desired_state(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow v2
  timeout: 30
  global_params:
    env: prod
  execution_type: PARALLEL
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract v2
  - name: verify
    type: SHELL
    command: echo verify
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    diff = _mapping(data["diff"])

    assert data["dry_run"] is True
    assert result.resolved["input_mode"] == "file"
    assert result.resolved["file"] == str(workflow_path)
    assert diff["workflow_changes"] == [
        {
            "field": "description",
            "before": "Daily ETL workflow",
            "after": "Daily ETL workflow v2",
        }
    ]
    assert diff["added_tasks"] == ["verify"]
    assert diff["task_changes"] == [
        {
            "task": "extract",
            "changes": [
                {
                    "field": "task_params",
                    "before": {"rawScript": "echo extract"},
                    "after": {"rawScript": "echo extract v2"},
                }
            ],
        }
    ]
    assert diff["renamed_tasks"] == []
    assert diff["deleted_tasks"] == ["load"]
    assert diff["added_edges"] == [
        {
            "from_task": "extract",
            "to_task": "verify",
        }
    ]
    assert diff["removed_edges"] == [
        {
            "from_task": "extract",
            "to_task": "load",
        }
    ]
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    verify_code = task_definitions[1]["code"]
    assert isinstance(verify_code, int)
    assert verify_code > 0
    assert verify_code not in {201, 202}
    assert [
        (item["name"], item["code"], item["version"]) for item in task_definitions
    ] == [
        ("extract", 201, 1),
        ("verify", verify_code, 1),
    ]
    assert form["releaseState"] == "ONLINE"


def test_edit_workflow_result_full_file_requires_confirmation_for_deletion(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    offline_workflow = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    workflow_adapter = replace(
        fake_workflow_adapter,
        workflows=[offline_workflow],
        dags={
            **fake_workflow_adapter.dags,
            101: replace(
                fake_workflow_adapter.dags[101],
                workflow_definition_value=offline_workflow,
            ),
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow
  timeout: 30
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_service.edit_workflow_result(
            "daily-sync",
            file=workflow_path,
            project="etl-prod",
        )

    assert captured.value.details["risk_type"] == (
        "workflow_full_edit_destructive_change"
    )
    assert captured.value.details["deleted_tasks"] == ["load"]
    confirmation = str(captured.value.details["confirmation_token"])

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
        confirm_risk=confirmation,
    )

    assert _mapping(result.data)["description"] == "Daily ETL workflow"
    assert len(workflow_adapter.update_calls) == 1
    task_definitions = json.loads(
        str(workflow_adapter.update_calls[0]["task_definition_json"])
    )
    assert [(item["name"], item["code"]) for item in task_definitions] == [
        ("extract", 201)
    ]


def test_edit_workflow_result_accepts_exported_schedule_as_read_only_snapshot(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )
    exported = workflow_service.export_workflow_yaml_result(
        "daily-sync",
        project="etl-prod",
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        str(_mapping(exported.data)["yaml"]),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
        dry_run=True,
    )

    data = _mapping(result.data)
    assert data["dry_run"] is True
    assert data["no_change"] is True
    assert data["requests"] == []
    assert data["schedule_impacts"] == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately"
    ]
    assert fake_workflow_adapter.update_calls == []
    assert len(schedule_adapter.list_calls) == 2


def test_edit_workflow_result_rejects_changed_schedule_snapshot_before_mutation(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )
    exported = workflow_service.export_workflow_yaml_result(
        "daily-sync",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    document["schedule"]["cron"] = "0 30 2 * * ?"
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(document), encoding="utf-8")

    with pytest.raises(ConflictError) as captured:
        workflow_service.edit_workflow_result(
            "daily-sync",
            file=workflow_path,
            project="etl-prod",
            dry_run=True,
        )

    error = captured.value
    assert error.details["reason"] == "schedule_snapshot_mismatch"
    assert error.details["mismatched_fields"] == ["cron"]
    assert error.details["mutation_applied"] is False
    assert fake_workflow_adapter.update_calls == []
    assert fake_task_adapter.generate_code_calls == []


def test_edit_workflow_result_rejects_schedule_snapshot_when_none_is_attached(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        schedule_adapter=FakeScheduleAdapter(schedules=[]),
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: 0 0 2 * * ?
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConflictError) as captured:
        workflow_service.edit_workflow_result(
            "daily-sync",
            file=workflow_path,
            project="etl-prod",
            dry_run=True,
        )

    assert captured.value.details["reason"] == "attached_schedule_missing"
    assert captured.value.details["mutation_applied"] is False
    assert fake_workflow_adapter.update_calls == []


def test_edit_workflow_result_accepts_workflow_offline_schedule_cascade(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )
    exported = workflow_service.export_workflow_yaml_result(
        "daily-sync",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    document["workflow"]["description"] = "updated from exported document"
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(document), encoding="utf-8")

    workflow_service.offline_workflow_result("daily-sync", project="etl-prod")
    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
    )

    assert _mapping(result.data)["description"] == "updated from exported document"
    assert len(fake_workflow_adapter.update_calls) == 1
    assert schedule_adapter.schedules[0].id == 23
    assert schedule_adapter.schedules[0].crontab == "0 0 0 * * ?"
    assert schedule_adapter.schedules[0].releaseState == FakeEnumValue("OFFLINE")
    assert result.warnings == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately",
        (
            "this edit can bring the workflow back online, but any attached "
            "schedule remains offline until `schedule online` is requested"
        ),
    ]


def test_edit_workflow_result_applies_ds_like_task_version_bumps(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    offline_daily = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[0] = offline_daily
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_daily,
    )
    embedded_schedule = offline_daily.schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
                release_state_value=FakeEnumValue("OFFLINE"),
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          command: echo load v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    updated_dag = fake_workflow_adapter.dags[101]
    tasks_by_name = {
        str(task.name): task for task in updated_dag.taskDefinitionList or []
    }
    relations = updated_dag.workflowTaskRelationList or []

    assert _mapping(result.data)["version"] == 2
    assert len(schedule_adapter.list_calls) == 1
    assert tasks_by_name["extract"].code == 201
    assert tasks_by_name["extract"].version == 1
    assert tasks_by_name["load"].code == 202
    assert tasks_by_name["load"].version == 2
    assert len(relations) == 2
    assert relations[0].preTaskCode == 0
    assert relations[0].preTaskVersion == 0
    assert relations[0].postTaskCode == 201
    assert relations[0].postTaskVersion == 1
    assert relations[1].preTaskCode == 201
    assert relations[1].preTaskVersion == 1
    assert relations[1].postTaskCode == 202
    assert relations[1].postTaskVersion == 2


def test_edit_workflow_result_treats_semantic_default_task_patch_as_no_op(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        description="Daily ETL workflow",
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        timeout=30,
        release_state_value=FakeEnumValue("ONLINE"),
        schedule_release_state_value=FakeEnumValue("ONLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
        timeout=15,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[extract, load],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          worker_group: default
          timeout_notify_strategy: WARN
          cpu_quota: -1
          memory_max: -1
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )

    assert result.warnings[0] == (
        "patch produced no persistent workflow change; no update request was sent"
    )
    assert workflow_adapter.update_calls == []


def test_edit_workflow_result_warns_when_patch_is_a_no_op(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["description"] == "Daily ETL workflow"
    assert result.warnings == [
        "patch produced no persistent workflow change; no update request was sent",
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately",
    ]
    assert result.warning_details == [
        {
            "code": "workflow_edit_no_persistent_change",
            "message": (
                "patch produced no persistent workflow change; "
                "no update request was sent"
            ),
            "no_change": True,
            "request_sent": False,
        },
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "ONLINE",
        },
    ]
    assert fake_workflow_adapter.update_calls == []
    assert fake_task_adapter.generate_code_calls == []


def test_edit_workflow_result_emits_schedule_impact_warning_details_after_apply(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    daily = fake_workflow_adapter.workflows[0]
    schedule = daily.schedule
    assert schedule is not None
    offline_workflow = replace(
        daily,
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
        schedule_value=replace(
            schedule,
            release_state_value=FakeEnumValue("OFFLINE"),
        ),
    )
    fake_workflow_adapter.workflows[0] = offline_workflow
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_workflow,
    )
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["description"] == "Daily ETL workflow v2"
    assert result.warnings == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately"
    ]
    assert result.warning_details == [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "OFFLINE",
        }
    ]
    assert fake_workflow_adapter.update_calls[-1]["workflow_code"] == 101
