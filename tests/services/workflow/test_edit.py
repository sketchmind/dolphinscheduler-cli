"""Task patch compilation, preservation, application, and edit errors."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.workflow import _types as workflow_types
from dsctl.upstream.wire import WireRequest


@pytest.mark.parametrize("dry_run", [False, True])
def test_203_workflow_dependency_rewire_never_reaches_mutation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    *,
    dry_run: bool,
) -> None:
    operations = workflow_harness.install(profile=make_profile(ds_version="2.0.3"))
    applied: list[object] = []
    monkeypatch.setattr(operations, "apply_update", applied.append)
    patch_path = tmp_path / "rewire.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match: {name: extract}
        set: {depends_on: [load]}
      - match: {name: load}
        set: {depends_on: []}
""".strip()
    )
    with pytest.raises(UnsupportedFeatureError, match="dependency edits"):
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=dry_run,
        )
    assert applied == []


def test_edit_workflow_dry_run_projects_the_domain_prepared_request(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    operations = workflow_harness.install()
    prepared_calls: list[dict[str, object]] = []
    applied: list[object] = []

    def prepare_update(_scope: object, **values: object) -> object:
        prepared_calls.append(values)
        return SimpleNamespace(
            request=WireRequest(
                method="PUT",
                path="/selected-version/process-definition/101",
                query=None,
                form={"exactUpdateField": "exact-value"},
                json=None,
                content=None,
            )
        )

    monkeypatch.setattr(operations, "prepare_update", prepare_update)
    monkeypatch.setattr(
        operations,
        "apply_update",
        applied.append,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: exact plan
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request == {
        "method": "PUT",
        "path": "/selected-version/process-definition/101",
        "form": {"exactUpdateField": "exact-value"},
    }
    assert len(prepared_calls) == 1
    assert applied == []


def test_edit_workflow_dry_run_translates_current_user_prepare_error_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install()
    applied: list[object] = []

    def prepare_update(_scope: object, **_values: object) -> object:
        raise ApiResultError(
            result_code=30001,
            result_message="current user has no operation permission",
        )

    monkeypatch.setattr(operations, "prepare_update", prepare_update)
    monkeypatch.setattr(
        operations,
        "apply_update",
        applied.append,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: exact plan
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        PermissionDeniedError,
        match="cannot edit workflow 'daily-sync'",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=True,
        )

    assert exc_info.value.details == {
        "resource": "workflow",
        "project": "etl-prod",
        "project_code": 7,
        "code": 101,
        "name": "daily-sync",
        "mutation_applied": False,
    }
    assert applied == []
    assert fake_workflow_adapter.update_calls == []


def test_edit_workflow_result_dry_run_emits_diff_and_update_request(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow v2
  tasks:
    rename:
      - from: extract
        to: extract-v2
    update:
      - match:
          name: load
        set:
          command: echo load v2
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    diff = _mapping(data["diff"])

    assert data["dry_run"] is True
    assert isinstance(result.failure, InvalidStateError)
    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/workflow-definition/101"
    assert diff["workflow_changes"] == [
        {
            "field": "description",
            "before": "Daily ETL workflow",
            "after": "Daily ETL workflow v2",
        }
    ]
    assert "workflow_updated_fields" not in diff
    assert diff["added_tasks"] == ["verify"]
    assert diff["task_changes"] == [
        {
            "task": "load",
            "changes": [
                {
                    "field": "task_params",
                    "before": {"rawScript": "echo load"},
                    "after": {"rawScript": "echo load v2"},
                }
            ],
        }
    ]
    assert "updated_tasks" not in diff
    assert diff["renamed_tasks"] == [
        {
            "from_name": "extract",
            "to_name": "extract-v2",
        }
    ]
    assert diff["added_edges"] == [
        {
            "from_task": "extract-v2",
            "to_task": "load",
        },
        {
            "from_task": "load",
            "to_task": "verify",
        },
    ]
    assert diff["removed_edges"] == [
        {
            "from_task": "extract",
            "to_task": "load",
        }
    ]
    assert data["workflow_state_constraints"] == [
        (
            "workflow is currently online; DolphinScheduler only allows "
            "whole-definition edits while offline"
        ),
        (
            "taking this workflow offline before apply will also take the "
            "attached schedule offline"
        ),
    ]
    assert data["workflow_state_constraint_details"] == [
        {
            "code": "workflow_must_be_offline",
            "message": (
                "workflow is currently online; DolphinScheduler only allows "
                "whole-definition edits while offline"
            ),
            "blocking": True,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": "ONLINE",
        },
        {
            "code": "offline_also_offlines_attached_schedule",
            "message": (
                "taking this workflow offline before apply will also take the "
                "attached schedule offline"
            ),
            "blocking": False,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": "ONLINE",
        },
    ]
    assert data["schedule_impacts"] == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately"
    ]
    assert data["schedule_impact_details"] == [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "ONLINE",
        }
    ]
    assert data["no_change"] is False
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    verify_code = task_definitions[2]["code"]
    assert isinstance(verify_code, int)
    assert verify_code > 0
    assert verify_code not in {201, 202}
    assert [
        (item["name"], item["code"], item["version"]) for item in task_definitions
    ] == [
        ("extract-v2", 201, 1),
        ("load", 202, 1),
        ("verify", verify_code, 1),
    ]
    assert json.loads(str(form["taskRelationJson"])) == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": 201,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 201,
            "preTaskVersion": 1,
            "postTaskCode": 202,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 202,
            "preTaskVersion": 1,
            "postTaskCode": verify_code,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_edit_workflow_result_requires_exactly_one_input_file(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text("patch:\n  workflow:\n    set:\n      timeout: 10\n")
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        "\n".join(
            [
                "workflow:",
                "  name: daily-sync",
                "tasks:",
                "  - name: extract",
                "    type: SHELL",
                "    command: echo extract",
                "",
            ]
        )
    )

    with pytest.raises(UserInputError, match="exactly one"):
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            file=workflow_path,
            project="etl-prod",
        )


def test_edit_workflow_result_dry_run_warns_on_risky_time_parameter_format(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          command: echo $[YYYYMMdd]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )

    assert result.warnings == [
        "dry run: no mutation was sent; lookup and verification reads may occur",
        "tasks[1].command contains $[YYYYMMdd]: uppercase year tokens such as "
        "YYYY use week-based year semantics in DS Java-style time patterns, not "
        "calendar year semantics.",
    ]
    assert [detail["code"] for detail in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "parameter_time_format_week_year_token",
    ]


def test_edit_workflow_result_dry_run_compiles_extended_task_execution_fields(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          flag: NO
          environment_code: 42
          task_group_id: 21
          task_group_priority: 7
          timeout: 15
          timeout_notify_strategy: FAILED
          cpu_quota: 50
          memory_max: 1024
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    diff = _mapping(data["diff"])
    form = _mapping(_mapping(first_dry_run_request(data))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))

    task_changes = diff["task_changes"]
    assert isinstance(task_changes, list)
    assert _mapping(task_changes[0])["task"] == "load"
    assert task_definitions[0]["code"] == 201
    assert task_definitions[0]["version"] == 1
    assert task_definitions[1]["code"] == 202
    assert task_definitions[1]["version"] == 1
    assert task_definitions[1]["flag"] == "NO"
    assert task_definitions[1]["environmentCode"] == 42
    assert task_definitions[1]["taskGroupId"] == 21
    assert task_definitions[1]["taskGroupPriority"] == 7
    assert task_definitions[1]["timeout"] == 15
    assert task_definitions[1]["timeoutFlag"] == "OPEN"
    assert task_definitions[1]["timeoutNotifyStrategy"] == "FAILED"
    assert task_definitions[1]["cpuQuota"] == 50
    assert task_definitions[1]["memoryMax"] == 1024


def test_edit_workflow_result_reports_invalid_task_patch_as_user_input_error(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          timeout_notify_strategy: FAILED
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="requires timeout > 0") as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=True,
        )

    assert exc_info.value.suggestion == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_edit_workflow_result_reports_patch_operation_conflict_suggestion(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - from: extract
        to: extract-v2
      - from: extract
        to: extract-v3
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="Patch renames task 'extract' more than once",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=True,
        )

    assert exc_info.value.suggestion == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_edit_workflow_result_requires_offline_before_apply(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
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

    with pytest.raises(InvalidStateError, match="must be offline") as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, then "
        "retry `dsctl workflow edit`. Review `schedule_impact_detail` before "
        "taking an attached schedule offline."
    )
    assert exc_info.value.details["current_release_state"] == "ONLINE"
    assert "schedule_impact" in exc_info.value.details
    assert exc_info.value.details["constraint_detail"] == {
        "code": "workflow_must_be_offline",
        "message": (
            "workflow is currently online; DolphinScheduler only allows "
            "whole-definition edits while offline"
        ),
        "blocking": True,
        "current_release_state": "ONLINE",
        "required_release_state": "OFFLINE",
        "current_schedule_release_state": "ONLINE",
    }
    assert exc_info.value.details["schedule_impact_detail"] == {
        "code": "offline_also_offlines_attached_schedule",
        "message": (
            "taking this workflow offline before apply will also take the "
            "attached schedule offline"
        ),
        "blocking": False,
        "current_release_state": "ONLINE",
        "required_release_state": "OFFLINE",
        "current_schedule_release_state": "ONLINE",
    }


def test_edit_workflow_result_rejects_invalid_patch_yaml_with_dry_run_suggestion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("DS_API_URL", "http://example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-token")
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
- patch:
    workflow:
      set:
        description: invalid
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="Workflow patch YAML root must be a mapping",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Fix the patch YAML, then retry the same command with `--dry-run` to "
        "inspect the compiled diff before apply."
    )


def test_edit_workflow_result_suggests_dry_run_for_remote_validation_error(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    offline_workflow = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    offline_workflow_adapter = replace(
        fake_workflow_adapter,
        workflows=[offline_workflow],
        dags={
            **fake_workflow_adapter.dags,
            101: replace(
                fake_workflow_adapter.dags[101],
                workflow_definition_value=offline_workflow,
            ),
        },
        update_errors_by_code={
            101: ApiResultError(
                result_code=workflow_types.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=offline_workflow_adapter,
    )
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

    with pytest.raises(
        UserInputError,
        match="workflow task relation invalid",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Retry the original workflow edit command with --dry-run to inspect the "
        "compiled diff and DS-native payload before sending it again."
    )


def test_edit_workflow_result_rewrites_switch_refs_after_task_rename(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    routed = FakeWorkflow(
        code=301,
        name="route-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    switch_task = FakeTaskDefinition(
        code=401,
        name="route",
        project_code_value=7,
        task_type_value="SWITCH",
        task_params_value=json.dumps(
            {
                "switchResult": {
                    "dependTaskList": [
                        {"condition": '${route} == "A"', "nextNode": 402}
                    ],
                    "nextNode": 403,
                },
                "nextBranch": 402,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        project_name_value="etl-prod",
    )
    task_a = FakeTaskDefinition(
        code=402,
        name="task-a",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo A"}',
        project_name_value="etl-prod",
    )
    task_default = FakeTaskDefinition(
        code=403,
        name="task-default",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo default"}',
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[routed],
        dags={
            301: FakeDag(
                workflow_definition_value=routed,
                task_definition_list_value=[switch_task, task_a, task_default],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=401,
                    ),
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=401,
                        post_task_code_value=402,
                    ),
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=401,
                        post_task_code_value=403,
                    ),
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
    rename:
      - from: task-a
        to: task-a-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "route-workflow",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)
    yaml_result = workflow_service.export_workflow_yaml_result(
        "301",
        project="etl-prod",
    )
    yaml_data = _mapping(yaml_result.data)
    document = yaml.safe_load(str(yaml_data["yaml"]))
    switch_params = document["tasks"][0]["task_params"]

    assert data["name"] == "route-workflow"
    assert workflow_adapter.update_calls[-1]["workflow_code"] == 301
    assert switch_params["switchResult"]["dependTaskList"][0]["nextNode"] == "task-a-v2"
    assert switch_params["switchResult"]["nextNode"] == "task-default"
    assert switch_params["nextBranch"] == "task-a-v2"


def test_edit_workflow_result_preserves_native_opaque_task_params_shape_after_apply(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    generic_workflow = FakeWorkflow(
        code=302,
        name="spark-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    spark_task = FakeTaskDefinition(
        code=410,
        name="spark-job",
        project_code_value=7,
        task_type_value="SPARK",
        task_params_value=json.dumps(
            {
                "programType": "JAVA",
                "mainClass": "com.example.jobs.SparkJob",
                "mainJar": {"id": 9},
                "deployMode": "cluster",
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[generic_workflow],
        dags={
            302: FakeDag(
                workflow_definition_value=generic_workflow,
                task_definition_list_value=[spark_task],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=410,
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
          name: spark-job
        set:
          task_params:
            programType: JAVA
            mainClass: com.example.jobs.SparkJobV2
            mainJar:
              id: 10
            deployMode: client
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "spark-workflow",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)
    yaml_result = workflow_service.export_workflow_yaml_result(
        "spark-workflow",
        project="etl-prod",
    )
    yaml_data = _mapping(yaml_result.data)
    document = yaml.safe_load(str(yaml_data["yaml"]))
    spec = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.1"),
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert data["name"] == "spark-workflow"
    assert workflow_adapter.update_calls[-1]["workflow_code"] == 302
    assert spec.tasks[0].type == "SPARK"
    assert spec.tasks[0].task_params == {
        "programType": "JAVA",
        "mainClass": "com.example.jobs.SparkJobV2",
        "mainJar": {"id": 10},
        "deployMode": "client",
    }
