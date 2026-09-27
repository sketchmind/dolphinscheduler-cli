"""Canonical task references and settings compiled into workflow graphs."""

import json
from pathlib import Path

import pytest
from tests.fakes import (
    FakeTaskAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    UserInputError,
)
from dsctl.services import workflow as workflow_service


def test_create_workflow_result_compiles_switch_branch_names_to_codes(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: switch-flow
  project: etl-prod
tasks:
  - name: route
    type: SWITCH
    task_params:
      switchResult:
        dependTaskList:
          - condition: ${route} == "A"
            nextNode: task-a
        nextNode: task-default
  - name: task-a
    type: SHELL
    command: echo A
  - name: task-default
    type: SHELL
    command: echo default
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    relations = json.loads(str(form["taskRelationJson"]))
    task_codes = {task["name"]: task["code"] for task in task_definitions}

    assert json.loads(task_definitions[0]["taskParams"]) == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "A"',
                    "nextNode": task_codes["task-a"],
                }
            ],
            "nextNode": task_codes["task-default"],
        }
    }
    assert relations == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": task_codes["route"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": task_codes["route"],
            "preTaskVersion": 1,
            "postTaskCode": task_codes["task-a"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": task_codes["route"],
            "preTaskVersion": 1,
            "postTaskCode": task_codes["task-default"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_workflow_result_compiles_task_group_settings(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: task-group-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    task_group_id: 21
    task_group_priority: 7
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    preview_code = task_definitions[0]["code"]
    assert isinstance(preview_code, int)
    assert preview_code > 0

    assert task_definitions == [
        {
            "code": preview_code,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {
                    "rawScript": "echo extract",
                    "localParams": [],
                    "resourceList": [],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "YES",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": -1,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "taskExecuteType": "BATCH",
            "taskGroupId": 21,
            "taskGroupPriority": 7,
            "cpuQuota": -1,
            "memoryMax": -1,
        }
    ]


def test_create_workflow_result_compiles_extended_task_execution_fields(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: task-runtime-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
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

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    preview_code = task_definitions[0]["code"]
    assert isinstance(preview_code, int)
    assert preview_code > 0

    assert task_definitions == [
        {
            "code": preview_code,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {
                    "rawScript": "echo extract",
                    "localParams": [],
                    "resourceList": [],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "NO",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": 42,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "OPEN",
            "timeoutNotifyStrategy": "FAILED",
            "timeout": 15,
            "delayTime": 0,
            "resourceIds": "",
            "taskExecuteType": "BATCH",
            "taskGroupId": 21,
            "taskGroupPriority": 7,
            "cpuQuota": 50,
            "memoryMax": 1024,
        }
    ]


def test_create_workflow_result_compiles_conditions_branch_names_to_codes(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: conditions-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: route
    type: CONDITIONS
    depends_on: [extract]
    task_params:
      dependence:
        relation: AND
        dependTaskList:
          - relation: AND
            dependItemList:
              - task: extract
                status: SUCCESS
      conditionResult:
        successNode: [on-success]
        failedNode: [on-failed]
  - name: on-success
    type: SHELL
    command: echo success
  - name: on-failed
    type: SHELL
    command: echo failed
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    relations = json.loads(str(form["taskRelationJson"]))
    task_codes = {task["name"]: task["code"] for task in task_definitions}

    assert json.loads(task_definitions[1]["taskParams"]) == {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "depTaskCode": task_codes["extract"],
                            "status": "SUCCESS",
                        }
                    ],
                }
            ],
        },
        "conditionResult": {
            "successNode": [task_codes["on-success"]],
            "failedNode": [task_codes["on-failed"]],
        },
    }
    assert relations == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": task_codes["extract"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": task_codes["extract"],
            "preTaskVersion": 1,
            "postTaskCode": task_codes["route"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": task_codes["route"],
            "preTaskVersion": 1,
            "postTaskCode": task_codes["on-success"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": task_codes["route"],
            "preTaskVersion": 1,
            "postTaskCode": task_codes["on-failed"],
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_workflow_result_rejects_dependency_cycles_locally(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    workflow_harness.install()
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: [load]
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="dependency cycle") as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        f"Run `dsctl lint workflow {spec_path}`, fix task names and references, "
        f"then retry `dsctl workflow create --file {spec_path} --dry-run`."
    )
    assert fake_task_adapter.generate_code_calls == []


def test_create_workflow_result_reports_unknown_task_dependency_with_suggestion(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install()
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: [missing]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="depends on unknown task") as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        f"Run `dsctl lint workflow {spec_path}`, fix task names and references, "
        f"then retry `dsctl workflow create --file {spec_path} --dry-run`."
    )
