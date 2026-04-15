from pathlib import Path

import pytest

from dsctl.models import load_workflow_spec


def test_load_workflow_spec_accepts_typed_http_task_params(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: notify-workflow
tasks:
  - name: notify
    type: HTTP
    task_params:
      url: https://example.test/health
      httpMethod: GET
      connectTimeout: 10000
      httpParams:
        - prop: Authorization
          httpParametersType: HEADERS
          value: Bearer demo
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "url": "https://example.test/health",
        "httpMethod": "GET",
        "connectTimeout": 10000,
        "httpParams": [
            {
                "prop": "Authorization",
                "httpParametersType": "HEADERS",
                "value": "Bearer demo",
            }
        ],
    }


def test_load_workflow_spec_rejects_invalid_http_method(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: notify-workflow
tasks:
  - name: notify
    type: HTTP
    task_params:
      url: https://example.test/health
      httpMethod: PATCH
      connectTimeout: 10000
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Task 'notify' task_params\.httpMethod"):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_rejects_invalid_sql_datasource(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: sql-workflow
tasks:
  - name: query
    type: SQL
    task_params:
      type: MYSQL
      datasource: 0
      sql: select 1
      sqlType: 0
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Task 'query' task_params\.datasource"):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_allows_unknown_task_type_task_params(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: spark-workflow
tasks:
  - name: spark
    type: SPARK_SQL
    task_params:
      sql: select 1
      deployMode: client
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "sql": "select 1",
        "deployMode": "client",
    }


def test_load_workflow_spec_accepts_remote_shell_command_shorthand(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: remote-workflow
tasks:
  - name: remote
    type: REMOTESHELL
    command: echo remote
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].command == "echo remote"
    assert spec.tasks[0].type == "REMOTESHELL"


def test_load_workflow_spec_normalizes_remote_shell_alias(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: remote-workflow
tasks:
  - name: remote
    type: REMOTE_SHELL
    command: echo remote
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].type == "REMOTESHELL"


def test_load_workflow_spec_accepts_sub_workflow_task_params(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: sub-workflow
tasks:
  - name: child
    type: SUB_WORKFLOW
    task_params:
      workflowDefinitionCode: 1000000000001
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "workflowDefinitionCode": 1000000000001,
    }


def test_load_workflow_spec_rejects_invalid_sub_workflow_code(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: sub-workflow
tasks:
  - name: child
    type: SUB_WORKFLOW
    task_params:
      workflowDefinitionCode: 0
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"Task 'child' task_params\.workflowDefinitionCode",
    ):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_accepts_dependent_task_params(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: dependent-workflow
tasks:
  - name: wait-upstream
    type: DEPENDENT
    task_params:
      dependence:
        relation: AND
        checkInterval: 10
        failurePolicy: DEPENDENT_FAILURE_FAILURE
        dependTaskList:
          - relation: AND
            dependItemList:
              - dependentType: DEPENDENT_ON_WORKFLOW
                projectCode: 1
                definitionCode: 1000000000001
                depTaskCode: 0
                cycle: day
                dateValue: last1Days
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "dependence": {
            "relation": "AND",
            "checkInterval": 10,
            "failurePolicy": "DEPENDENT_FAILURE_FAILURE",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_WORKFLOW",
                            "projectCode": 1,
                            "definitionCode": 1000000000001,
                            "depTaskCode": 0,
                            "cycle": "day",
                            "dateValue": "last1Days",
                        }
                    ],
                }
            ],
        }
    }


def test_load_workflow_spec_rejects_empty_dependent_branch_list(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: dependent-workflow
tasks:
  - name: wait-upstream
    type: DEPENDENT
    task_params:
      dependence:
        relation: AND
        dependTaskList: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"Task 'wait-upstream' task_params\.dependence\.dependTaskList",
    ):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_accepts_switch_task_params(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: switch-workflow
tasks:
  - name: route
    type: SWITCH
    task_params:
      switchResult:
        dependTaskList:
          - condition: ${route} == "A"
            nextNode: task-a
        nextNode: task-default
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "A"',
                    "nextNode": "task-a",
                }
            ],
            "nextNode": "task-default",
        }
    }


def test_load_workflow_spec_rejects_switch_without_any_branch_target(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: switch-workflow
tasks:
  - name: route
    type: SWITCH
    task_params:
      switchResult:
        dependTaskList: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"Task 'route' task_params\.switchResult",
    ):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_accepts_conditions_task_params(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: conditions-workflow
tasks:
  - name: route
    type: CONDITIONS
    task_params:
      dependence:
        relation: AND
        dependTaskList:
          - relation: AND
            dependItemList:
              - dependentType: DEPENDENT_ON_TASK
                projectCode: 1
                definitionCode: 1000000000001
                depTaskCode: 1000000000002
                cycle: day
                dateValue: today
                status: SUCCESS
      conditionResult:
        successNode:
          - on-success
        failedNode:
          - on-failed
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 1,
                            "definitionCode": 1000000000001,
                            "depTaskCode": 1000000000002,
                            "cycle": "day",
                            "dateValue": "today",
                            "status": "SUCCESS",
                        }
                    ],
                }
            ],
        },
        "conditionResult": {
            "successNode": ["on-success"],
            "failedNode": ["on-failed"],
        },
    }


def test_load_workflow_spec_rejects_empty_conditions_success_branch(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: conditions-workflow
tasks:
  - name: route
    type: CONDITIONS
    task_params:
      dependence:
        relation: AND
        dependTaskList:
          - relation: AND
            dependItemList:
              - dependentType: DEPENDENT_ON_TASK
                projectCode: 1
                definitionCode: 1000000000001
                depTaskCode: 1000000000002
                cycle: day
                dateValue: today
                status: SUCCESS
      conditionResult:
        successNode: []
        failedNode:
          - on-failed
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"Task 'route' task_params\.conditionResult\.successNode",
    ):
        load_workflow_spec(spec_path)
