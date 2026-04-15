import json

import pytest
import yaml

from dsctl.errors import UserInputError
from dsctl.models import WorkflowSpec
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.services import _workflow_compile as workflow_compile_service
from dsctl.services._workflow_patch import apply_workflow_patch
from dsctl.services.template import supported_task_template_types, task_template_result


def _compilable_workflow_document(
    task_document: dict[str, object],
) -> dict[str, object]:
    task_type = task_document["type"]
    tasks: list[dict[str, object]] = [task_document]
    if task_type == "SWITCH":
        tasks.extend(
            [
                {
                    "name": "task-a",
                    "type": "SHELL",
                    "command": "echo A",
                },
                {
                    "name": "task-b",
                    "type": "SHELL",
                    "command": "echo B",
                },
                {
                    "name": "task-default",
                    "type": "SHELL",
                    "command": "echo default",
                },
            ]
        )
    if task_type == "CONDITIONS":
        tasks.extend(
            [
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
            ]
        )
    return {
        "workflow": {"name": "patched-workflow"},
        "tasks": tasks,
    }


def _task_patch_set(
    task_type: str,
) -> dict[str, object]:
    if task_type == "SHELL":
        return {"command": "echo patched shell"}
    if task_type == "PYTHON":
        return {"command": 'print("patched python")'}
    if task_type == "REMOTESHELL":
        return {
            "task_params": {
                "rawScript": "echo patched remote",
                "type": "SSH",
                "datasource": 2,
            }
        }
    if task_type == "SQL":
        return {
            "task_params": {
                "type": "MYSQL",
                "datasource": 2,
                "sql": "select 2;",
                "sqlType": 0,
                "sendEmail": False,
                "displayRows": 20,
                "showType": "TABLE",
                "connParams": "",
                "preStatements": [],
                "postStatements": [],
                "groupId": 0,
                "title": "",
                "limit": 0,
            }
        }
    if task_type == "HTTP":
        return {
            "task_params": {
                "url": "https://example.test/patched",
                "httpMethod": "GET",
                "httpParams": [],
                "httpBody": "",
                "httpCheckCondition": "STATUS_CODE_DEFAULT",
                "condition": "",
                "connectTimeout": 5000,
            }
        }
    if task_type == "SUB_WORKFLOW":
        return {
            "task_params": {
                "workflowDefinitionCode": 1000000000002,
            }
        }
    if task_type == "DEPENDENT":
        return {
            "task_params": {
                "dependence": {
                    "relation": "AND",
                    "checkInterval": 20,
                    "failurePolicy": "DEPENDENT_FAILURE_FAILURE",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 1,
                                    "definitionCode": 1000000000002,
                                    "depTaskCode": 0,
                                    "cycle": "day",
                                    "dateValue": "last7Days",
                                }
                            ],
                        }
                    ],
                }
            }
        }
    if task_type == "SWITCH":
        return {
            "task_params": {
                "switchResult": {
                    "dependTaskList": [
                        {
                            "condition": '${route} == "patched-A"',
                            "nextNode": "task-a",
                        },
                        {
                            "condition": '${route} == "patched-B"',
                            "nextNode": "task-b",
                        },
                    ],
                    "nextNode": "task-default",
                }
            }
        }
    if task_type == "CONDITIONS":
        return {
            "task_params": {
                "dependence": {
                    "relation": "OR",
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
                                    "status": "FAILURE",
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
        }
    return {
        "task_params": {
            "patched": True,
            "taskType": task_type,
        }
    }


def _expected_compiled_task_params(
    task_type: str,
    patch_set: dict[str, object],
) -> dict[str, object]:
    if "task_params" not in patch_set:
        command = patch_set["command"]
        assert isinstance(command, str)
        return {
            "rawScript": command,
            "localParams": [],
            "resourceList": [],
        }
    task_params = patch_set["task_params"]
    assert isinstance(task_params, dict)
    if task_type == "SWITCH":
        return {
            "switchResult": {
                "dependTaskList": [
                    {
                        "condition": '${route} == "patched-A"',
                        "nextNode": 8002,
                    },
                    {
                        "condition": '${route} == "patched-B"',
                        "nextNode": 8003,
                    },
                ],
                "nextNode": 8004,
            }
        }
    if task_type == "CONDITIONS":
        return {
            "dependence": {
                "relation": "OR",
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
                                "status": "FAILURE",
                            }
                        ],
                    }
                ],
            },
            "conditionResult": {
                "successNode": [8002],
                "failedNode": [8003],
            },
        }
    return dict(task_params)


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_apply_workflow_patch_updates_template_task_payloads(
    monkeypatch: pytest.MonkeyPatch,
    task_type: str,
) -> None:
    codes = iter(range(8001, 8100))
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    template = task_template_result(task_type)
    data = template.data
    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    task_document = yaml.safe_load(yaml_text)
    assert isinstance(task_document, dict)
    baseline = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    primary_task = baseline.tasks[0]
    patch_set = _task_patch_set(task_type)
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": primary_task.name},
                            "set": patch_set,
                        }
                    ]
                }
            }
        }
    ).patch

    merged, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )
    payload = workflow_compile_service.compile_workflow_create_payload(merged)
    task_definitions = json.loads(payload["taskDefinitionJson"])
    compiled_primary = task_definitions[0]

    assert diff["updated_tasks"] == [primary_task.name]
    assert compiled_primary["taskType"] == task_type
    assert json.loads(compiled_primary["taskParams"]) == _expected_compiled_task_params(
        task_type,
        patch_set,
    )
    if "command" in patch_set:
        assert merged.tasks[0].command == patch_set["command"]
        assert merged.tasks[0].task_params is None
    else:
        assert merged.tasks[0].command is None
        assert merged.tasks[0].task_params == patch_set["task_params"]


def test_apply_workflow_patch_updates_extended_task_execution_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: 8101)
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {
                                "flag": "NO",
                                "environment_code": 42,
                                "task_group_id": 21,
                                "task_group_priority": 7,
                                "timeout": 15,
                                "timeout_notify_strategy": "FAILED",
                                "cpu_quota": 50,
                                "memory_max": 1024,
                            },
                        }
                    ]
                }
            }
        }
    ).patch

    merged, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )
    payload = workflow_compile_service.compile_workflow_create_payload(merged)
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert diff["updated_tasks"] == ["extract"]
    assert task_definitions == [
        {
            "code": 8101,
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


def test_apply_workflow_patch_treats_semantic_defaults_as_no_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: 8101)
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                    "timeout": 15,
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {
                                "worker_group": None,
                                "timeout_notify_strategy": "WARN",
                                "cpu_quota": -1,
                                "memory_max": -1,
                            },
                        }
                    ]
                }
            }
        }
    ).patch

    _, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )

    assert diff["updated_tasks"] == []


def test_apply_workflow_patch_rejects_invalid_task_execution_combination() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {"timeout_notify_strategy": "FAILED"},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="requires timeout > 0"):
        apply_workflow_patch(
            baseline,
            patch,
            edge_builder=workflow_compile_service.workflow_edges,
        )
