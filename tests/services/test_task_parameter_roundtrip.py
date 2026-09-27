import json
from dataclasses import replace

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowTaskRelation,
)

from dsctl.services._workflow.authoring import workflow_authoring_catalog_for_version
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.upstream.resolver import ResolvedProject


def _project() -> ResolvedProject:
    return ResolvedProject(code=7, name="analytics", description=None)


def _task(
    code: int,
    name: str,
    task_type: str,
    task_params: dict[str, object],
) -> FakeTaskDefinition:
    return FakeTaskDefinition(
        code=code,
        name=name,
        project_code_value=7,
        project_name_value="analytics",
        task_type_value=task_type,
        task_params_value=json.dumps(task_params),
        worker_group_value="default",
    )


def _legacy_logic_dag() -> FakeDag:
    tasks = [
        _task(
            101,
            "upstream",
            "SHELL",
            {"rawScript": "echo up", "localParams": [], "resourceList": []},
        ),
        _task(
            102,
            "success",
            "SHELL",
            {"rawScript": "echo ok", "localParams": [], "resourceList": []},
        ),
        _task(
            103,
            "failed",
            "SHELL",
            {"rawScript": "echo no", "localParams": [], "resourceList": []},
        ),
        _task(
            104,
            "switch",
            "SWITCH",
            {
                "switchResult": {
                    "dependTaskList": [{"condition": "true", "nextNode": "102"}],
                    "nextNode": "103",
                }
            },
        ),
        _task(
            105,
            "conditions",
            "CONDITIONS",
            {
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {"depTaskCode": 101, "status": "SUCCESS"}
                            ],
                        }
                    ],
                },
                "conditionResult": {
                    "successNode": ["102"],
                    "failedNode": ["103"],
                },
            },
        ),
        _task(
            106,
            "dependent",
            "DEPENDENT",
            {
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "day",
                                    "dateValue": "today",
                                }
                            ],
                        }
                    ],
                }
            },
        ),
        _task(
            107,
            "http",
            "HTTP",
            {
                "url": "https://example.test/health",
                "httpMethod": "GET",
                "httpParams": [],
                "httpCheckCondition": "STATUS_CODE_DEFAULT",
                "connectTimeout": 60000,
                "socketTimeout": 60000,
            },
        ),
        _task(
            108,
            "child",
            "SUB_PROCESS",
            {
                "processDefinitionCode": 9001,
                "localParams": [],
                "varPool": [],
            },
        ),
    ]
    relations = [
        FakeWorkflowTaskRelation(101, 105),
        FakeWorkflowTaskRelation(104, 102),
        FakeWorkflowTaskRelation(104, 103),
        FakeWorkflowTaskRelation(105, 102),
        FakeWorkflowTaskRelation(105, 103),
    ]
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="legacy-logic",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=tasks,
        workflow_task_relation_list_value=relations,
    )


def _procedure_dag(task_params: dict[str, object], *, ds_version: str) -> FakeDag:
    task = _task(101, "call-procedure", "PROCEDURE", task_params)
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="procedure-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )


def _native_procedure_params(*, method: str) -> dict[str, object]:
    return {
        "type": "MYSQL",
        "datasource": 7,
        "method": method,
        "localParams": [
            {
                "prop": "result",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "ready",
            }
        ],
        "varPool": [],
    }


def test_2000_native_logic_export_and_preserved_compile_roundtrip() -> None:
    catalog = workflow_authoring_catalog_for_version("2.0.0")
    dag = _legacy_logic_dag()

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_project(),
            attached_schedule=None,
            catalog=catalog,
        )
    )
    exported = {task["name"]: task for task in document["tasks"]}

    assert exported["switch"]["task_params"]["switchResult"] == {
        "dependTaskList": [{"condition": "true", "nextNode": "success"}],
        "nextNode": "failed",
    }
    assert exported["conditions"]["task_params"]["dependence"]["dependTaskList"][0][
        "dependItemList"
    ] == [{"task": "upstream", "status": "SUCCESS"}]
    assert exported["conditions"]["task_params"]["conditionResult"] == {
        "successNode": ["success"],
        "failedNode": ["failed"],
    }
    assert (
        exported["dependent"]["task_params"]["dependence"]["dependTaskList"][0][
            "dependItemList"
        ][0]["dependentType"]
        == "DEPENDENT_ON_WORKFLOW"
    )
    assert "socketTimeout" not in exported["http"]["task_params"]
    assert exported["child"]["type"] == "SUB_WORKFLOW"
    assert exported["child"]["task_params"]["workflowDefinitionCode"] == 9001

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = {
        task["name"]: task for task in json.loads(payload["taskDefinitionJson"])
    }

    assert compiled["child"]["taskType"] == "SUB_PROCESS"
    assert json.loads(compiled["child"]["taskParams"])["processDefinitionCode"] == 9001
    assert json.loads(compiled["http"]["taskParams"])["socketTimeout"] == 60000
    assert (
        json.loads(compiled["switch"]["taskParams"])["switchResult"]["nextNode"]
        == "103"
    )
    assert json.loads(compiled["conditions"]["taskParams"])["dependence"][
        "dependTaskList"
    ][0]["dependItemList"] == [{"depTaskCode": 101, "status": "SUCCESS"}]


def test_legacy_unrepresentable_http_timeout_is_preserved_losslessly() -> None:
    catalog = workflow_authoring_catalog_for_version("3.1.9")
    native_params = {
        "url": "https://example.test/health",
        "httpMethod": "GET",
        "connectTimeout": 60000,
        "socketTimeout": 12345,
    }
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="opaque-http",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[_task(101, "http", "HTTP", native_params)],
        workflow_task_relation_list_value=[],
    )

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    assert baseline.spec.tasks[0].task_params == native_params

    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native_params


def test_legacy_runtime_switch_state_survives_metadata_only_compilation() -> None:
    catalog = workflow_authoring_catalog_for_version("2.0.0")
    native_params = {
        "switchResult": {
            "dependTaskList": [{"condition": "true", "nextNode": "102"}],
            "nextNode": "103",
        },
        "nextBranch": "102",
        "resultConditionLocation": 0,
    }
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="opaque-switch",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            _task(101, "switch", "SWITCH", native_params),
            _task(
                102,
                "success",
                "SHELL",
                {"rawScript": "echo ok", "localParams": [], "resourceList": []},
            ),
            _task(
                103,
                "failed",
                "SHELL",
                {"rawScript": "echo no", "localParams": [], "resourceList": []},
            ),
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(101, 102),
            FakeWorkflowTaskRelation(101, 103),
        ],
    )

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    assert baseline.spec.tasks[0].task_params == native_params

    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native_params


@pytest.mark.parametrize(
    "ds_version",
    [
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
    ],
)
def test_procedure_runtime_state_and_native_method_survive_metadata_roundtrip(
    ds_version: str,
) -> None:
    catalog = workflow_authoring_catalog_for_version(ds_version)
    out_property = {
        "result": {
            "prop": "result",
            "direct": "OUT",
            "type": "VARCHAR",
            "value": "ready",
        }
    }
    native_params = {
        **_native_procedure_params(method="{call reporting.refresh_daily(${result})}"),
        "outProperty": out_property,
    }
    dag = _procedure_dag(native_params, ds_version=ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_project(),
            attached_schedule=None,
            catalog=catalog,
        )
    )
    exported_params = document["tasks"][0]["task_params"]

    assert exported_params["method"] == native_params["method"]
    assert exported_params["outProperty"] == out_property

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled["method"] == native_params["method"]
    assert compiled["outProperty"] == out_property


@pytest.mark.parametrize(
    ("ds_version", "native_method"),
    [
        ("2.0.0", "{call reporting.refresh_daily(?)}"),
        ("3.4.1", "{call reporting.refresh_daily(${result})}"),
        ("3.4.2", "{call reporting.refresh_daily(${result})}"),
    ],
)
def test_procedure_profiles_without_runtime_state_do_not_synthesize_it(
    ds_version: str,
    native_method: str,
) -> None:
    catalog = workflow_authoring_catalog_for_version(ds_version)
    native_params = _native_procedure_params(method=native_method)
    dag = _procedure_dag(native_params, ds_version=ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_project(),
            attached_schedule=None,
            catalog=catalog,
        )
    )
    exported_params = document["tasks"][0]["task_params"]

    assert exported_params["method"] == "{call reporting.refresh_daily(?)}"
    assert "outProperty" not in exported_params

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled["method"] == native_method
    assert "outProperty" not in compiled


def test_procedure_unrepresentable_native_method_is_opaque_across_metadata_edit() -> (
    None
):
    catalog = workflow_authoring_catalog_for_version("3.4.0")
    native_params = {
        **_native_procedure_params(method="{call reporting.refresh_daily(?)}"),
        "outProperty": {"result": {"value": "ready"}},
    }
    dag = _procedure_dag(native_params, ds_version="3.4.0")

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert baseline.spec.tasks[0].task_params == native_params
    assert compiled == native_params
