"""Roundtrip task parameters and richer existing native state."""

import json
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeResourceAdapter,
    FakeResourceItem,
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
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)


def test_209_waterdrop_create_export_and_edit_bind_one_config_resource(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    version = "2.0.9"
    fake_task_adapter.generated_codes = [7101]
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="orders.conf",
                full_name_value="/waterdrop/orders.conf",
                is_directory_value=False,
                id_value=811,
            )
        ]
    )
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        resource_adapter=resource_adapter,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / "waterdrop.yaml"
    spec_path.write_text(
        """
workflow:
  name: waterdrop-orders
  project: etl-prod
tasks:
  - name: sync-orders
    type: WATERDROP
    task_params:
      configResource: /waterdrop/orders.conf
""".strip(),
        encoding="utf-8",
    )

    workflow_service.create_workflow_result(file=spec_path)

    definitions = json.loads(
        str(fake_workflow_adapter.create_calls[0]["task_definition_json"])
    )
    assert json.loads(definitions[0]["taskParams"]) == {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }

    exported = workflow_service.export_workflow_yaml_result(
        "waterdrop-orders",
        project="etl-prod",
    )
    exported_document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert exported_document["tasks"][0]["task_params"] == {
        "configResource": "/waterdrop/orders.conf"
    }

    patch_path = tmp_path / "waterdrop.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Resolved Waterdrop resource edit
""".strip(),
        encoding="utf-8",
    )
    workflow_service.edit_workflow_result(
        "waterdrop-orders",
        patch=patch_path,
        project="etl-prod",
    )
    edited_definitions = json.loads(
        str(fake_workflow_adapter.update_calls[-1]["task_definition_json"])
    )
    assert json.loads(edited_definitions[0]["taskParams"]) == {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }


def test_322_dynamic_create_export_and_edit_bind_same_project_child_name(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    version = "3.2.2"
    fake_task_adapter.generated_codes = [7201]
    child = FakeWorkflow(
        code=9001,
        name="102",
        project_code_value=7,
        project_name_value="etl-prod",
    )
    fake_workflow_adapter.workflows.append(child)
    fake_workflow_adapter.dags[9001] = FakeDag(
        workflow_definition_value=child,
        task_definition_list_value=[],
        workflow_task_relation_list_value=[],
    )
    workflow_harness.install(
        profile=make_profile(ds_version=version),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / "dynamic.yaml"
    spec_path.write_text(
        """
workflow:
  name: dynamic-parent
  project: etl-prod
tasks:
  - name: fanout-region
    type: DYNAMIC
    task_params:
      childWorkflowName: "102"
      parameterName: region
      values: [east, west]
      degreeOfParallelism: 2
""".strip(),
        encoding="utf-8",
    )

    workflow_service.create_workflow_result(file=spec_path)

    definitions = json.loads(
        str(fake_workflow_adapter.create_calls[0]["task_definition_json"])
    )
    assert json.loads(definitions[0]["taskParams"])["processDefinitionCode"] == 9001

    exported = workflow_service.export_workflow_yaml_result(
        "dynamic-parent",
        project="etl-prod",
    )
    exported_document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert exported_document["tasks"][0]["task_params"] == {
        "childWorkflowName": "102",
        "parameterName": "region",
        "values": ["east", "west"],
        "degreeOfParallelism": 2,
    }

    patch_path = tmp_path / "dynamic.patch.yaml"
    patch_path.write_text(
        "patch:\n  workflow:\n    set:\n      description: resolved child edit\n",
        encoding="utf-8",
    )
    workflow_service.edit_workflow_result(
        "dynamic-parent",
        patch=patch_path,
        project="etl-prod",
    )
    edited_definitions = json.loads(
        str(fake_workflow_adapter.update_calls[-1]["task_definition_json"])
    )
    assert (
        json.loads(edited_definitions[0]["taskParams"])["processDefinitionCode"] == 9001
    )


def test_200_waterdrop_export_and_metadata_edit_preserve_native_state(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    native_params = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            "sh ${WATERDROP_HOME}/bin/start-waterdrop.sh --master local "
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }
    workflow = FakeWorkflow(
        code=310,
        name="waterdrop-preserve",
        project_code_value=7,
        project_name_value="etl-prod",
        description="before",
    )
    task = FakeTaskDefinition(
        code=7100,
        name="sync-orders",
        project_code_value=7,
        project_name_value="etl-prod",
        task_type_value="WATERDROP",
        task_params_value=json.dumps(native_params, separators=(",", ":")),
        worker_group_value="default",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            310: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=7100,
                    )
                ],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        profile=make_profile(ds_version="2.0.0"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("2.0.0"),
    )

    exported = workflow_service.export_workflow_yaml_result(
        "waterdrop-preserve",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == native_params

    patch_path = tmp_path / "waterdrop-preserve.patch.yaml"
    patch_path.write_text(
        "patch:\n  workflow:\n    set:\n      description: after\n",
        encoding="utf-8",
    )
    workflow_service.edit_workflow_result(
        "waterdrop-preserve",
        patch=patch_path,
        project="etl-prod",
    )

    definitions = json.loads(
        str(workflow_adapter.update_calls[-1]["task_definition_json"])
    )
    assert json.loads(definitions[0]["taskParams"]) == native_params


@pytest.mark.parametrize(
    ("version", "extra"),
    [
        ("3.2.0", {}),
        ("3.2.1", {}),
        ("3.2.2", {"futureField": {"keep": True}}),
    ],
)
def test_dynamic_preserve_only_export_and_metadata_edit_is_lossless(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    version: str,
    extra: dict[str, object],
) -> None:
    native_params = {
        "processDefinitionCode": 404,
        "maxNumOfSubWorkflowInstances": 2,
        "degreeOfParallelism": 1,
        "filterCondition": "",
        "listParameters": [{"name": "region", "value": "east,west", "separator": ","}],
        **extra,
    }
    workflow = FakeWorkflow(
        code=310,
        name="dynamic-preserve",
        project_code_value=7,
        project_name_value="etl-prod",
        description="before",
    )
    task = FakeTaskDefinition(
        code=7100,
        name="fanout-region",
        project_code_value=7,
        project_name_value="etl-prod",
        task_type_value="DYNAMIC",
        task_params_value=json.dumps(native_params, separators=(",", ":")),
        worker_group_value="default",
        is_cache_value=FakeEnumValue("NO"),
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            310: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=7100,
                    )
                ],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        profile=make_profile(ds_version=version),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    exported = workflow_service.export_workflow_yaml_result(
        "dynamic-preserve",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == native_params
    patch_path = tmp_path / "dynamic-preserve.patch.yaml"
    patch_path.write_text(
        "patch:\n  workflow:\n    set:\n      description: after\n",
        encoding="utf-8",
    )

    workflow_service.edit_workflow_result(
        "dynamic-preserve",
        patch=patch_path,
        project="etl-prod",
    )

    definitions = json.loads(
        str(workflow_adapter.update_calls[-1]["task_definition_json"])
    )
    assert json.loads(definitions[0]["taskParams"]) == native_params
    assert 404 not in workflow_adapter.get_calls


def test_workflow_create_get_edit_roundtrip_preserves_switch_and_native_opaque_tasks(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_adapter = FakeWorkflowAdapter(workflows=[], dags={})
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        task_adapter=FakeTaskAdapter(workflow_tasks={}),
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: roundtrip-flow
  project: etl-prod
  description: Roundtrip workflow
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: route
    type: SWITCH
    depends_on: [extract]
    task_params:
      switchResult:
        dependTaskList:
          - condition: ${route} == "spark"
            nextNode: spark-job
        nextNode: fallback
  - name: spark-job
    type: SPARK
    task_params:
      programType: JAVA
      mainClass: com.example.jobs.SparkJob
      mainJar:
        id: 9
      deployMode: cluster
  - name: fallback
    type: SHELL
    command: echo fallback
""".strip(),
        encoding="utf-8",
    )

    created = workflow_service.create_workflow_result(file=spec_path)
    created_data = _mapping(created.data)
    assert created_data["name"] == "roundtrip-flow"

    exported = workflow_service.export_workflow_yaml_result(
        "roundtrip-flow",
        project="etl-prod",
    )
    exported_data = _mapping(exported.data)
    exported_document = yaml.safe_load(str(exported_data["yaml"]))
    exported_spec = validate_workflow_document(
        exported_document,
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.1"),
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert exported_spec.tasks[1].type == "SWITCH"
    assert exported_spec.tasks[1].task_params == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": "spark-job",
                }
            ],
            "nextNode": "fallback",
        }
    }
    assert exported_spec.tasks[2].type == "SPARK"
    assert exported_spec.tasks[2].task_params == {
        "programType": "JAVA",
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }

    exported_workflow = exported_document["workflow"]
    assert isinstance(exported_workflow, dict)
    exported_workflow["name"] = "roundtrip-flow-copy"
    exported_path = tmp_path / "exported.workflow.yaml"
    exported_path.write_text(
        yaml.safe_dump(exported_document, sort_keys=False),
        encoding="utf-8",
    )
    dry_run = workflow_service.create_workflow_result(file=exported_path, dry_run=True)
    dry_run_form = _mapping(
        _mapping(first_dry_run_request(_mapping(dry_run.data)))["form"]
    )
    dry_run_definitions = json.loads(str(dry_run_form["taskDefinitionJson"]))
    dry_run_codes = {task["name"]: task["code"] for task in dry_run_definitions}

    assert json.loads(dry_run_definitions[1]["taskParams"]) == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": dry_run_codes["spark-job"],
                }
            ],
            "nextNode": dry_run_codes["fallback"],
        }
    }
    assert json.loads(dry_run_definitions[2]["taskParams"]) == {
        "programType": "JAVA",
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }

    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - from: spark-job
        to: spark-job-v2
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

    updated = workflow_service.edit_workflow_result(
        "roundtrip-flow",
        patch=patch_path,
        project="etl-prod",
    )
    updated_data = _mapping(updated.data)
    assert updated_data["name"] == "roundtrip-flow"

    updated_export = workflow_service.export_workflow_yaml_result(
        "roundtrip-flow",
        project="etl-prod",
    )
    updated_document = yaml.safe_load(str(_mapping(updated_export.data)["yaml"]))
    updated_spec = validate_workflow_document(
        updated_document,
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.1"),
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert updated_spec.tasks[1].task_params == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": "spark-job-v2",
                }
            ],
            "nextNode": "fallback",
        }
    }
    assert updated_spec.tasks[2].name == "spark-job-v2"
    assert updated_spec.tasks[2].task_params == {
        "programType": "JAVA",
        "mainClass": "com.example.jobs.SparkJobV2",
        "mainJar": {"id": 10},
        "deployMode": "client",
    }
