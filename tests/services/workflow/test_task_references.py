"""Resource and child-workflow references resolved before native mutations."""

import json
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import (
    FakeResourceAdapter,
    FakeResourceItem,
    FakeTaskAdapter,
    FakeWorkflowAdapter,
)
from tests.fakes.task_definitions import FakeTaskDefinitionWire
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    NotFoundError,
)
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services.task_authoring_catalog import (
    get_task_authoring_catalog,
)

if TYPE_CHECKING:
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire


def test_322_dynamic_create_preflights_child_and_compiles_exact_wire(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    profile = make_profile(ds_version="3.2.2")
    workflow_harness.install(
        profile=profile,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("3.2.2"),
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
      childWorkflowName: adhoc-backfill
      parameterName: region
      values: [east, west]
      degreeOfParallelism: 1
    retry: {times: 0, interval: 0}
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(task_definitions[0]["taskParams"]) == {
        "processDefinitionCode": 102,
        "maxNumOfSubWorkflowInstances": 2,
        "degreeOfParallelism": 1,
        "filterCondition": "",
        "listParameters": [{"name": "region", "value": "east,west", "separator": ","}],
    }
    assert fake_workflow_adapter.get_calls[-2:] == [102, 102]


def test_139_mr_create_export_and_edit_bind_the_visible_positive_resource_id(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="wordcount.jar",
                full_name_value="/jobs/wordcount.jar",
                is_directory_value=False,
                id_value=731,
            )
        ]
    )
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        resource_adapter=resource_adapter,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-mr.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-mr
  project: etl-prod
tasks:
  - name: wordcount
    type: MR
    task_params:
      mainJar: /jobs/wordcount.jar
      mainClass: com.example.WordCount
      mainArgs: [hdfs:///input, hdfs:///output]
""".strip(),
        encoding="utf-8",
    )

    created = workflow_service.create_workflow_result(file=spec_path)
    workflow_id = _mapping(created.data)["id"]
    assert isinstance(workflow_id, int)
    created_process = json.loads(operations.legacy_definitions[workflow_id][0])
    assert created_process["tasks"][0]["params"]["mainJar"] == {"id": 731}

    exported = workflow_service.export_workflow_yaml_result(
        "legacy-mr",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == {
        "mainJar": "/jobs/wordcount.jar",
        "mainClass": "com.example.WordCount",
        "mainArgs": ["hdfs:///input", "hdfs:///output"],
    }

    patch_path = tmp_path / "legacy-mr.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Resolved legacy MR resource edit
""".strip(),
        encoding="utf-8",
    )
    workflow_service.edit_workflow_result(
        "legacy-mr",
        patch=patch_path,
        project="etl-prod",
    )
    edited_process = json.loads(operations.legacy_definitions[workflow_id][0])
    assert edited_process["tasks"][0]["params"]["mainJar"] == {"id": 731}


def test_139_mr_missing_resource_fails_before_legacy_workflow_mutation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        resource_adapter=FakeResourceAdapter(resources=[]),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-mr-missing.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-mr-missing
  project: etl-prod
tasks:
  - name: wordcount
    type: MR
    task_params:
      mainJar: /jobs/missing.jar
      mainClass: com.example.WordCount
""".strip(),
        encoding="utf-8",
    )
    previous_workflows = list(fake_workflow_adapter.workflows)

    with pytest.raises(NotFoundError, match="not found or visible"):
        workflow_service.create_workflow_result(file=spec_path)

    assert fake_workflow_adapter.workflows == previous_workflows
    assert operations.legacy_definitions == {}


@pytest.mark.parametrize(
    "version",
    ["2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9"],
)
def test_old_task_definition_mr_create_resolves_one_positive_resource_id(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    version: str,
) -> None:
    fake_task_adapter.generated_codes = [7101]
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="wordcount.jar",
                full_name_value="/jobs/wordcount.jar",
                is_directory_value=False,
                id_value=731,
            )
        ]
    )
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        resource_adapter=resource_adapter,
        task_definition_wire=(
            cast(
                "TaskDefinitionWire",
                FakeTaskDefinitionWire(fake_task_adapter, ds_version=version),
            )
            if version == "3.1.0"
            else None
        ),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / f"mr-{version}.yaml"
    spec_path.write_text(
        """
workflow:
  name: mr-wordcount
  project: etl-prod
tasks:
  - name: wordcount
    type: MR
    task_params:
      mainJar: /jobs/wordcount.jar
      mainClass: com.example.WordCount
      mainArgs: [hdfs:///input, hdfs:///output]
""".strip(),
        encoding="utf-8",
    )

    workflow_service.create_workflow_result(file=spec_path)

    if version == "3.1.0":
        created_workflow = next(
            workflow
            for workflow in fake_workflow_adapter.workflows
            if workflow.name == "mr-wordcount"
        )
        created_dag = fake_workflow_adapter.dags[created_workflow.code]
        fake_task_adapter.workflow_tasks[created_workflow.code] = [
            replace(task, id=88001) for task in created_dag.taskDefinitionList or ()
        ]

    assert fake_task_adapter.generate_code_calls == [{"project_code": 7, "count": 1}]
    task_definitions = json.loads(
        str(fake_workflow_adapter.create_calls[0]["task_definition_json"])
    )
    assert json.loads(task_definitions[0]["taskParams"])["mainJar"] == {"id": 731}

    exported = workflow_service.export_workflow_yaml_result(
        "mr-wordcount",
        project="etl-prod",
    )
    exported_document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert exported_document["tasks"][0]["task_params"] == {
        "mainJar": "/jobs/wordcount.jar",
        "mainClass": "com.example.WordCount",
        "mainArgs": ["hdfs:///input", "hdfs:///output"],
    }

    patch_path = tmp_path / f"mr-{version}.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Resolved MR resource edit
""".strip(),
        encoding="utf-8",
    )
    workflow_service.edit_workflow_result(
        "mr-wordcount",
        patch=patch_path,
        project="etl-prod",
    )
    edited_definitions = json.loads(
        str(fake_workflow_adapter.update_calls[-1]["task_definition_json"])
    )
    assert json.loads(edited_definitions[0]["taskParams"])["mainJar"] == {"id": 731}
    if version == "3.1.0":
        assert edited_definitions[0]["id"] == 88001
        assert fake_task_adapter.get(code=7101).id == 88001


def test_old_task_definition_mr_missing_resource_fails_before_any_mutation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    version = "3.1.9"
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        resource_adapter=FakeResourceAdapter(resources=[]),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / "mr-missing-resource.yaml"
    spec_path.write_text(
        """
workflow:
  name: mr-wordcount
  project: etl-prod
tasks:
  - name: wordcount
    type: MR
    task_params:
      mainJar: /jobs/missing.jar
      mainClass: com.example.WordCount
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(NotFoundError, match="not found or visible"):
        workflow_service.create_workflow_result(file=spec_path)

    assert fake_task_adapter.generate_code_calls == []
    assert fake_workflow_adapter.create_calls == []
