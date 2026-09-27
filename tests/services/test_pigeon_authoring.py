from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow

from dsctl.models import WorkflowSpec
from dsctl.services._workflow.authoring import workflow_authoring_catalog_for_version
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


_PIGEON_VERSIONS = (
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
)


def _pigeon_spec(task_params: YamlObject) -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": "pigeon-roundtrip"},
            "tasks": [
                {
                    "name": "trigger-tis-job",
                    "type": "PIGEON",
                    "task_params": task_params,
                }
            ],
        }
    )


def _template_task(ds_version: str) -> YamlObject:
    result = task_template_result(
        "PIGEON",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    yaml_text = result.data["yaml"]
    assert isinstance(yaml_text, str)
    assert "p_host" in yaml_text
    document = yaml.safe_load(yaml_text)
    assert isinstance(document, dict)
    return cast("YamlObject", document)


@pytest.mark.parametrize("ds_version", _PIGEON_VERSIONS)
def test_pigeon_schema_is_reviewed_typed_surface(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    result = task_type_schema_result("PIGEON", catalog=catalog)

    assert catalog.supports_typed_authoring("PIGEON") is True
    assert isinstance(result.data, dict)
    assert result.data["kind"] == "typed"
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    target = fields["task_params.targetJobName"]
    assert target["required"] is True
    assert target["compile_path"].endswith("taskParams.targetJobName")
    assert "p_host" in target["description"]
    assert not any(path.startswith("task_params.localParams") for path in fields)
    assert not any(path.startswith("task_params.varPool") for path in fields)


@pytest.mark.parametrize("ds_version", _PIGEON_VERSIONS)
def test_pigeon_minimal_template_compiles_to_exact_native_params(
    ds_version: str,
) -> None:
    task = _template_task(ds_version)
    params = task["task_params"]
    assert isinstance(params, dict)

    payload = prepare_workflow_create_compilation(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": f"pigeon-{ds_version}"},
                "tasks": [task],
            }
        ),
        catalog=get_task_authoring_catalog(ds_version),
    ).materialize([20_001])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert task["type"] == "PIGEON"
    assert params == {"targetJobName": "daily-orders"}
    assert json.loads(definition["taskParams"]) == params


@pytest.mark.parametrize(
    "invalid_params",
    [
        {"targetJobName": ""},
        {"targetJobName": "   "},
        {"targetJobName": "daily-orders", "futureField": True},
        {"targetJobName": "daily-orders", "varPool": []},
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_pigeon_typed_authoring_fails_closed(
    invalid_params: YamlObject,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(ValueError):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            "PIGEON",
            invalid_params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_pigeon_typed_authoring_trims_target_job_name(
    intent: TaskAuthoringIntent,
) -> None:
    normalized = get_task_authoring_catalog("3.2.2").normalize_task_params(
        "PIGEON",
        {"targetJobName": "  daily-orders  "},
        intent=intent,
    )

    assert normalized == {"targetJobName": "daily-orders"}


def test_pigeon_opaque_preserve_keeps_native_future_fields() -> None:
    native: YamlObject = {
        "targetJobName": "daily-orders",
        "futureField": {"enabled": True},
        "localParams": [],
        "varPool": [],
    }

    preserved = get_task_authoring_catalog("3.2.2").normalize_task_params(
        "PIGEON",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    native["targetJobName"] = "mutated"

    assert preserved == {
        "targetJobName": "daily-orders",
        "futureField": {"enabled": True},
        "localParams": [],
        "varPool": [],
    }


@pytest.mark.parametrize("ds_version", _PIGEON_VERSIONS)
def test_pigeon_export_and_metadata_compile_preserve_native_payload(
    ds_version: str,
) -> None:
    native_params: YamlObject = {
        "targetJobName": "daily-orders",
        "localParams": [],
        "varPool": [],
        "futureField": {"enabled": True},
    }
    task = FakeTaskDefinition(
        code=101,
        name="trigger-tis-job",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="PIGEON",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="pigeon-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert document["tasks"][0]["task_params"] == native_params

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native_params


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.3.1", "3.4.2"])
def test_pigeon_remains_unavailable_when_exact_source_has_no_plugin(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("PIGEON") is False
    assert "PIGEON" not in catalog.authoring_task_types
