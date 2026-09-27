"""Source-matched v0.3.0 / exact 3.4.1 SHELL command compatibility."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import yaml
from tests.compatibility.test_published_cli_corpus import _sql_schema_semantics
from tests.compatibility.test_published_cli_workflow import (
    _assert_historical_yaml_fields,
    _form,
    _install,
)
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.request_assertions import first_dry_run_request
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.errors import UserInputError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.lint.workflow import lint_workflow_result
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.services.template import task_template_result
from dsctl.services.workflow import create as workflow_create_service

_CORPUS = Path(__file__).parent / "corpus/v0.3.0-ds3.4.1-shell-command.json"


def _corpus() -> dict[str, object]:
    corpus = cast("dict[str, object]", json.loads(_CORPUS.read_text(encoding="utf-8")))
    source = assert_mapping(corpus["source_cli"])
    assert source["tag"] == "v0.3.0"
    assert source["artifact_origin"] == "local_rebuild_from_git_tag"
    assert source["reconstructed_wheel_sha256"] == (
        "1966fe1bee4b86396b1f2fb29fca0d0a4f8f311b719ae7dbb9cf073dd19051f2"
    )
    assert source["source_manifest_sha256"] == (
        "c0d0ec9dd6df7619c2fc446ab8562b4915559bb4f99c29b6fae0df80f4dda2a0"
    )
    assert corpus["ds_profile"] == "3.4.1"
    assert corpus["task_type"] == "SHELL"
    assert corpus["facet"] == "command_shorthand"
    return corpus


def _task_from_compiled(payload: object) -> dict[str, object]:
    task = cast(
        "dict[str, object]",
        json.loads(str(assert_mapping(payload)["taskDefinitionJson"]))[0],
    )
    task["taskParams"] = json.loads(str(task["taskParams"]))
    return task


def test_published_shell_parse_schema_template_and_lint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    semantics = assert_mapping(_corpus()["semantics"])
    catalog = workflow_authoring_catalog_for_version("3.4.1")
    path = tmp_path / "shell.yaml"
    path.write_text(yaml.safe_dump(semantics["input"]), encoding="utf-8")
    context = workflow_authoring_context(
        catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
    )
    spec = validate_workflow_document(
        yaml.safe_load(path.read_text()), authoring_context=context
    )
    old_input = assert_mapping(semantics["input_semantics"])
    assert spec.tasks[0].model_dump(exclude_unset=True) == old_input["parsed_task"]
    compiled = prepare_workflow_create_compilation(spec, catalog=catalog).materialize(
        [7001]
    )
    assert _task_from_compiled(compiled) == old_input["wire_task"]

    schema = assert_mapping(task_type_schema_result("SHELL", catalog=catalog).data)
    current_fields = {
        assert_mapping(item)["path"]: assert_mapping(item)
        for item in assert_sequence(schema["fields"])
    }
    for item in assert_sequence(semantics["schema_fields"]):
        old = assert_mapping(item)
        current = current_fields[old["path"]]
        for key, value in old.items():
            if key == "choices":
                assert set(assert_sequence(current[key])) == set(assert_sequence(value))
            else:
                assert current[key] == value

    old_schema = cast(
        "dict[str, object]", _sql_schema_semantics(semantics["json_schema"])
    )
    schema_data = assert_mapping(
        task_type_schema_result("SHELL", catalog=catalog, json_schema=True).data
    )
    new_schema = cast("dict[str, object]", _sql_schema_semantics(schema_data["schema"]))
    for schema_body in (old_schema, new_schema):
        properties = cast("dict[str, object]", schema_body["properties"])
        properties.pop("task_params")  # resourceList is a separate authoring facet
        schema_body.pop("$defs")
        flag = cast("dict[str, object]", properties["flag"])
        flag["enum"] = sorted(cast("list[str]", flag["enum"]))
    assert old_schema == new_schema

    template_data = assert_mapping(task_template_result("SHELL", catalog=catalog).data)
    template = yaml.safe_load(str(template_data["yaml"]))
    template_doc = {
        "workflow": assert_mapping(semantics["input"])["workflow"],
        "tasks": [template],
    }
    template_spec = validate_workflow_document(
        yaml.safe_load(yaml.safe_dump(template_doc)), authoring_context=context
    )
    historical_template = assert_mapping(semantics["template_semantics"])
    assert (
        template_spec.tasks[0].model_dump(exclude_unset=True)
        == historical_template["parsed_task"]
    )
    template_wire = prepare_workflow_create_compilation(
        template_spec, catalog=catalog
    ).materialize([7001])
    assert _task_from_compiled(template_wire) == historical_template["wire_task"]

    lint = assert_mapping(lint_workflow_result(file=path, catalog=catalog).data)
    historical_lint = assert_mapping(semantics["valid_lint"])
    for key in ("kind", "valid", "summary", "compilation"):
        assert lint[key] == historical_lint[key]
    invalid = {
        "workflow": assert_mapping(semantics["input"])["workflow"],
        "tasks": [{"name": "shell", "type": "SHELL"}],
    }
    path.write_text(yaml.safe_dump(invalid), encoding="utf-8")
    diagnostic = assert_mapping(lint_workflow_result(file=path, catalog=catalog).data)
    assert diagnostic["valid"] is False
    assert any(
        assert_mapping(item)["code"] == "workflow_model_value_error"
        and "either task_params or command" in str(assert_mapping(item)["message"])
        for item in assert_sequence(diagnostic["diagnostics"])
    )
    monkeypatch.setattr(
        workflow_create_service,
        "resolve_runtime_selection",
        lambda _env_file: SimpleNamespace(
            execution_profile=SimpleNamespace(ds_version=catalog.profile_version)
        ),
    )
    with pytest.raises(UserInputError) as caught:
        workflow_create_service.create_workflow_result(file=path, dry_run=True)
    historical_error = assert_mapping(semantics["missing_command_create_error"])
    assert caught.value.error_type == historical_error["error_type"]
    assert sorted(caught.value.details) == historical_error["details_keys"]
    assert bool(caught.value.suggestion) == historical_error["suggestion_present"]


def test_published_shell_create_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    semantics = assert_mapping(_corpus()["semantics"])
    historical = assert_mapping(semantics["create_dry_run"])
    _install(
        monkeypatch,
        workflows=FakeWorkflowAdapter(workflows=[], dags={}),
        tasks=FakeTaskAdapter(workflow_tasks={}),
    )
    path = tmp_path / "shell.yaml"
    path.write_text(yaml.safe_dump(semantics["input"]), encoding="utf-8")
    result = workflow_service.create_workflow_result(file=path, dry_run=True)
    request = first_dry_run_request(result.data)
    assert request["method"] == historical["method"]
    assert request["path"] == historical["path"]
    actual_form = _form(request, canonical_task_code=7001)
    old_form = dict(assert_mapping(historical["form"]))
    assert old_form.pop("description") is None
    old_form.pop("locations")
    assert actual_form == old_form


def test_published_shell_export_reparse_edit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    semantics = assert_mapping(_corpus()["semantics"])
    historical = assert_mapping(semantics["round_trip"])
    native = assert_mapping(historical["native_dag_input"])
    native_params = assert_mapping(native["task_params"])
    workflow = FakeWorkflow(
        code=301,
        name="published-shell-corpus",
        project_code_value=7,
        project_name_value="etl-prod",
        description="Synthetic native workflow",
        release_state_value=FakeEnumValue("OFFLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    task = FakeTaskDefinition(
        code=410,
        name="shell",
        project_code_value=7,
        project_name_value="etl-prod",
        description="Synthetic native SHELL task",
        task_type_value="SHELL",
        task_params_value=json.dumps(native_params, separators=(",", ":")),
        task_priority_value=FakeEnumValue("HIGH"),
    )
    object.__setattr__(task, "nativeUnmodeledMarker", native["native_unmodeled_marker"])
    workflows = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={301: FakeDag(workflow, [task], [FakeWorkflowTaskRelation(0, 410)])},
    )
    _install(
        monkeypatch,
        workflows=workflows,
        tasks=FakeTaskAdapter(workflow_tasks={301: [task]}),
    )
    exported = workflow_service.export_workflow_yaml_result(
        "published-shell-corpus", project="etl-prod"
    )
    document = yaml.safe_load(str(assert_mapping(exported.data)["yaml"]))
    _assert_historical_yaml_fields(document, historical["rendered_yaml_document"])
    catalog = workflow_authoring_catalog_for_version("3.4.1")
    parsed = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog, intent=TaskAuthoringIntent.OPAQUE_PRESERVE
        ),
    )
    assert parsed.tasks[0].task_params == native_params
    edited = deepcopy(document)
    cast("dict[str, object]", edited["workflow"])["description"] = (
        "Synthetic updated workflow"
    )
    _assert_historical_yaml_fields(edited, historical["edited_yaml_document"])
    path = tmp_path / "edited.yaml"
    path.write_text(yaml.safe_dump(edited), encoding="utf-8")
    result = workflow_service.edit_workflow_result(
        "published-shell-corpus", file=path, project="etl-prod", dry_run=True
    )
    old_update = assert_mapping(historical["update_dry_run"])
    request = first_dry_run_request(result.data)
    assert request["method"] == old_update["method"]
    assert request["path"] == old_update["path"]
    assert assert_mapping(result.data)["no_change"] is historical["no_change"]
    actual_form = _form(request)
    old_form = deepcopy(dict(assert_mapping(old_update["form"])))
    old_form.pop("locations")
    current_task = assert_mapping(assert_sequence(actual_form["taskDefinitionJson"])[0])
    assert current_task["code"] == 410
    assert current_task["version"] == 1
    assert assert_mapping(current_task["taskParams"]) == native_params
    if "nativeUnmodeledMarker" in current_task:
        assert (
            current_task["nativeUnmodeledMarker"] == native["native_unmodeled_marker"]
        )
        cast("dict[str, object]", current_task).pop("nativeUnmodeledMarker")
    assert actual_form == old_form
