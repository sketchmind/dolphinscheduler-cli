"""Workflow wire and round-trip compatibility with the published 0.3.0 corpus."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING

import yaml
from tests.compatibility.test_published_cli_corpus import _published_sql_corpus
from tests.fakes import (
    FakeDag,
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import _WorkflowServiceHarness
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    workflows: FakeWorkflowAdapter,
    tasks: FakeTaskAdapter,
) -> None:
    harness = _WorkflowServiceHarness(
        monkeypatch=monkeypatch,
        project_adapter=FakeProjectAdapter([FakeProject(code=7, name="etl-prod")]),
        workflow_adapter=workflows,
        task_adapter=tasks,
        schedule_adapter=FakeScheduleAdapter(schedules=[]),
    )
    harness.install(
        profile=make_profile(ds_version="3.4.1"),
        datasource_adapter=FakeDataSourceAdapter(
            [
                FakeDataSource(
                    id=1,
                    name="synthetic-mysql",
                    type_value=FakeEnumValue("MYSQL"),
                )
            ]
        ),
    )


def _form(
    request: object, *, canonical_task_code: int | None = None
) -> dict[str, object]:
    wire = assert_mapping(request)
    form = dict(assert_mapping(wire["form"]))
    form.pop("locations", None)  # UI placement is outside the historical corpus.
    for field in ("globalParams", "taskRelationJson", "taskDefinitionJson"):
        form[field] = json.loads(str(form[field]))
    definitions = form["taskDefinitionJson"]
    assert isinstance(definitions, list)
    assert len(definitions) == 1
    task = definitions[0]
    assert isinstance(task, dict)
    actual_task_code = task["code"]
    assert type(actual_task_code) is int
    assert actual_task_code > 0
    if canonical_task_code is not None:
        task["code"] = canonical_task_code
    task["taskParams"] = json.loads(str(task["taskParams"]))
    relations = form["taskRelationJson"]
    assert isinstance(relations, list)
    for row in relations:
        assert isinstance(row, dict)
        if canonical_task_code is not None and row["preTaskCode"] == actual_task_code:
            row["preTaskCode"] = canonical_task_code
        if canonical_task_code is not None and row["postTaskCode"] == actual_task_code:
            row["postTaskCode"] = canonical_task_code
    return form


def _assert_historical_yaml_fields(document: object, historical: object) -> None:
    actual = assert_mapping(document)
    old = assert_mapping(historical)
    actual_workflow = assert_mapping(actual["workflow"])
    for field, value in assert_mapping(old["workflow"]).items():
        assert actual_workflow[field] == value
    actual_tasks = assert_sequence(actual["tasks"])
    old_tasks = assert_sequence(old["tasks"])
    assert len(actual_tasks) == len(old_tasks) == 1
    actual_task = assert_mapping(actual_tasks[0])
    old_task = assert_mapping(old_tasks[0])
    assert actual_task["name"] == old_task["name"]
    for field, value in old_task.items():
        assert actual_task[field] == value


def test_published_030_workflow_create_dry_run_matches_wire_form(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    corpus = _published_sql_corpus()
    semantics = assert_mapping(corpus["semantics"])
    historical = assert_mapping(semantics["dry_run"])
    old_form = assert_mapping(historical["form"])
    old_task = assert_mapping(assert_sequence(old_form["taskDefinitionJson"])[0])
    old_task_code = old_task["code"]
    assert isinstance(old_task_code, int)
    _install(
        monkeypatch,
        workflows=FakeWorkflowAdapter(workflows=[], dags={}),
        tasks=FakeTaskAdapter(workflow_tasks={}),
    )
    path = tmp_path / "published-sql.yaml"
    path.write_text(yaml.safe_dump(semantics["input"]), encoding="utf-8")

    result = workflow_service.create_workflow_result(file=path, dry_run=True)
    request = first_dry_run_request(result.data)

    assert request["method"] == historical["method"]
    assert request["path"] == historical["path"]
    actual_form = _form(request, canonical_task_code=old_task_code)
    # The current prepared call omits an absent optional description; the old
    # request carried the same absence as an explicit null value.
    assert old_form["description"] is None
    assert actual_form.pop("description", None) is None
    expected_form = dict(old_form)
    expected_form.pop("description")
    assert actual_form == expected_form


def test_published_030_workflow_export_reparse_edit_keeps_native_sql_params(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    corpus = _published_sql_corpus()
    semantics = assert_mapping(corpus["semantics"])
    historical = assert_mapping(semantics["round_trip"])
    native = assert_mapping(historical["native_dag_input"])
    native_params = assert_mapping(native["task_params"])
    workflow = FakeWorkflow(
        code=301,
        name="published-sql-corpus",
        project_code_value=7,
        project_name_value="etl-prod",
        description="Synthetic native workflow",
        release_state_value=FakeEnumValue("OFFLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    task = FakeTaskDefinition(
        code=410,
        name="query",
        project_code_value=7,
        project_name_value="etl-prod",
        description="Synthetic native SQL task",
        task_type_value="SQL",
        task_params_value=json.dumps(native_params, separators=(",", ":")),
        task_priority_value=FakeEnumValue("HIGH"),
    )
    object.__setattr__(task, "nativeUnmodeledMarker", native["native_unmodeled_marker"])
    workflows = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            301: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[FakeWorkflowTaskRelation(0, 410)],
            )
        },
    )
    _install(
        monkeypatch,
        workflows=workflows,
        tasks=FakeTaskAdapter(workflow_tasks={301: [task]}),
    )

    exported = workflow_service.export_workflow_yaml_result(
        "published-sql-corpus", project="etl-prod"
    )
    document = yaml.safe_load(str(assert_mapping(exported.data)["yaml"]))
    _assert_historical_yaml_fields(document, historical["rendered_yaml_document"])
    catalog = workflow_authoring_catalog_for_version(str(corpus["ds_profile"]))
    parsed = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )
    assert parsed.tasks[0].task_params == native_params
    historical_parsed_params = assert_mapping(historical["reparsed_task_params"])
    historical_without_defaults = dict(historical_parsed_params)
    for field in ("localParams", "varPool", "preStatements", "postStatements"):
        assert historical_parsed_params[field] == []
        historical_without_defaults.pop(field)
    assert historical_without_defaults == native_params
    edited = deepcopy(document)
    edited_workflow = edited["workflow"]
    assert isinstance(edited_workflow, dict)
    edited_workflow["description"] = "Synthetic updated workflow"
    _assert_historical_yaml_fields(edited, historical["edited_yaml_document"])
    path = tmp_path / "exported-and-edited.yaml"
    path.write_text(yaml.safe_dump(edited), encoding="utf-8")

    result = workflow_service.edit_workflow_result(
        "published-sql-corpus", file=path, project="etl-prod", dry_run=True
    )
    old_update = assert_mapping(historical["update_dry_run"])
    old_form = assert_mapping(old_update["form"])
    old_task = assert_mapping(assert_sequence(old_form["taskDefinitionJson"])[0])
    old_task_code = old_task["code"]
    assert isinstance(old_task_code, int)
    assert old_task_code == native["task_code"] == 410
    request = first_dry_run_request(result.data)

    assert request["method"] == old_update["method"]
    assert request["path"] == old_update["path"]
    assert assert_mapping(result.data)["no_change"] is historical["no_change"]
    actual_form = _form(request)
    current_task = assert_mapping(assert_sequence(actual_form["taskDefinitionJson"])[0])
    assert current_task["code"] == old_task_code
    assert current_task["version"] == old_task["version"] == 1
    current_relations = assert_sequence(actual_form["taskRelationJson"])
    assert len(current_relations) == 1
    assert assert_mapping(current_relations[0])["postTaskCode"] == old_task_code
    current_params = assert_mapping(current_task["taskParams"])
    assert current_params == native_params
    expected_form = deepcopy(dict(old_form))
    expected_task = assert_mapping(
        assert_sequence(expected_form["taskDefinitionJson"])[0]
    )
    expected_params = dict(assert_mapping(expected_task["taskParams"]))
    # Current existing-baseline preservation keeps the native JSON unchanged.
    # The old compiler synthesized these four absent empty arrays on update.
    for field in ("localParams", "varPool", "preStatements", "postStatements"):
        assert expected_params[field] == []
        assert field not in native_params
        if field not in current_params:
            expected_params.pop(field)
    assert isinstance(expected_task, dict)
    expected_task["taskParams"] = expected_params
    # The old exporter lost this native top-level field. Allow improved
    # preservation only when it retains the actual native value; other added
    # wire fields still participate in the historical comparison below.
    if "nativeUnmodeledMarker" in current_task:
        assert (
            current_task["nativeUnmodeledMarker"] == native["native_unmodeled_marker"]
        )
        assert isinstance(current_task, dict)
        current_task.pop("nativeUnmodeledMarker")
    assert actual_form == expected_form
    assert (
        assert_mapping(current_task["taskParams"])["legacyOpaque"]
        == native_params["legacyOpaque"]
    )
