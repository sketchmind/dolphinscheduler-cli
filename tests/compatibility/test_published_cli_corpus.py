from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import yaml
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.errors import UserInputError
from dsctl.models import load_workflow_spec
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.lint.workflow import lint_workflow_result
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.services.template import task_template_result
from dsctl.services.workflow import create as workflow_create_service

_CORPUS_PATH = Path(__file__).parent / "corpus" / "v0.3.0-ds3.4.1-sql-inline.json"


def _published_sql_corpus() -> dict[str, object]:
    corpus = cast(
        "dict[str, object]", json.loads(_CORPUS_PATH.read_text(encoding="utf-8"))
    )
    source = assert_mapping(corpus["source_cli"])
    assert source["kind"] == "git_tag"
    assert source["tag"] == "v0.3.0"
    assert source["package_version"] == "0.3.0"
    assert source["commit"] == "bf7a1174b4a726f68f36d341e67d4132b68e65e8"
    assert source["artifact_origin"] == "local_rebuild_from_git_tag"
    assert source["reconstructed_wheel_sha256"] == (
        "1966fe1bee4b86396b1f2fb29fca0d0a4f8f311b719ae7dbb9cf073dd19051f2"
    )
    assert source["source_manifest_sha256"] == (
        "c0d0ec9dd6df7619c2fc446ab8562b4915559bb4f99c29b6fae0df80f4dda2a0"
    )
    assert corpus["ds_profile"] == "3.4.1"
    assert corpus["task_type"] == "SQL"
    assert corpus["facet"] == "inline_script"
    return corpus


def _sql_schema_semantics(value: object, *, field: str = "") -> object:
    """Ignore only presentation annotations and unordered schema sets."""
    if field in {"default", "const"}:
        return value
    if field in {"enum", "required"} and isinstance(value, list):
        return sorted(value, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, dict):
        if field in {
            "properties",
            "$defs",
            "definitions",
            "patternProperties",
            "dependentSchemas",
        }:
            return {
                name: _sql_schema_semantics(schema) for name, schema in value.items()
            }
        return {
            key: _sql_schema_semantics(item, field=key)
            for key, item in value.items()
            if key not in {"title", "description", "examples", "x-dsctl"}
        }
    if isinstance(value, list):
        return [_sql_schema_semantics(item) for item in value]
    return value


def _retain_published_optional_properties(old: object, current: object) -> None:
    """Project new optional fields away while retaining every old schema branch."""
    if not isinstance(old, dict) or not isinstance(current, dict):
        return
    if "properties" in old:
        old_properties = assert_mapping(old["properties"])
        current_properties = assert_mapping(current["properties"])
        assert old_properties.keys() <= current_properties.keys()
        current["properties"] = {key: current_properties[key] for key in old_properties}
    for key, old_value in old.items():
        if key in current:
            _retain_published_optional_properties(old_value, current[key])


def test_published_030_sql_inline_semantics_remain_compatible(
    tmp_path: Path,
) -> None:
    corpus = _published_sql_corpus()
    authoring = cast("dict[str, object]", corpus["authoring"])
    semantics = assert_mapping(corpus["semantics"])
    catalog = workflow_authoring_catalog_for_version(cast("str", corpus["ds_profile"]))
    task_type = cast("str", corpus["task_type"])
    assert catalog.profile_version == corpus["ds_profile"]

    summary = task_type_summary_data(task_type, catalog=catalog)
    assert summary["kind"] == authoring["kind"]
    assert summary["required_paths"] == authoring["required_paths"]
    schema = cast(
        "dict[str, object]", task_type_schema_result(task_type, catalog=catalog).data
    )
    state_rules = cast("list[dict[str, object]]", schema["state_rules"])
    projected_rules = [
        {key: rule.get(key, []) for key in ("when", "active_paths", "inactive_paths")}
        for rule in state_rules
    ]
    expected_rules = cast("list[dict[str, object]]", authoring["state_rules"])
    expected_projected = [
        {key: rule.get(key, []) for key in ("when", "active_paths", "inactive_paths")}
        for rule in expected_rules
    ]
    assert projected_rules == expected_projected

    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(yaml.safe_dump(semantics["input"]), encoding="utf-8")
    spec = load_workflow_spec(
        workflow_path,
        authoring_context=workflow_authoring_context(
            catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
        ),
    )
    input_semantics = assert_mapping(semantics["input_semantics"])
    expected_params = cast(
        "dict[str, object]", input_semantics["normalized_task_params"]
    )
    assert expected_params == corpus["normalized_task_params"]
    assert spec.tasks[0].task_params == expected_params

    compiled = prepare_workflow_create_compilation(spec, catalog=catalog).materialize(
        [7001]
    )
    tasks = json.loads(compiled["taskDefinitionJson"])
    tasks[0]["taskParams"] = json.loads(tasks[0]["taskParams"])
    assert tasks[0] == input_semantics["compiled_task"]


def test_published_030_template_aliases_have_explicit_current_discovery_guidance() -> (
    None
):
    # Preserve the published corpus. Removing redundant selectors is a deliberate
    # discovery-contract change; the SQL state rules and native payload above
    # remain independent compatibility assertions.
    corpus = _published_sql_corpus()
    authoring = assert_mapping(corpus["authoring"])
    catalog = workflow_authoring_catalog_for_version(cast("str", corpus["ds_profile"]))
    task_type = cast("str", corpus["task_type"])
    default = task_template_result(task_type, catalog=catalog)
    data = assert_mapping(default.data)
    assert isinstance(data["yaml"], str)
    assert "variant" not in default.resolved
    assert "--variant" not in str(assert_mapping(data["artifact"])["raw_command"])

    for variant in assert_sequence(authoring["variants"]):
        assert isinstance(variant, str)
        with pytest.raises(UserInputError) as caught:
            task_template_result(task_type, variant=variant, catalog=catalog)
        assert variant not in assert_sequence(
            caught.value.details["available_variants"]
        )
        assert "Omit --variant for the default template" in str(caught.value.suggestion)


def test_published_030_sql_schema_accepts_the_old_fields_and_integer_datasource() -> (
    None
):
    corpus = _published_sql_corpus()
    semantics = assert_mapping(corpus["semantics"])
    catalog = workflow_authoring_catalog_for_version(cast("str", corpus["ds_profile"]))
    task_type = cast("str", corpus["task_type"])
    field_data = assert_mapping(
        task_type_schema_result(task_type, catalog=catalog).data
    )
    fields = assert_sequence(field_data["fields"])
    current = {assert_mapping(row)["path"]: assert_mapping(row) for row in fields}
    for raw_old in assert_sequence(semantics["schema_fields"]):
        old = assert_mapping(raw_old)
        path = old["path"]
        actual = current[path]
        for key, value in old.items():
            if key == "type" and path == "task_params.datasource":
                assert value == "integer"
                assert actual[key] == "integer|string"
            elif key == "choices":
                assert set(assert_sequence(actual[key])) == set(assert_sequence(value))
            else:
                assert actual[key] == value, (path, key)

    old_schema = cast(
        "dict[str, object]", _sql_schema_semantics(semantics["json_schema"])
    )
    schema_data = assert_mapping(
        task_type_schema_result(task_type, catalog=catalog, json_schema=True).data
    )
    new_schema = cast("dict[str, object]", _sql_schema_semantics(schema_data["schema"]))
    old_root_properties = assert_mapping(old_schema["properties"])
    new_root_properties = assert_mapping(new_schema["properties"])
    assert "description" in old_root_properties
    assert "description" in new_root_properties
    old_params = assert_mapping(assert_mapping(old_schema["$defs"])["task_params"])
    new_params = assert_mapping(assert_mapping(new_schema["$defs"])["task_params"])
    old_properties = assert_mapping(old_params["properties"])
    new_properties = cast("dict[str, object]", new_params["properties"])
    old_integer_datasource = assert_mapping(old_properties["datasource"])
    assert old_integer_datasource == {"type": "integer", "minimum": 1}
    datasource = cast("dict[str, object]", new_properties["datasource"])
    datasource_branches = assert_sequence(datasource["anyOf"])
    assert len(datasource_branches) == 2
    assert old_integer_datasource in datasource_branches
    assert {"type": "string", "pattern": r"\S"} in datasource_branches
    new_properties["datasource"] = old_integer_datasource

    old_sql_type = assert_mapping(old_properties["sqlType"])
    assert old_sql_type == {"type": "integer", "minimum": 0, "maximum": 1}
    current_sql_type = cast("dict[str, object]", new_properties["sqlType"])
    assert current_sql_type["enum"] == [0, 1]
    del current_sql_type["enum"]

    _retain_published_optional_properties(old_schema, new_schema)
    assert new_schema == old_schema


def test_published_030_sql_template_fragment_still_parses_and_compiles(
    tmp_path: Path,
) -> None:
    corpus = _published_sql_corpus()
    semantics = assert_mapping(corpus["semantics"])
    catalog = workflow_authoring_catalog_for_version(cast("str", corpus["ds_profile"]))
    template = assert_mapping(semantics["template"])
    document = {
        "workflow": assert_mapping(assert_mapping(semantics["input"])["workflow"]),
        "tasks": [template["task"]],
    }
    path = tmp_path / "published-template.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    spec = load_workflow_spec(
        path,
        authoring_context=workflow_authoring_context(
            catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
        ),
    )
    assert spec.tasks[0].task_params == template["normalized_task_params"]
    compiled = prepare_workflow_create_compilation(spec, catalog=catalog).materialize(
        [7001]
    )
    task = json.loads(compiled["taskDefinitionJson"])[0]
    task["taskParams"] = json.loads(task["taskParams"])
    assert task == template["compiled_task"]


def test_published_030_sql_lint_and_create_error_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = _published_sql_corpus()
    semantics = assert_mapping(corpus["semantics"])
    catalog = workflow_authoring_catalog_for_version(cast("str", corpus["ds_profile"]))
    path = tmp_path / "published-sql.yaml"
    path.write_text(yaml.safe_dump(semantics["input"]), encoding="utf-8")
    current_lint = assert_mapping(lint_workflow_result(file=path, catalog=catalog).data)
    historical_lint = assert_mapping(semantics["lint"])
    for key in ("kind", "valid", "summary", "compilation"):
        assert current_lint[key] == historical_lint[key]
    current_pass_codes = {
        assert_mapping(item)["code"]
        for item in assert_sequence(current_lint["diagnostics"])
        if assert_mapping(item)["severity"] == "info"
    }
    old_pass_codes = {
        assert_mapping(item)["code"]
        for item in assert_sequence(historical_lint["checks"])
        if assert_mapping(item)["status"] == "pass"
    }
    assert old_pass_codes <= current_pass_codes

    invalid = cast("dict[str, object]", deepcopy(semantics["input"]))
    task = cast("dict[str, object]", assert_sequence(invalid["tasks"])[0])
    params = cast("dict[str, object]", task["task_params"])
    params.pop("sql")
    path.write_text(yaml.safe_dump(invalid), encoding="utf-8")
    rejected = assert_mapping(lint_workflow_result(file=path, catalog=catalog).data)
    assert rejected["valid"] is False
    assert any(
        assert_mapping(item)["code"] == "workflow_task_model_missing"
        and assert_mapping(item)["path"] == "tasks[0].task_params.sql"
        for item in assert_sequence(rejected["diagnostics"])
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
    historical_error = assert_mapping(semantics["create_missing_sql_error"])
    assert caught.value.error_type == historical_error["type"]
    assert sorted(caught.value.details) == historical_error["details_keys"]
    assert bool(caught.value.suggestion) == historical_error["suggestion_present"]
