from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services._task_authoring_prep import parameter_example_yaml

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
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


_HIVECLI_INLINE_FACET = "HIVECLI/inline_script"
_HIVECLI_VERSIONS = (
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_HIVECLI_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_HIVECLI_WHOLE_COMMAND_VERSIONS = ("3.1.0", "3.1.9")


def _hivecli_spec(task_params: YamlObject, *, suffix: str) -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": f"hivecli-{suffix}"},
            "tasks": [
                {
                    "name": "query-daily-orders",
                    "type": "HIVECLI",
                    "task_params": task_params,
                }
            ],
        }
    )


def _template_task(ds_version: str, variant: str) -> YamlObject:
    if variant == "params":
        yaml_text = parameter_example_yaml("HIVECLI", ds_version)
    else:
        result = task_template_result(
            "HIVECLI",
            variant=None if variant == "minimal" else variant,
            catalog=get_task_authoring_catalog(ds_version),
        )
        assert isinstance(result.data, dict)
        yaml_text = result.data["yaml"]
        assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    prepared = prepare_workflow_create_compilation(
        _hivecli_spec(task_params, suffix=ds_version),
        catalog=get_task_authoring_catalog(ds_version),
    )
    payload = prepared.materialize([31_001])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "HIVECLI"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
def test_hivecli_schema_exposes_reviewed_inline_script_surface(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    result = task_type_schema_result("HIVECLI", catalog=catalog)
    membership = catalog.require_facet("HIVECLI", _HIVECLI_INLINE_FACET)
    source_review = catalog.task_type_facts["HIVECLI"].typed_authoring_review

    assert catalog.supports_typed_authoring("HIVECLI") is True
    assert source_review is not None
    assert membership.contract.review == source_review.review
    assert isinstance(result.data, dict)
    assert result.data["kind"] == "typed"
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    execution_type = fields["task_params.hiveCliTaskExecutionType"]
    assert execution_type["required"] is True
    assert execution_type["choices"] == ["SCRIPT"]
    assert execution_type["compile_path"].endswith(
        "taskParams.hiveCliTaskExecutionType"
    )
    assert fields["task_params.hiveSqlScript"]["required"] is True
    assert fields["task_params.hiveSqlScript"]["compile_path"].endswith(
        "taskParams.hiveSqlScript"
    )
    assert fields["task_params.hiveCliOptions"]["required"] is False
    assert fields["task_params.hiveCliOptions"]["compile_path"].endswith(
        "taskParams.hiveCliOptions"
    )
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]
    assert fields["task_params.localParams[].type"]["choices"] == ["VARCHAR"]
    assert not any(path.startswith("task_params.resourceList") for path in fields)
    assert not any(path.startswith("task_params.varPool") for path in fields)


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
def test_hivecli_discovery_explains_exact_execution_epoch_and_worker_requirement(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "HIVECLI",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    descriptions = " ".join(
        str(field.get("description", ""))
        for field in result.data["fields"]
        if isinstance(field, dict)
    ).lower()

    assert "worker" in descriptions
    assert "hive cli" in descriptions
    if ds_version in _HIVECLI_WHOLE_COMMAND_VERSIONS:
        assert "whole command" in descriptions or "entire command" in descriptions
        assert "hive -e" in descriptions
    else:
        assert "sql-only" in descriptions or "sql text only" in descriptions
        assert "temporary" in descriptions or "temp file" in descriptions
        assert "hive -f" in descriptions


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_hivecli_json_schema_is_closed_and_matches_inline_contract(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "HIVECLI",
        json_schema=True,
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)
    nested_definitions = task_params["$defs"]
    assert isinstance(nested_definitions, dict)

    assert task_params["additionalProperties"] is False
    assert set(task_params["required"]) == {
        "hiveCliTaskExecutionType",
        "hiveSqlScript",
    }
    assert set(properties) == {
        "hiveCliTaskExecutionType",
        "hiveSqlScript",
        "hiveCliOptions",
        "localParams",
    }
    assert properties["hiveCliTaskExecutionType"]["enum"] == ["SCRIPT"]
    assert properties["hiveSqlScript"]["type"] == "string"
    options_schema = properties["hiveCliOptions"]
    assert isinstance(options_schema, dict)
    assert options_schema.get("type") == "string" or {"type": "string"} in (
        options_schema.get("anyOf", [])
    )
    assert "placeholder" in str(options_schema["description"]).lower()
    assert nested_definitions["Direct"]["enum"] == ["IN"]
    assert nested_definitions["DataType"]["enum"] == ["VARCHAR"]


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_hivecli_summary_and_compile_mappings_are_bounded_to_inline_fields(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("HIVECLI", catalog=catalog)
    result = task_type_schema_result(
        "HIVECLI",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_param_mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["kind"] == "typed"
    assert summary["variants"] == []
    assert {
        "task_params.hiveCliTaskExecutionType",
        "task_params.hiveSqlScript",
    }.issubset(summary["required_paths"])
    assert "task_params.hiveCliOptions" not in summary["required_paths"]
    assert "task_params.localParams[]" not in summary["required_paths"]
    assert set(task_param_mappings) == {
        "task_params.hiveCliTaskExecutionType",
        "task_params.hiveSqlScript",
        "task_params.hiveCliOptions",
        "task_params.localParams[]",
        "task_params.localParams[].prop",
        "task_params.localParams[].direct",
        "task_params.localParams[].type",
        "task_params.localParams[].value",
    }
    for authoring_path, payload_path in task_param_mappings.items():
        relative_path = authoring_path.removeprefix("task_params.")
        if relative_path == "localParams[]":
            relative_path = "localParams"
        assert payload_path == f"taskDefinitionJson[].taskParams.{relative_path}"


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_hivecli_templates_compile_without_rewriting_canonical_params(
    ds_version: str,
    variant: str,
) -> None:
    task = _template_task(ds_version, variant)
    params = task["task_params"]
    assert isinstance(params, dict)
    script = params["hiveSqlScript"]
    local_params = params["localParams"]

    assert task["type"] == "HIVECLI"
    assert params["hiveCliTaskExecutionType"] == "SCRIPT"
    assert isinstance(script, str)
    assert script.strip()
    assert isinstance(local_params, list)
    assert not {"resourceList", "varPool"}.intersection(params)
    if variant == "minimal":
        assert local_params == []
    else:
        assert local_params
        for raw_parameter in local_params:
            assert isinstance(raw_parameter, dict)
            assert raw_parameter["direct"] == "IN"
            assert raw_parameter["type"] == "VARCHAR"
            prop = raw_parameter["prop"]
            assert isinstance(prop, str)
            assert f"${{{prop}}}" in script
    options = params.get("hiveCliOptions")
    if options is not None:
        assert isinstance(options, str)
        assert "${" not in options
        assert "$[" not in options

    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
def test_hivecli_prepared_compilation_is_exact_identity(ds_version: str) -> None:
    canonical: YamlObject = {
        "hiveCliTaskExecutionType": "SCRIPT",
        "hiveSqlScript": "SELECT '${bizdate}', '$[yyyyMMdd]';\n",
        "hiveCliOptions": "  --silent --showHeader=false  ",
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${system.biz.date}",
            }
        ],
    }

    assert _compiled_task_params(ds_version, canonical) == canonical


@pytest.mark.parametrize(
    ("invalid_params", "field"),
    [
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "   ",
                "localParams": [],
            },
            "hiveSqlScript",
        ),
        (
            {
                "hiveCliTaskExecutionType": "FILE",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [],
            },
            "hiveCliTaskExecutionType",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "hiveCliOptions": "--hiveconf queue=${queue}",
                "localParams": [],
            },
            "hiveCliOptions",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "hiveCliOptions": "--hiveconf day=$[yyyyMMdd]",
                "localParams": [],
            },
            "hiveCliOptions",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT '${bizdate}';",
                "localParams": [
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "2026-08-20",
                    },
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "2026-08-21",
                    },
                ],
            },
            "bizdate",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [
                    {
                        "prop": "result",
                        "direct": "OUT",
                        "type": "VARCHAR",
                        "value": "",
                    }
                ],
            },
            "OUT",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [
                    {
                        "prop": "limit",
                        "direct": "IN",
                        "type": "INTEGER",
                        "value": "10",
                    }
                ],
            },
            "VARCHAR",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [],
                "resourceList": [],
            },
            "resourceList",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [],
                "varPool": [],
            },
            "varPool",
        ),
        (
            {
                "hiveCliTaskExecutionType": "SCRIPT",
                "hiveSqlScript": "SELECT 1;",
                "localParams": [],
                "futureField": {"enabled": True},
            },
            "futureField",
        ),
    ],
    ids=[
        "blank-script",
        "file-mode",
        "named-placeholder-in-options",
        "date-placeholder-in-options",
        "duplicate-local-param",
        "out-local-param",
        "non-varchar-local-param",
        "resource-list",
        "var-pool",
        "unknown-field",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_hivecli_typed_create_and_edit_fail_closed(
    invalid_params: YamlObject,
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "HIVECLI",
            invalid_params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_hivecli_typed_create_and_edit_preserve_raw_inline_values(
    intent: TaskAuthoringIntent,
) -> None:
    params: YamlObject = {
        "hiveCliTaskExecutionType": "SCRIPT",
        "hiveSqlScript": "  SELECT '${bizdate}';\n",
        "hiveCliOptions": "  --silent  ",
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${system.biz.date}",
            }
        ],
    }

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "HIVECLI",
        params,
        intent=intent,
    )

    assert normalized == params


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
def test_hivecli_opaque_file_runtime_and_future_fields_are_deep_preserved(
    ds_version: str,
) -> None:
    native: YamlObject = {
        "hiveCliTaskExecutionType": "FILE",
        "hiveCliOptions": "--hiveconf queue=${runtime_queue}",
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/queries/daily-orders.sql",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "localParams": [
            {
                "prop": "runtime_queue",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "futureField": {"nested": ["native"]},
    }
    expected = deepcopy(native)
    catalog = get_task_authoring_catalog(ds_version)

    preserved = catalog.normalize_task_params(
        "HIVECLI",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    resources = native["resourceList"]
    assert isinstance(resources, list)
    resource = resources[0]
    assert isinstance(resource, dict)
    metadata = resource["futureMetadata"]
    assert isinstance(metadata, dict)
    metadata["checksum"] = "mutated"
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


@pytest.mark.parametrize("ds_version", _HIVECLI_VERSIONS)
def test_hivecli_file_export_and_metadata_compile_preserve_native_payload(
    ds_version: str,
) -> None:
    native_params: YamlObject = {
        "hiveCliTaskExecutionType": "FILE",
        "hiveCliOptions": "--hiveconf queue=${runtime_queue}",
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/queries/daily-orders.sql",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "localParams": [
            {
                "prop": "runtime_queue",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "futureField": {"nested": ["native"]},
    }
    task = FakeTaskDefinition(
        code=101,
        name="query-from-file",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="HIVECLI",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="hivecli-file-roundtrip",
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


@pytest.mark.parametrize("ds_version", _HIVECLI_ABSENT_VERSIONS)
def test_hivecli_is_unavailable_when_the_exact_source_has_no_plugin(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("HIVECLI") is False
    assert catalog.supports_opaque_authoring("HIVECLI") is False
    assert "HIVECLI" not in catalog.authoring_task_types
