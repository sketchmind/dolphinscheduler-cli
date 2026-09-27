from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.models import WorkflowSpec
from dsctl.models.task_spec import EmrServerlessTaskParamsSpec
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
from dsctl.upstream.resolver import ResolvedProject

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


_EMR_SERVERLESS_FACET = "EMR_SERVERLESS/start_job_run"
_EMR_SERVERLESS_VERSION = "3.4.2"
_EMR_SERVERLESS_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
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
    "3.4.1",
)


def _emr_serverless_spec(task_params: YamlObject, *, suffix: str) -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": f"emr-serverless-{suffix}"},
            "tasks": [
                {
                    "name": "submit-serverless-job",
                    "type": "EMR_SERVERLESS",
                    "task_params": task_params,
                }
            ],
        }
    )


def _template_yaml(variant: str) -> str:
    if variant == "params":
        return authoring_prep.parameter_example_yaml("EMR_SERVERLESS", "3.4.2")
    return authoring_prep.template_yaml(
        "EMR_SERVERLESS", _EMR_SERVERLESS_VERSION, variant=variant
    )


def _template_task(variant: str) -> YamlObject:
    document = yaml.safe_load(_template_yaml(variant))
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _compiled_task_params(task_params: YamlObject) -> YamlObject:
    prepared = prepare_workflow_create_compilation(
        _emr_serverless_spec(task_params, suffix="prepared"),
        catalog=get_task_authoring_catalog(_EMR_SERVERLESS_VERSION),
    )
    payload = prepared.materialize([34_200])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "EMR_SERVERLESS"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def test_emr_serverless_catalog_exposes_the_single_reviewed_exact_membership() -> None:
    catalog = get_task_authoring_catalog(_EMR_SERVERLESS_VERSION)
    membership = catalog.require_facet(
        "EMR_SERVERLESS",
        _EMR_SERVERLESS_FACET,
    )
    source_review = catalog.task_type_facts["EMR_SERVERLESS"].typed_authoring_review

    assert catalog.supports_typed_authoring("EMR_SERVERLESS") is True
    assert catalog.supports_opaque_authoring("EMR_SERVERLESS") is True
    assert source_review is not None
    assert membership.contract.review == source_review.review
    assert membership.profile_version == _EMR_SERVERLESS_VERSION
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_preserve is True


def test_emr_serverless_schema_exposes_only_the_reviewed_raw_request_surface() -> None:
    catalog = get_task_authoring_catalog(_EMR_SERVERLESS_VERSION)
    result = task_type_schema_result("EMR_SERVERLESS", catalog=catalog)

    assert isinstance(result.data, dict)
    assert result.data["kind"] == "typed"
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_paths = {path for path in fields if path.startswith("task_params.")}

    assert task_param_paths == {
        "task_params.applicationId",
        "task_params.executionRoleArn",
        "task_params.jobName",
        "task_params.startJobRunRequestJson",
        "task_params.localParams[]",
        "task_params.localParams[].prop",
        "task_params.localParams[].direct",
        "task_params.localParams[].type",
        "task_params.localParams[].value",
    }
    assert fields["task_params.applicationId"]["required"] is True
    assert fields["task_params.executionRoleArn"]["required"] is True
    assert fields["task_params.jobName"]["required"] is False
    assert fields["task_params.startJobRunRequestJson"]["required"] is True
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]
    assert fields["task_params.localParams[].type"]["choices"] == ["VARCHAR"]
    assert not any(path.startswith("task_params.resourceList") for path in fields)
    assert not any(path.startswith("task_params.varPool") for path in fields)

    for field_name in (
        "applicationId",
        "executionRoleArn",
        "jobName",
    ):
        description = str(fields[f"task_params.{field_name}"]["description"])
        assert "placeholder" in description.lower()
        assert "not" in description.lower() or "forbid" in description.lower()

    request_description = str(
        fields["task_params.startJobRunRequestJson"]["description"]
    )
    assert "${...}" in request_description
    assert "$[...]" in request_description
    assert "override" in request_description.lower()
    assert "applicationid" in request_description.lower()
    assert "executionrolearn" in request_description.lower()
    assert "quoted" in request_description.lower()
    assert "unresolved" in request_description.lower()

    all_descriptions = " ".join(
        str(field.get("description", ""))
        for field in result.data["fields"]
        if isinstance(field, dict)
    ).lower()
    assert "aws.emr" in all_descriptions
    assert "credentials" in all_descriptions
    assert "failover" in all_descriptions
    assert "jobrunid" in all_descriptions


def test_emr_serverless_json_schema_is_closed_and_keeps_the_aws_dto_raw() -> None:
    result = task_type_schema_result(
        "EMR_SERVERLESS",
        json_schema=True,
        catalog=get_task_authoring_catalog(_EMR_SERVERLESS_VERSION),
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
        "applicationId",
        "executionRoleArn",
        "startJobRunRequestJson",
    }
    assert set(properties) == {
        "applicationId",
        "executionRoleArn",
        "jobName",
        "startJobRunRequestJson",
        "localParams",
    }
    for field_name in (
        "applicationId",
        "executionRoleArn",
        "startJobRunRequestJson",
    ):
        assert properties[field_name]["type"] == "string"
    job_name_schema = properties["jobName"]
    assert isinstance(job_name_schema, dict)
    assert job_name_schema.get("type") == "string" or {"type": "string"} in (
        job_name_schema.get("anyOf", [])
    )
    assert nested_definitions["Direct"]["enum"] == ["IN"]
    assert nested_definitions["DataType"]["enum"] == ["VARCHAR"]
    assert "JobDriver" not in properties
    assert "ConfigurationOverrides" not in properties


@pytest.mark.parametrize("reference_shape", ["direct", "wrapped", "intersection"])
def test_emr_serverless_schema_narrows_both_pydantic_reference_shapes(
    monkeypatch: pytest.MonkeyPatch,
    reference_shape: str,
) -> None:
    raw_schema = EmrServerlessTaskParamsSpec.model_json_schema(
        by_alias=True,
        ref_template="#/$defs/task_params/$defs/{model}",
    )
    local_properties = raw_schema["$defs"]["GlobalParamSpec"]["properties"]
    for field_name, enum_name in (("direct", "Direct"), ("type", "DataType")):
        field_schema = local_properties[field_name]
        field_schema.pop("$ref", None)
        field_schema.pop("allOf", None)
        reference = {"$ref": f"#/$defs/task_params/$defs/{enum_name}"}
        if reference_shape == "direct":
            field_schema.update(reference)
        else:
            field_schema["allOf"] = [reference]
            if reference_shape == "intersection":
                field_schema["allOf"].append({"type": "string"})
    monkeypatch.setattr(
        EmrServerlessTaskParamsSpec,
        "model_json_schema",
        lambda **_: deepcopy(raw_schema),
    )

    task_params = authoring_prep.task_params_schema(
        "EMR_SERVERLESS", _EMR_SERVERLESS_VERSION
    )
    definitions = task_params["$defs"]
    assert isinstance(definitions, dict)
    for field_name, enum_name, expected in (
        ("direct", "Direct", "IN"),
        ("type", "DataType", "VARCHAR"),
    ):
        field_schema = definitions["GlobalParamSpec"]["properties"][field_name]
        assert field_schema["default"] == expected
        if reference_shape == "intersection":
            assert field_schema["allOf"] == local_properties[field_name]["allOf"]
            assert field_schema["enum"] == [expected]
            if enum_name == "Direct":
                assert definitions[enum_name]["enum"] == ["IN", "OUT"]
        else:
            assert definitions[enum_name]["enum"] == [expected]
            assert "enum" not in field_schema


def test_emr_serverless_summary_and_compile_mappings_match_the_stable_intent() -> None:
    catalog = get_task_authoring_catalog(_EMR_SERVERLESS_VERSION)
    summary = task_type_summary_data("EMR_SERVERLESS", catalog=catalog)
    result = task_type_schema_result(
        "EMR_SERVERLESS",
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
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.applicationId",
        "task_params.executionRoleArn",
        "task_params.startJobRunRequestJson",
    }
    assert set(task_param_mappings) == {
        "task_params.applicationId",
        "task_params.executionRoleArn",
        "task_params.jobName",
        "task_params.startJobRunRequestJson",
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


@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_emr_serverless_templates_compile_as_raw_native_request_json(
    variant: str,
) -> None:
    yaml_text = _template_yaml(variant)
    task = _template_task(variant)
    params = task["task_params"]
    assert isinstance(params, dict)
    raw_request = params["startJobRunRequestJson"]
    local_params = params["localParams"]

    assert task["type"] == "EMR_SERVERLESS"
    assert isinstance(params["applicationId"], str)
    assert params["applicationId"].strip()
    assert isinstance(params["executionRoleArn"], str)
    assert params["executionRoleArn"].strip()
    assert isinstance(raw_request, str)
    assert isinstance(json.loads(raw_request), dict)
    assert isinstance(local_params, list)
    assert not {"resourceList", "varPool"}.intersection(params)
    for top_level_field in ("applicationId", "executionRoleArn", "jobName"):
        value = params.get(top_level_field)
        if value is not None:
            assert isinstance(value, str)
            assert "${" not in value
            assert "$[" not in value
    if variant == "minimal":
        assert local_params == []
    else:
        assert local_params
        assert "${" in raw_request or "$[" in raw_request
        for raw_parameter in local_params:
            assert isinstance(raw_parameter, dict)
            assert raw_parameter["direct"] == "IN"
            assert raw_parameter["type"] == "VARCHAR"
            prop = raw_parameter["prop"]
            assert isinstance(prop, str)
            assert f"${{{prop}}}" in raw_request

    guidance = yaml_text.lower()
    assert "aws.emr" in guidance
    assert "override" in guidance
    assert "applicationid" in guidance
    assert "executionrolearn" in guidance
    assert "failover" in guidance
    assert _compiled_task_params(params) == params


def test_emr_serverless_prepared_compilation_preserves_the_canonical_raw_values() -> (
    None
):
    canonical: YamlObject = {
        "applicationId": "00fkht2eodujab09",
        "executionRoleArn": (
            "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole"
        ),
        "jobName": "nightly-orders",
        "startJobRunRequestJson": (
            '{"ApplicationId":"ignored-in-request","ExecutionRoleArn":'
            '"ignored-in-request","JobDriver":${job_driver},'
            '"ConfigurationOverrides":{}}'
        ),
        "localParams": [
            {
                "prop": "job_driver",
                "direct": "IN",
                "type": "VARCHAR",
                "value": (
                    '{"SparkSubmit":{"EntryPoint":"s3://analytics/jobs/orders.py"}}'
                ),
            }
        ],
    }

    assert _compiled_task_params(canonical) == canonical


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
@pytest.mark.parametrize("job_name", [None, ""])
def test_emr_serverless_typed_create_and_edit_allow_the_task_name_default(
    intent: TaskAuthoringIntent,
    job_name: str | None,
) -> None:
    params: YamlObject = {
        "applicationId": "00fkht2eodujab09",
        "executionRoleArn": (
            "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole"
        ),
        "startJobRunRequestJson": (
            '{"ApplicationId":"ignored","ExecutionRoleArn":"ignored",'
            '"JobDriver":{"Hive":${hive_driver}}}'
        ),
        "localParams": [
            {
                "prop": "hive_driver",
                "direct": "IN",
                "type": "VARCHAR",
                "value": '{"Query":"s3://analytics/query.sql"}',
            }
        ],
    }
    if job_name is not None:
        params["jobName"] = job_name

    normalized = get_task_authoring_catalog(
        _EMR_SERVERLESS_VERSION
    ).normalize_task_params(
        "EMR_SERVERLESS",
        params,
        intent=intent,
    )

    assert normalized == params


@pytest.mark.parametrize(
    ("invalid_params", "field"),
    [
        (
            {
                "applicationId": "   ",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
            },
            "applicationId",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "  ",
                "startJobRunRequestJson": "{}",
            },
            "executionRoleArn",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "   ",
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{invalid json}",
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "[]",
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": '{"JobDriver":NaN}',
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "${application_id}",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
            },
            "applicationId",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "$[role_arn]",
                "startJobRunRequestJson": "{}",
            },
            "executionRoleArn",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "jobName": "${job_name}",
                "startJobRunRequestJson": "{}",
            },
            "jobName",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": '{"JobDriver":${driver}}',
                "localParams": [
                    {
                        "prop": "driver",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "not-json",
                    }
                ],
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": '{"JobDriver":${runtime_driver}}',
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": ('{"Name":$[this_day(yyyy-MM-dd)]}'),
            },
            "startJobRunRequestJson",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": '{"Name":"${name}"}',
                "localParams": [
                    {
                        "prop": "name",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "first",
                    },
                    {
                        "prop": "name",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "second",
                    },
                ],
            },
            "name",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
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
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "localParams": [
                    {
                        "prop": "count",
                        "direct": "IN",
                        "type": "INTEGER",
                        "value": "1",
                    }
                ],
            },
            "VARCHAR",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "resourceList": [],
            },
            "resourceList",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "varPool": [],
            },
            "varPool",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "futureField": {"enabled": True},
            },
            "futureField",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "jobRunId": "runtime-job-run",
            },
            "jobRunId",
        ),
        (
            {
                "applicationId": "app-1",
                "executionRoleArn": "arn:aws:iam::1:role/example",
                "startJobRunRequestJson": "{}",
                "appIds": "runtime-job-run",
            },
            "appIds",
        ),
    ],
    ids=[
        "blank-application-id",
        "blank-execution-role-arn",
        "blank-request-json",
        "invalid-request-json",
        "request-json-array",
        "nonstandard-json-constant",
        "application-id-placeholder",
        "execution-role-placeholder",
        "job-name-placeholder",
        "invalid-bound-json-fragment",
        "unquoted-unbound-placeholder",
        "unquoted-date-placeholder",
        "duplicate-local-param",
        "out-local-param",
        "non-varchar-local-param",
        "resource-list",
        "var-pool",
        "unknown-field",
        "runtime-job-run-id",
        "runtime-app-ids",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_emr_serverless_typed_create_and_edit_fail_closed(
    invalid_params: YamlObject,
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog(_EMR_SERVERLESS_VERSION).normalize_task_params(
            "EMR_SERVERLESS",
            invalid_params,
            intent=intent,
        )


def test_emr_serverless_allows_unresolved_placeholders_inside_json_strings() -> None:
    params: YamlObject = {
        "applicationId": "app-1",
        "executionRoleArn": "arn:aws:iam::1:role/example",
        "startJobRunRequestJson": (
            '{"Driver":"${runtime_driver}","RunDate":"$[this_day(yyyy-MM-dd)]"}'
        ),
    }

    assert (
        get_task_authoring_catalog(_EMR_SERVERLESS_VERSION).normalize_task_params(
            "EMR_SERVERLESS",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == params
    )


def test_emr_serverless_opaque_runtime_and_future_fields_are_deep_preserved() -> None:
    native: YamlObject = {
        "applicationId": "${native_application}",
        "executionRoleArn": "$[native_role]",
        "jobName": "${native_job_name}",
        "startJobRunRequestJson": '{"FutureDriver":{"Mode":"native"}}',
        "localParams": [
            {
                "prop": "runtime",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/native/request.json",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "jobRunId": "native-job-run-id",
        "appIds": "native-app-ids",
        "futureField": {"nested": ["native"]},
    }
    expected = deepcopy(native)
    catalog = get_task_authoring_catalog(_EMR_SERVERLESS_VERSION)

    preserved = catalog.normalize_task_params(
        "EMR_SERVERLESS",
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
    var_pool = native["varPool"]
    assert isinstance(var_pool, list)
    runtime = var_pool[0]
    assert isinstance(runtime, dict)
    runtime["value"] = {"state": "mutated"}
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


def test_emr_serverless_opaque_export_and_metadata_compile_are_lossless() -> None:
    native_params: YamlObject = {
        "applicationId": "native-app",
        "executionRoleArn": "native-role",
        "jobName": "native-job",
        "startJobRunRequestJson": '{"FutureDriver":{"Mode":"native"}}',
        "localParams": [
            {
                "prop": "runtime",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "jobRunId": "native-job-run-id",
        "appIds": "native-app-ids",
        "futureField": {"nested": ["native"]},
    }
    task = FakeTaskDefinition(
        code=101,
        name="submit-native-serverless-job",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="EMR_SERVERLESS",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="emr-serverless-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(_EMR_SERVERLESS_VERSION)

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


@pytest.mark.parametrize("ds_version", _EMR_SERVERLESS_ABSENT_VERSIONS)
def test_emr_serverless_is_unavailable_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("EMR_SERVERLESS") is False
    assert catalog.supports_opaque_authoring("EMR_SERVERLESS") is False
    assert "EMR_SERVERLESS" not in catalog.authoring_task_types
