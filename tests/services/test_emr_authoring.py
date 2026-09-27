from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models import WorkflowSpec
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    EMR_OPERATION_FACET,
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


_EMR_VERSIONS = (
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
    "3.4.2",
)
_EMR_ABSENT_VERSIONS = ("1.3.9", "2.0.0", "2.0.9")
_EMR_RUN_ONLY_VERSIONS = ("3.0.0", "3.0.6")
_EMR_LITERAL_JSON_VERSIONS = (
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
)
_EMR_PARAMETERIZED_JSON_VERSIONS = (
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)

_RUN_JOB_FLOW_JSON = (
    '{"Name":"daily-orders","ReleaseLabel":"emr-6.15.0",'
    '"Instances":{"InstanceCount":1}}'
)
_ADD_ONE_STEP_JSON = '{"JobFlowId":"j-daily-orders","Steps":[{"Name":"daily-orders"}]}'


def _emr_spec(task_params: YamlObject, *, suffix: str = "run") -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": f"emr-{suffix}"},
            "tasks": [
                {
                    "name": f"emr-{suffix}",
                    "type": "EMR",
                    "task_params": task_params,
                }
            ],
        }
    )


def _template_task(ds_version: str, variant: str = "minimal") -> YamlObject:
    if variant == "params":
        yaml_text = parameter_example_yaml("EMR", ds_version)
    else:
        result = task_template_result(
            "EMR",
            variant=None if variant == "minimal" else variant,
            catalog=get_task_authoring_catalog(ds_version),
        )
        assert isinstance(result.data, dict)
        yaml_text = result.data["yaml"]
        assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _template_yaml(ds_version: str, variant: str = "minimal") -> str:
    result = task_template_result(
        "EMR",
        variant=None if variant == "minimal" else variant,
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    yaml_text = result.data["yaml"]
    assert isinstance(yaml_text, str)
    return yaml_text


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    prepared = prepare_workflow_create_compilation(
        _emr_spec(task_params, suffix=ds_version),
        catalog=get_task_authoring_catalog(ds_version),
    )
    payload = prepared.materialize([30_001])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "EMR"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _mode_rule(
    rules: list[object],
    *,
    program_type: str,
    active_path: str,
    inactive_path: str,
) -> bool:
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        when = raw_rule.get("when")
        condition_paths = raw_rule.get("condition_paths")
        active_paths = raw_rule.get("active_paths")
        inactive_paths = raw_rule.get("inactive_paths")
        if (
            isinstance(when, str)
            and program_type in when
            and isinstance(condition_paths, list)
            and "task_params.programType" in condition_paths
            and isinstance(active_paths, list)
            and active_path in active_paths
            and isinstance(inactive_paths, list)
            and inactive_path in inactive_paths
        ):
            return True
    return False


@pytest.mark.parametrize("ds_version", _EMR_VERSIONS)
def test_emr_schema_is_reviewed_and_exact_version_adaptive(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    result = task_type_schema_result("EMR", catalog=catalog)
    membership = catalog.require_facet("EMR", EMR_OPERATION_FACET)
    source_review = catalog.task_type_facts["EMR"].typed_authoring_review

    assert catalog.supports_typed_authoring("EMR") is True
    assert source_review is not None
    assert membership.contract.review == source_review.review
    assert isinstance(result.data, dict)
    assert result.data["kind"] == "typed"
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    program_type = fields["task_params.programType"]
    expected_program_types = (
        ["RUN_JOB_FLOW"]
        if ds_version in _EMR_RUN_ONLY_VERSIONS
        else ["RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"]
    )

    assert program_type["required"] is True
    assert program_type["choices"] == expected_program_types
    if ds_version in _EMR_RUN_ONLY_VERSIONS:
        assert "compile_path" not in program_type
    else:
        assert program_type["compile_path"].endswith("taskParams.programType")
    assert "task_params.jobFlowDefineJson" in fields
    assert fields["task_params.jobFlowDefineJson"]["compile_path"].endswith(
        "taskParams.jobFlowDefineJson"
    )
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]
    assert fields["task_params.localParams[].type"]["choices"] == ["VARCHAR"]
    assert not any(path.startswith("task_params.varPool") for path in fields)

    if ds_version in _EMR_RUN_ONLY_VERSIONS:
        assert fields["task_params.jobFlowDefineJson"]["required"] is True
        assert "task_params.stepsDefineJson" not in fields
    else:
        assert "task_params.stepsDefineJson" in fields
        assert fields["task_params.stepsDefineJson"]["compile_path"].endswith(
            "taskParams.stepsDefineJson"
        )
        rules = result.data["state_rules"]
        assert isinstance(rules, list)
        assert _mode_rule(
            rules,
            program_type="RUN_JOB_FLOW",
            active_path="task_params.jobFlowDefineJson",
            inactive_path="task_params.stepsDefineJson",
        )
        assert _mode_rule(
            rules,
            program_type="ADD_JOB_FLOW_STEPS",
            active_path="task_params.stepsDefineJson",
            inactive_path="task_params.jobFlowDefineJson",
        )


def test_emr_schema_describes_exact_placeholder_epoch() -> None:
    literal_result = task_type_schema_result(
        "EMR",
        catalog=get_task_authoring_catalog("3.2.1"),
    )
    parameterized_result = task_type_schema_result(
        "EMR",
        catalog=get_task_authoring_catalog("3.2.2"),
    )
    assert isinstance(literal_result.data, dict)
    assert isinstance(parameterized_result.data, dict)

    literal_fields = {
        field["path"]: field
        for field in literal_result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    parameterized_fields = {
        field["path"]: field
        for field in parameterized_result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    literal_description = str(
        literal_fields["task_params.jobFlowDefineJson"]["description"]
    ).lower()
    parameterized_description = str(
        parameterized_fields["task_params.jobFlowDefineJson"]["description"]
    )
    literal_prop = literal_fields["task_params.localParams[].prop"]
    parameterized_prop = parameterized_fields["task_params.localParams[].prop"]

    assert "placeholder" in literal_description
    assert "not supported" in literal_description
    assert "${...}" in parameterized_description
    assert "$[...]" in parameterized_description
    assert "does not substitute" in str(literal_prop["description"])
    assert "referenced as ${name}" in str(parameterized_prop["description"])
    assert (
        "dsctl template params --topic output"
        not in literal_fields["task_params.localParams[]"]["related_commands"]
    )
    assert (
        "dsctl template params --topic output"
        not in parameterized_fields["task_params.localParams[]"]["related_commands"]
    )


@pytest.mark.parametrize(
    ("ds_version", "configuration_marker"),
    [
        ("3.0.0", "aws.access.key.id"),
        ("3.0.6", "resource.aws.access.key.id"),
        ("3.2.2", "resource.aws.access.key.id"),
        ("3.3.1", "aws.emr.credentials.provider.type"),
        ("3.4.2", "aws.emr.credentials.provider.type"),
    ],
)
def test_emr_discovery_exposes_exact_worker_prerequisites(
    ds_version: str,
    configuration_marker: str,
) -> None:
    result = task_type_schema_result(
        "EMR",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    description = str(fields["task_params.programType"]["description"])
    template = _template_yaml(ds_version)

    assert configuration_marker in description
    assert configuration_marker in template
    assert "credentials" in description.lower()
    assert "failover" in description.lower()
    assert "not implemented" in description.lower()
    assert "failover" in template.lower()


@pytest.mark.parametrize(
    ("ds_version", "program_types", "payload_fields"),
    [
        ("3.0.0", ["RUN_JOB_FLOW"], {"jobFlowDefineJson"}),
        (
            "3.1.0",
            ["RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"],
            {"jobFlowDefineJson", "stepsDefineJson"},
        ),
    ],
)
def test_emr_json_schema_encodes_exact_modes_and_parameter_direction(
    ds_version: str,
    program_types: list[str],
    payload_fields: set[str],
) -> None:
    result = task_type_schema_result(
        "EMR",
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
    nested_definitions = task_params["$defs"]
    assert isinstance(nested_definitions, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)

    assert nested_definitions["EmrProgramType"]["enum"] == program_types
    assert nested_definitions["Direct"]["enum"] == ["IN"]
    assert payload_fields == {
        field for field in payload_fields if properties[field]["type"] == "string"
    }
    variants = task_params["oneOf"]
    assert isinstance(variants, list)
    assert len(variants) == len(program_types)
    assert {
        variant["properties"]["programType"]["const"] for variant in variants
    } == set(program_types)
    for variant in variants:
        required = variant["required"]
        assert "programType" in required
        assert len(payload_fields.intersection(required)) == 1


def test_emr_300_compile_mappings_omit_the_implied_native_discriminator() -> None:
    legacy = task_type_schema_result(
        "EMR",
        compile_mappings=True,
        catalog=get_task_authoring_catalog("3.0.0"),
    )
    modern = task_type_schema_result(
        "EMR",
        compile_mappings=True,
        catalog=get_task_authoring_catalog("3.1.0"),
    )
    assert isinstance(legacy.data, dict)
    assert isinstance(modern.data, dict)
    legacy_paths = {
        mapping["authoring_path"] for mapping in legacy.data["compile_mappings"]
    }
    modern_paths = {
        mapping["authoring_path"] for mapping in modern.data["compile_mappings"]
    }

    assert "task_params.programType" not in legacy_paths
    assert "task_params.programType" in modern_paths


def test_emr_summary_does_not_claim_both_mode_payloads_are_required() -> None:
    legacy_summary = task_type_summary_data(
        "EMR",
        catalog=get_task_authoring_catalog("3.0.0"),
    )
    dual_mode_summary = task_type_summary_data(
        "EMR",
        catalog=get_task_authoring_catalog("3.1.0"),
    )

    assert "task_params.programType" in legacy_summary["required_paths"]
    assert "task_params.jobFlowDefineJson" in legacy_summary["required_paths"]
    assert "task_params.programType" in dual_mode_summary["required_paths"]
    assert "task_params.jobFlowDefineJson" not in dual_mode_summary["required_paths"]
    assert "task_params.stepsDefineJson" not in dual_mode_summary["required_paths"]


@pytest.mark.parametrize("ds_version", _EMR_VERSIONS)
def test_emr_minimal_template_compiles_to_exact_native_params(
    ds_version: str,
) -> None:
    task = _template_task(ds_version)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "EMR"
    assert params["programType"] == "RUN_JOB_FLOW"
    assert params["localParams"] == []
    assert set(params) == {"programType", "jobFlowDefineJson", "localParams"}
    parsed_definition = json.loads(cast("str", params["jobFlowDefineJson"]))
    assert isinstance(parsed_definition, dict)

    expected = deepcopy(params)
    if ds_version in _EMR_RUN_ONLY_VERSIONS:
        expected.pop("programType")
    assert _compiled_task_params(ds_version, params) == expected


@pytest.mark.parametrize("ds_version", _EMR_LITERAL_JSON_VERSIONS)
def test_emr_params_template_is_absent_before_parameter_substitution(
    ds_version: str,
) -> None:
    with pytest.raises(UserInputError) as captured:
        task_template_result(
            "EMR",
            variant="params",
            catalog=get_task_authoring_catalog(ds_version),
        )

    available_variants = captured.value.details["available_variants"]
    assert isinstance(available_variants, list)
    assert "params" not in available_variants


@pytest.mark.parametrize("ds_version", _EMR_PARAMETERIZED_JSON_VERSIONS)
def test_emr_params_template_starts_at_322_and_compiles_without_rewriting_json(
    ds_version: str,
) -> None:
    task = _template_task(ds_version, "params")
    params = task["task_params"]
    assert isinstance(params, dict)
    raw_json = params["jobFlowDefineJson"]
    local_params = params["localParams"]

    assert isinstance(raw_json, str)
    assert "${" in raw_json
    assert isinstance(local_params, list)
    assert local_params
    assert all(
        isinstance(item, dict)
        and item.get("direct") == "IN"
        and item.get("type") == "VARCHAR"
        for item in local_params
    )
    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize(
    ("ds_version", "canonical", "expected_native"),
    [
        (
            "3.0.0",
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
                "localParams": [],
            },
            {"jobFlowDefineJson": _RUN_JOB_FLOW_JSON, "localParams": []},
        ),
        (
            "3.1.0",
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": _ADD_ONE_STEP_JSON,
                "localParams": [],
            },
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": _ADD_ONE_STEP_JSON,
                "localParams": [],
            },
        ),
        (
            "3.2.2",
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": (
                    '{"Name":"daily-orders","Instances":'
                    '{"InstanceCount":${instance_count}}}'
                ),
                "localParams": [
                    {
                        "prop": "instance_count",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "2",
                    }
                ],
            },
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": (
                    '{"Name":"daily-orders","Instances":'
                    '{"InstanceCount":${instance_count}}}'
                ),
                "localParams": [
                    {
                        "prop": "instance_count",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "2",
                    }
                ],
            },
        ),
        (
            "3.4.2",
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": (
                    '{"JobFlowId":"${job_flow_id}","Steps":[{"Name":"daily-orders"}]}'
                ),
                "localParams": [
                    {
                        "prop": "job_flow_id",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "j-daily-orders",
                    }
                ],
            },
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": (
                    '{"JobFlowId":"${job_flow_id}","Steps":[{"Name":"daily-orders"}]}'
                ),
                "localParams": [
                    {
                        "prop": "job_flow_id",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "j-daily-orders",
                    }
                ],
            },
        ),
    ],
)
def test_emr_prepared_compilation_projects_each_execution_epoch_exactly(
    ds_version: str,
    canonical: YamlObject,
    expected_native: YamlObject,
) -> None:
    assert _compiled_task_params(ds_version, canonical) == expected_native


@pytest.mark.parametrize(
    ("invalid_params", "field"),
    [
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": "   ",
                "localParams": [],
            },
            "jobFlowDefineJson",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": "[]",
                "localParams": [],
            },
            "object",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": "{broken-json}",
                "localParams": [],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Instances":{"InstanceCount":NaN}}',
                "localParams": [],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Instances":{"InstanceCount":Infinity}}',
                "localParams": [],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Name":"${cluster_name}", garbage}',
                "localParams": [
                    {
                        "prop": "cluster_name",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "nightly",
                    }
                ],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": "not-json ${cluster_name}",
                "localParams": [
                    {
                        "prop": "cluster_name",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "nightly",
                    }
                ],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Name":${cluster_name}}',
                "localParams": [
                    {
                        "prop": "cluster_name",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "nightly",
                    }
                ],
            },
            "JSON",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
                "stepsDefineJson": _ADD_ONE_STEP_JSON,
                "localParams": [],
            },
            "stepsDefineJson",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "stepsDefineJson": _ADD_ONE_STEP_JSON,
                "localParams": [],
            },
            "jobFlowDefineJson",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
                "localParams": [],
                "futureField": True,
            },
            "futureField",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
                "localParams": [
                    {
                        "prop": "instance_count",
                        "direct": "IN",
                        "type": "INTEGER",
                        "value": "2",
                    }
                ],
            },
            "VARCHAR",
        ),
        (
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
                "localParams": [
                    {
                        "prop": "cluster_id",
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
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": (
                    '{"JobFlowId":"j-daily-orders","Steps":'
                    '[{"Name":"one"},{"Name":"two"}]}'
                ),
                "localParams": [],
            },
            "Steps",
        ),
        (
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": (
                    '{"JobFlowId":"j-daily-orders","Steps":[${steps}]}'
                ),
                "localParams": [
                    {
                        "prop": "steps",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": '{"Name":"one"},{"Name":"two"}',
                    }
                ],
            },
            "Steps",
        ),
        (
            {
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": (
                    '{"JobFlowId":"j-daily-orders","Steps":'
                    '[{"Name":${first_step}}, {"Name":"second"}]}'
                ),
                "localParams": [
                    {
                        "prop": "first_step",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": '"first"',
                    }
                ],
            },
            "Steps",
        ),
    ],
    ids=[
        "blank-json",
        "json-array",
        "invalid-json",
        "nan-is-not-json",
        "infinity-is-not-json",
        "placeholder-does-not-hide-bad-member",
        "placeholder-does-not-hide-non-json",
        "bound-string-must-remain-valid-json",
        "both-mode-fields",
        "inactive-mode-field-only",
        "unknown-field",
        "non-varchar-local-param",
        "out-local-param",
        "multiple-steps",
        "multiple-steps-injected-by-bound-value",
        "multiple-steps-with-placeholder",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_emr_typed_model_fails_closed(
    invalid_params: YamlObject,
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "EMR",
            invalid_params,
            intent=intent,
        )


def test_emr_300_rejects_add_steps_during_prepared_compilation() -> None:
    with pytest.raises(UserInputError, match="ADD_JOB_FLOW_STEPS"):
        prepare_workflow_create_compilation(
            _emr_spec(
                {
                    "programType": "ADD_JOB_FLOW_STEPS",
                    "stepsDefineJson": _ADD_ONE_STEP_JSON,
                    "localParams": [],
                },
                suffix="unsupported-add",
            ),
            catalog=get_task_authoring_catalog("3.0.0"),
        )


def test_emr_321_rejects_json_placeholders_before_execution_support() -> None:
    with pytest.raises(UnsupportedFeatureError, match="placeholder"):
        prepare_workflow_create_compilation(
            _emr_spec(
                {
                    "programType": "RUN_JOB_FLOW",
                    "jobFlowDefineJson": (
                        '{"Name":"daily-orders","Instances":'
                        '{"InstanceCount":${instance_count}}}'
                    ),
                    "localParams": [
                        {
                            "prop": "instance_count",
                            "direct": "IN",
                            "type": "VARCHAR",
                            "value": "2",
                        }
                    ],
                },
                suffix="unsupported-placeholder",
            ),
            catalog=get_task_authoring_catalog("3.2.1"),
        )


def test_emr_321_reports_unsupported_before_inspecting_bound_json_value() -> None:
    with pytest.raises(UnsupportedFeatureError, match="placeholder"):
        prepare_workflow_create_compilation(
            _emr_spec(
                {
                    "programType": "RUN_JOB_FLOW",
                    "jobFlowDefineJson": '{"Name":"${name}"}',
                    "localParams": [
                        {
                            "prop": "name",
                            "direct": "IN",
                            "type": "VARCHAR",
                            "value": 'bad"',
                        }
                    ],
                },
                suffix="unsupported-placeholder-value",
            ),
            catalog=get_task_authoring_catalog("3.2.1"),
        )


def test_emr_321_requires_literal_json_when_placeholder_is_not_substituted() -> None:
    catalog = get_task_authoring_catalog("3.2.1")

    with pytest.raises(ValueError, match="JSON object"):
        catalog.normalize_task_params(
            "EMR",
            {
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Instances":{"InstanceCount":${literal}}}',
                "localParams": [],
            },
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    literal = catalog.normalize_task_params(
        "EMR",
        {
            "programType": "RUN_JOB_FLOW",
            "jobFlowDefineJson": '{"Name":"${literal}"}',
            "localParams": [],
        },
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert literal["jobFlowDefineJson"] == '{"Name":"${literal}"}'

    date_literal = catalog.normalize_task_params(
        "EMR",
        {
            "programType": "RUN_JOB_FLOW",
            "jobFlowDefineJson": '{"Name":"$[yyyyMMdd]"}',
            "localParams": [
                {
                    "prop": "yyyyMMdd",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "ignored-before-3.2.2",
                }
            ],
        },
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert date_literal["jobFlowDefineJson"] == '{"Name":"$[yyyyMMdd]"}'

    spaced_literal = catalog.normalize_task_params(
        "EMR",
        {
            "programType": "RUN_JOB_FLOW",
            "jobFlowDefineJson": '{"Name":"${ name }"}',
            "localParams": [
                {
                    "prop": "name",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "not-bound-by-upstream",
                }
            ],
        },
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert spaced_literal["jobFlowDefineJson"] == '{"Name":"${ name }"}'


def test_emr_bound_parameter_can_supply_the_exact_single_steps_array() -> None:
    params: YamlObject = {
        "programType": "ADD_JOB_FLOW_STEPS",
        "stepsDefineJson": '{"JobFlowId":"j-daily-orders","Steps":${steps}}',
        "localParams": [
            {
                "prop": "steps",
                "direct": "IN",
                "type": "VARCHAR",
                "value": '[{"Name":"one"}]',
            }
        ],
    }

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "EMR",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    assert _compiled_task_params("3.4.2", params) == params


@pytest.mark.parametrize(
    ("raw_json", "value"),
    [
        ("${request}", '{"Name":"whole-request"}'),
        ("{${members}}", '"Name":"member-fragment"'),
    ],
)
def test_emr_bound_parameter_can_supply_raw_json_fragments(
    raw_json: str,
    value: str,
) -> None:
    params: YamlObject = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": raw_json,
        "localParams": [
            {
                "prop": "request" if raw_json == "${request}" else "members",
                "direct": "IN",
                "type": "VARCHAR",
                "value": value,
            }
        ],
    }

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "EMR",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    assert _compiled_task_params("3.4.2", params) == params


def test_emr_opaque_preserve_keeps_both_mode_fields_and_unknown_native_data() -> None:
    native: YamlObject = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": _RUN_JOB_FLOW_JSON,
        "stepsDefineJson": _ADD_ONE_STEP_JSON,
        "localParams": [
            {
                "prop": "native-output",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": "ready"}],
        "futureField": {"nested": ["native"]},
    }
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "EMR",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


@pytest.mark.parametrize("ds_version", _EMR_ABSENT_VERSIONS)
def test_emr_remains_unavailable_when_exact_source_has_no_plugin(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("EMR") is False
    assert catalog.supports_opaque_authoring("EMR") is False
    assert "EMR" not in catalog.authoring_task_types
