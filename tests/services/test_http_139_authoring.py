from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_VERSION = "1.3.9"
_TASK_TYPE = "HTTP"
_FINGERPRINT = "sha256:655cf0ac16d2e8428c743e0a67af4e4988b717afe50c4ca27c2f0618d4970430"
_CANONICAL_FIELDS = {
    "url",
    "httpMethod",
    "httpParams",
    "httpCheckCondition",
    "condition",
    "connectTimeout",
    "localParams",
}
_CANONICAL_FIELD_PATHS = {
    "task_params.url",
    "task_params.httpMethod",
    "task_params.httpParams[]",
    "task_params.httpParams[].prop",
    "task_params.httpParams[].httpParametersType",
    "task_params.httpParams[].value",
    "task_params.httpCheckCondition",
    "task_params.condition",
    "task_params.connectTimeout",
    "task_params.localParams[]",
    "task_params.localParams[].prop",
    "task_params.localParams[].direct",
    "task_params.localParams[].type",
    "task_params.localParams[].value",
}
_FORBIDDEN_TYPED_FIELDS = {
    "httpBody",
    "socketTimeout",
    "varPool",
}
_REFS = TaskRefIndex.from_code_by_name({})


def _canonical() -> YamlObject:
    return {
        "url": "https://example.test/jobs/${bizdate}",
        "httpMethod": "GET",
        "httpParams": [
            {
                "prop": "X-Bizdate",
                "httpParametersType": "HEADERS",
                "value": "${bizdate}",
            },
            {
                "prop": "date",
                "httpParametersType": "PARAMETER",
                "value": "${bizdate}",
            },
        ],
        "httpCheckCondition": "BODY_CONTAINS",
        "condition": "ready",
        "connectTimeout": 12_345,
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "2026-08-20",
            }
        ],
    }


def _native(**overrides: YamlValue) -> YamlObject:
    native = {**_canonical(), "socketTimeout": 60_000}
    native.update(overrides)
    return native


def _native_body_parameter() -> YamlObject:
    native = _native()
    native["httpParams"] = [
        {
            "prop": "job",
            "httpParametersType": "BODY",
            "value": "daily",
        }
    ]
    return native


def _spec(params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(_VERSION)
    return validate_workflow_document(
        {
            "workflow": {"name": "http-139-authoring"},
            "tasks": [
                {
                    "name": "call-http-endpoint",
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _encode(params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decode(
    params: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _legacy_graph(native_params: YamlObject) -> DecodedLegacyWorkflowGraph:
    return decode_legacy_workflow_graph(
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "tasks-http",
                        "name": "call-http-endpoint",
                        "type": _TASK_TYPE,
                        "description": "Original HTTP task",
                        "params": native_params,
                        "preTasks": [],
                    }
                ],
                "timeout": 0,
                "tenantId": -1,
            }
        ),
        locations=json.dumps(
            {
                "tasks-http": {
                    "name": "call-http-endpoint",
                    "targetarr": "",
                    "nodenumber": 0,
                    "x": 0,
                    "y": 0,
                }
            }
        ),
        connects="[]",
    )


def _exported_params(graph: DecodedLegacyWorkflowGraph) -> YamlObject:
    document = graph.workflow_document(name="http-139-roundtrip")
    tasks = document["tasks"]
    assert isinstance(tasks, list)
    task = tasks[0]
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _recompiled_params(
    graph: DecodedLegacyWorkflowGraph,
    *,
    spec: WorkflowSpec | None = None,
) -> YamlObject:
    selected_spec = (
        graph.to_workflow_spec(name="http-139-roundtrip") if spec is None else spec
    )
    payload = prepare_legacy_workflow_graph(
        selected_spec,
        baseline=graph,
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])
    params = process_definition["tasks"][0]["params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _task_params_json_schema() -> YamlObject:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    return cast("YamlObject", task_params)


def test_http_139_exact_source_fact_has_reviewed_typed_membership() -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert _TASK_TYPE in catalog.upstream_task_types
    assert _TASK_TYPE in catalog.reviewed_typed_task_types
    assert _TASK_TYPE in catalog.legacy_typed_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is True
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review

    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.common.task.http.HttpParameters"
    )
    assert fact.registration_kind == "legacy_switch"
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.source_task_type == _TASK_TYPE
    assert review.cli_task_type == _TASK_TYPE
    assert review.cli_model == "HttpTaskParamsSpec"
    assert review.review == "legacy-1.3.9-http-projected-canonical-subset"
    assert review.semantic_fingerprint == fact.semantic_fingerprint


def test_http_139_catalog_normalizes_the_existing_canonical_model() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    canonical = _canonical()

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == canonical
    assert set(normalized) == _CANONICAL_FIELDS


def test_http_139_schema_exposes_only_exact_authored_fields_and_choices() -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_paths = {path for path in fields if path.startswith("task_params.")}

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert task_param_paths == _CANONICAL_FIELD_PATHS
    assert fields["task_params.httpMethod"]["choices"] == [
        "GET",
        "POST",
        "PUT",
        "DELETE",
    ]
    assert fields["task_params.httpParams[].httpParametersType"]["choices"] == [
        "PARAMETER",
        "HEADERS",
    ]
    assert fields["task_params.httpCheckCondition"]["choices"] == [
        "STATUS_CODE_DEFAULT",
        "STATUS_CODE_CUSTOM",
        "BODY_CONTAINS",
        "BODY_NOT_CONTAINS",
    ]
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]


def test_http_139_json_schema_is_closed_without_native_only_fields() -> None:
    task_params = _task_params_json_schema()
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _CANONICAL_FIELDS
    assert set(required) == {"url", "httpMethod", "connectTimeout"}
    assert _FORBIDDEN_TYPED_FIELDS.isdisjoint(properties)


def test_http_139_summary_and_mappings_publish_only_the_canonical_surface() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    mapping_result = task_type_schema_result(
        _TASK_TYPE,
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(mapping_result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in mapping_result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.url",
        "task_params.httpMethod",
        "task_params.connectTimeout",
    }
    summary_text = json.dumps(summary)
    for forbidden in _FORBIDDEN_TYPED_FIELDS:
        assert forbidden not in summary_text
    assert set(mappings) == _CANONICAL_FIELD_PATHS
    for path, native_path in mappings.items():
        suffix = path.removeprefix("task_params.")
        if suffix.endswith("[]"):
            suffix = suffix.removesuffix("[]")
        assert native_path == f"processDefinitionJson.tasks[].params.{suffix}"


@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_http_139_templates_exclude_native_only_fields_and_validate(
    variant: str,
) -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_template_result(
        _TASK_TYPE,
        variant=None if variant in {"minimal", "params"} else variant,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    metadata = result.data["template"]
    assert isinstance(metadata, dict)
    document = yaml.safe_load(
        parameter_example_yaml(_TASK_TYPE, _VERSION)
        if variant == "params"
        else cast("str", result.data["yaml"])
    )
    assert isinstance(document, dict)
    params = document["task_params"]
    assert isinstance(params, dict)

    assert metadata["kind"] == "typed"
    assert metadata["variants"] == []
    assert _FORBIDDEN_TYPED_FIELDS.isdisjoint(params)
    if variant == "params":
        local_params = params["localParams"]
        assert isinstance(local_params, list)
        assert {item["direct"] for item in local_params} == {"IN"}
    validate_workflow_document(
        {
            "workflow": {"name": f"http-139-{variant}"},
            "tasks": [document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def test_http_139_minimal_template_compiles_to_legacy_wire_with_derived_timeout() -> (
    None
):
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_template_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(result.data, dict)
    task_document = yaml.safe_load(cast("str", result.data["yaml"]))
    assert isinstance(task_document, dict)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "http-139-minimal"},
            "tasks": [task_document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _task_name: "tasks-http",
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])
    native_task = process_definition["tasks"][0]
    native_params = native_task["params"]
    authored_params = task_document["task_params"]

    assert native_task["type"] == _TASK_TYPE
    assert isinstance(authored_params, dict)
    assert native_params == {**authored_params, "socketTimeout": 60_000}
    assert _FORBIDDEN_TYPED_FIELDS.isdisjoint(authored_params)
    assert "httpBody" not in native_params
    assert "varPool" not in native_params


def test_http_139_projector_preserves_spelling_and_owns_socket_timeout() -> None:
    canonical = _canonical()

    native = _encode(canonical)
    decoded, provenance = _decode(native)

    assert native == {**canonical, "socketTimeout": 60_000}
    assert set(native) == _CANONICAL_FIELDS | {"socketTimeout"}
    assert decoded == canonical
    assert provenance is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    "invalid_params",
    [
        {**_canonical(), "httpBody": '{"job":"daily"}'},
        {**_canonical(), "socketTimeout": 12_345},
        {**_canonical(), "futureField": {"native": True}},
    ],
    ids=["http-body", "explicit-socket-timeout", "unknown-field"],
)
def test_http_139_explicit_typed_invalid_fields_do_not_downgrade_to_opaque(
    invalid_params: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=invalid_params,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises(ValueError, match=r"httpBody|socketTimeout|futureField"):
        _spec(invalid_params)


def test_http_139_explicit_var_pool_fails_the_exact_parameter_gate() -> None:
    params = {**_canonical(), "varPool": []}
    catalog = get_task_authoring_catalog(_VERSION)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=params,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises(UnsupportedFeatureError) as captured:
        _spec(params)

    assert captured.value.details["field"] == "tasks[].task_params.varPool"


def test_http_139_local_params_accept_only_in_direction() -> None:
    params = _canonical()
    local_params = params["localParams"]
    assert isinstance(local_params, list)
    first_param = local_params[0]
    assert isinstance(first_param, dict)
    first_param["direct"] = "OUT"

    with pytest.raises(UnsupportedFeatureError) as captured:
        _spec(params)

    assert captured.value.details["field"] == (
        "tasks[].task_params.localParams[].direct"
    )
    assert captured.value.details["value"] == "OUT"


@pytest.mark.parametrize(
    "native",
    [
        _native(socketTimeout=12_345),
        _native(httpBody='{"job":"daily"}'),
        _native(varPool=[]),
        _native(futureField={"nested": ["preserve", {"value": True}]}),
    ],
    ids=["nondefault-socket-timeout", "http-body", "var-pool", "unknown-field"],
)
def test_http_139_richer_native_shapes_decode_as_opaque_preserve(
    native: YamlObject,
) -> None:
    decoded, provenance = _decode(native)

    assert decoded == native
    assert provenance is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize(
    "native",
    [
        _native(httpMethod="HEAD"),
        _native_body_parameter(),
    ],
    ids=["native-head-method", "native-body-parameter"],
)
def test_http_139_native_head_and_body_modes_remain_opaque_identity(
    native: YamlObject,
) -> None:
    decoded, provenance = _decode(native)
    reencoded = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(decoded)),
        refs=_REFS,
        source=provenance,
    )

    assert decoded == native
    assert provenance is ProjectionSource.OPAQUE_PRESERVE
    assert reencoded.task_params == native


@pytest.mark.parametrize(
    "invalid_params",
    [
        {**_canonical(), "httpMethod": "HEAD"},
        {
            **_canonical(),
            "httpParams": [
                {
                    "prop": "job",
                    "httpParametersType": "BODY",
                    "value": "daily",
                }
            ],
        },
    ],
    ids=["typed-head-method", "typed-body-parameter"],
)
def test_http_139_typed_head_and_body_modes_fail_closed_without_opaque_fallback(
    invalid_params: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=invalid_params,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises(ValueError, match=r"httpMethod|httpParametersType"):
        _spec(invalid_params)


def test_http_139_typed_decode_rejects_nondefault_native_socket_timeout() -> None:
    with pytest.raises(TaskParameterProjectionError) as captured:
        _decode(
            _native(socketTimeout=12_345),
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.socketTimeout"
    assert captured.value.details["reason"] == "unrepresentable-native-value"


def test_http_139_safe_native_legacy_export_is_canonical_and_recompiles_exactly() -> (
    None
):
    native = _native()
    graph = _legacy_graph(native)

    assert _exported_params(graph) == _canonical()
    assert _recompiled_params(graph) == native


def test_http_139_richer_native_legacy_metadata_edit_is_lossless() -> None:
    native = _native(
        socketTimeout=12_345,
        httpBody='{"job":"native"}',
        varPool=[],
        futureField={"nested": ["preserve", {"value": True}]},
    )
    graph = _legacy_graph(native)
    baseline_spec = graph.to_workflow_spec(name="http-139-roundtrip")
    current = baseline_spec.tasks[0]
    edited_spec = baseline_spec.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"description": "Metadata-only edit"},
                    deep=True,
                )
            ]
        },
        deep=True,
    )

    assert _exported_params(graph) == native
    assert _recompiled_params(graph, spec=edited_spec) == native


def test_http_139_guidance_discloses_placeholder_logging_and_response_body() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    schema = task_type_schema_result(_TASK_TYPE, catalog=catalog)
    template = task_template_result(
        _TASK_TYPE,
        catalog=catalog,
    )
    assert isinstance(schema.data, dict)
    assert isinstance(template.data, dict)
    field_guidance = " ".join(
        str(field.get("description", ""))
        for field in schema.data["fields"]
        if isinstance(field, dict)
        and isinstance(field.get("path"), str)
        and field["path"].startswith("task_params.")
    ).lower()
    template_guidance = cast("str", template.data["yaml"]).lower()

    for guidance in (field_guidance, template_guidance):
        for phrase in (
            "placeholder",
            "task params",
            "request params",
            "response body",
            "info",
            "not secret storage",
            "does not redact",
            "configured url",
            "no reliable cancel",
            "failover",
            "retry",
            "reissues the whole http request",
            "side effects",
            "duplicate",
        ):
            assert phrase in guidance
        assert "final url" not in guidance
        assert "expanded url" not in guidance
