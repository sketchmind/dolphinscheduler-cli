from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import TYPE_CHECKING, Protocol, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
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
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
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
    from dsctl.services._workflow.compile import PreparedWorkflowCompilation
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.workflow_graph import WorkflowCreatePayload


_TASK_TYPE = "GRPC"
_FACET = "GRPC/literal_unary_string_record_call"
_TYPED_VERSIONS = ("3.4.0", "3.4.1", "3.4.2")
_ABSENT_VERSIONS = (
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
)
_FINGERPRINT = "sha256:4f4c4ce2e6af30256fb724c0b5037ea14ccb789c4ab50b5a9d2ec69aca26e180"
_CANONICAL_FIELDS = {
    "url",
    "channelCredentialType",
    "serviceName",
    "methodName",
    "requestFields",
    "responseFields",
    "message",
    "grpcConnectTimeoutMs",
}
_WIRE_FIELDS = {
    "url",
    "channelCredentialType",
    "grpcServiceDefinition",
    "grpcServiceDefinitionJSON",
    "methodName",
    "message",
    "grpcCheckCondition",
    "condition",
    "grpcConnectTimeoutMs",
}
_REQUEST_SECRET_FIELD_NAMES = (
    "apiKey",
    "authToken",
    "clientSecret",
    "credential",
    "password",
    "passwd",
    "privateKey",
    "secret",
    "token",
)
_EXCLUDED_FIELDS = {
    "localParams",
    "varPool",
    "resourceList",
    "socketTimeout",
    "grpcCredentialType",
    "grpcCheckCondition",
    "condition",
    "grpcServiceDefinition",
    "grpcServiceDefinitionJSON",
    "streaming",
    "futureField",
}
_REFS = TaskRefIndex.from_code_by_name({})


class _GrpcRuntimeSurface(Protocol):
    available: bool
    wire_epoch: str | None
    parameter_substitution: bool
    authored_values_logged: bool
    result_output_supported: bool
    cancel_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_may_duplicate: bool
    channel_shutdown: bool
    event_loop_group_shutdown: bool
    tls_uses_default_trust: bool
    tls_hostname_verification: bool
    custom_ca_supported: bool
    mutual_tls_supported: bool
    request_authentication_supported: bool


class _Surface(Protocol):
    grpc: _GrpcRuntimeSurface


def _canonical(
    *,
    url: str = "grpc.example.internal:7443",
    credential_type: str = "TLS_DEFAULT",
    service_name: str = "EchoService",
    method_name: str = "Echo",
    request_fields: list[YamlObject] | None = None,
    response_fields: list[YamlObject] | None = None,
    message: dict[str, str] | None = None,
    timeout_ms: int = 10_000,
) -> YamlObject:
    return {
        "url": url,
        "channelCredentialType": credential_type,
        "serviceName": service_name,
        "methodName": method_name,
        "requestFields": cast(
            "YamlValue",
            (
                [
                    {"name": "account", "number": 1},
                    {"name": "region", "number": 2},
                ]
                if request_fields is None
                else request_fields
            ),
        ),
        "responseFields": cast(
            "YamlValue",
            (
                [{"name": "result", "number": 1}]
                if response_fields is None
                else response_fields
            ),
        ),
        "message": cast(
            "YamlValue",
            ({"region": "cn", "account": "alice"} if message is None else message),
        ),
        "grpcConnectTimeoutMs": timeout_ms,
    }


def _descriptor(
    *,
    service_name: str = "EchoService",
    method_name: str = "Echo",
    request_fields: list[YamlObject] | None = None,
    response_fields: list[YamlObject] | None = None,
) -> YamlObject:
    request = (
        [{"name": "account", "number": 1}, {"name": "region", "number": 2}]
        if request_fields is None
        else request_fields
    )
    response = (
        [{"name": "result", "number": 1}]
        if response_fields is None
        else response_fields
    )
    return {
        "nested": {
            service_name: {
                "methods": {
                    method_name: {
                        "requestType": "Request",
                        "responseType": "Response",
                    }
                }
            },
            "Request": {
                "fields": {
                    cast("str", field["name"]): {
                        "type": "string",
                        "id": cast("YamlValue", field["number"]),
                    }
                    for field in request
                }
            },
            "Response": {
                "fields": {
                    cast("str", field["name"]): {
                        "type": "string",
                        "id": cast("YamlValue", field["number"]),
                    }
                    for field in response
                }
            },
        }
    }


def _proto(
    *,
    service_name: str = "EchoService",
    method_name: str = "Echo",
    request_fields: list[YamlObject] | None = None,
    response_fields: list[YamlObject] | None = None,
) -> str:
    request = (
        [{"name": "account", "number": 1}, {"name": "region", "number": 2}]
        if request_fields is None
        else request_fields
    )
    response = (
        [{"name": "result", "number": 1}]
        if response_fields is None
        else response_fields
    )
    request_lines = "\n".join(
        f"  string {field['name']} = {field['number']};" for field in request
    )
    response_lines = "\n".join(
        f"  string {field['name']} = {field['number']};" for field in response
    )
    return (
        'syntax = "proto3";\n\n'
        f"service {service_name} {{\n"
        f"  rpc {method_name} (Request) returns (Response);\n"
        "}\n\n"
        f"message Request {{\n{request_lines}\n}}\n\n"
        f"message Response {{\n{response_lines}\n}}\n"
    )


def _native(params: YamlObject | None = None) -> YamlObject:
    canonical = _canonical() if params is None else params
    service_name = cast("str", canonical["serviceName"])
    method_name = cast("str", canonical["methodName"])
    request_fields = cast("list[YamlObject]", canonical["requestFields"])
    response_fields = cast("list[YamlObject]", canonical["responseFields"])
    message = cast("dict[str, str]", canonical["message"])
    descriptor = _descriptor(
        service_name=service_name,
        method_name=method_name,
        request_fields=request_fields,
        response_fields=response_fields,
    )
    return {
        "url": canonical["url"],
        "channelCredentialType": canonical["channelCredentialType"],
        "grpcServiceDefinition": _proto(
            service_name=service_name,
            method_name=method_name,
            request_fields=request_fields,
            response_fields=response_fields,
        ),
        "grpcServiceDefinitionJSON": json.dumps(
            descriptor,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "methodName": f"{service_name}/{method_name}",
        "message": json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        "grpcCheckCondition": "STATUS_CODE_DEFAULT",
        "condition": "",
        "grpcConnectTimeoutMs": canonical["grpcConnectTimeoutMs"],
    }


def _opaque_native() -> YamlObject:
    return {
        **_native(),
        "socketTimeout": 60_000,
        "localParams": [],
        "varPool": [{"prop": "native", "value": "preserve"}],
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _spec(version: str, params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"grpc-{version}"},
            "tasks": [
                {
                    "name": "call-grpc-service",
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


def _prepared(
    version: str,
    params: YamlObject,
) -> PreparedWorkflowCompilation[WorkflowCreatePayload]:
    catalog = get_task_authoring_catalog(version)
    return prepare_workflow_create_compilation(_spec(version, params), catalog=catalog)


def _fake_dag(params: YamlObject, *, workflow_name: str) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name="call-grpc-service",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value=_TASK_TYPE,
                task_params_value=json.dumps(params, ensure_ascii=False),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )


def _compiled(
    version: str, params: YamlObject, *, task_code: int = 34_000
) -> YamlObject:
    prepared = _prepared(version, params)
    payload = prepared.materialize([task_code])
    definition = json.loads(payload["taskDefinitionJson"])[0]
    assert isinstance(definition, dict)
    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == _TASK_TYPE
    assert definition["taskExecuteType"] == "BATCH"
    native = json.loads(definition["taskParams"])
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decode(
    version: str,
    params: YamlObject,
) -> tuple[YamlObject, ProjectionSource]:
    projected = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    return cast("YamlObject", projected.task.task_params), projected.reencode_source


def _template_yaml(version: str) -> str:
    return authoring_prep.template_yaml(_TASK_TYPE, version, variant="minimal")


def _task_params_schema(version: str) -> JsonObject:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    task_params = schema["properties"]["task_params"]
    assert isinstance(task_params, dict)
    reference = task_params.get("$ref")
    if isinstance(reference, str):
        task_params = schema["$defs"][reference.rsplit("/", maxsplit=1)[-1]]
        assert isinstance(task_params, dict)
    return cast("JsonObject", task_params)


def _resolve_schema(root: JsonObject, schema: JsonObject) -> JsonObject:
    reference = schema.get("$ref")
    if not isinstance(reference, str):
        return schema
    definitions = root["$defs"]
    assert isinstance(definitions, dict)
    resolved = definitions[reference.rsplit("/", maxsplit=1)[-1]]
    assert isinstance(resolved, dict)
    return cast("JsonObject", resolved)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_catalog_exposes_one_exact_literal_unary_string_record_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert profile.category == "Universal"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.plugin.task.grpc.GrpcParameters"
    )
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.review == "grpc-literal-unary-string-record-call-exact-subset"
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == review.review
    assert membership.contract.family == "grpc-literal-unary-string-record-call-v1"
    assert membership.contract.params_model is not None
    assert membership.contract.opaque_authoring_selector is None
    assert membership.profile_version == version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_grpc_is_upstream_absent_before_3_4_0(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert _TASK_TYPE not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is False
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert _TASK_TYPE not in catalog.authoring_task_types


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_grpc_projector_rejects_versions_without_the_plugin(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError, match=_TASK_TYPE):
        _encode(version, _canonical())


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_schema_exposes_only_the_owned_canonical_fields(version: str) -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_fields = {
        path.removeprefix("task_params."): field
        for path, field in fields.items()
        if path.startswith("task_params.")
    }

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert result.data["state_rules"] == []
    assert set(task_fields) == {
        *_CANONICAL_FIELDS,
        "requestFields[]",
        "requestFields[].name",
        "requestFields[].number",
        "responseFields[]",
        "responseFields[].name",
        "responseFields[].number",
        "message.*",
    }
    for field_name in _CANONICAL_FIELDS:
        assert task_fields[field_name]["required"] is True
    assert task_fields["channelCredentialType"]["choices"] == [
        "INSECURE",
        "TLS_DEFAULT",
    ]
    for excluded in _EXCLUDED_FIELDS:
        assert excluded not in task_fields
    for field_name in _CANONICAL_FIELDS:
        description = task_fields[field_name]["description"].lower()
        assert "info" in description
        assert "not secret storage" in description


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_json_schema_is_closed_and_models_two_string_records(version: str) -> None:
    task_params = _task_params_schema(version)
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _CANONICAL_FIELDS
    assert set(required) == _CANONICAL_FIELDS
    credential = cast("JsonObject", properties["channelCredentialType"])
    assert credential["enum"] == ["INSECURE", "TLS_DEFAULT"]
    timeout = cast("JsonObject", properties["grpcConnectTimeoutMs"])
    assert timeout["type"] == "integer"
    assert timeout["exclusiveMinimum"] == 0
    message = cast("JsonObject", properties["message"])
    assert message["type"] == "object"
    message_value = cast("JsonObject", message["additionalProperties"])
    assert message_value["type"] == "string"
    message_pattern = message_value["pattern"]
    assert isinstance(message_pattern, str)
    assert re.fullmatch(message_pattern, "ordinary Unicode 中文") is not None
    assert re.fullmatch(message_pattern, "${secret}") is None
    assert re.fullmatch(message_pattern, "line\nfeed") is None

    service_name = cast("JsonObject", properties["serviceName"])
    service_name_pattern = cast("str", service_name["pattern"])
    assert re.fullmatch(service_name_pattern, "Request") is None
    assert re.fullmatch(service_name_pattern, "Response") is None

    record_items: dict[str, JsonObject] = {}
    for field_name in ("requestFields", "responseFields"):
        record = cast("JsonObject", properties[field_name])
        assert record["type"] == "array"
        assert record["uniqueItems"] is True
        item = _resolve_schema(task_params, cast("JsonObject", record["items"]))
        record_items[field_name] = item
        assert item["additionalProperties"] is False
        assert item["required"] == ["name", "number"]
        item_properties = cast("JsonObject", item["properties"])
        assert set(item_properties) == {"name", "number"}
        number = cast("JsonObject", item_properties["number"])
        assert number["type"] == "integer"
        assert number["minimum"] == 1
        assert number["maximum"] == 536_870_911
        assert number["not"] == {"minimum": 19_000, "maximum": 19_999}

    request_name = cast(
        "JsonObject",
        cast("JsonObject", record_items["requestFields"]["properties"])["name"],
    )
    response_name = cast(
        "JsonObject",
        cast("JsonObject", record_items["responseFields"]["properties"])["name"],
    )
    request_name_pattern = cast("str", request_name["pattern"])
    response_name_pattern = cast("str", response_name["pattern"])
    for secret_name in _REQUEST_SECRET_FIELD_NAMES:
        assert re.fullmatch(request_name_pattern, secret_name) is None
        assert re.fullmatch(request_name_pattern, secret_name.upper()) is None
        assert re.fullmatch(response_name_pattern, secret_name) is not None
    assert re.fullmatch(request_name_pattern, "clientSecretValue") is not None

    assert task_params["x-dsctl-runtime-validations"] == [
        "requestFields and responseFields each require unique raw names, "
        "ASCII-case-insensitive protobuf JSON camel-case names, and field numbers",
        "message keys must exactly match requestFields names",
    ]


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_summary_and_mappings_publish_only_the_canonical_model(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    result = task_type_schema_result(
        _TASK_TYPE,
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["task_type"] == _TASK_TYPE
    assert summary["category"] == "Universal"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        *{f"task_params.{field_name}" for field_name in _CANONICAL_FIELDS},
        "task_params.requestFields[].name",
        "task_params.requestFields[].number",
        "task_params.responseFields[].name",
        "task_params.responseFields[].number",
    }
    assert mappings == {
        "task_params.url": "taskDefinitionJson[].taskParams.url",
        "task_params.channelCredentialType": (
            "taskDefinitionJson[].taskParams.channelCredentialType"
        ),
        "task_params.serviceName": (
            "taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"
        ),
        "task_params.methodName": "taskDefinitionJson[].taskParams.methodName",
        "task_params.requestFields": (
            "taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"
        ),
        "task_params.responseFields": (
            "taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"
        ),
        "task_params.message": "taskDefinitionJson[].taskParams.message",
        "task_params.grpcConnectTimeoutMs": (
            "taskDefinitionJson[].taskParams.grpcConnectTimeoutMs"
        ),
    }


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_minimal_template_validates_and_compiles_to_the_exact_wire(
    version: str,
) -> None:
    yaml_text = _template_yaml(version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == _TASK_TYPE
    assert set(params) == _CANONICAL_FIELDS
    normalized = get_task_authoring_catalog(version).normalize_task_params(
        _TASK_TYPE,
        cast("YamlObject", params),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == params
    assert set(_compiled(version, normalized)) == _WIRE_FIELDS

    guidance = yaml_text.lower().replace("_", " ")
    for term in (
        "info",
        "not secret storage",
        "retry",
        "duplicate",
        "cancel",
        "failover",
        "default trust",
        "custom ca",
        "mtls",
        "token",
        "channel",
        "event loop",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_grpc_normalize_projector_and_prepared_compile_are_exact(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    canonical = _canonical()
    expected = _native(canonical)
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params(_TASK_TYPE, canonical, intent=intent)
    decoded, source = _decode(version, expected)

    assert normalized == canonical
    assert normalized is not canonical
    assert _encode(version, canonical) == expected
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _compiled(version, canonical) == expected


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_prepared_compilation_is_pure_and_materializes_one_unique_wire(
    version: str,
) -> None:
    canonical = _canonical(message={"account": "alice", "region": "中文"})
    prepared = _prepared(version, canonical)

    first = prepared.materialize([40_001])
    second = prepared.materialize([40_002])
    first_definition = json.loads(first["taskDefinitionJson"])[0]
    second_definition = json.loads(second["taskDefinitionJson"])[0]
    first_native = json.loads(first_definition["taskParams"])
    second_native = json.loads(second_definition["taskParams"])

    assert prepared.required_task_code_count == 1
    assert first_definition["code"] == 40_001
    assert second_definition["code"] == 40_002
    assert first_native == second_native == _native(canonical)
    assert first_native["grpcServiceDefinitionJSON"] == json.dumps(
        _descriptor(),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert first_native["grpcServiceDefinition"] == _proto()
    assert first_native["methodName"] == "EchoService/Echo"
    assert first_native["message"] == '{"account":"alice","region":"中文"}'


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    ("request_fields", "response_fields", "message"),
    [
        ([], [], {}),
        ([], [{"name": "result", "number": 1}], {}),
        ([{"name": "account", "number": 1}], [], {"account": "alice"}),
    ],
)
def test_grpc_empty_request_and_response_records_compile_without_ambiguity(
    version: str,
    request_fields: list[YamlObject],
    response_fields: list[YamlObject],
    message: dict[str, str],
) -> None:
    canonical = _canonical(
        request_fields=request_fields,
        response_fields=response_fields,
        message=message,
    )

    normalized = get_task_authoring_catalog(version).normalize_task_params(
        _TASK_TYPE,
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == canonical
    assert _encode(version, canonical) == _native(canonical)
    assert _compiled(version, canonical) == _native(canonical)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("field_name", sorted(_CANONICAL_FIELDS))
def test_grpc_requires_every_canonical_field(version: str, field_name: str) -> None:
    params = _canonical()
    params.pop(field_name)

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog(version).normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("field_name", ["serviceName", "methodName"])
@pytest.mark.parametrize(
    "value",
    ["", "1invalid", "has-hyphen", "has.dot", "has space", "服务", "${dynamic}"],
)
def test_grpc_service_and_method_names_are_strict_proto_identifiers(
    field_name: str,
    value: str,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("service_name", ["Request", "Response"])
def test_grpc_service_name_does_not_collide_with_fixed_message_types(
    service_name: str,
) -> None:
    params = _canonical(service_name=service_name)

    with pytest.raises(ValueError, match="serviceName"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("record_name", ["requestFields", "responseFields"])
@pytest.mark.parametrize(
    "fields",
    [
        [{"name": "1invalid", "number": 1}],
        [{"name": "has-hyphen", "number": 1}],
        [{"name": "${dynamic}", "number": 1}],
        [{"name": "first", "number": 0}],
        [{"name": "first", "number": 536_870_912}],
        [{"name": "first", "number": 19_000}],
        [{"name": "first", "number": 19_999}],
        [{"name": "same", "number": 1}, {"name": "same", "number": 2}],
        [{"name": "first", "number": 1}, {"name": "second", "number": 1}],
    ],
)
def test_grpc_record_fields_are_unique_and_use_valid_proto_numbers(
    record_name: str,
    fields: list[YamlObject],
) -> None:
    params = _canonical()
    params[record_name] = cast("YamlValue", fields)
    if record_name == "requestFields":
        params["message"] = {
            cast("str", field["name"]): "value"
            for field in fields
            if isinstance(field.get("name"), str)
        }

    with pytest.raises(ValueError, match=record_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("record_name", ["requestFields", "responseFields"])
@pytest.mark.parametrize(
    ("first_name", "second_name"),
    [
        ("foo_bar", "fooBar"),
        ("fooBar", "foobar"),
        ("FOO", "foo"),
    ],
)
def test_grpc_record_fields_reject_protobuf_json_name_collisions(
    record_name: str,
    first_name: str,
    second_name: str,
) -> None:
    fields: list[YamlObject] = [
        {"name": first_name, "number": 1},
        {"name": second_name, "number": 2},
    ]
    params = _canonical()
    params[record_name] = cast("YamlValue", fields)
    if record_name == "requestFields":
        params["message"] = {first_name: "first", second_name: "second"}

    with pytest.raises(ValueError, match=r"protobuf JSON|JSON name"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "message",
    [
        {},
        {"account": "alice"},
        {"account": "alice", "region": "cn", "extra": "no"},
        {"account": "alice", "region": 7},
        {"account": "${secret}", "region": "cn"},
        {"account": "alice\nadmin", "region": "cn"},
    ],
)
def test_grpc_message_has_exact_declared_request_keys_and_literal_strings(
    message: dict[str, object],
) -> None:
    params = _canonical(message=cast("dict[str, str]", message))

    with pytest.raises(ValueError, match="message"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "secret_name",
    [
        *_REQUEST_SECRET_FIELD_NAMES,
        *[name.upper() for name in _REQUEST_SECRET_FIELD_NAMES],
    ],
)
def test_grpc_logged_request_records_reject_secret_like_field_names(
    secret_name: str,
) -> None:
    params = _canonical(
        request_fields=[{"name": secret_name, "number": 1}],
        message={secret_name: "do-not-log-secrets"},
    )

    with pytest.raises(ValueError, match="not secret storage"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("field_name", _REQUEST_SECRET_FIELD_NAMES)
def test_grpc_response_field_names_follow_only_proto_identifier_rules(
    field_name: str,
) -> None:
    params = _canonical(response_fields=[{"name": field_name, "number": 1}])

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    assert _compiled("3.4.2", params) == _native(params)


@pytest.mark.parametrize("field_name", ["tokenValue", "clientSecretValue"])
def test_grpc_request_secret_denylist_is_exact_not_substring_based(
    field_name: str,
) -> None:
    params = _canonical(
        request_fields=[{"name": field_name, "number": 1}],
        message={field_name: "ordinary-value"},
    )

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    assert _compiled("3.4.2", params) == _native(params)


@pytest.mark.parametrize("credential_type", ["", "PLAINTEXT", "MTLS", 1, None])
def test_grpc_credential_type_is_one_exact_native_enum(
    credential_type: object,
) -> None:
    params = _canonical()
    params["channelCredentialType"] = cast("YamlValue", credential_type)

    with pytest.raises(ValueError, match="channelCredentialType"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "timeout",
    [0, -1, 1.5, "10000", True, None, 9_223_372_036_854_775_808],
)
def test_grpc_connect_timeout_is_one_strict_positive_integer(timeout: object) -> None:
    params = _canonical()
    params["grpcConnectTimeoutMs"] = cast("YamlValue", timeout)

    with pytest.raises(ValueError, match="grpcConnectTimeoutMs"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "proto_keyword",
    ["message", "service", "rpc", "returns", "stream", "string", "package"],
)
@pytest.mark.parametrize("target", ["service", "method", "request", "response"])
def test_grpc_generated_proto_rejects_reserved_identifiers(
    proto_keyword: str,
    target: str,
) -> None:
    params = _canonical()
    if target == "service":
        params["serviceName"] = proto_keyword
    elif target == "method":
        params["methodName"] = proto_keyword
    elif target == "request":
        params["requestFields"] = [{"name": proto_keyword, "number": 1}]
        params["message"] = {proto_keyword: "value"}
    else:
        params["responseFields"] = [{"name": proto_keyword, "number": 1}]

    with pytest.raises(ValueError, match=r"reserved|identifier"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("url", "${grpc_target}"),
        ("url", "grpc.example.internal:7443\nnext"),
        ("url", "grpc.example.internal"),
        ("url", "grpc.example.internal:0"),
        ("url", "user:password@grpc.example.internal:7443"),
        ("serviceName", "Service\x00"),
        ("methodName", "Method\x7f"),
    ],
)
def test_grpc_literal_identity_rejects_placeholders_controls_and_credentialed_urls(
    field_name: str,
    value: str,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("excluded", sorted(_EXCLUDED_FIELDS))
def test_grpc_typed_authoring_rejects_native_runtime_and_future_fields(
    version: str,
    excluded: str,
) -> None:
    params = _canonical()
    params[excluded] = cast(
        "YamlValue",
        {
            "localParams": [],
            "varPool": [],
            "resourceList": [],
            "socketTimeout": 60_000,
            "grpcCredentialType": "INSECURE",
            "grpcCheckCondition": "STATUS_CODE_CUSTOM",
            "condition": "UNAUTHENTICATED",
            "grpcServiceDefinition": _proto(),
            "grpcServiceDefinitionJSON": json.dumps(_descriptor()),
            "streaming": True,
            "futureField": {"native": True},
        }[excluded],
    )

    with pytest.raises(ValueError, match=excluded):
        get_task_authoring_catalog(version).normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_invalid_canonical_never_downgrades_to_opaque(version: str) -> None:
    params = _canonical(message={"account": "alice"})

    with pytest.raises(TaskParameterProjectionError, match="message"):
        _encode(version, params)
    with pytest.raises(ValueError, match="message"):
        _spec(version, params)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_grpc_has_no_public_raw_native_create_or_edit_selector(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog(version).normalize_task_params(
            _TASK_TYPE,
            _opaque_native(),
            intent=intent,
        )

    assert str(captured.value) == (
        f"GRPC opaque authoring is unsupported for DolphinScheduler {version}."
    )
    assert captured.value.details == {
        "selected_version": version,
        "task_type": _TASK_TYPE,
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {version} policy permits only opaque "
            "preservation for GRPC."
        ),
    }


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_safe_and_extra_native_wire_have_exact_projection_provenance(
    version: str,
) -> None:
    canonical = _canonical()
    safe = _native(canonical)
    opaque = _opaque_native()

    decoded_safe, safe_source = _decode(version, safe)
    decoded_opaque, opaque_source = _decode(version, opaque)

    assert decoded_safe == canonical
    assert safe_source is ProjectionSource.TYPED_AUTHORING
    assert decoded_opaque == opaque
    assert opaque_source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_native(), _canonical(), ProjectionSource.TYPED_AUTHORING),
        (_opaque_native(), _opaque_native(), ProjectionSource.OPAQUE_PRESERVE),
    ],
)
def test_grpc_export_and_unchanged_update_preserve_projection_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    dag = _fake_dag(native, workflow_name="grpc-export")
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        preserved_projection_sources=baseline.projection_sources,
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert baseline.projection_sources["call-grpc-service"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported
    assert compiled == native


def test_grpc_opaque_native_metadata_patch_preserves_every_wire_field() -> None:
    native = _opaque_native()
    dag = _fake_dag(native, workflow_name="grpc-metadata-edit")
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "call-grpc-service"},
                            "set": {"description": "metadata only"},
                        }
                    ]
                }
            }
        }
    ).patch
    plan = prepare_workflow_mutation_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog("3.4.2"),
    )
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]

    assert definition["description"] == "metadata only"
    assert json.loads(definition["taskParams"]) == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_opaque_preserve_is_a_deep_identity_copy(version: str) -> None:
    native = _opaque_native()
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", native),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert projected.task_params == native
    assert projected.task_params is not native
    projected.task_params["futureField"] = {"changed": True}
    assert native["futureField"] == {"nested": ["native", {"preserve": True}]}


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_grpc_runtime_surface_is_absent_before_3_4_0(version: str) -> None:
    surface = cast("_Surface", get_task_authoring_surface(version)).grpc

    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.parameter_substitution is False
    assert surface.authored_values_logged is False
    assert surface.result_output_supported is False
    assert surface.cancel_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_may_duplicate is False


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_runtime_surface_locks_the_exact_execution_and_tls_limits(
    version: str,
) -> None:
    surface = cast("_Surface", get_task_authoring_surface(version)).grpc

    assert surface.available is True
    assert surface.wire_epoch == "literal-unary-string-record-call"
    assert surface.parameter_substitution is False
    assert surface.authored_values_logged is True
    assert surface.result_output_supported is False
    assert surface.cancel_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_may_duplicate is True
    assert surface.channel_shutdown is False
    assert surface.event_loop_group_shutdown is False
    assert surface.tls_uses_default_trust is True
    assert surface.tls_hostname_verification is True
    assert surface.custom_ca_supported is False
    assert surface.mutual_tls_supported is False
    assert surface.request_authentication_supported is False


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_grpc_schema_guidance_discloses_logging_replay_and_transport_limits(
    version: str,
) -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        full=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    guidance = json.dumps(result.data, ensure_ascii=False).lower().replace("_", " ")

    for term in (
        "info",
        "not secret storage",
        "unary",
        "string",
        "status code default",
        "retry",
        "duplicate",
        "cancel",
        "failover",
        "output",
        "channel",
        "event loop",
        "default trust",
        "hostname",
        "custom ca",
        "mtls",
        "token",
    ):
        assert term in guidance
