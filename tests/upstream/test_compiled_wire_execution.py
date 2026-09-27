from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Annotated, ClassVar, Self, cast
from urllib.parse import parse_qs

import httpx
import pytest
from pydantic import (
    AfterValidator,
    Field,
    StrictInt,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel
from dsctl.upstream.wire import (
    CompiledWireCodec,
    CompiledWireProgram,
    WireContractError,
    WireExecutionMode,
    WireExecutor,
    WireResponseDecodeError,
    WireResultEnvelope,
    compiled_wire_program,
)
from tests.fake_wire import FakePreparedWireCall
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.wire import PreparedCompiledWireCall

_DIGEST = "sha256:" + "1" * 64
_OTHER_DIGEST = "sha256:" + "2" * 64


class _PathParams(BaseParamsModel):
    code: int = Field(strict=True)
    validation_count: ClassVar[int] = 0

    @model_validator(mode="after")
    def count_validation(self) -> Self:
        _PathParams.validation_count += 1
        return self


class _Params(_PathParams):
    label: str | None = None


class _TwoPathParams(_PathParams):
    project_code: int = Field(alias="projectCode", strict=True)


class _TwoPathFormParams(_TwoPathParams):
    label: str | None = None


class _ListFormParams(_PathParams):
    worker_groups: list[str] = Field(alias="workerGroups")


def _program(
    method: str = "GET",
    channel: str = "query",
    *,
    response_projection: str = "direct",
) -> tuple[CompiledWireProgram, list[int]]:
    """Use tiny schemas to test the executor, not duplicate exact DS fixtures."""
    decoded: list[int] = []

    def record_decode(value: int) -> int:
        decoded.append(value)
        return value

    _PathParams.validation_count = 0
    path_fields = ("code",) if channel in {"path", "path_form", "path_query"} else ()
    fields: tuple[tuple[str, str], ...] = (
        ("code", "path_variable" if path_fields else "request_param"),
    )
    if channel != "path":
        fields += (("label", "request_param"),)
    codec = CompiledWireCodec(
        method=method,
        path="mechanism/{code}" if path_fields else "mechanism",
        channel=channel,
        path_encoding="percent-encoded-utf8-segment-v1" if path_fields else None,
        field_bindings=fields,
        path_fields=path_fields,
        params_model=_PathParams if channel == "path" else _Params,
        response_adapter=TypeAdapter(
            Annotated[StrictInt, AfterValidator(record_decode)]
        ),
        # Compiled execution must not consume this legacy capture sample.
        capture_payload="not a valid response",
        request_fingerprint=_DIGEST,
        request_schema_fingerprint=_DIGEST,
        response_fingerprint=_DIGEST,
        response_schema_fingerprint=_DIGEST,
        result_envelope=WireResultEnvelope.REQUIRED,
        response_projection=response_projection,
    )
    return (
        compiled_wire_program(
            ds_version="3.4.1",
            source_operation="Mechanism.exchange",
            source_contract_digest=_DIGEST,
            execution_mode=WireExecutionMode.MUTATION_ONCE,
            codec=codec,
        ),
        decoded,
    )


@pytest.mark.parametrize(
    ("method", "channel"),
    [
        ("GET", "query"),
        ("GET", "path"),
        ("GET", "path_query"),
        ("POST", "form"),
        ("POST", "path_form"),
        ("DELETE", "path"),
        ("PUT", "path_form"),
    ],
)
def test_compiled_exchange_encodes_once_and_decodes_only_the_real_response(
    method: str,
    channel: str,
) -> None:
    program, decoded = _program(method, channel)
    args: JsonObject = {"code": 7}
    if channel != "path":
        args["label"] = "审阅 & exact"
    prepared = program.prepare(args)
    preview = prepared.request
    assert _PathParams.validation_count == 1
    assert decoded == []

    args["code"] = 99
    args["label"] = "changed after prepare"
    if preview.query is not None:
        assert isinstance(preview.query, dict)
        preview.query["code"] = 100
    if preview.form is not None:
        assert isinstance(preview.form, dict)
        preview.form["label"] = "changed preview"
    expected = prepared.request
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == method
        assert request.url.path == f"/dolphinscheduler{expected.path}"
        assert request.headers["token"] == "execution-secret"
        assert request.headers["accept"] == "application/json"
        if channel in {"query", "path_query"}:
            assert dict(request.url.params) == (
                {"code": "7", "label": "审阅 & exact"}
                if channel == "query"
                else {"label": "审阅 & exact"}
            )
            assert request.content == b""
        elif channel == "path":
            assert dict(request.url.params) == {}
            assert request.content == b""
        else:
            form = parse_qs(request.content.decode())
            assert form == (
                {"code": ["7"], "label": ["审阅 & exact"]}
                if channel == "form"
                else {"label": ["审阅 & exact"]}
            )
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": 7})

    with DolphinSchedulerClient(
        make_profile(api_token="execution-secret"),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )

    assert result.payload == result.raw_payload == 7
    assert result.request == expected
    assert _PathParams.validation_count == 1
    assert decoded == [7]
    assert len(calls) == 1


def _two_path_program(method: str) -> CompiledWireProgram:
    program, _decoded = _program()
    fields: tuple[tuple[str, str], ...] = (
        ("code", "path_variable"),
        ("projectCode", "path_variable"),
    )
    if method == "PUT":
        fields += (("label", "request_param"),)
    return replace(
        program,
        codec=replace(
            program.codec,
            method=method,
            path="projects/{projectCode}/project-parameter/{code}",
            channel="path_form" if method == "PUT" else "path",
            path_encoding="percent-encoded-utf8-segment-v1",
            field_bindings=fields,
            path_fields=("code", "projectCode"),
            params_model=_TwoPathFormParams if method == "PUT" else _TwoPathParams,
        ),
    )


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("codes", [(0, -1), (2**53 + 1, 2**63 - 1)])
def test_two_integer_paths_bind_by_name_and_keep_codes_out_of_the_body(
    method: str, codes: tuple[int, int]
) -> None:
    project_code, code = codes
    program = _two_path_program(method)
    args: JsonObject = {"projectCode": project_code, "code": code}
    if method == "PUT":
        args["label"] = "空格 & value"
    prepared = program.prepare(args)
    expected_path = f"/projects/{project_code}/project-parameter/{code}"
    assert prepared.request.path == expected_path
    assert prepared.request.query is None
    assert prepared.request.form == (
        {"label": "空格 & value"} if method == "PUT" else None
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == method
        assert request.url.path == f"/dolphinscheduler{expected_path}"
        assert dict(request.url.params) == {}
        assert parse_qs(request.content.decode()) == (
            {"label": ["空格 & value"]} if method == "PUT" else {}
        )
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(calls) == 1
    assert _PathParams.validation_count == 1


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("field", ["projectCode", "code"])
@pytest.mark.parametrize("value", [True, 7.0, "7", None])
def test_two_path_codes_require_strict_integers(
    method: str, field: str, value: object
) -> None:
    program = _two_path_program(method)
    with pytest.raises(ValidationError):
        program.codec.params_model.model_validate(
            {"projectCode": 1, "code": 2, field: value}
        )


@pytest.mark.parametrize("channel", ["path_query", "path_form"])
def test_mixed_path_fields_do_not_materialize_an_unset_optional_field(
    channel: str,
) -> None:
    program, _decoded = _program("GET" if channel == "path_query" else "POST", channel)
    request = program.prepare({"code": 7}).request
    assert request.path == "/mechanism/7"
    assert request.query == ({} if channel == "path_query" else None)
    assert request.form == ({} if channel == "path_form" else None)


@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_two_path_segments_remain_closed_for_unreviewed_methods(method: str) -> None:
    program = _two_path_program("GET" if method == "PUT" else "PUT")
    with pytest.raises(WireContractError, match="unsupported transport shape"):
        replace(program.codec, method=method)


@pytest.mark.parametrize(
    "path",
    [
        "projects/{projectCode}/items/{projectCode}",
        "projects/{projectCode}/items/{other}",
        "projects/{projectCode}/items/{code}/{third}",
    ],
)
def test_two_path_placeholder_inventory_is_exact(path: str) -> None:
    program = _two_path_program("GET")
    with pytest.raises(WireContractError, match="path-variable inventory"):
        replace(program.codec, path=path)


def test_path_field_declaration_order_does_not_change_name_binding() -> None:
    program = _two_path_program("GET")
    reordered = replace(program.codec, path_fields=("projectCode", "code"))
    args: JsonObject = {"projectCode": 7, "code": 11}
    assert reordered.encode(args) == program.codec.encode(args)
    with pytest.raises(WireContractError, match="field-binding inventory"):
        replace(program.codec, path_fields=("code", "projectCode", "code"))


@pytest.mark.parametrize("groups", [[""], ["default,analytics"], ["a", "b"]])
def test_path_form_preserves_list_values_and_explicit_empty_elements(
    groups: list[str],
) -> None:
    program, _decoded = _program("POST", "path_form")
    program = replace(
        program,
        codec=replace(
            program.codec,
            params_model=_ListFormParams,
            field_bindings=(
                ("code", "path_variable"),
                ("workerGroups", "request_param"),
            ),
        ),
    )
    args_groups = groups.copy()
    prepared = program.prepare({"code": 7, "workerGroups": args_groups})
    expected_groups = groups.copy()
    args_groups.append("not part of the prepared request")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/dolphinscheduler/mechanism/7"
        assert parse_qs(request.content.decode(), keep_blank_values=True) == {
            "workerGroups": expected_groups
        }
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(calls) == 1


def test_compiled_empty_get_uses_an_explicit_closed_request_schema() -> None:
    program, decoded = _program()
    program = replace(
        program,
        codec=replace(
            program.codec,
            field_bindings=(),
            params_model=BaseParamsModel,
        ),
    )
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        program.prepare({"unreviewed": "value"})
    prepared = program.prepare({})
    assert prepared.request.query is None
    assert prepared.request.form is None
    assert decoded == []
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/mechanism"
        assert dict(request.url.params) == {}
        assert request.content == b""
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": 7})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )
    assert result.payload == 7
    assert decoded == [7]
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("method", "channel"),
    [("GET", "path"), ("POST", "form"), ("DELETE", "path"), ("PUT", "path_form")],
)
def test_compiled_empty_request_is_only_allowed_for_get_query(
    method: str, channel: str
) -> None:
    program, _decoded = _program(method, channel)
    with pytest.raises(WireContractError, match="field-binding inventory"):
        replace(program.codec, field_bindings=(), params_model=BaseParamsModel)


def test_compiled_get_path_rejects_mixed_query_fields() -> None:
    program, _decoded = _program("PUT", "path_form")
    with pytest.raises(WireContractError, match="path-variable inventory"):
        replace(program.codec, method="GET", channel="path")


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (7, 7),
        ([7], [7]),
        (None, None),
        ({"data": 7}, {"data": 7}),
        ({"status": None, "data": 7}, {"status": None, "data": 7}),
        ({"status": "SUCCESS", "data": 7, "dataList": 8}, 7),
        ({"status": "SUCCESS", "dataList": [7]}, [7]),
        ({"status": "SUCCESS", "data": None, "dataList": [7]}, None),
        ({"status": "SUCCESS", "data": 0, "dataList": [7]}, 0),
        ({"status": "SUCCESS", "data": False, "dataList": [7]}, False),
        ({"status": "SUCCESS", "data": [], "dataList": [7]}, []),
        ({"status": "SUCCESS"}, None),
    ],
)
def test_compiled_status_data_keeps_legacy_projection_and_raw_response(
    payload: JsonValue,
    expected: JsonValue,
) -> None:
    program, _decoded = _program(response_projection="status_data")
    program = replace(
        program, codec=replace(program.codec, response_adapter=TypeAdapter(object))
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": payload})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert result.payload == expected
    assert result.raw_payload == payload
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("payload", "message", "data"),
    [
        (
            {"status": "FAILURE", "msg": "denied", "data": {"id": 7}, "dataList": []},
            "denied",
            {"id": 7},
        ),
        ({"status": "", "msg": "", "dataList": [7]}, "DS API error", [7]),
        ({"status": False, "msg": 7, "data": None, "dataList": [7]}, "7", None),
        ({"status": 0}, "DS API error", None),
    ],
)
def test_compiled_status_failure_precedes_decode_even_for_void_responses(
    payload: JsonObject,
    message: str,
    data: JsonValue,
) -> None:
    program, decoded = _program(response_projection="status_data")
    program = replace(
        program,
        codec=replace(
            program.codec, response_adapter=None, response_schema_fingerprint=None
        ),
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "data": payload})

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiResultError) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert caught.value.message == message
    expected_details: dict[str, object] = {"result_code": None}
    if data is not None:
        expected_details["data"] = data
    assert caught.value.details == expected_details
    assert decoded == []
    assert len(calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "SUCCESS", "data": "7"},
        {"status": "SUCCESS", "data": None, "dataList": 7},
        {"status": None, "data": 7},
        {"data": 7},
    ],
)
def test_compiled_status_data_still_requires_the_exact_response_adapter(
    payload: JsonObject,
) -> None:
    program, decoded = _program(response_projection="status_data")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "data": payload})

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireResponseDecodeError) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert caught.value.details["wire_response_received"] is True
    assert caught.value.details["validation_error_count"] == 1
    assert decoded == []
    assert len(calls) == 1


@pytest.mark.parametrize("projection", ["single_data_list", "unknown"])
def test_compiled_response_projection_is_closed(projection: str) -> None:
    with pytest.raises(WireContractError, match="response projection is unsupported"):
        _program(response_projection=projection)


@pytest.mark.parametrize("failure", ["version", "program", "source", "codec"])
def test_compiled_binding_mismatch_rejects_without_http(failure: str) -> None:
    program, decoded = _program()
    prepared = program.prepare({"code": 7})
    source_digest = _DIGEST
    if failure == "version":
        prepared = replace(prepared, ds_version="3.4.2")
    elif failure == "program":
        prepared = replace(prepared, program_fingerprint=_OTHER_DIGEST)
    elif failure == "codec":
        program = replace(program, codec=replace(program.codec, path="other"))
    else:
        source_digest = _OTHER_DIGEST
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireContractError, match="does not match"),
    ):
        WireExecutor(client, source_contract_digest=source_digest).execute(
            program, prepared
        )
    assert calls == []
    assert decoded == []
    assert _PathParams.validation_count == 1


def test_structural_token_cannot_replace_an_encoded_compiled_request() -> None:
    program, decoded = _program()
    prepared = program.prepare({"code": 7})
    token = FakePreparedWireCall(
        ds_version=prepared.ds_version,
        program_fingerprint=prepared.program_fingerprint,
        args={"code": 7},
        request=prepared.request,
    )

    def reject_http(_request: httpx.Request) -> httpx.Response:
        pytest.fail("A structural token must fail before HTTP")

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(reject_http)
        ) as client,
        pytest.raises(WireContractError, match="requires an encoded request"),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, cast("PreparedCompiledWireCall", token)
        )
    assert decoded == []


def test_compiled_response_failure_retains_completed_exchange_evidence() -> None:
    program, decoded = _program()
    prepared = program.prepare({"code": 7, "label": None})
    assert prepared.request.query == {"code": 7}
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": "7"})

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireResponseDecodeError) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert caught.value.details["wire_response_received"] is True
    assert caught.value.details["validation_error_count"] == 1
    assert caught.value.source is not None
    assert caught.value.source["layer"] == "response"
    assert len(calls) == 1
    assert decoded == []
    assert _PathParams.validation_count == 1


@pytest.mark.parametrize("channel", ["query", "path_query"])
@pytest.mark.parametrize("change", ["method", "path", "query", "nested_query"])
def test_compiled_prepared_request_tampering_fails_before_http(
    change: str, channel: str
) -> None:
    program, decoded = _program(channel=channel)
    prepared = program.prepare({"code": 7})
    request = prepared.request
    if change == "method":
        prepared = replace(prepared, _request=replace(request, method="POST"))
    elif change == "path":
        prepared = replace(prepared, _request=replace(request, path="/other"))
    elif change == "query":
        prepared = replace(prepared, _request=replace(request, query={"code": 99}))
    else:
        assert isinstance(prepared._request.query, dict)
        prepared._request.query["code"] = 99
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireContractError, match="no longer matches its encoded content"),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert calls == []
    assert decoded == []
    assert _PathParams.validation_count == 1


@pytest.mark.parametrize("error_count", [1, 6])
def test_response_diagnostics_bound_errors_and_hide_dynamic_keys_and_values(
    error_count: int,
) -> None:
    program, _decoded = _program()
    program = replace(
        program,
        codec=replace(
            program.codec, response_adapter=TypeAdapter(dict[str, StrictInt])
        ),
    )
    payload = {f"secret/key/{index}": "secret-value" for index in range(error_count)}

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 0, "data": payload})

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireResponseDecodeError) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    details = caught.value.details
    assert details["wire_response_received"] is True
    assert details["validation_error_count"] == error_count
    assert "secret" not in str(details)
    if error_count == 1:
        assert details["validation_errors"] == [
            {
                "field": "<dynamic-key>",
                "type": "int_type",
                "message": "Response value has the wrong type.",
            }
        ]
    else:
        assert details["validation_errors_truncated"] is True
        assert "validation_errors" not in details


@pytest.mark.parametrize("mode", list(WireExecutionMode))
@pytest.mark.parametrize("method", ["GET", "POST"])
def test_execution_uses_reviewed_retry_policy_independently_of_http_method(
    mode: WireExecutionMode, method: str
) -> None:
    program, decoded = _program(method, "query" if method == "GET" else "form")
    program = replace(program, execution_mode=mode)
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            message = "lost response"
            raise httpx.ReadTimeout(message, request=request)
        return httpx.Response(200, json={"code": 0, "data": 7})

    profile = make_profile().model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        executor = WireExecutor(client, source_contract_digest=_DIGEST)
        prepared = program.prepare({"code": 7})
        if mode.retryable:
            assert executor.execute(program, prepared).payload == 7
        else:
            with pytest.raises(ApiTransportError) as caught:
                executor.execute(program, prepared)
            assert "wire_response_received" not in caught.value.details
    assert len(calls) == (2 if mode.retryable else 1)
    assert decoded == ([7] if mode.retryable else [])


@pytest.mark.parametrize(
    "suffix", ["?token=secret", "#fragment", "?", "#", "\t", "\r", "\n"]
)
def test_nonlegacy_paths_reject_url_syntax_before_dispatch(suffix: str) -> None:
    program, _decoded = _program()

    with pytest.raises(WireContractError):
        replace(program.codec, path=f"mechanism{suffix}")
