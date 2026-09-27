from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, ClassVar, Self

import httpx
import pytest
from pydantic import Field, StrictInt, TypeAdapter, ValidationError, model_validator

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiHttpError
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel
from dsctl.upstream.wire import (
    CompiledWireCodec,
    CompiledWireProgram,
    WireContractError,
    WireExecutionMode,
    WireExecutor,
    WireResultEnvelope,
    compiled_wire_program,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

_DIGEST = "sha256:" + "1" * 64


class _Connection(BaseParamsModel):
    display_name: str = Field(alias="displayName")
    note: str | None = None
    attempts: int = 3
    labels: list[str] = Field(default_factory=list)
    options: dict[str, str | None] = Field(default_factory=dict)


class _Body(BaseParamsModel):
    connection: _Connection
    enabled: bool = True
    extra_note: str | None = Field(default=None, alias="extraNote")


class _Params(BaseParamsModel):
    validation_count: ClassVar[int] = 0

    @model_validator(mode="after")
    def count_validation(self) -> Self:
        _Params.validation_count += 1
        return self


class _JsonParams(_Params):
    body: _Body = Field(alias="requestBody")


class _PathJsonParams(_JsonParams):
    item_id: StrictInt = Field(alias="id")


class _TextParams(_Params):
    body: str = Field(alias="requestBody")


class _PathTextParams(_TextParams):
    item_id: StrictInt = Field(alias="id")


class _OptionalJsonParams(_Params):
    body: _Body | None = Field(alias="requestBody")


class _DefaultJsonParams(_Params):
    body: _Body = Field(
        default_factory=lambda: _Body(connection=_Connection(display_name="default")),
        alias="requestBody",
    )


class _OptionalTextParams(_Params):
    body: str | None = Field(alias="requestBody")


class _DefaultTextParams(_Params):
    body: str = Field(default="", alias="requestBody")


class _EmptyBody(BaseParamsModel):
    note: str | None = None


class _EmptyJsonParams(_Params):
    body: _EmptyBody = Field(alias="requestBody")


class _MixedBodyParams(_JsonParams):
    second: _Body


def _program(
    channel: str,
    *,
    execution_mode: WireExecutionMode = WireExecutionMode.MUTATION_ONCE,
) -> CompiledWireProgram:
    """Independent mechanism schemas, unrelated to any exact DS artifact."""
    models: dict[str, type[_Params]] = {
        "json": _JsonParams,
        "path_json": _PathJsonParams,
        "json_text": _TextParams,
        "path_json_text": _PathTextParams,
    }
    model = models[channel]
    has_path = channel.startswith("path_")
    bindings: tuple[tuple[str, str], ...] = (("requestBody", "request_body"),)
    if has_path:
        bindings += (("id", "path_variable"),)
    _Params.validation_count = 0
    return compiled_wire_program(
        ds_version="3.4.1",
        source_operation="Mechanism.body",
        source_contract_digest=_DIGEST,
        execution_mode=execution_mode,
        codec=CompiledWireCodec(
            method="PUT" if has_path else "POST",
            path="mechanism/{id}" if has_path else "mechanism",
            channel=channel,
            path_encoding="percent-encoded-utf8-segment-v1" if has_path else None,
            field_bindings=bindings,
            path_fields=("id",) if has_path else (),
            params_model=model,
            response_adapter=TypeAdapter(StrictInt),
            capture_payload="not a valid response",
            request_fingerprint=_DIGEST,
            request_schema_fingerprint=_DIGEST,
            response_fingerprint=_DIGEST,
            response_schema_fingerprint=_DIGEST,
            result_envelope=WireResultEnvelope.REQUIRED,
        ),
    )


@pytest.mark.parametrize("channel", ["json", "path_json"])
def test_structured_body_preserves_native_alias_default_null_and_empty_semantics(
    channel: str,
) -> None:
    program = _program(channel)
    body: JsonObject = {
        "connection": {
            "display_name": "中文 & exact",
            "note": None,
            "labels": [],
            "options": {"empty": "", "null": None},
        },
        "extraNote": None,
    }
    args: JsonObject = {"requestBody": body}
    if channel == "path_json":
        args["id"] = 7
    prepared = program.prepare(args)
    assert _Params.validation_count == 1
    assert prepared.request.json == {
        "connection": {
            "displayName": "中文 & exact",
            "labels": [],
            "options": {"empty": "", "null": None},
        }
    }
    assert prepared.request.content is None
    assert prepared.request.query is prepared.request.form is None
    connection = body["connection"]
    assert isinstance(connection, dict)
    labels = connection["labels"]
    assert isinstance(labels, list)
    labels.append("changed after prepare")
    preview = prepared.request
    assert isinstance(preview.json, dict)
    preview_connection = preview.json["connection"]
    assert isinstance(preview_connection, dict)
    preview_labels = preview_connection["labels"]
    assert isinstance(preview_labels, list)
    preview_labels.append("changed preview")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == ("PUT" if channel == "path_json" else "POST")
        assert request.url.path == (
            "/dolphinscheduler/mechanism/7"
            if channel == "path_json"
            else "/dolphinscheduler/mechanism"
        )
        assert not request.url.params
        assert request.headers["content-type"] == "application/json"
        assert request.headers["token"] == "execution-secret"
        assert request.headers["accept"] == "application/json"
        assert request.content == (
            '{"connection":{"displayName":"中文 & exact","labels":[],'.encode()
            + b'"options":{"empty":"","null":null}}}'
        )
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(api_token="execution-secret"),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )
    assert result.payload == result.raw_payload == 7
    assert len(calls) == 1
    assert _Params.validation_count == 1


def test_structured_body_preserves_explicit_defaults_but_omits_unset_defaults() -> None:
    program = _program("json")
    omitted = program.prepare(
        {"requestBody": {"connection": {"displayName": ""}}}
    ).request
    explicit = program.prepare(
        {
            "requestBody": {
                "connection": {
                    "displayName": "",
                    "attempts": 3,
                    "labels": [],
                    "options": {},
                },
                "enabled": True,
            }
        }
    ).request
    assert omitted.json == {"connection": {"displayName": ""}}
    assert explicit.json == {
        "connection": {
            "displayName": "",
            "attempts": 3,
            "labels": [],
            "options": {},
        },
        "enabled": True,
    }


@pytest.mark.parametrize(
    "channel", ["json", "path_json", "json_text", "path_json_text"]
)
@pytest.mark.parametrize("failure", ["missing", "null", "wrong_type", "extra"])
def test_invalid_required_body_fails_without_http(channel: str, failure: str) -> None:
    program = _program(channel)
    args: JsonObject = {
        "requestBody": "{}"
        if channel.endswith("text")
        else {"connection": {"displayName": "name"}}
    }
    if channel.startswith("path_"):
        args["id"] = 7
    if failure == "missing":
        del args["requestBody"]
    elif failure == "null":
        args["requestBody"] = None
    elif failure == "wrong_type":
        args["requestBody"] = 7
    else:
        args["unreviewed"] = "value"
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    with (
        DolphinSchedulerClient(
            make_profile(), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ValidationError),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare(args)
        )
    assert calls == []


@pytest.mark.parametrize("channel", ["path_json", "path_json_text"])
@pytest.mark.parametrize("item_id", [True, "7", 7.0, None])
def test_body_path_rejects_non_integer_identity_before_http(
    channel: str, item_id: JsonValue
) -> None:
    program = _program(channel)
    with pytest.raises(ValidationError):
        program.prepare(
            {
                "id": item_id,
                "requestBody": "{}"
                if channel.endswith("text")
                else {"connection": {"displayName": "name"}},
            }
        )


@pytest.mark.parametrize(
    "channel", ["json", "path_json", "json_text", "path_json_text"]
)
@pytest.mark.parametrize("change", ["body", "other_channel"])
def test_prepared_body_tampering_fails_before_http(channel: str, change: str) -> None:
    program = _program(channel)
    args: JsonObject = {
        "requestBody": "{}"
        if channel.endswith("text")
        else {"connection": {"displayName": "name"}}
    }
    if channel.startswith("path_"):
        args["id"] = 7
    prepared = program.prepare(args)
    request = prepared.request
    if change == "body":
        request = (
            replace(request, content="changed")
            if channel.endswith("text")
            else replace(request, json={"changed": True})
        )
    else:
        request = replace(request, form={"injected": "value"})
    prepared = replace(prepared, _request=request)
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
    assert _Params.validation_count == 1


@pytest.mark.parametrize(
    "channel", ["json", "path_json", "json_text", "path_json_text"]
)
def test_json_mutation_is_sent_once_even_when_transport_can_retry(channel: str) -> None:
    program = _program(channel)
    args: JsonObject = {
        "requestBody": "{}"
        if channel.endswith("text")
        else {"connection": {"displayName": "name"}}
    }
    if channel.startswith("path_"):
        args["id"] = 7
    prepared = program.prepare(args)
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(503)

    with (
        DolphinSchedulerClient(
            make_profile().model_copy(
                update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
            ),
            transport=httpx.MockTransport(handler),
        ) as client,
        pytest.raises(ApiHttpError),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(calls) == 1
    assert _Params.validation_count == 1


@pytest.mark.parametrize("channel", ["json_text", "path_json_text"])
@pytest.mark.parametrize(
    "body", [' \n { "name" : "中文", "note": null } \t', "", " \t\n", "null"]
)
def test_json_text_sends_original_utf8_without_json_encoding(
    channel: str, body: str
) -> None:
    program = _program(channel)
    args: JsonObject = {"requestBody": body}
    if channel == "path_json_text":
        args["id"] = 7
    prepared = program.prepare(args)
    assert prepared.request.content == body
    assert prepared.request.json is None
    assert prepared.request.query is prepared.request.form is None
    args["requestBody"] = "changed after prepare"
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == ("PUT" if channel == "path_json_text" else "POST")
        assert request.url.path == (
            "/dolphinscheduler/mechanism/7"
            if channel == "path_json_text"
            else "/dolphinscheduler/mechanism"
        )
        assert not request.url.params
        assert request.headers["content-type"] == "application/json"
        assert request.headers["token"] == "execution-secret"
        assert request.content == body.encode("utf-8")
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(api_token="execution-secret"),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )
    assert result.payload == 7
    assert len(calls) == 1
    assert _Params.validation_count == 1


@pytest.mark.parametrize(
    ("channel", "method"),
    [
        ("json", "GET"),
        ("json", "PUT"),
        ("path_json", "POST"),
        ("path_json", "DELETE"),
        ("json_text", "GET"),
        ("json_text", "PUT"),
        ("path_json_text", "POST"),
        ("path_json_text", "PATCH"),
    ],
)
def test_body_channels_reject_unreviewed_method_path_combinations(
    channel: str, method: str
) -> None:
    program = _program(channel)
    with pytest.raises(WireContractError, match="transport shape"):
        replace(program.codec, method=method)


@pytest.mark.parametrize(
    "channel", ["json", "path_json", "json_text", "path_json_text"]
)
@pytest.mark.parametrize("binding", ["request_param", "path_variable"])
def test_body_channel_requires_an_explicit_request_body_binding(
    channel: str, binding: str
) -> None:
    program = _program(channel)
    bindings = tuple(
        (name, binding if name == "requestBody" else original)
        for name, original in program.codec.field_bindings
    )
    with pytest.raises(WireContractError):
        replace(program.codec, field_bindings=bindings)


@pytest.mark.parametrize("binding", ["request_body", "request_param"])
def test_body_channel_rejects_an_additional_bound_field(binding: str) -> None:
    program = _program("json")
    with pytest.raises(WireContractError):
        replace(
            program.codec,
            params_model=_MixedBodyParams,
            field_bindings=(("requestBody", "request_body"), ("second", binding)),
        )


@pytest.mark.parametrize(
    ("channel", "model"),
    [
        ("json", _OptionalJsonParams),
        ("json", _DefaultJsonParams),
        ("json_text", _OptionalTextParams),
        ("json_text", _DefaultTextParams),
        ("json", _TextParams),
        ("json_text", _JsonParams),
    ],
)
def test_body_codec_requires_a_required_nonnullable_field_of_its_declared_kind(
    channel: str, model: type[_Params]
) -> None:
    program = _program(channel)
    with pytest.raises(WireContractError):
        replace(program.codec, params_model=model)


@pytest.mark.parametrize("channel", ["path_json", "path_json_text"])
def test_body_paths_do_not_admit_a_second_path_segment(channel: str) -> None:
    program = _program(channel)
    with pytest.raises(WireContractError):
        replace(program.codec, path="mechanism/{id}/children/{id}")


@pytest.mark.parametrize("channel", ["json", "json_text"])
def test_body_execution_keeps_explicit_read_retry_policy(channel: str) -> None:
    program = _program(channel, execution_mode=WireExecutionMode.READ_RETRY_SAFE)
    prepared = program.prepare(
        {
            "requestBody": "{}"
            if channel == "json_text"
            else {"connection": {"displayName": "name"}}
        }
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile().model_copy(
            update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
        ),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )
    assert result.payload == 7
    assert len(calls) == 2
    assert calls[0].content == calls[1].content
    assert _Params.validation_count == 1


def test_an_explicit_empty_dto_is_sent_as_an_object_not_an_absent_body() -> None:
    program = _program("json")
    program = replace(
        program, codec=replace(program.codec, params_model=_EmptyJsonParams)
    )
    prepared = program.prepare({"requestBody": {"note": None}})
    assert prepared.request.json == {}
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["content-type"] == "application/json"
        assert request.content == b"{}"
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, prepared
        )
    assert result.payload == 7
    assert len(calls) == 1
