from __future__ import annotations

from dataclasses import replace
from enum import Enum, StrEnum
from typing import TYPE_CHECKING

import httpx
import pytest
from pydantic import Field, ValidationError

from dsctl.client import DolphinSchedulerClient
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel
from dsctl.upstream.wire import (
    CompiledWireCodec,
    CompiledWireProgram,
    WireContractError,
    WireExecutionMode,
    WireExecutor,
    WireRequest,
    WireResultEnvelope,
    compiled_wire_program,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.support.json_types import JsonObject

_DIGEST = "sha256:" + "1" * 64


class _NodeType(StrEnum):
    MASTER = "MASTER"
    CUSTOM = "custom-wire-value"
    SLASH = "a/b"
    SPACE = "a b"
    UNICODE = "中文"
    PERCENT = "50%"


class _UnknownType(StrEnum):
    UNKNOWN = "UNKNOWN"


class _LegacyStringEnum(str, Enum):  # noqa: UP042 - prove this is not a StrEnum.
    MASTER = "MASTER"


class _EnumPathParams(BaseParamsModel):
    node_type: _NodeType = Field(alias="nodeType")


class _EnumFormParams(_EnumPathParams):
    label: str


class _TwoEnumPathParams(_EnumPathParams):
    other: _NodeType


class _StringPathParams(BaseParamsModel):
    node_type: str = Field(alias="nodeType")


class _LegacyEnumPathParams(BaseParamsModel):
    node_type: _LegacyStringEnum = Field(alias="nodeType")


class _BooleanPathParams(BaseParamsModel):
    node_type: bool = Field(alias="nodeType")


class _FloatPathParams(BaseParamsModel):
    node_type: float = Field(alias="nodeType")


class _IntegerPathParams(BaseParamsModel):
    node_type: int = Field(alias="nodeType", strict=True)


class _IntegerFormParams(_IntegerPathParams):
    label: str


def _program(
    params_model: type[BaseParamsModel] = _EnumPathParams,
    *,
    method: str = "GET",
) -> CompiledWireProgram:
    fields: tuple[tuple[str, str], ...] = (("nodeType", "path_variable"),)
    if method == "PUT":
        fields += (("label", "request_param"),)
    codec = CompiledWireCodec(
        method=method,
        path="monitors/{nodeType}",
        channel="path_form" if method == "PUT" else "path",
        path_encoding="percent-encoded-utf8-segment-v1",
        field_bindings=fields,
        path_fields=("nodeType",),
        params_model=params_model,
        response_adapter=None,
        capture_payload=None,
        request_fingerprint=_DIGEST,
        request_schema_fingerprint=_DIGEST,
        response_fingerprint=_DIGEST,
        response_schema_fingerprint=None,
        result_envelope=WireResultEnvelope.REQUIRED,
    )
    return compiled_wire_program(
        ds_version="3.4.1",
        source_operation="PathMechanism.exchange",
        source_contract_digest=_DIGEST,
        execution_mode=(
            WireExecutionMode.READ_RETRY_SAFE
            if method == "GET"
            else WireExecutionMode.MUTATION_ONCE
        ),
        codec=codec,
    )


@pytest.fixture
def client_and_requests() -> Iterator[
    tuple[DolphinSchedulerClient, list[httpx.Request]]
]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": None})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        yield client, requests


@pytest.mark.parametrize(
    ("value", "segment"),
    [
        (_NodeType.MASTER, "MASTER"),
        (_NodeType.CUSTOM, "custom-wire-value"),
        (_NodeType.SLASH, "a%2Fb"),
        (_NodeType.SPACE, "a%20b"),
        (_NodeType.UNICODE, "%E4%B8%AD%E6%96%87"),
        (_NodeType.PERCENT, "50%25"),
        ("custom-wire-value", "custom-wire-value"),
    ],
)
def test_get_enum_path_sends_the_validated_wire_value_as_one_utf8_segment(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    value: str,
    segment: str,
) -> None:
    client, requests = client_and_requests
    program = _program()
    args: JsonObject = {"nodeType": value}
    prepared = program.prepare(args)
    assert prepared.request == WireRequest(
        method="GET",
        path=f"/monitors/{segment}",
        query=None,
        form=None,
        json=None,
        content=None,
    )
    args["nodeType"] = "changed after prepare"

    result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
        program, prepared
    )

    assert result.request == prepared.request
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert requests[0].url.raw_path == f"/dolphinscheduler/monitors/{segment}".encode()
    assert requests[0].content == b""


@pytest.mark.parametrize(
    "args",
    [
        {},
        {"nodeType": "UNKNOWN"},
        {"nodeType": _UnknownType.UNKNOWN},
        {"nodeType": 7},
        {"nodeType": True},
        {"nodeType": 1.0},
        {"nodeType": None},
        {"nodeType": []},
        {"nodeType": {}},
        {"nodeType": "MASTER", "unreviewed": "value"},
    ],
)
def test_enum_path_rejects_unknown_or_malformed_arguments_before_http(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    args: JsonObject,
) -> None:
    client, requests = client_and_requests
    program = _program()
    with pytest.raises(ValidationError):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare(args)
        )
    assert requests == []


@pytest.mark.parametrize(
    ("params_model", "value"),
    [
        (_StringPathParams, "MASTER"),
        (_StringPathParams, "7"),
        (_StringPathParams, _NodeType.MASTER),
        (_LegacyEnumPathParams, _LegacyStringEnum.MASTER),
        (_BooleanPathParams, True),
        (_FloatPathParams, 7.0),
    ],
)
def test_get_path_rejects_other_validated_scalar_types(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    params_model: type[BaseParamsModel],
    *,
    value: str | bool | float,
) -> None:
    client, requests = client_and_requests
    program = _program(params_model)
    with pytest.raises(WireContractError, match="path value"):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"nodeType": value})
        )
    assert requests == []


@pytest.mark.parametrize(
    ("method", "params_model", "args"),
    [
        ("DELETE", _EnumPathParams, {"nodeType": _NodeType.MASTER}),
        ("PUT", _EnumFormParams, {"nodeType": _NodeType.MASTER, "label": "value"}),
    ],
)
def test_mutation_paths_remain_integer_only(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    method: str,
    params_model: type[BaseParamsModel],
    args: JsonObject,
) -> None:
    client, requests = client_and_requests
    program = _program(params_model, method=method)
    with pytest.raises(WireContractError, match="integer segment"):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare(args)
        )
    assert requests == []


def test_enum_path_does_not_expand_to_a_mixed_query() -> None:
    program = _program(_EnumFormParams, method="PUT")
    codec = replace(program.codec, method="GET", channel="path_query")
    with pytest.raises(WireContractError, match="integer segment"):
        codec.encode({"nodeType": "MASTER", "label": "value"})


def test_enum_path_does_not_expand_to_two_segments() -> None:
    program = _program()
    codec = replace(
        program.codec,
        path="monitors/{nodeType}/{other}",
        params_model=_TwoEnumPathParams,
        field_bindings=(("nodeType", "path_variable"), ("other", "path_variable")),
        path_fields=("nodeType", "other"),
    )
    with pytest.raises(WireContractError, match="integer segment"):
        codec.encode({"nodeType": "MASTER", "other": "MASTER"})


@pytest.mark.parametrize(
    ("method", "params_model", "args", "segment", "body"),
    [
        ("GET", _IntegerPathParams, {"nodeType": 0}, "0", b""),
        ("DELETE", _IntegerPathParams, {"nodeType": -1}, "-1", b""),
        (
            "PUT",
            _IntegerFormParams,
            {"nodeType": 7, "label": "value"},
            "7",
            b"label=value",
        ),
    ],
)
def test_integer_paths_keep_existing_rendering_and_form_separation(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    method: str,
    params_model: type[BaseParamsModel],
    args: JsonObject,
    segment: str,
    body: bytes,
) -> None:
    client, requests = client_and_requests
    program = _program(params_model, method=method)
    prepared = program.prepare(args)
    WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(requests) == 1
    assert requests[0].method == method
    assert requests[0].url.raw_path == f"/dolphinscheduler/monitors/{segment}".encode()
    assert requests[0].content == body


@pytest.mark.parametrize("value", [True, 7.0, "7"])
def test_integer_path_parameters_remain_strict(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
    *,
    value: str | bool | float,
) -> None:
    client, requests = client_and_requests
    program = _program(_IntegerPathParams)
    with pytest.raises(ValidationError):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"nodeType": value})
        )
    assert requests == []


def test_prepared_enum_path_cannot_be_replaced_with_a_decoded_path(
    client_and_requests: tuple[DolphinSchedulerClient, list[httpx.Request]],
) -> None:
    client, requests = client_and_requests
    program = _program()
    prepared = program.prepare({"nodeType": _NodeType.SLASH})
    assert prepared.request.path == "/monitors/a%2Fb"
    tampered = replace(
        prepared, _request=replace(prepared.request, path="/monitors/a/b")
    )
    with pytest.raises(
        WireContractError, match="no longer matches its encoded content"
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, tampered)
    assert requests == []
