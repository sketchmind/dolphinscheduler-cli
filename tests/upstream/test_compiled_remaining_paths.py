"""Closed path extensions, including the observable old project-name URL wire."""

from __future__ import annotations

from dataclasses import replace
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
    WireResultEnvelope,
    compiled_wire_program,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

_DIGEST = "sha256:" + "1" * 64
_LEGACY = "legacy-url-interpolation-v1"
_SEGMENT = "percent-encoded-utf8-segment-v1"


class _TwoPaths(BaseParamsModel):
    code: int = Field(strict=True)
    project_code: int = Field(alias="projectCode", strict=True)


class _TwoPathsValues(_TwoPaths):
    label: str


class _Name(BaseParamsModel):
    project_name: str = Field(alias="projectName")


class _NameValues(_Name):
    label: str


class _Values(BaseParamsModel):
    label: str


def _codec(method: str, channel: str, *, legacy: bool = False) -> CompiledWireCodec:
    has_path = channel != "query"
    mixed = channel in {"path_query", "path_form"}
    path_fields: tuple[str, ...] = (
        ("projectName",) if legacy else ("code", "projectCode")
    )
    params_model: type[BaseParamsModel] = (
        (_NameValues if mixed else _Name)
        if legacy
        else (_TwoPathsValues if mixed else _TwoPaths)
    )
    if not has_path:
        params_model = _Values
        path_fields = ()
    return CompiledWireCodec(
        method=method,
        path=(
            "projects/{projectName}/items"
            if legacy
            else "projects/{projectCode}/items/{code}"
        )
        if has_path
        else "resources",
        channel=channel,
        path_encoding=(_LEGACY if legacy else _SEGMENT) if has_path else None,
        field_bindings=tuple((name, "path_variable") for name in path_fields)
        + ((("label", "request_param"),) if mixed or not has_path else ()),
        path_fields=path_fields,
        params_model=params_model,
        response_adapter=None,
        capture_payload=None,
        request_fingerprint=_DIGEST,
        request_schema_fingerprint=_DIGEST,
        response_fingerprint=_DIGEST,
        response_schema_fingerprint=None,
        result_envelope=WireResultEnvelope.REQUIRED,
    )


def _program(
    codec: CompiledWireCodec, *, version: str = "3.4.1"
) -> CompiledWireProgram:
    return compiled_wire_program(
        ds_version=version,
        source_operation="Mechanism.path",
        source_contract_digest=_DIGEST,
        execution_mode=WireExecutionMode.MUTATION_ONCE,
        codec=codec,
    )


@pytest.mark.parametrize(
    ("method", "channel"),
    [
        ("DELETE", "path"),
        ("POST", "path"),
        ("POST", "path_form"),
        ("GET", "path_query"),
        ("DELETE", "query"),
    ],
)
def test_added_integer_and_query_shapes_keep_path_values_out_of_payload(
    method: str,
    channel: str,
) -> None:
    program = _program(_codec(method, channel))
    args: JsonObject = (
        {} if channel == "query" else {"projectCode": 9000000000, "code": 8}
    )
    if channel != "path":
        args["label"] = "value"
    prepared = program.prepare(args)
    assert prepared.request.path == (
        "/resources" if channel == "query" else "/projects/9000000000/items/8"
    )
    assert prepared.request.query == (
        {"label": "value"} if channel in {"query", "path_query"} else None
    )
    assert prepared.request.form == (
        {"label": "value"} if channel == "path_form" else None
    )
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"code": 0, "data": None})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handle)
    ) as client:
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(captured) == 1
    assert captured[0].method == method
    assert captured[0].content == (b"label=value" if channel == "path_form" else b"")
    assert "projectCode" not in captured[0].url.params
    assert "code" not in captured[0].url.params


@pytest.mark.parametrize(
    ("name", "raw_prefix", "embedded_query", "fragment"),
    [
        ("plain", "/projects/plain", "", ""),
        ("a/b", "/projects/a/b", "", ""),
        ("a?x=1", "/projects/a", "x=1", ""),
        ("a#tail", "/projects/a", "", "tail"),
        ("a%2Fb", "/projects/a%2Fb", "", ""),
        ("雪 空", "/projects/%E9%9B%AA%20%E7%A9%BA", "", ""),
    ],
)
@pytest.mark.parametrize(
    ("method", "channel"),
    [("GET", "path"), ("GET", "path_query"), ("POST", "path_form")],
)
def test_legacy_interpolation_matches_frozen_old_http_behavior(
    name: str,
    raw_prefix: str,
    embedded_query: str,
    fragment: str,
    method: str,
    channel: str,
) -> None:
    # Expected URL parsing was frozen against the original generated wrappers,
    # not computed by the compiled encoder or by quoting its output.
    program = _program(_codec(method, channel, legacy=True), version="1.3.9")
    args: JsonObject = {"projectName": name}
    if channel != "path":
        args["label"] = "value"
    prepared = program.prepare(args)
    assert prepared.request.path == f"/projects/{name}/items"
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"code": 0, "data": None})

    with DolphinSchedulerClient(
        make_profile(ds_version="1.3.9"), transport=httpx.MockTransport(handle)
    ) as client:
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, prepared)
    assert len(captured) == 1
    url = captured[0].url
    suffix = "" if fragment or embedded_query else "/items"
    # Explicit request params replace an embedded URL query, as in HTTPX's old
    # wrapper path; they never turn it back into a path segment.
    query = (
        "label=value"
        if channel == "path_query"
        else (embedded_query + "/items" if embedded_query else "")
    )
    assert url.raw_path.decode() == "/dolphinscheduler" + raw_prefix + suffix + (
        "?" + query if query else ""
    )
    assert url.fragment == (fragment + "/items" if fragment else "")
    assert captured[0].content == (b"label=value" if channel == "path_form" else b"")


def test_legacy_path_encoding_and_version_are_explicit() -> None:
    codec = _codec("GET", "path", legacy=True)
    with pytest.raises(WireContractError, match=r"exact DS 1\.3\.9"):
        _program(codec)
    with pytest.raises(WireContractError, match="integer segment"):
        replace(codec, path_encoding=_SEGMENT).encode({"projectName": "a/b"})
    with pytest.raises(WireContractError, match="path encoding"):
        replace(codec, path_encoding="guess-from-string")
    with pytest.raises(WireContractError, match="legacy project-name"):
        replace(codec, method="DELETE")
    with pytest.raises(ValidationError):
        codec.encode({"projectName": 7})
    with pytest.raises(ValidationError):
        codec.encode({})


@pytest.mark.parametrize(
    ("method", "channel"),
    [
        ("POST", "path"),
        ("DELETE", "path"),
        ("GET", "path_query"),
        ("POST", "path_form"),
    ],
)
@pytest.mark.parametrize("value", [True, "7", 7.0])
def test_new_path_shapes_still_require_strict_integers(
    method: str, channel: str, *, value: bool | str | float
) -> None:
    codec = _codec(method, channel)
    with pytest.raises(ValidationError):
        codec.encode(
            {
                "projectCode": value,
                "code": 8,
                **({"label": "value"} if channel != "path" else {}),
            }
        )


def test_post_path_rejects_single_segment_and_empty_delete_query() -> None:
    codec = _codec("POST", "path")
    with pytest.raises(WireContractError, match="path-variable inventory"):
        replace(codec, path="items/{code}")
    with pytest.raises(WireContractError, match="field-binding inventory"):
        replace(
            _codec("DELETE", "query"), params_model=BaseParamsModel, field_bindings=()
        )


def test_legacy_prepared_url_is_content_bound() -> None:
    program = _program(_codec("GET", "path", legacy=True), version="1.3.9")
    prepared = program.prepare({"projectName": "a?x=1"})
    tampered = replace(
        prepared, _request=replace(prepared.request, path="/projects/other/items")
    )
    captured: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"code": 0, "data": None})

    with (
        DolphinSchedulerClient(
            make_profile(ds_version="1.3.9"), transport=httpx.MockTransport(handle)
        ) as client,
        pytest.raises(WireContractError, match="content"),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(program, tampered)
    assert captured == []
