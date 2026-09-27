"""File transfer mechanisms: separate live handles, exact policy, and raw bytes."""

from __future__ import annotations

from dataclasses import replace
from io import BytesIO
from typing import TYPE_CHECKING, TypedDict

import httpx
import pytest
from pydantic import ValidationError

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiHttpError, ApiResultError
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    CompiledDomainPrograms,
)
from dsctl.upstream.wire import (
    CompiledWireProfile,
    WireContractError,
    WireExecutionMode,
    WireExecutor,
    WireResultEnvelope,
    compiled_wire_program,
)
from tests.support import make_profile
from tests.upstream.test_compiled_wire_execution import _DIGEST, _PathParams, _program

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.wire import CompiledWireProgram


class _CodecChanges(TypedDict, total=False):
    file_fields: tuple[str, ...]
    channel: str
    method: str
    result_envelope: WireResultEnvelope
    response_transport: str
    response_projection: str


def _transfer(kind: str) -> CompiledWireProgram:
    original, _ = _program("POST", "form")
    upload = kind == "upload"
    codec = replace(
        original.codec,
        method="POST" if upload else "GET",
        channel="multipart" if upload else "query",
        file_fields=("file",) if upload else (),
        field_bindings=original.codec.field_bindings
        + ((("file", "request_param"),) if upload else ()),
        response_transport="json" if upload else "binary",
        response_adapter=None,
        response_schema_fingerprint=None,
        result_envelope=WireResultEnvelope.REQUIRED
        if upload
        else WireResultEnvelope.OPTIONAL,
    )
    return compiled_wire_program(
        ds_version="3.4.1",
        source_operation=f"Probe.{kind}",
        source_contract_digest=_DIGEST,
        execution_mode=WireExecutionMode.MUTATION_ONCE
        if upload
        else WireExecutionMode.READ_RETRY_SAFE,
        codec=codec,
    )


def test_upload_keeps_file_outside_prepared_json_and_ignores_result_data() -> None:
    program = _transfer("upload")
    file = BytesIO(b"\x00raw\xff data")
    args: JsonObject = {"code": 7, "label": "a & b"}
    prepared = program.prepare(args)
    assert _PathParams.validation_count == 1
    args["code"] = 99
    assert prepared.request.form == {"code": 7, "label": "a & b"}
    assert file.tell() == 0
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/mechanism"
        assert request.headers["token"] == "transfer-secret"
        assert request.headers["content-type"].startswith("multipart/form-data;")
        assert b'name="code"\r\n\r\n7\r\n' in request.content
        assert b'name="label"\r\n\r\na & b\r\n' in request.content
        assert b'name="file"; filename="native.bin"' in request.content
        assert b"\x00raw\xff data" in request.content
        return httpx.Response(200, json={"code": 0, "data": ["ignored", 7]})

    with DolphinSchedulerClient(
        make_profile(api_token="transfer-secret"),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).upload(
            program, prepared, files={"file": ("native.bin", file)}
        )
    assert result.payload is None
    assert result.raw_payload == ["ignored", 7]
    assert result.request == prepared.request
    assert len(calls) == 1
    assert not file.closed
    assert _PathParams.validation_count == 1


@pytest.mark.parametrize("status", [200, 503])
def test_upload_requires_result_envelope_and_never_retries(status: int) -> None:
    program = _transfer("upload")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json={"notAnEnvelope": True})

    with (
        DolphinSchedulerClient(
            make_profile().model_copy(
                update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
            ),
            transport=httpx.MockTransport(handler),
        ) as client,
        pytest.raises(ApiHttpError if status == 503 else ApiResultError),
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).upload(
            program, program.prepare({"code": 7}), files={"file": b"x"}
        )
    assert len(calls) == 1


@pytest.mark.parametrize("retry", [False, True])
def test_download_preserves_binary_and_headers_with_reviewed_retry(
    *, retry: bool
) -> None:
    program = _transfer("download")
    if not retry:
        program = replace(program, execution_mode=WireExecutionMode.READ_ONCE)
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "GET"
        assert dict(request.url.params) == {"code": "7", "label": "a & b"}
        assert request.content == b""
        if retry and len(calls) == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            content=b"\xff\x00raw\r\n",
            headers={
                "content-type": "application/octet-stream",
                "x-native": "preserved",
            },
        )

    with DolphinSchedulerClient(
        make_profile().model_copy(
            update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
        ),
        transport=httpx.MockTransport(handler),
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).download(
            program, program.prepare({"code": 7, "label": "a & b"})
        )
    assert result.content == b"\xff\x00raw\r\n"
    assert result.content_type == "application/octet-stream"
    assert result.headers["x-native"] == "preserved"
    assert len(calls) == (2 if retry else 1)


@pytest.mark.parametrize("kind", ["upload", "download"])
def test_transfer_rejects_json_executor_and_invalid_args_before_io(kind: str) -> None:
    program = _transfer(kind)
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200)

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        executor = WireExecutor(client, source_contract_digest=_DIGEST)
        with pytest.raises(ValidationError):
            program.prepare({"code": "7"})
        with pytest.raises(ValidationError):
            program.prepare({"code": 7, "file": "not a JSON field"})
        with pytest.raises(WireContractError, match="dedicated executor"):
            executor.execute(program, program.prepare({"code": 7}))
        if kind == "upload":
            for files in ({}, {"wrong": b"x"}, {"file": b"x", "extra": b"y"}):
                with pytest.raises(WireContractError, match="file inventory"):
                    executor.upload(program, program.prepare({"code": 7}), files=files)
        else:
            with pytest.raises(WireContractError, match="file inventory"):
                executor.upload(
                    program, program.prepare({"code": 7}), files={"file": b"x"}
                )
    assert not calls


@pytest.mark.parametrize("kind", ["upload", "download"])
def test_bound_transfer_requires_trusted_transport_policy(kind: str) -> None:
    program = _transfer(kind)
    profile = CompiledWireProfile(
        ds_version="3.4.1",
        status="supported",
        recipe_id="probe",
        source_commit="probe",
        source_tree="probe",
        source_contract_digest=_DIGEST,
        codec_names={"exchange": "probe"},
        programs={"exchange": program},
    )
    expectation = MUTATION_ONCE_REQUIRED if kind == "upload" else READ_RETRY_OPTIONAL
    domain = CompiledDomainPrograms[str](
        name="probe",
        schema_constant="PROBE",
        schema_version=1,
        expectations={"exchange": expectation},
    )
    with DolphinSchedulerClient(
        make_profile(),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"code": 0})),
    ) as client:
        with pytest.raises(WireContractError, match="reviewed policy"):
            domain.bind(profile, client.profile, http_client=client)
        reviewed = replace(
            expectation,
            file_fields=("file",) if kind == "upload" else (),
            response_transport="json" if kind == "upload" else "binary",
        )
        bound = replace(domain, expectations={"exchange": reviewed}).bind(
            profile, client.profile, http_client=client
        )
        if kind == "upload":
            assert (
                bound.upload("exchange", {"code": 7}, files={"file": b"x"}).payload
                is None
            )
        else:
            assert bound.download("exchange", {"code": 7}).content == b'{"code":0}'


@pytest.mark.parametrize(
    ("kind", "change"),
    [
        ("upload", {"file_fields": ()}),
        ("upload", {"file_fields": ("other",)}),
        ("upload", {"channel": "form"}),
        ("upload", {"method": "PUT"}),
        ("upload", {"result_envelope": WireResultEnvelope.OPTIONAL}),
        ("download", {"method": "POST"}),
        ("download", {"response_transport": "text"}),
        ("download", {"response_projection": "single_data"}),
        ("download", {"result_envelope": WireResultEnvelope.REQUIRED}),
    ],
)
def test_transfer_codec_rejects_unreviewed_shapes(
    kind: str, change: _CodecChanges
) -> None:
    program = _transfer(kind)
    with pytest.raises(WireContractError):
        replace(program.codec, **change)
