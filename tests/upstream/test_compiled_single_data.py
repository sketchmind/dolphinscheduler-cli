"""The compiled single-field projection retains legacy response semantics."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import httpx
import pytest
from pydantic import TypeAdapter

from dsctl.client import DolphinSchedulerClient
from dsctl.upstream.wire import WireExecutor, WireResponseDecodeError
from tests.support import make_profile
from tests.upstream.test_compiled_wire_execution import _DIGEST, _program

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (7, 7),
        ([7], [7]),
        (None, None),
        ({"data": 7}, 7),
        ({"data": None, "dataList": 8}, None),
        ({"data": 0}, 0),
        ({"data": False}, False),
        ({"data": []}, []),
        ({"data": {"data": 7}}, {"data": 7}),
        ({"status": "FAILURE", "data": 7}, 7),
    ],
)
def test_single_data_projects_once_and_retains_raw_payload(
    payload: JsonValue, expected: JsonValue
) -> None:
    program, _ = _program(response_projection="single_data")
    program = replace(
        program, codec=replace(program.codec, response_adapter=TypeAdapter(object))
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "data": payload})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        result = WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert result.payload == expected
    assert result.raw_payload == payload
    assert len(calls) == 1


@pytest.mark.parametrize("payload", [{}, {"dataList": 7}, {"status": "SUCCESS"}])
@pytest.mark.parametrize("void", [False, True])
def test_missing_single_data_is_a_received_response_error_even_for_void(
    payload: JsonValue, *, void: bool
) -> None:
    program, decoded = _program(response_projection="single_data")
    if void:
        program = replace(
            program,
            codec=replace(
                program.codec, response_adapter=None, response_schema_fingerprint=None
            ),
        )
    with (
        DolphinSchedulerClient(
            make_profile(),
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"code": 0, "data": payload})
            ),
        ) as client,
        pytest.raises(
            WireResponseDecodeError,
            match="DolphinScheduler response payload did not match",
        ) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert caught.value.details["wire_response_received"] is True
    assert decoded == []


@pytest.mark.parametrize(
    "payload", [{"data": "7"}, {"data": None}, {"data": {"data": 7}}]
)
def test_single_data_still_validates_exact_response(payload: JsonValue) -> None:
    program, decoded = _program(response_projection="single_data")
    with (
        DolphinSchedulerClient(
            make_profile(),
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"code": 0, "data": payload})
            ),
        ) as client,
        pytest.raises(WireResponseDecodeError) as caught,
    ):
        WireExecutor(client, source_contract_digest=_DIGEST).execute(
            program, program.prepare({"code": 7})
        )
    assert caught.value.details["validation_error_count"] == 1
    assert caught.value.details["wire_response_received"] is True
    assert decoded == []
