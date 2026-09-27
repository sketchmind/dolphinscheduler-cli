"""Exercise the same prepared-call interface used by mutation dispatchers."""

from __future__ import annotations

import httpx
import pytest
from pydantic import ValidationError

from dsctl.client import DolphinSchedulerClient
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    CompiledDomainPrograms,
)
from dsctl.upstream.wire import CompiledWireProfile
from tests.support import make_profile
from tests.upstream.test_compiled_wire_execution import _DIGEST, _PathParams, _program


@pytest.mark.parametrize("prepared_call", [False, True])
def test_bound_calls_validate_once_and_prepared_calls_retain_transport_evidence(
    *, prepared_call: bool
) -> None:
    program, decoded = _program("POST", "form")
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
    domain = CompiledDomainPrograms[str](
        name="probe",
        schema_constant="PROBE_SCHEMA_VERSION",
        schema_version=1,
        expectations={"exchange": MUTATION_ONCE_REQUIRED},
    )
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"code": 0, "data": 7})

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    ) as client:
        bound = domain.bind(profile, client.profile, http_client=client)
        with pytest.raises(ValidationError):
            bound.prepare("exchange", {"code": "7"})
        assert not calls
        assert _PathParams.validation_count == 0
        if prepared_call:
            prepared = bound.prepare("exchange", {"code": 7})
            assert not calls
            assert _PathParams.validation_count == 1
            result = bound.execute("exchange", prepared)
            assert result.payload == 7
            assert result.raw_payload == 7
            assert result.request == prepared.request
        else:
            assert bound.call("exchange", {"code": 7}) == 7
    assert _PathParams.validation_count == 1
    assert decoded == [7]
    assert len(calls) == 1
