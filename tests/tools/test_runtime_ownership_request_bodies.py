from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
)
from live_gate import runtime_ownership

if TYPE_CHECKING:
    from ds_codegen.ir import OperationSpec

_REQUESTS = (
    CompiledRequestEpoch(
        method="POST",
        path="datasources",
        channel="json",
        request_schema="create_json",
        request_model="CreateJsonParams",
        request_fields=("dataSourceParam",),
    ),
    CompiledRequestEpoch(
        method="PUT",
        path="datasources/{id}",
        channel="path_json",
        request_schema="update_json",
        request_model="UpdateJsonParams",
        request_fields=("id", "dataSourceParam"),
        path_fields=("id",),
    ),
    CompiledRequestEpoch(
        method="POST",
        path="datasources",
        channel="json_text",
        request_schema="create_json_text",
        request_model="CreateJsonTextParams",
        request_fields=("jsonStr",),
    ),
    CompiledRequestEpoch(
        method="PUT",
        path="datasources/{id}",
        channel="path_json_text",
        request_schema="update_json_text",
        request_model="UpdateJsonTextParams",
        request_fields=("id", "jsonStr"),
        path_fields=("id",),
    ),
)


@pytest.mark.parametrize("epoch", _REQUESTS, ids=lambda epoch: epoch.channel)
def test_static_ownership_accepts_reviewed_body_request_bindings(
    epoch: CompiledRequestEpoch,
) -> None:
    definition, program, codecs = _probe(epoch)

    assert runtime_ownership._program(
        definition, definition.primitives[0], program, codecs, label="body probe"
    ) == (program["source_operation"], "write")


@pytest.mark.parametrize("epoch", _REQUESTS, ids=lambda epoch: epoch.channel)
@pytest.mark.parametrize("drift", ["source", "method", "body_binding", "channel"])
def test_static_ownership_rejects_body_request_tampering_after_digest_rebinding(
    epoch: CompiledRequestEpoch, drift: str
) -> None:
    definition, program, codecs = _probe(epoch)
    codec = codecs["write"]
    message = "request epoch is not uniquely reviewed"
    if drift == "source":
        program["source_operation"] = "DataSourceController.queryDataSource"
        message = "source primitive"
    elif drift == "method":
        codec["method"] = "GET"
    elif drift == "body_binding":
        codec["fields"] = [
            {"name": "id", "binding": "path_variable"} for _ in epoch.path_fields
        ] + [{"name": epoch.request_fields[-1], "binding": "request_param"}]
    else:
        codec["channel"] = "path_form" if epoch.path_fields else "form"
    _bind_program_digest(program, codec)

    with pytest.raises(ValueError, match=message):
        runtime_ownership._program(
            definition, definition.primitives[0], program, codecs, label="body probe"
        )


@pytest.mark.parametrize("epoch", [_REQUESTS[1], _REQUESTS[3]])
def test_static_ownership_rejects_path_field_rebound_as_body(
    epoch: CompiledRequestEpoch,
) -> None:
    definition, program, codecs = _probe(epoch)
    codecs["write"]["fields"] = [
        {"name": name, "binding": "request_body"} for name in epoch.request_fields
    ]
    _bind_program_digest(program, codecs["write"])

    with pytest.raises(ValueError, match="request epoch is not uniquely reviewed"):
        runtime_ownership._program(
            definition, definition.primitives[0], program, codecs, label="body probe"
        )


def _probe(
    request: CompiledRequestEpoch,
) -> tuple[CompiledDomainDefinition, dict[str, object], dict[str, dict[str, object]]]:
    source_method = (
        "createDataSource" if request.method == "POST" else "updateDataSource"
    )
    source = f"DataSourceController.{source_method}"

    def classify(operation: OperationSpec) -> str | None:
        coordinate = (
            operation.operation_id,
            operation.controller,
            operation.method_name,
            operation.http_method,
            operation.path,
        )
        return (
            "write"
            if coordinate
            == (
                source,
                "DataSourceController",
                source_method,
                request.method,
                request.path,
            )
            else None
        )

    definition = CompiledDomainDefinition(
        name="body_probe",
        schema_constant="COMPILED_BODY_PROBE_SCHEMA_VERSION",
        schema_version=1,
        semantic_operations=frozenset({"datasource.create", "datasource.update"}),
        absent_versions=frozenset(),
        primitives=(CompiledPrimitive("write", (request,), "required"),),
        classify_operation=classify,
        response_policy=lambda *_: CompiledResponsePolicy("write", None, None),
        recipe_policy=lambda _: "body_probe",
    )
    # Static request probes exercise the ownership guard without building an
    # artifact or asserting a new datasource ownership claim.
    schema_digest = "sha256:" + "a" * 64
    codec: dict[str, object] = {
        "method": request.method,
        "path": request.path,
        "channel": request.channel,
        "path_encoding": "percent-encoded-utf8-segment-v1"
        if request.path_fields
        else None,
        "fields": [
            {"name": "id", "binding": "path_variable"} for _ in request.path_fields
        ]
        + [{"name": request.request_fields[-1], "binding": "request_body"}],
        "path_fields": list(request.path_fields),
        "params": request.request_schema,
        "request_schema_digest": schema_digest,
        "response": None,
        "response_schema_digest": None,
        "response_projection": "direct",
        "capture": None,
    }
    program: dict[str, object] = {
        "source_operation": source,
        "codec": "write",
        "codec_digest": schema_digest,
        "request_schema_digest": schema_digest,
        "response_digest": schema_digest,
        "response_schema_digest": None,
        "result_envelope": "required",
    }
    _bind_program_digest(program, codec)
    return definition, program, {"write": codec}


def _bind_program_digest(program: dict[str, object], codec: dict[str, object]) -> None:
    payload = {
        "schema_version": 4,
        "codec_record": codec,
        **{key: value for key, value in program.items() if key != "program_digest"},
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    program["program_digest"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
