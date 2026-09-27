"""A candidate artifact cannot choose its own legacy URL interpolation policy."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
)
from ds_codegen.contract_inputs import canonical_json_digest
from live_gate.runtime_ownership import _program

if TYPE_CHECKING:
    from ds_codegen.ir import OperationSpec
    from dsctl.support.json_types import JsonObject

_SOURCE = "ProcessDefinitionController.queryProcessDefinitionList"
_PATH = "projects/{projectName}/process/list"


@pytest.mark.parametrize("drift", [None, "encoding", "binding", "method", "source"])
def test_static_ownership_binds_the_reviewed_path_encoding(drift: str | None) -> None:
    epoch = CompiledRequestEpoch(
        method="GET",
        path=_PATH,
        channel="path",
        request_schema="legacy",
        request_model="LegacyParams",
        request_fields=("projectName",),
        path_fields=("projectName",),
        versions=frozenset({"1.3.9"}),
        path_encoding="legacy-url-interpolation-v1",
    )

    def classify(operation: OperationSpec) -> str | None:
        return (
            "list"
            if (operation.operation_id, operation.http_method, operation.path)
            == (_SOURCE, "GET", _PATH)
            else None
        )

    definition = CompiledDomainDefinition(
        name="path_probe",
        schema_constant="COMPILED_PATH_PROBE_SCHEMA_VERSION",
        schema_version=1,
        semantic_operations=frozenset(),
        absent_versions=frozenset(),
        primitives=(CompiledPrimitive("list", (epoch,), "optional"),),
        classify_operation=classify,
        response_policy=lambda *_: CompiledResponsePolicy("list", None, None),
        recipe_policy=lambda _: "legacy",
    )
    digest = "sha256:" + "1" * 64
    codec: JsonObject = {
        "method": "GET",
        "path": _PATH,
        "channel": "path",
        "path_encoding": "legacy-url-interpolation-v1",
        "fields": [{"name": "projectName", "binding": "path_variable"}],
        "path_fields": ["projectName"],
        "params": "legacy",
        "request_schema_digest": digest,
        "response": None,
        "response_schema_digest": None,
        "response_projection": "direct",
        "capture": None,
    }
    program: JsonObject = {
        "source_operation": _SOURCE,
        "codec": "list",
        "codec_digest": digest,
        "request_schema_digest": digest,
        "response_digest": digest,
        "response_schema_digest": None,
        "result_envelope": "optional",
    }
    if drift == "encoding":
        codec["path_encoding"] = "percent-encoded-utf8-segment-v1"
    elif drift == "binding":
        codec["fields"] = [{"name": "projectName", "binding": "request_param"}]
    elif drift == "method":
        codec["method"] = "DELETE"
    elif drift == "source":
        program["source_operation"] = (
            "ProcessDefinitionController.deleteProcessDefinitionById"
        )
    program["program_digest"] = canonical_json_digest(
        {"schema_version": 4, "codec_record": codec, **program}
    )
    if drift is None:
        assert _program(
            definition,
            definition.primitives[0],
            program,
            {"list": codec},
            label="path probe",
        ) == (_SOURCE, "list")
    else:
        message = (
            "source primitive"
            if drift == "source"
            else "request epoch is not uniquely reviewed"
        )
        with pytest.raises(ValueError, match=message):
            _program(
                definition,
                definition.primitives[0],
                program,
                {"list": codec},
                label="path probe",
            )
