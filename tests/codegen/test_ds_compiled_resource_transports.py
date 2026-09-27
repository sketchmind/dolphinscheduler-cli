"""Closed transfer mechanics keep native evidence separate from JSON models."""

from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledResponsePolicy,
    _codec_record,
    _compile_program,
    _compile_request,
    _request_record,
    _select_request_epoch,
    _validate_request_epoch,
    _validate_response_policy,
    _validate_response_transport,
)
from ds_codegen.compiled_resources import RESOURCE_COMPILED_DOMAIN
from ds_codegen.contract_inputs import canonical_json_digest, contract_snapshot_digest
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from live_gate.runtime_ownership import _program

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledPrimitive, CompiledRequestEpoch
    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonObject

pytestmark = pytest.mark.source_contract
_DOMAIN = RESOURCE_COMPILED_DOMAIN


@pytest.mark.parametrize("version", REVIEWED_DS_VERSIONS)
def test_native_upload_models_exclude_files_but_programs_bind_complete_source(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    before = contract_snapshot_digest(snapshot)
    primitive, operation, epoch = _source(snapshot, "upload")
    request = _compile_request(
        _DOMAIN,
        primitive,
        epoch,
        snapshot,
        operation,
        version=version,
        requests={},
    )
    source = request.content or request.source
    assert "MultipartFile" not in source
    assert "UploadFileLike" not in source
    tree = ast.parse(source)
    model = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == request.class_name
    )
    fields = {
        node.target.id
        for node in model.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    assert fields == set(epoch.request_fields) - {"file"}
    assert "file" not in fields
    native = next(
        parameter for parameter in operation.parameters if parameter.wire_name == "file"
    )
    assert native.java_type == "MultipartFile"
    record = _request_record(_DOMAIN, "upload", epoch, operation, snapshot)
    assert record["file_fields"] == ["file"]
    assert record["fields"] == [
        {"name": name, "binding": "request_param"} for name in epoch.request_fields
    ]
    program, codec = _compiled(snapshot, "upload")
    assert program["source_operation"] == operation.operation_id
    assert "file_fields" in codec
    assert "response_transport" not in codec
    assert _program(
        _DOMAIN,
        primitive,
        program,
        {str(program["codec"]): codec},
        label="upload source",
        version=version,
    ) == (operation.operation_id, program["codec"])
    assert contract_snapshot_digest(snapshot) == before


@pytest.mark.parametrize("version", REVIEWED_DS_VERSIONS)
def test_binary_programs_keep_native_response_identity_without_json_schema(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    primitive, operation, _ = _source(snapshot, "download")
    program, codec = _compiled(snapshot, "download")
    assert codec["response_transport"] == "binary"
    assert codec["response"] is codec["capture"] is None
    assert program["response_schema_digest"] is None
    assert "file_fields" not in codec
    assert operation.logical_return_type in {
        "void",
        "org.springframework.core.io.Resource",
    }
    assert _program(
        _DOMAIN,
        primitive,
        program,
        {str(program["codec"]): codec},
        label="binary source",
        version=version,
    ) == (operation.operation_id, program["codec"])


@pytest.mark.parametrize(
    "drift", ["method", "binding", "optional", "default", "content_type", "extra_file"]
)
def test_multipart_shape_rejects_unreviewed_native_file_transports(
    exact_contract_corpus: ExactContractCorpus, drift: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    primitive, operation, _ = _source(snapshot, "upload")
    native = next(
        parameter for parameter in operation.parameters if parameter.wire_name == "file"
    )
    if drift == "method":
        operation = replace(operation, http_method="GET")
    elif drift == "content_type":
        operation = replace(operation, consumes=["application/json"])
    elif drift == "extra_file":
        operation = replace(
            operation,
            parameters=[
                *operation.parameters,
                replace(native, name="other", wire_name="other"),
            ],
        )
    else:
        if drift == "binding":
            changed = replace(native, binding="request_body")
        elif drift == "optional":
            changed = replace(native, required=False)
        else:
            changed = replace(native, default_value="invalid")
        operation = replace(
            operation,
            parameters=[
                changed if parameter == native else parameter
                for parameter in operation.parameters
            ],
        )
    with pytest.raises(ValueError, match=r"multipart transport|matching epochs"):
        _select_request_epoch(_DOMAIN, primitive, operation, snapshot)


@pytest.mark.parametrize("files", [(), ("file", "file"), ("missing",)])
def test_multipart_epochs_require_complete_unique_native_file_inventory(
    exact_contract_corpus: ExactContractCorpus, files: tuple[str, ...]
) -> None:
    _, _, epoch = _source(exact_contract_corpus.snapshot("3.4.2"), "upload")
    with pytest.raises(ValueError, match="request epoch is invalid"):
        _validate_request_epoch(_DOMAIN, "upload", replace(epoch, file_fields=files))


def test_binary_policy_is_a_trusted_primitive_decision_not_candidate_metadata(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    primitive, operation, _ = _source(snapshot, "download")
    binary = _DOMAIN.response_policy(snapshot, operation, primitive.name)
    with pytest.raises(ValueError, match="response transport changed"):
        _validate_response_transport(
            replace(primitive, response_transport="json"), binary, operation
        )
    with pytest.raises(ValueError, match="response transport changed"):
        _validate_response_transport(
            primitive, replace(binary, response_transport="json"), operation
        )
    for policy in (
        replace(binary, schema="forbidden"),
        replace(binary, capture={}),
        replace(binary, scalar_annotation="str"),
    ):
        with pytest.raises(ValueError, match="response policy is invalid"):
            _validate_response_policy(_DOMAIN, policy)


@pytest.mark.parametrize("name", ["upload", "download", "base_dir"])
@pytest.mark.parametrize(
    "drift", ["source", "method", "binding", "file_fields", "response_transport"]
)
def test_static_transfer_ownership_rejects_rebound_candidate_tampering(
    exact_contract_corpus: ExactContractCorpus, name: str, drift: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    primitive, _, _ = _source(snapshot, name)
    original_program, original_codec = _compiled(snapshot, name)
    program, codec = deepcopy(original_program), deepcopy(original_codec)
    if drift == "source":
        program["source_operation"] = "ResourcesController.unreviewed"
    elif drift == "method":
        codec["method"] = "PUT"
    elif drift == "binding":
        codec["fields"] = [{"name": "file", "binding": "request_body"}]
    elif drift == "file_fields":
        codec["file_fields"] = ["unreviewed"]
    elif name == "download":
        codec.pop("response_transport")
    else:
        codec["response_transport"] = "binary"
    program["program_digest"] = canonical_json_digest(
        {
            "schema_version": 4,
            "codec_record": codec,
            **{key: value for key, value in program.items() if key != "program_digest"},
        }
    )
    with pytest.raises(ValueError):
        _program(
            _DOMAIN,
            primitive,
            program,
            {str(program["codec"]): codec},
            label="tampered transfer",
            version="3.4.2",
        )


def test_ordinary_json_codec_has_no_transfer_extension_keys() -> None:
    primitive = next(item for item in _DOMAIN.primitives if item.name == "base_dir")
    epoch = primitive.requests[0]
    codec = _codec_record(
        epoch,
        CompiledResponsePolicy("probe", None, None),
        response_projection="direct",
        request_schema_digest="sha256:" + "a" * 64,
        response_schema_digest=None,
    )
    assert set(codec) == {
        "method",
        "path",
        "channel",
        "path_encoding",
        "fields",
        "path_fields",
        "params",
        "response",
        "response_projection",
        "capture",
        "request_schema_digest",
        "response_schema_digest",
    }


def _source(
    snapshot: ContractSnapshot, name: str
) -> tuple[CompiledPrimitive, OperationSpec, CompiledRequestEpoch]:
    primitive = next(item for item in _DOMAIN.primitives if item.name == name)
    operation = next(
        item for item in snapshot.operations if _DOMAIN.classify_operation(item) == name
    )
    return (
        primitive,
        operation,
        _select_request_epoch(_DOMAIN, primitive, operation, snapshot),
    )


def _compiled(snapshot: ContractSnapshot, name: str) -> tuple[JsonObject, JsonObject]:
    primitive, operation, epoch = _source(snapshot, name)
    request = _compile_request(
        _DOMAIN,
        primitive,
        epoch,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )
    epoch = replace(epoch, request_schema=request.schema)
    # These transport probes intentionally have no JSON response schema. The
    # base-dir case supplies a normal read to exercise the negative trust boundary.
    policy = CompiledResponsePolicy(
        codec=f"probe_{name}",
        schema=None,
        capture=None,
        response_transport=primitive.response_transport,
    )
    codec = _codec_record(
        epoch,
        policy,
        response_projection=operation.response_projection,
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    program = _compile_program(
        _DOMAIN,
        snapshot,
        operation,
        primitive=primitive,
        request_epoch=epoch,
        policy=policy,
        codec_record=codec,
        resolver=SnapshotTypeResolver.compile(snapshot),
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    return program.record(), codec
