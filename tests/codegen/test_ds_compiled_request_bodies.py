"""Compile native body requests from source without migrating any domain owner."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import load_schema_pool

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    _codec_record,
    _compile_request,
    _operation_request_shape,
    _request_record,
    _select_request_epoch,
    _validate_request_epoch,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledRequest
    from ds_codegen.ir import ContractSnapshot, HttpMethod, OperationSpec
    from dsctl.support.json_types import JsonObject

pytestmark = pytest.mark.source_contract
_DTO_VERSIONS = frozenset(
    {
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }
)
_DEFINITION = CompiledDomainDefinition(
    name="request_body_probe",
    schema_constant="REQUEST_BODY_PROBE_SCHEMA_VERSION",
    schema_version=1,
    semantic_operations=frozenset(),
    absent_versions=frozenset(),
    primitives=(),
    classify_operation=lambda _operation: None,
    response_policy=lambda _snapshot, _operation, _primitive: CompiledResponsePolicy(
        codec="unused", schema=None, capture=None
    ),
    recipe_policy=lambda _codecs: "unused",
)
_JSON = CompiledRequestEpoch(
    method="POST",
    path="datasources",
    channel="json",
    request_schema="dto",
    request_model="BodyDtoParams",
    request_fields=("dataSourceParam",),
    required_fields=frozenset({"dataSourceParam"}),
)
_PATH_JSON = replace(
    _JSON,
    method="PUT",
    path="datasources/{id}",
    channel="path_json",
    request_fields=("id", "dataSourceParam"),
    path_fields=("id",),
    required_fields=frozenset({"id", "dataSourceParam"}),
)
_TEXT = replace(
    _JSON,
    channel="json_text",
    request_schema="text",
    request_model="BodyTextParams",
    request_fields=("jsonStr",),
    required_fields=frozenset({"jsonStr"}),
)
_PATH_TEXT = replace(
    _TEXT,
    method="PUT",
    path="datasources/{id}",
    channel="path_json_text",
    request_fields=("id", "jsonStr"),
    path_fields=("id",),
    required_fields=frozenset({"id", "jsonStr"}),
)


@pytest.mark.parametrize("version", REVIEWED_DS_VERSIONS[1:])
@pytest.mark.parametrize("action", ["create", "update"])
def test_all_native_datasource_body_coordinates_compile_exact_request_models(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    action: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = _operation(snapshot, action)
    structured = version in _DTO_VERSIONS
    if action == "create":
        epoch = _JSON if structured else _TEXT
    else:
        epoch = _PATH_JSON if structured else _PATH_TEXT
    compiled, model = _compile(snapshot, operation, epoch, monkeypatch)
    body_name = "dataSourceParam" if structured else "jsonStr"
    body: JsonObject = {
        "id": 91,
        "name": "native",
        "note": None,
        "type": "MYSQL",
        "other": {"key": "value"},
        "extension": {"nested": None},
    }
    values: JsonObject = {body_name: body if structured else ' \n{"名称":null}\t '}
    if action == "update":
        values["id"] = 7
    validated = model.model_validate(values)
    dumped = validated.model_dump(
        mode="json", by_alias=True, exclude_none=True, exclude_unset=True
    )
    if structured:
        expected = {key: value for key, value in body.items() if key != "note"}
        if version == "2.0.0":
            expected.pop("extension")
        assert dumped[body_name] == expected
        assert compiled.content is not None
        assert "class BaseDataSourceParamDTO(" in compiled.content
        assert "import DbType as DbType" in compiled.content
        closure = "\n".join(
            (compiled.content, *(module.content for module in compiled.pool_modules))
        )
        assert "class DbType(" in closure
        assert "class DataSource(" not in closure
        assert "class User(" not in closure
    else:
        assert dumped[body_name] == values[body_name]
        assert compiled.content is None
    if action == "update":
        assert dumped["id"] == 7
    for malformed in ({}, {**values, body_name: None}, {**values, body_name: 42}):
        with pytest.raises(ValidationError):
            model.model_validate(malformed)

    expected_fields = [
        {"name": field, "binding": "path_variable" if field == "id" else "request_body"}
        for field in epoch.request_fields
    ]
    request_record = _request_record(_DEFINITION, action, epoch, operation, snapshot)
    codec = _codec_record(
        epoch,
        CompiledResponsePolicy(codec="unused", schema=None, capture=None),
        response_projection="direct",
        request_schema_digest=compiled.executable_digest,
        response_schema_digest=None,
    )
    assert request_record["fields"] == codec["fields"] == expected_fields
    assert codec["channel"] == epoch.channel
    assert codec["path_fields"] == list(epoch.path_fields)


@pytest.mark.parametrize("text", ["", "null", " \t\n", ' {"x":null} \n'])
def test_native_string_body_preserves_empty_null_and_whitespace_spelling(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    text: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    _, model = _compile(snapshot, _operation(snapshot, "create"), _TEXT, monkeypatch)
    assert model.model_validate({"jsonStr": text}).model_dump()["jsonStr"] == text


@pytest.mark.parametrize(
    "drift",
    [
        "GET",
        "PATCH",
        "query",
        "extra_body",
        "optional",
        "default",
        "media",
        "path_string",
        "two_paths",
    ],
)
def test_native_body_source_drift_stays_outside_the_closed_protocol(
    exact_contract_corpus: ExactContractCorpus, drift: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "update")
    body = next(item for item in operation.parameters if item.binding == "request_body")
    path = next(
        item for item in operation.parameters if item.binding == "path_variable"
    )
    if drift in {"GET", "PATCH"}:
        operation = replace(operation, http_method=cast("HttpMethod", drift))
    elif drift == "query":
        operation = replace(
            operation,
            parameters=[
                *operation.parameters,
                replace(body, name="extra", wire_name="extra", binding="request_param"),
            ],
        )
    elif drift == "extra_body":
        operation = replace(
            operation,
            parameters=[
                *operation.parameters,
                replace(body, name="extra", wire_name="extra"),
            ],
        )
    elif drift == "media":
        operation = replace(operation, consumes=["application/xml"])
    elif drift == "two_paths":
        operation = replace(
            operation,
            path=f"{operation.path}/{{other}}",
            parameters=[
                *operation.parameters,
                replace(path, name="other", wire_name="other"),
            ],
        )
    else:
        target = body
        if drift == "optional":
            changed = replace(body, required=False)
        elif drift == "default":
            changed = replace(body, default_value="null")
        else:
            assert drift == "path_string"
            target = path
            changed = replace(path, java_type="String")
        operation = replace(
            operation,
            parameters=[
                changed if item == target else item for item in operation.parameters
            ],
        )
    with pytest.raises(
        ValueError,
        match=r"request transport|JSON body|mandatory|path transport",
    ):
        _operation_request_shape(_DEFINITION, "update", operation, snapshot)


@pytest.mark.parametrize(
    "body_type",
    [
        "int",
        "boolean",
        "Object",
        "Map<String, String>",
        "org.apache.dolphinscheduler.missing.BodyDTO",
        "org.apache.dolphinscheduler.spi.enums.DbType",
        "List<org.apache.dolphinscheduler.plugin.datasource.api.datasource."
        "BaseDataSourceParamDTO>",
    ],
)
def test_body_root_must_be_a_resolved_structured_type_or_native_string(
    exact_contract_corpus: ExactContractCorpus, body_type: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "create")
    operation = replace(
        operation,
        parameters=[
            replace(item, java_type=body_type)
            if item.binding == "request_body"
            else item
            for item in operation.parameters
        ],
    )
    with pytest.raises(ValueError, match="resolved structured type or native String"):
        _operation_request_shape(_DEFINITION, "create", operation, snapshot)


@pytest.mark.parametrize(
    ("version", "epoch"),
    [("2.0.0", _TEXT), ("2.0.0", _PATH_TEXT), ("3.4.2", _JSON), ("3.4.2", _PATH_JSON)],
)
def test_body_source_cannot_select_a_crossed_structured_or_text_epoch(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    epoch: CompiledRequestEpoch,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = _operation(snapshot, "create" if epoch.method == "POST" else "update")
    body_name = "dataSourceParam" if version == "2.0.0" else "jsonStr"
    crossed = replace(
        epoch,
        request_fields=(*epoch.path_fields, body_name),
        required_fields=frozenset((*epoch.path_fields, body_name)),
    )
    primitive = CompiledPrimitive(
        name="request", requests=(crossed,), result_envelope="required"
    )
    with pytest.raises(ValueError, match="0 matching epochs"):
        _select_request_epoch(_DEFINITION, primitive, operation, snapshot)


@pytest.mark.parametrize(
    "epoch",
    [
        replace(_JSON, method="GET"),
        replace(_JSON, method="PUT"),
        replace(_PATH_JSON, method="POST"),
        replace(_JSON, request_fields=(), required_fields=frozenset()),
        replace(
            _PATH_TEXT,
            request_fields=("id", "jsonStr", "extra"),
            required_fields=frozenset({"id", "jsonStr", "extra"}),
        ),
    ],
)
def test_body_epoch_declarations_require_one_body_and_a_reviewed_method_path(
    epoch: CompiledRequestEpoch,
) -> None:
    with pytest.raises(ValueError, match="request epoch is invalid"):
        _validate_request_epoch(_DEFINITION, "request", epoch)


def test_request_body_requires_its_complete_exact_dto_enum_closure(
    exact_contract_corpus: ExactContractCorpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = exact_contract_corpus.snapshot("2.0.0")
    snapshot = replace(
        snapshot, enums=[item for item in snapshot.enums if item.name != "DbType"]
    )
    with pytest.raises(ValueError, match=r"no exact target|unresolved"):
        _compile(snapshot, _operation(snapshot, "create"), _JSON, monkeypatch)


def _operation(snapshot: ContractSnapshot, action: str) -> OperationSpec:
    source = f"DataSourceController.{action}DataSource"
    return next(item for item in snapshot.operations if item.operation_id == source)


def _compile(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    epoch: CompiledRequestEpoch,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[CompiledRequest, type[BaseParamsModel]]:
    _validate_request_epoch(_DEFINITION, "request", epoch)
    primitive = CompiledPrimitive(
        name="request", requests=(epoch,), result_envelope="required"
    )
    assert _select_request_epoch(_DEFINITION, primitive, operation, snapshot) == epoch
    compiled = _compile_request(
        _DEFINITION,
        primitive,
        epoch,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )
    name = "dsctl.generated.wire_programs._test_body_request"
    load_schema_pool(compiled.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if compiled.module_name is not None:
        module.__package__ += f".{compiled.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - execute only deterministic request renderer output
        compile(compiled.content or compiled.source, f"<{name}>", "exec"),
        module.__dict__,
    )
    return compiled, cast("type[BaseParamsModel]", getattr(module, compiled.class_name))
