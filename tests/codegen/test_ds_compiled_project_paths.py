"""Exercise shared request compilation from real project-scoped source contracts."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import load_schema_pool

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    _codec_record,
    _compile_program,
    _compile_request,
    _operation_request_shape,
    _select_request_epoch,
    _validate_request_epoch,
)
from ds_codegen.contract_inputs import contract_snapshot_digest
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from dsctl.generated.wire_runtime._compiled_schema import CompiledRequestSchema
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel
from dsctl.upstream.wire import (
    WireExecutionMode,
    WireResultEnvelope,
    _load_compiled_program,
)

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledRequest, _Method
    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonObject

pytestmark = pytest.mark.source_contract
_BASE = "projects/{projectCode}"
_PARAMETER = f"{_BASE}/project-parameter"
_PREFERENCE = f"{_BASE}/project-preference"
_WORKER = f"{_BASE}/worker-group"
_NODE_TYPE = "org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType"

# Request-only probe: it neither compiles domain ownership nor reviews responses.
_DEFINITION = CompiledDomainDefinition(
    name="project_path_probe",
    schema_constant="PROJECT_PATH_PROBE_SCHEMA_VERSION",
    schema_version=1,
    semantic_operations=frozenset(),
    absent_versions=frozenset(),
    primitives=(),
    classify_operation=lambda _operation: None,
    response_policy=lambda _snapshot, _operation, _primitive: CompiledResponsePolicy(
        codec="unused",
        schema=None,
        capture=None,
    ),
    recipe_policy=lambda _codecs: "unused",
)
_PAGE = CompiledRequestEpoch(
    method="GET",
    path=_PARAMETER,
    channel="path_query",
    request_schema="page",
    request_model="ProjectPathPageParams",
    request_fields=("projectCode", "searchVal", "pageNo", "pageSize"),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode", "pageNo", "pageSize"}),
)
_CREATE = CompiledRequestEpoch(
    method="POST",
    path=_PARAMETER,
    channel="path_form",
    request_schema="create",
    request_model="ProjectPathCreateParams",
    request_fields=("projectCode", "projectParameterName", "projectParameterValue"),
    path_fields=("projectCode",),
)
_DETAIL = CompiledRequestEpoch(
    method="GET",
    path=f"{_PARAMETER}/{{code}}",
    channel="path",
    request_schema="detail",
    request_model="ProjectPathDetailParams",
    request_fields=("projectCode", "code"),
    path_fields=("projectCode", "code"),
    required_fields=frozenset({"projectCode", "code"}),
)
_UPDATE = CompiledRequestEpoch(
    method="PUT",
    path=f"{_PARAMETER}/{{code}}",
    channel="path_form",
    request_schema="update",
    request_model="ProjectPathUpdateParams",
    request_fields=(
        "projectCode",
        "code",
        "projectParameterName",
        "projectParameterValue",
    ),
    path_fields=("projectCode", "code"),
)
_DELETE = CompiledRequestEpoch(
    method="POST",
    path=f"{_PARAMETER}/delete",
    channel="path_form",
    request_schema="delete",
    request_model="ProjectPathDeleteParams",
    request_fields=("projectCode", "code"),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode", "code"}),
)
_PREFERENCE_GET = CompiledRequestEpoch(
    method="GET",
    path=_PREFERENCE,
    channel="path",
    request_schema="preference_get",
    request_model="ProjectPreferenceGetParams",
    request_fields=("projectCode",),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode"}),
)
_PREFERENCE_STATE = CompiledRequestEpoch(
    method="POST",
    path=_PREFERENCE,
    channel="path_form",
    request_schema="preference_state",
    request_model="ProjectPreferenceStateParams",
    request_fields=("projectCode", "state"),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode", "state"}),
)
_PREFERENCE_UPDATE = CompiledRequestEpoch(
    method="PUT",
    path=_PREFERENCE,
    channel="path_form",
    request_schema="preference_update",
    request_model="ProjectPreferenceUpdateParams",
    request_fields=("projectCode", "projectPreferences"),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode", "projectPreferences"}),
)
_WORKER_GET = replace(
    _PREFERENCE_GET,
    path=_WORKER,
    request_schema="worker_get",
    request_model="ProjectWorkerGetParams",
)
_WORKER_SET = CompiledRequestEpoch(
    method="POST",
    path=_WORKER,
    channel="path_form",
    request_schema="worker_set",
    request_model="ProjectWorkerSetParams",
    request_fields=("projectCode", "workerGroups"),
    path_fields=("projectCode",),
    required_fields=frozenset({"projectCode", "workerGroups"}),
)
_NAME_VALUE = {"projectParameterName": "key", "projectParameterValue": "value"}


@pytest.mark.parametrize(
    ("version", "source", "epoch", "values"),
    [
        (
            "3.2.0",
            "ProjectParameterController.queryProjectParameterListPaging",
            _PAGE,
            {
                "projectCode": 9000000000,
                "searchVal": "key",
                "pageNo": 1,
                "pageSize": 20,
            },
        ),
        (
            "3.3.1",
            "ProjectParameterController.queryProjectParameterListPaging",
            replace(
                _PAGE,
                request_fields=(
                    "projectCode",
                    "searchVal",
                    "projectParameterDataType",
                    "pageNo",
                    "pageSize",
                ),
            ),
            {
                "projectCode": 9000000000,
                "searchVal": "key",
                "projectParameterDataType": "VARCHAR",
                "pageNo": 1,
                "pageSize": 20,
            },
        ),
        (
            "3.2.0",
            "ProjectParameterController.createProjectParameter",
            _CREATE,
            {"projectCode": 9000000000, **_NAME_VALUE},
        ),
        (
            "3.3.1",
            "ProjectParameterController.createProjectParameter",
            replace(
                _CREATE,
                request_fields=(*_CREATE.request_fields, "projectParameterDataType"),
            ),
            {
                "projectCode": 9000000000,
                **_NAME_VALUE,
                "projectParameterDataType": "VARCHAR",
            },
        ),
        (
            "3.2.0",
            "ProjectParameterController.queryProjectParameterByCode",
            _DETAIL,
            {"projectCode": 9000000000, "code": 8000000000},
        ),
        (
            "3.2.0",
            "ProjectParameterController.updateProjectParameter",
            _UPDATE,
            {"projectCode": 9000000000, "code": 8000000000, **_NAME_VALUE},
        ),
        (
            "3.3.1",
            "ProjectParameterController.updateProjectParameter",
            replace(
                _UPDATE,
                request_fields=(*_UPDATE.request_fields, "projectParameterDataType"),
            ),
            {
                "projectCode": 9000000000,
                "code": 8000000000,
                **_NAME_VALUE,
                "projectParameterDataType": "VARCHAR",
            },
        ),
        (
            "3.2.0",
            "ProjectParameterController.deleteProjectParametersByCode",
            _DELETE,
            {"projectCode": 9000000000, "code": 8000000000},
        ),
        (
            "3.2.0",
            "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
            _PREFERENCE_GET,
            {"projectCode": 9000000000},
        ),
        (
            "3.2.0",
            "ProjectPreferenceController.enableProjectPreference",
            _PREFERENCE_STATE,
            {"projectCode": 9000000000, "state": 1},
        ),
        (
            "3.2.0",
            "ProjectPreferenceController.updateProjectPreference",
            _PREFERENCE_UPDATE,
            {"projectCode": 9000000000, "projectPreferences": "{}"},
        ),
        (
            "3.2.2",
            "ProjectWorkerGroupController.queryWorkerGroups",
            _WORKER_GET,
            {"projectCode": 9000000000},
        ),
        (
            "3.3.1",
            "ProjectWorkerGroupController.queryAssignedWorkerGroups",
            _WORKER_GET,
            {"projectCode": 9000000000},
        ),
        (
            "3.2.2",
            "ProjectWorkerGroupController.assignWorkerGroups",
            _WORKER_SET,
            {"projectCode": 9000000000, "workerGroups": [""]},
        ),
        (
            "3.3.1",
            "ProjectWorkerGroupController.assignWorkerGroups",
            _WORKER_SET,
            {"projectCode": 9000000000, "workerGroups": ["a,b"]},
        ),
    ],
)
def test_real_project_requests_select_and_render_reviewed_fields(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    source: str,
    epoch: CompiledRequestEpoch,
    values: JsonObject,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = _operation(snapshot, source)
    assert operation.http_method == epoch.method
    assert operation.path == epoch.path
    assert tuple(
        item.java_type
        for item in operation.parameters
        if item.binding == "path_variable"
    ) == (
        ("long", "Long")
        if source.endswith("updateProjectParameter")
        else ("long", "long")
        if source.endswith("queryProjectParameterByCode")
        else ("long",)
    )
    model = _compile_model(snapshot, operation, epoch, monkeypatch)
    assert (
        model.model_validate(values).model_dump(
            mode="json", by_alias=True, exclude_unset=True
        )
        == values
    )
    assert (
        tuple(field.alias or name for name, field in model.model_fields.items())
        == epoch.request_fields
    )
    with pytest.raises(ValidationError):
        model.model_validate({**values, "unreviewed": True})
    for name in epoch.path_fields:
        with pytest.raises(ValidationError):
            model.model_validate(
                {key: value for key, value in values.items() if key != name}
            )


@pytest.mark.parametrize("java_type", ["int", "Integer", "long", "Long"])
def test_integer_source_spellings_and_parameter_order_bind_by_name(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    java_type: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.2.0")
    source = _operation(
        snapshot, "ProjectParameterController.queryProjectParameterByCode"
    )
    path_parameters = [
        replace(item, java_type=java_type)
        for item in source.parameters
        if item.binding == "path_variable"
    ]
    operation = replace(source, parameters=list(reversed(path_parameters)))
    request = replace(
        _DETAIL,
        request_fields=("code", "projectCode"),
        path_fields=("code", "projectCode"),
    )
    model = _compile_model(snapshot, operation, request, monkeypatch)
    assert model.model_validate({"code": 20, "projectCode": 10}).model_dump() == {
        "code": 20,
        "projectCode": 10,
    }


def test_reversed_path_declaration_loads_and_encodes_the_real_request_model(
    exact_contract_corpus: ExactContractCorpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.2.0")
    operation = _operation(
        snapshot, "ProjectParameterController.queryProjectParameterByCode"
    )
    epoch = replace(_DETAIL, path_fields=("code", "projectCode"))
    primitive = CompiledPrimitive(
        name="request", requests=(epoch,), result_envelope="optional"
    )
    _validate_request_epoch(_DEFINITION, primitive.name, epoch)
    assert _select_request_epoch(_DEFINITION, primitive, operation, snapshot) == epoch
    request = _compile_request(
        _DEFINITION,
        primitive,
        epoch,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )
    # This probe loads real compiler records but never executes or reviews a response.
    policy = CompiledResponsePolicy(codec="request_probe", schema=None, capture=None)
    codec = _codec_record(
        epoch,
        policy,
        response_projection=operation.response_projection,
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    compiled = _compile_program(
        _DEFINITION,
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
    program, _, _, _ = _load_compiled_program(
        name=_DEFINITION.name,
        ds_version=snapshot.ds_version,
        primitive=primitive.name,
        source_digest=contract_snapshot_digest(snapshot),
        record=compiled.record(),
        request_schemas={
            epoch.request_schema: CompiledRequestSchema(
                model=_request_model(request, monkeypatch),
                digest=request.executable_digest,
            )
        },
        response_schemas={},
        codecs={policy.codec: codec},
        execution_mode=WireExecutionMode.READ_RETRY_SAFE,
        expected_envelope=WireResultEnvelope.OPTIONAL,
    )
    encoded = program.codec.encode({"projectCode": 9000000000, "code": 8000000000})
    assert encoded.path == "/projects/9000000000/project-parameter/8000000000"
    assert encoded.query is None
    assert encoded.form is None
    assert program.codec.path_fields == ("code", "projectCode")


@pytest.mark.parametrize("projection", ["single_data_list", "unknown"])
def test_compiled_codec_rejects_unreviewed_response_projections(
    projection: str,
) -> None:
    policy = CompiledResponsePolicy(codec="request_probe", schema=None, capture=None)
    with pytest.raises(ValueError, match="response projection is unsupported"):
        _codec_record(
            _DETAIL,
            policy,
            response_projection=projection,
            request_schema_digest="unused",
            response_schema_digest=None,
        )


def test_compiled_codec_records_single_data_without_changing_request_identity() -> None:
    policy = CompiledResponsePolicy(codec="request_probe", schema=None, capture=None)
    direct = _codec_record(
        _DETAIL,
        policy,
        response_projection="direct",
        request_schema_digest="unused",
        response_schema_digest=None,
    )
    projected = _codec_record(
        _DETAIL,
        policy,
        response_projection="single_data",
        request_schema_digest="unused",
        response_schema_digest=None,
    )
    assert projected == {**direct, "response_projection": "single_data"}


@pytest.mark.parametrize(
    "change",
    [
        "duplicate_placeholder",
        "missing_placeholder",
        "extra_placeholder",
        "duplicate_parameter",
        "String",
        "boolean",
        "body",
        "enum_query",
        "enum_form",
        "delete_three",
        "post_three",
        "query_three",
        "put_three",
    ],
)
def test_unreviewed_source_path_shapes_still_fail(
    exact_contract_corpus: ExactContractCorpus,
    change: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.3.1")
    page = _operation(
        snapshot, "ProjectParameterController.queryProjectParameterListPaging"
    )
    detail = _operation(
        snapshot, "ProjectParameterController.queryProjectParameterByCode"
    )
    update = _operation(snapshot, "ProjectParameterController.updateProjectParameter")
    path_parameter = next(
        item for item in page.parameters if item.binding == "path_variable"
    )
    if change == "duplicate_placeholder":
        operation = replace(page, path=f"{page.path}/{{projectCode}}")
    elif change == "missing_placeholder":
        operation = replace(page, path="projects/project-parameter")
    elif change == "extra_placeholder":
        operation = replace(page, path=f"{page.path}/{{other}}")
    elif change == "duplicate_parameter":
        operation = replace(page, parameters=[*page.parameters, path_parameter])
    elif change in {"String", "boolean", "enum_query", "enum_form"}:
        operation = replace(
            page,
            http_method="POST" if change == "enum_form" else "GET",
            parameters=[
                replace(
                    item, java_type=_NODE_TYPE if change.startswith("enum_") else change
                )
                if item == path_parameter
                else item
                for item in page.parameters
            ],
        )
    elif change == "body":
        operation = replace(
            page,
            parameters=[
                replace(item, binding="request_body")
                if item == path_parameter
                else item
                for item in page.parameters
            ],
        )
    elif change in {"delete_three", "post_three"}:
        operation = replace(
            {"delete_three": detail, "post_three": update}[change],
            http_method=cast(
                "_Method", {"delete_three": "DELETE", "post_three": "POST"}[change]
            ),
        )
    elif change == "query_three":
        operation = replace(
            detail,
            parameters=[
                *detail.parameters,
                next(item for item in page.parameters if item.wire_name == "pageNo"),
            ],
        )
    else:
        operation = replace(
            update,
            path=f"{update.path}/{{third}}",
            parameters=[
                *update.parameters,
                replace(path_parameter, name="third", wire_name="third"),
            ],
        )
    if change in {"delete_three", "post_three", "query_three"}:
        operation = replace(
            operation,
            path=f"{operation.path}/{{third}}",
            parameters=[
                *operation.parameters,
                replace(path_parameter, name="third", wire_name="third"),
            ],
        )
    with pytest.raises(ValueError):
        _operation_request_shape(_DEFINITION, "request", operation, snapshot)


@pytest.mark.parametrize(
    "epoch",
    [
        replace(_DETAIL, method="PUT"),
        replace(_UPDATE, method="DELETE"),
        replace(_PAGE, channel="path_form"),
        replace(_UPDATE, channel="path_query", method="POST"),
        replace(_DETAIL, path=f"{_PARAMETER}/{{projectCode}}"),
        replace(_DETAIL, path_fields=("projectCode", "projectCode")),
        replace(_DETAIL, path_fields=("projectCode",)),
        replace(_DETAIL, request_fields=("projectCode", "code", "extra")),
        replace(
            _UPDATE,
            path=f"{_UPDATE.path}/{{third}}",
            path_fields=(*_UPDATE.path_fields, "third"),
            request_fields=(*_UPDATE.request_fields, "third"),
        ),
    ],
)
def test_declarations_reject_shapes_outside_the_closed_protocol(
    epoch: CompiledRequestEpoch,
) -> None:
    with pytest.raises(ValueError):
        _validate_request_epoch(_DEFINITION, "request", epoch)


def _compile_model(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    request: CompiledRequestEpoch,
    monkeypatch: pytest.MonkeyPatch,
) -> type[BaseParamsModel]:
    _validate_request_epoch(_DEFINITION, "request", request)
    primitive = CompiledPrimitive(
        name="request", requests=(request,), result_envelope="optional"
    )
    assert _select_request_epoch(_DEFINITION, primitive, operation, snapshot) == request
    compiled = _compile_request(
        _DEFINITION,
        primitive,
        request,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )
    return _request_model(compiled, monkeypatch)


def _request_model(
    request: CompiledRequest, monkeypatch: pytest.MonkeyPatch
) -> type[BaseParamsModel]:
    name = f"dsctl.generated.wire_programs._test_project_path_{request.schema}"
    load_schema_pool(request.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if request.module_name is not None:
        module.__package__ += f".{request.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - execute only the request renderer's output under test
        compile(request.content or request.source, f"<{name}>", "exec"),
        module.__dict__,
    )
    return cast("type[BaseParamsModel]", getattr(module, request.class_name))


def _operation(snapshot: ContractSnapshot, source: str) -> OperationSpec:
    return next(item for item in snapshot.operations if item.operation_id == source)
