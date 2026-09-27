"""Request-only source probes for the remaining finite path transport shapes."""

from __future__ import annotations

import sys
from dataclasses import dataclass, replace
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
    _request_record,
    _select_request_epoch,
    _validate_request_epoch,
)
from ds_codegen.contract_inputs import contract_snapshot_digest
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.operation_paths import (
    executable_path_operation,
    operation_path_arguments,
)
from ds_codegen.render.package.operations_renderer import (
    _render_operation_path_arguments,
)
from ds_codegen.render.package.planner import build_package_context
from ds_codegen.render.package.renderer_deps import operation_render_deps
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

    from ds_codegen.compiled_domains import CompiledRequest, _Channel, _Method
    from ds_codegen.ir import ContractSnapshot, OperationSpec

pytestmark = pytest.mark.source_contract
_MODERN = (
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
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_PROCESS = (
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
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
)
_WORKFLOW = ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2")
_EARLY = (
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
)
_LATER = ("3.1.0", "3.1.9", "3.2.0", "3.2.1", "3.2.2", *_WORKFLOW)
_LEGACY_ENCODING = "legacy-url-interpolation-v1"
_SEGMENT_ENCODING = "percent-encoded-utf8-segment-v1"
_DEFINITION = CompiledDomainDefinition(
    name="remaining_path_probe",
    schema_constant="REMAINING_PATH_PROBE_SCHEMA_VERSION",
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


@dataclass(frozen=True)
class _Case:
    version: str
    source: str
    method: str
    path: str
    channel: str
    path_fields: tuple[str, ...]
    implicit: bool = False


# Independent reviewed route inventory; this is not loaded from a migration
# report, installed wrapper, or compiler-produced expected data.
_LEGACY_ROUTES = (
    ("ExecutorController.execute", "POST", "executors/execute", "path_form"),
    (
        "ExecutorController.startProcessInstance",
        "POST",
        "executors/start-process-instance",
        "path_form",
    ),
    (
        "ProcessDefinitionController.createProcessDefinition",
        "POST",
        "process/save",
        "path_form",
    ),
    (
        "ProcessDefinitionController.deleteProcessDefinitionById",
        "GET",
        "process/delete",
        "path_query",
    ),
    (
        "ProcessDefinitionController.queryProcessDefinitionById",
        "GET",
        "process/select-by-id",
        "path_query",
    ),
    (
        "ProcessDefinitionController.queryProcessDefinitionList",
        "GET",
        "process/list",
        "path",
    ),
    (
        "ProcessDefinitionController.queryProcessDefinitionListPaging",
        "GET",
        "process/list-paging",
        "path_query",
    ),
    (
        "ProcessDefinitionController.releaseProcessDefinition",
        "POST",
        "process/release",
        "path_form",
    ),
    (
        "ProcessDefinitionController.updateProcessDefinition",
        "POST",
        "process/update",
        "path_form",
    ),
    (
        "ProcessInstanceController.queryParentInstanceBySubId",
        "GET",
        "instance/select-parent-process",
        "path_query",
    ),
    (
        "ProcessInstanceController.queryProcessInstanceById",
        "GET",
        "instance/select-by-id",
        "path_query",
    ),
    (
        "ProcessInstanceController.queryProcessInstanceList",
        "GET",
        "instance/list-paging",
        "path_query",
    ),
    (
        "ProcessInstanceController.querySubProcessInstanceByTaskId",
        "GET",
        "instance/select-sub-process",
        "path_query",
    ),
    (
        "ProcessInstanceController.updateProcessInstance",
        "POST",
        "instance/update",
        "path_form",
    ),
    ("SchedulerController.createSchedule", "POST", "schedule/create", "path_form"),
    ("SchedulerController.deleteScheduleById", "GET", "schedule/delete", "path_query"),
    ("SchedulerController.offline", "POST", "schedule/offline", "path_form"),
    ("SchedulerController.online", "POST", "schedule/online", "path_form"),
    ("SchedulerController.previewSchedule", "POST", "schedule/preview", "path_form"),
    (
        "SchedulerController.queryScheduleListPaging",
        "GET",
        "schedule/list-paging",
        "path_query",
    ),
    ("SchedulerController.updateSchedule", "POST", "schedule/update", "path_form"),
    (
        "TaskInstanceController.queryTaskListPaging",
        "GET",
        "task-instance/list-paging",
        "path_query",
    ),
)
_MODERN_ROUTES = (
    (
        _PROCESS,
        "ProcessDefinitionController.deleteProcessDefinitionByCode",
        "DELETE",
        "process-definition/{code}",
        "path",
        ("projectCode", "code"),
        False,
    ),
    (
        _WORKFLOW,
        "WorkflowDefinitionController.deleteWorkflowDefinitionByCode",
        "DELETE",
        "workflow-definition/{code}",
        "path",
        ("projectCode", "code"),
        False,
    ),
    (
        (*_EARLY, "3.1.0"),
        "TaskDefinitionController.deleteTaskDefinitionByCode",
        "DELETE",
        "task-definition/{code}",
        "path",
        ("projectCode", "code"),
        False,
    ),
    (
        _MODERN,
        "SchedulerController.deleteScheduleById",
        "DELETE",
        "schedules/{id}",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _PROCESS,
        "ProcessDefinitionController.releaseProcessDefinition",
        "POST",
        "process-definition/{code}/release",
        "path_form",
        ("projectCode", "code"),
        False,
    ),
    (
        _WORKFLOW,
        "WorkflowDefinitionController.releaseWorkflowDefinition",
        "POST",
        "workflow-definition/{code}/release",
        "path_form",
        ("projectCode", "code"),
        False,
    ),
    (
        _EARLY,
        "SchedulerController.offline",
        "POST",
        "schedules/{id}/offline",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _EARLY,
        "SchedulerController.online",
        "POST",
        "schedules/{id}/online",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _LATER,
        "SchedulerController.offlineSchedule",
        "POST",
        "schedules/{id}/offline",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _LATER,
        "SchedulerController.publishScheduleOnline",
        "POST",
        "schedules/{id}/online",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _MODERN,
        "TaskInstanceController.forceTaskSuccess",
        "POST",
        "task-instances/{id}/force-success",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _LATER,
        "TaskInstanceController.stopTask",
        "POST",
        "task-instances/{id}/stop",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        _LATER,
        "TaskInstanceController.taskSavePoint",
        "POST",
        "task-instances/{id}/savepoint",
        "path",
        ("projectCode", "id"),
        False,
    ),
    (
        ("3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8", "3.1.9"),
        "TaskDefinitionController.queryTaskDefinitionVersions",
        "GET",
        "task-definition/{code}/versions",
        "path_query",
        ("projectCode", "code"),
        False,
    ),
    (
        _MODERN,
        "SchedulerController.previewSchedule",
        "POST",
        "schedules/preview",
        "path_form",
        ("projectCode",),
        True,
    ),
    (
        _MODERN,
        "TaskDefinitionController.genTaskCodeList",
        "GET",
        "task-definition/gen-task-codes",
        "path_query",
        ("projectCode",),
        True,
    ),
    (
        ("3.2.2",),
        "WorkFlowLineageController.queryDownstreamDependentTaskList",
        "GET",
        "lineages/query-dependent-tasks",
        "path_query",
        ("projectCode",),
        True,
    ),
    (
        _WORKFLOW,
        "ExecutorController.controlWorkflowInstance",
        "POST",
        "executors/execute",
        "path_form",
        ("projectCode",),
        True,
    ),
    (
        ("3.3.1", "3.3.2", "3.4.0", "3.4.1"),
        "ExecutorController.triggerWorkflowDefinition",
        "POST",
        "executors/start-workflow-instance",
        "path_form",
        ("projectCode",),
        True,
    ),
)
_CASES = (
    *(
        _Case(
            "1.3.9",
            source,
            method,
            f"projects/{{projectName}}/{suffix}",
            channel,
            ("projectName",),
        )
        for source, method, suffix, channel in _LEGACY_ROUTES
    ),
    *(
        _Case(
            version,
            source,
            method,
            f"projects/{{projectCode}}/{suffix}",
            channel,
            paths,
            implicit,
        )
        for versions, source, method, suffix, channel, paths, implicit in _MODERN_ROUTES
        for version in versions
    ),
    *(
        _Case(
            version,
            "ResourcesController.deleteResource",
            "DELETE",
            "resources",
            "query",
            (),
        )
        for version in ("3.2.0", "3.2.1", "3.2.2", *_WORKFLOW)
    ),
)


@pytest.mark.parametrize(
    "case", _CASES, ids=lambda case: f"{case.version}-{case.source}"
)
def test_exact_source_coordinates_render_and_select_the_closed_request_shape(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    case: _Case,
) -> None:
    snapshot = exact_contract_corpus.snapshot(case.version)
    operation = _operation(snapshot, case.source)
    assert (operation.http_method, operation.path) == (case.method, case.path)
    paths = operation_path_arguments(operation)
    assert tuple(argument.name for argument in paths) == case.path_fields
    assert tuple(argument.name for argument in paths if argument.parameter is None) == (
        ("projectCode",) if case.implicit else ()
    )
    for argument in paths:
        if argument.parameter is not None:
            assert argument.parameter.binding == "path_variable"
            assert argument.parameter.java_type in (
                {"String"}
                if case.version == "1.3.9"
                else {"int", "Integer", "long", "Long"}
            )
    native_fields = tuple(
        parameter.wire_name
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    )
    expected_fields = (("projectCode",) if case.implicit else ()) + native_fields
    # Full native order and aliases survive; only the old wrapper's missing route
    # argument precedes them in the executable schema.
    epoch = CompiledRequestEpoch(
        method=cast("_Method", case.method),
        path=case.path,
        channel=cast("_Channel", case.channel),
        request_schema="probe",
        request_model="PathProbeParams",
        request_fields=cast("tuple[str, ...]", expected_fields),
        path_fields=case.path_fields,
        versions=frozenset({case.version}),
        path_encoding=_LEGACY_ENCODING
        if case.version == "1.3.9"
        else _SEGMENT_ENCODING,
    )
    assert _operation_request_shape(_DEFINITION, "probe", operation, snapshot) == (
        case.method,
        case.path,
        case.channel,
        expected_fields,
        case.path_fields,
    )
    request = _compile(snapshot, operation, epoch)
    model = _request_model(request, monkeypatch)
    assert (
        tuple(field.alias or name for name, field in model.model_fields.items())
        == expected_fields
    )
    assert _request_record(_DEFINITION, "probe", epoch, operation, snapshot)[
        "fields"
    ] == [
        {
            "name": name,
            "binding": "path_variable" if name in case.path_fields else "request_param",
        }
        for name in expected_fields
    ]
    if case.implicit:
        path_field = next(
            field
            for name, field in model.model_fields.items()
            if (field.alias or name) == "projectCode"
        )
        assert path_field.is_required()
        assert path_field.annotation is int


def test_inventory_is_finite_and_implicit_schema_does_not_change_source(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert len(_CASES) == len({(case.version, case.source) for case in _CASES}) == 349
    assert sum(case.implicit for case in _CASES) == 80
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "SchedulerController.previewSchedule")
    before = contract_snapshot_digest(snapshot)
    assert [
        (p.wire_name, p.binding)
        for p in operation.parameters
        if is_client_supplied_parameter(p)
    ] == [("schedule", "request_param")]
    original_parameters = operation.parameters.copy()
    assert _render_operation_path_arguments(
        operation, build_package_context(snapshot), deps=operation_render_deps()
    ) == [("project_code", "int")]
    epoch = CompiledRequestEpoch(
        method="POST",
        path="projects/{projectCode}/schedules/preview",
        channel="path_form",
        request_schema="preview",
        request_model="PreviewParams",
        request_fields=("projectCode", "schedule"),
        path_fields=("projectCode",),
        required_fields=frozenset({"projectCode", "schedule"}),
    )
    request = _compile(snapshot, operation, epoch)
    model = _request_model(request, monkeypatch)
    with pytest.raises(ValidationError):
        model.model_validate({"schedule": "{}"})
    policy = CompiledResponsePolicy(codec="probe", schema=None, capture=None)
    codec = _codec_record(
        epoch,
        policy,
        response_projection="direct",
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    primitive = CompiledPrimitive("probe", (epoch,), "optional")
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
        ds_version="3.4.2",
        primitive="probe",
        source_digest=before,
        record=compiled.record(),
        request_schemas={
            "preview": CompiledRequestSchema(
                model=model, digest=request.executable_digest
            )
        },
        response_schemas={},
        codecs={"probe": codec},
        execution_mode=WireExecutionMode.READ_ONCE,
        expected_envelope=WireResultEnvelope.OPTIONAL,
    )
    encoded = program.prepare({"projectCode": 7, "schedule": "{}"}).request
    assert (encoded.path, encoded.form, encoded.query) == (
        "/projects/7/schedules/preview",
        {"schedule": "{}"},
        None,
    )
    assert operation.parameters == original_parameters
    assert contract_snapshot_digest(snapshot) == before
    assert compiled.request_schema_digest == request.executable_digest
    declared = replace(
        operation, parameters=executable_path_operation(operation).parameters
    )
    assert (
        _compile(snapshot, declared, epoch).executable_digest
        == request.executable_digest
    )
    # The same executable schema does not erase original source identity.
    declared_compiled = _compile_program(
        _DEFINITION,
        snapshot,
        declared,
        primitive=primitive,
        request_epoch=epoch,
        policy=policy,
        codec_record=codec,
        resolver=SnapshotTypeResolver.compile(snapshot),
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    assert declared_compiled.codec_digest != compiled.codec_digest


@pytest.mark.parametrize(
    "change",
    [
        "other_missing",
        "duplicate",
        "server_attribute",
        "binding",
        "String",
        "boolean",
        "POST_one_path",
        "DELETE_empty",
    ],
)
def test_unreviewed_implicit_and_path_shapes_are_rejected(
    exact_contract_corpus: ExactContractCorpus, change: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "SchedulerController.previewSchedule")
    view = executable_path_operation(operation)
    path_parameter = view.parameters[0]
    if change == "other_missing":
        operation = replace(
            operation, path=operation.path.replace("projectCode", "other")
        )
    elif change == "duplicate":
        operation = replace(operation, path=operation.path + "/{projectCode}")
    elif change in {"server_attribute", "binding", "String", "boolean"}:
        parameter = (
            replace(path_parameter, binding="request_attribute", hidden=True)
            if change == "server_attribute"
            else replace(path_parameter, binding="request_param")
            if change == "binding"
            else replace(path_parameter, java_type=change)
        )
        operation = replace(view, parameters=[parameter, *view.parameters[1:]])
    elif change == "POST_one_path":
        operation = replace(view, parameters=[path_parameter])
    else:
        operation = replace(
            operation, http_method="DELETE", path="resources", parameters=[]
        )
    with pytest.raises(ValueError):
        _operation_request_shape(_DEFINITION, "probe", operation, snapshot)


@pytest.mark.parametrize(
    "change",
    [
        "default_encoding",
        "modern_version",
        "unknown_encoding",
        "wrong_fields",
        "wrong_method",
    ],
)
def test_legacy_interpolation_requires_explicit_exact_epoch(
    exact_contract_corpus: ExactContractCorpus, change: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("1.3.9")
    operation = _operation(
        snapshot, "ProcessDefinitionController.queryProcessDefinitionList"
    )
    epoch = CompiledRequestEpoch(
        method="GET",
        path="projects/{projectName}/process/list",
        channel="path",
        request_schema="legacy",
        request_model="LegacyParams",
        request_fields=("projectName",),
        path_fields=("projectName",),
        versions=frozenset({"1.3.9"}),
        path_encoding=_LEGACY_ENCODING,
    )
    if change == "default_encoding":
        epoch = replace(epoch, path_encoding=_SEGMENT_ENCODING)
    elif change == "modern_version":
        epoch = replace(epoch, versions=frozenset({"3.4.2"}))
    elif change == "unknown_encoding":
        epoch = replace(epoch, path_encoding="automatic")
    elif change == "wrong_fields":
        epoch = replace(epoch, path_fields=("other",))
    else:
        epoch = replace(epoch, method="DELETE")
    with pytest.raises(ValueError):
        _compile(snapshot, operation, epoch)


def _compile(
    snapshot: ContractSnapshot, operation: OperationSpec, epoch: CompiledRequestEpoch
) -> CompiledRequest:
    _validate_request_epoch(_DEFINITION, "probe", epoch)
    primitive = CompiledPrimitive("probe", (epoch,), "optional")
    assert _select_request_epoch(_DEFINITION, primitive, operation, snapshot) == epoch
    return _compile_request(
        _DEFINITION,
        primitive,
        epoch,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )


def _request_model(
    request: CompiledRequest, monkeypatch: pytest.MonkeyPatch
) -> type[BaseParamsModel]:
    name = f"dsctl.generated.wire_programs._test_remaining_path_{request.schema}"
    load_schema_pool(request.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if request.module_name is not None:
        module.__package__ += f".{request.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - execute only the deterministic renderer output under test
        compile(request.content or request.source, f"<{name}>", "exec"), module.__dict__
    )
    return cast("type[BaseParamsModel]", getattr(module, request.class_name))


def _operation(snapshot: ContractSnapshot, source: str) -> OperationSpec:
    return next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == source
    )
