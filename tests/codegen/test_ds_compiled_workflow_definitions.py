"""Exact definition requests, native container roots, and content identities."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledDomainPlan,
    _codec_record,
    _compile_program,
    _compile_request,
    _compile_response,
    _render_domain_module,
    _select_request_epoch,
)
from ds_codegen.compiled_workflow_definitions import (
    PRIMITIVES,
    classify,
    recipe_policy,
    response_policy,
)
from ds_codegen.contract_inputs import canonical_json_digest
from ds_codegen.render.package.executable_schema import (
    _render_response_model_rebuilds,
    render_operation_response_module,
)
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from live_gate.runtime_ownership import _program

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import (
        CompiledRequest,
        CompiledResponseModule,
        CompiledScalarResponse,
    )
    from ds_codegen.ir import ContractSnapshot, OperationSpec

pytestmark = pytest.mark.source_contract
_DEFINITION = CompiledDomainDefinition(
    name="workflow_runtime",
    schema_constant="COMPILED_WORKFLOW_RUNTIME_SCHEMA_VERSION",
    schema_version=1,
    semantic_operations=frozenset(),
    absent_versions=frozenset(),
    primitives=PRIMITIVES,
    classify_operation=classify,
    response_policy=response_policy,
    recipe_policy=recipe_policy,
)


@pytest.fixture(scope="module")
def definition_plan(exact_contract_corpus: ExactContractCorpus) -> CompiledDomainPlan:
    requests: dict[str, tuple[str, CompiledRequest]] = {}
    responses: dict[str, tuple[str, CompiledResponseModule]] = {}
    scalars: dict[str, CompiledScalarResponse] = {}
    count = 0
    for version in exact_contract_corpus.versions:
        snapshot = exact_contract_corpus.snapshot(version)
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        source_ids = {
            source
            for binding in bindings.values()
            for source in binding.source_operations
        }
        selected = {}
        selected_sources = {}
        for operation in snapshot.operations:
            name = classify(operation)
            if name is None or operation.operation_id not in source_ids:
                continue
            primitive = next(item for item in PRIMITIVES if item.name == name)
            epoch = _select_request_epoch(_DEFINITION, primitive, operation, snapshot)
            _compile_request(
                _DEFINITION,
                primitive,
                epoch,
                snapshot,
                operation,
                version=version,
                requests=requests,
            )
            policy = response_policy(snapshot, operation, name)
            selected[name] = policy.codec
            selected_sources[name] = operation.operation_id
            _compile_response(
                _DEFINITION,
                snapshot,
                operation,
                policy,
                version=version,
                responses=responses,
                scalar_responses=scalars,
            )
            count += 1
        assert recipe_policy(selected) == f"exact_{version.replace('.', '_')}"
        if version == "3.4.3":
            assert selected_sources == {
                "definition_refs": (
                    "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList"
                ),
                "definition_page": (
                    "WorkflowDefinitionController.queryWorkflowDefinitionListPaging"
                ),
                "definition_get": (
                    "WorkflowDefinitionController.queryWorkflowDefinitionByCode"
                ),
                "definition_create": (
                    "WorkflowDefinitionController.createWorkflowDefinition"
                ),
                "definition_update": (
                    "WorkflowDefinitionController.updateWorkflowDefinition"
                ),
                "definition_delete": (
                    "WorkflowDefinitionController.deleteWorkflowDefinitionByCode"
                ),
                "definition_release": (
                    "WorkflowDefinitionController.releaseWorkflowDefinition"
                ),
                "workflow_execute": "ExecutorController.triggerWorkflowDefinition",
            }
    assert count == 296
    return CompiledDomainPlan(
        definition=_DEFINITION,
        profiles=(),
        requests=tuple(item for _, item in requests.values()),
        responses=tuple(item for _, item in responses.values()),
        scalar_responses=tuple(scalars.values()),
        codecs=(),
    )


def test_original_exact_requests_share_complete_executable_content(
    definition_plan: CompiledDomainPlan,
) -> None:
    assert len(definition_plan.requests) == 31
    # 3.4.3 adds Schedule.missedFirePolicy to create/update/detail/page closures.
    assert len(definition_plan.responses) == 40
    assert {item.annotation for item in definition_plan.scalar_responses} == {
        "bool",
        "int",
    }
    for request in definition_plan.requests:
        assert request.schema.endswith(
            request.executable_digest.removeprefix("sha256:")
        )
        assert not any(character.isdigit() for character in request.class_name)


def test_inline_same_named_classes_preserve_each_epoch_model(
    definition_plan: CompiledDomainPlan,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = tuple(
        item
        for item in definition_plan.requests
        if item.class_name == "DefinitionPageParams" and item.module_name is None
    )
    assert len(requests) > 1
    plan = replace(
        definition_plan, requests=requests, responses=(), scalar_responses=()
    )
    module = ModuleType("dsctl.generated.wire_programs._inline_definition_probe")
    module.__package__ = "dsctl.generated.wire_programs"
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(  # noqa: S102 - execute the compiler's deterministic module only.
        compile(_render_domain_module(plan), "<inline-models>", "exec"), module.__dict__
    )
    registry = module.REQUEST_SCHEMAS
    assert len({registry[item.schema].model for item in requests}) == len(requests)
    fields = {
        tuple(
            field.alias or name
            for name, field in registry[item.schema].model.model_fields.items()
        )
        for item in requests
    }
    assert any("projectName" in names for names in fields)
    assert any("projectCode" in names for names in fields)


@pytest.mark.parametrize(
    ("version", "primitive", "annotation", "valid", "invalid"),
    [
        ("1.3.9", "definition_release", "dict[str, object] | None", None, []),
        ("1.3.9", "definition_release", "dict[str, object] | None", {"x": None}, 1),
        ("3.4.2", "workflow_execute", "list[int]", [1, "2"], None),
        ("3.4.2", "workflow_execute", "list[int]", [], [{}]),
    ],
)
def test_native_container_roots_keep_exact_validation(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    primitive: str,
    annotation: str,
    valid: object,
    invalid: object,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    rendered = render_operation_response_module(
        snapshot,
        _operation(snapshot, primitive),
        module_parts=("wire_programs", "_container_probe"),
        root_model_module_parts=("wire_runtime", "_models"),
    )
    assert rendered.import_paths == ()
    assert rendered.adapter_annotation == annotation
    module = ModuleType("dsctl.generated.wire_programs._container_probe")
    module.__package__ = "dsctl.generated.wire_programs"
    module.__dict__["TypeAdapter"] = TypeAdapter
    monkeypatch.setitem(sys.modules, module.__name__, module)
    source = (
        rendered.source + f"\nADAPTER = TypeAdapter({rendered.adapter_annotation})\n"
    )
    exec(compile(source, "<container>", "exec"), module.__dict__)  # noqa: S102
    module.ADAPTER.validate_python(valid)
    with pytest.raises(ValidationError):
        module.ADAPTER.validate_python(invalid)


def test_structured_response_rebuilds_module_owned_models(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = exact_contract_corpus.snapshot("2.0.2")
    rendered = render_operation_response_module(
        snapshot,
        _operation(snapshot, "definition_page"),
        module_parts=("wire_programs", "_forward_ref_probe"),
        root_model_module_parts=("wire_runtime", "_models"),
    )
    rebuilds = tuple(
        line for line in rendered.source.splitlines() if ".model_rebuild(" in line
    )
    assert rebuilds
    assert all("_types_namespace=globals()" in line for line in rebuilds)
    assert any(line.startswith("Property.model_rebuild(") for line in rebuilds)

    module = ModuleType("dsctl.generated.wire_programs._forward_ref_probe")
    module.__package__ = "dsctl.generated.wire_programs"
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(rendered.source, "<forward-ref>", "exec"), module.__dict__)  # noqa: S102
    response_type = eval(rendered.adapter_annotation, module.__dict__)  # noqa: S307

    assert (
        TypeAdapter(response_type)
        .validate_python(
            {
                "totalList": [],
                "total": 0,
                "totalPage": 0,
                "pageSize": 100,
                "currentPage": 1,
            }
        )
        .total
        == 0
    )


def test_response_model_rebuilds_cover_recursive_and_inherited_namespaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("dsctl.generated.wire_programs._recursive_probe")
    module.__package__ = "dsctl.generated.wire_programs"
    module.__dict__["BaseModel"] = BaseModel
    monkeypatch.setitem(sys.modules, module.__name__, module)
    source = """
from __future__ import annotations

class Recursive(BaseModel):
    children: list[Recursive]

class Parent(BaseModel):
    later: Later

class Child(Parent):
    pass

class Later(BaseModel):
    value: str
"""
    exec(compile(source, "<recursive-models>", "exec"), module.__dict__)  # noqa: S102

    rebuild_source = _render_response_model_rebuilds(module)

    assert rebuild_source.splitlines() == [
        "Recursive.model_rebuild(_types_namespace=globals())",
        "Parent.model_rebuild(_types_namespace=globals())",
        "Child.model_rebuild(_types_namespace=globals())",
        "Later.model_rebuild(_types_namespace=globals())",
    ]
    exec(  # noqa: S102 - fixed generated rebuild statements under test.
        compile(rebuild_source, "<recursive-rebuilds>", "exec"),
        module.__dict__,
    )
    assert (
        TypeAdapter(module.Recursive)
        .validate_python({"children": [{"children": []}]})
        .children[0]
        .children
        == []
    )
    assert (
        TypeAdapter(module.Child)
        .validate_python({"later": {"value": "ready"}})
        .later.value
        == "ready"
    )


@pytest.mark.parametrize("invalid", [[True], ["1"], [1.0], None])
def test_task_code_container_preserves_native_strict_integer_elements(
    exact_contract_corpus: ExactContractCorpus,
    monkeypatch: pytest.MonkeyPatch,
    invalid: object,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "TaskDefinitionController.genTaskCodeList"
    )
    rendered = render_operation_response_module(
        snapshot,
        operation,
        module_parts=("wire_programs", "_strict_container_probe"),
        root_model_module_parts=("wire_runtime", "_models"),
    )
    assert rendered.import_paths == ()
    assert rendered.adapter_annotation == "list[StrictInt]"
    module = ModuleType("dsctl.generated.wire_programs._strict_container_probe")
    module.__package__ = "dsctl.generated.wire_programs"
    module.__dict__["TypeAdapter"] = TypeAdapter
    monkeypatch.setitem(sys.modules, module.__name__, module)
    source = rendered.source + "\nADAPTER = TypeAdapter(list[StrictInt])\n"
    exec(compile(source, "<strict-container>", "exec"), module.__dict__)  # noqa: S102
    assert module.ADAPTER.validate_python([1, 2]) == [1, 2]
    with pytest.raises(ValidationError):
        module.ADAPTER.validate_python(invalid)


@pytest.mark.parametrize("primitive", [item.name for item in PRIMITIVES])
def test_mandatory_request_fields_cannot_silently_become_optional(
    exact_contract_corpus: ExactContractCorpus, primitive: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, primitive)
    changed = replace(
        operation,
        parameters=[
            replace(item, required=False) if item.wire_name == "projectCode" else item
            for item in operation.parameters
        ],
    )
    with pytest.raises(ValueError, match="request field contract changed"):
        response_policy(snapshot, changed, primitive)


@pytest.mark.parametrize("field", ["code", "name", "releaseState", "schedule"])
def test_consumed_definition_fields_reject_type_drift(
    exact_contract_corpus: ExactContractCorpus, field: str
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    changed = replace(
        snapshot,
        models=[
            replace(
                model,
                fields=[
                    replace(item, java_type="boolean") if item.name == field else item
                    for item in model.fields
                ],
            )
            if model.name == "WorkflowDefinition"
            else model
            for model in snapshot.models
        ],
    )
    with pytest.raises(ValueError, match="consumed response fields changed"):
        response_policy(
            changed, _operation(snapshot, "definition_get"), "definition_get"
        )


@pytest.mark.parametrize(
    ("version", "primitive", "logical"),
    [
        ("1.3.9", "definition_release", "HashMap<String, Object>"),
        ("3.4.2", "workflow_execute", "List<String>"),
    ],
)
def test_container_nullability_and_element_types_remain_reviewed(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    primitive: str,
    logical: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    changed = replace(_operation(snapshot, primitive), logical_return_type=logical)
    with pytest.raises(ValueError, match="logical response changed"):
        response_policy(snapshot, changed, primitive)


@pytest.mark.parametrize("tamper", [None, "role", "suffix"])
def test_static_evidence_binds_request_role_to_its_digest(
    exact_contract_corpus: ExactContractCorpus, tamper: str | None
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "definition_delete")
    primitive = next(item for item in PRIMITIVES if item.name == "definition_delete")
    epoch = _select_request_epoch(_DEFINITION, primitive, operation, snapshot)
    request = _compile_request(
        _DEFINITION,
        primitive,
        epoch,
        snapshot,
        operation,
        version=snapshot.ds_version,
        requests={},
    )
    epoch = replace(epoch, request_schema=request.schema)
    policy = response_policy(snapshot, operation, primitive.name)
    codec = _codec_record(
        epoch,
        policy,
        response_projection="direct",
        request_schema_digest=request.executable_digest,
        response_schema_digest=None,
    )
    program = _compile_program(
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
    ).record()
    if tamper == "role":
        codec["params"] = "unreviewed_" + request.schema
    elif tamper == "suffix":
        codec["params"] = "definition_delete_" + "0" * 64
    if tamper is not None:
        program["program_digest"] = canonical_json_digest(
            {
                "schema_version": 4,
                "codec_record": codec,
                **{
                    key: value
                    for key, value in program.items()
                    if key != "program_digest"
                },
            }
        )
        with pytest.raises(ValueError, match="request epoch is not uniquely reviewed"):
            _program(
                _DEFINITION,
                primitive,
                program,
                {policy.codec: codec},
                label="probe",
                version="3.4.2",
            )
    else:
        assert _program(
            _DEFINITION,
            primitive,
            program,
            {policy.codec: codec},
            label="probe",
            version="3.4.2",
        ) == (operation.operation_id, policy.codec)


def _operation(snapshot: ContractSnapshot, primitive: str) -> OperationSpec:
    return next(item for item in snapshot.operations if classify(item) == primitive)


def test_container_roots_do_not_allow_unresolved_element_types(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = replace(
        _operation(snapshot, "workflow_execute"),
        logical_return_type="List<UnreviewedWorkflowResult>",
    )
    with pytest.raises(ValueError, match="unresolved contract snapshot reference"):
        render_operation_response_module(
            snapshot,
            operation,
            module_parts=("wire_programs", "_unknown_root_probe"),
            root_model_module_parts=("wire_runtime", "_models"),
        )


def test_unconsumed_response_fields_change_content_identity_without_new_recipe(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = _operation(snapshot, "definition_get")
    changed = replace(
        snapshot,
        models=[
            replace(
                model,
                fields=[
                    replace(item, java_type="long") if item.name == "modifyBy" else item
                    for item in model.fields
                ],
            )
            if model.name == "WorkflowDefinition"
            else model
            for model in snapshot.models
        ],
    )
    policies = [
        response_policy(item, operation, "definition_get")
        for item in (snapshot, changed)
    ]
    assert policies[0] == policies[1]
    resolved = [
        _compile_response(
            _DEFINITION,
            item,
            operation,
            policy,
            version=item.ds_version,
            responses={},
            scalar_responses={},
        )
        for item, policy in zip((snapshot, changed), policies, strict=True)
    ]
    assert resolved[0][0].schema != resolved[1][0].schema
    assert resolved[0][1] != resolved[1][1]
