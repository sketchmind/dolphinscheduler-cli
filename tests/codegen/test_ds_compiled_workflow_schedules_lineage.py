"""Exact-source contracts for the schedule and lineage workflow fragments."""

from __future__ import annotations

import ast
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen import compiled_workflow_lineage as lineage
from ds_codegen import compiled_workflow_schedules as schedules
from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledDomainPlan,
    CompiledResponsePolicy,
    _compile_request,
    _compile_response,
    _render_domain_module,
    _select_request_epoch,
    _validate_request_epoch,
)

if TYPE_CHECKING:
    from types import ModuleType

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import (
        CompiledRequest,
        CompiledResponseModule,
        CompiledScalarResponse,
    )

pytestmark = pytest.mark.source_contract
_DEFINITION = CompiledDomainDefinition(
    name="workflow_fragment_probe",
    schema_constant="WORKFLOW_FRAGMENT_PROBE_SCHEMA_VERSION",
    schema_version=1,
    semantic_operations=frozenset(),
    absent_versions=frozenset(),
    primitives=(),
    classify_operation=lambda _: None,
    response_policy=lambda *_: CompiledResponsePolicy("unused", None, None),
    recipe_policy=lambda _: "unused",
)
_SCHEDULE_RECIPES = {
    "1.3.9": "legacy_id",
    "2.0.0": "code",
    "2.0.1": "code",
    "2.0.9": "code",
    "2.0.2": "code",
    "2.0.3": "code",
    "2.0.4": "code",
    "2.0.5": "code",
    "2.0.6": "code",
    "2.0.7": "code",
    "2.0.8": "code",
    "3.0.0": "code",
    "3.0.1": "code",
    "3.0.6": "code",
    "3.0.2": "code",
    "3.0.3": "code",
    "3.0.4": "code",
    "3.0.5": "code",
    "3.1.0": "code",
    "3.1.1": "code",
    "3.1.2": "code",
    "3.1.9": "code",
    "3.1.3": "code",
    "3.1.4": "code",
    "3.1.5": "code",
    "3.1.6": "code",
    "3.1.7": "code",
    "3.1.8": "code",
    "3.2.0": "tenant_void",
    "3.2.1": "tenant_boolean",
    "3.2.2": "tenant_entity",
    "3.3.1": "workflow",
    "3.3.2": "workflow",
    "3.4.0": "workflow",
    "3.4.1": "workflow",
    "3.4.2": "workflow",
    "3.4.3": "workflow_missed_fire",
}
_CANONICAL = frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"})


@pytest.mark.parametrize(
    ("fragment", "coordinate_count"), [(schedules, 259), (lineage, 79)]
)
def test_original_exact_sources_compile_complete_fragment_inventories(
    exact_contract_corpus: ExactContractCorpus,
    fragment: ModuleType,
    coordinate_count: int,
) -> None:
    requests: dict[str, tuple[str, CompiledRequest]] = {}
    responses: dict[str, tuple[str, CompiledResponseModule]] = {}
    scalars: dict[str, CompiledScalarResponse] = {}
    total = 0
    for version in exact_contract_corpus.versions:
        snapshot = exact_contract_corpus.snapshot(version)
        operations = {
            fragment.classify(operation): operation
            for operation in snapshot.operations
            if fragment.classify(operation) is not None
        }
        if fragment is schedules:
            expected_count = 7
        elif version == "1.3.9":
            expected_count = 0
        else:
            expected_count = 3 if version in _CANONICAL | {"3.2.2"} else 2
        assert len(operations) == expected_count
        codecs = {}
        for primitive in fragment.PRIMITIVES:
            if version in primitive.absent_versions:
                assert primitive.name not in operations
                continue
            operation = operations[primitive.name]
            epoch = _select_request_epoch(_DEFINITION, primitive, operation, snapshot)
            _validate_request_epoch(_DEFINITION, primitive.name, epoch)
            request = _compile_request(
                _DEFINITION,
                primitive,
                epoch,
                snapshot,
                operation,
                version=version,
                requests=requests,
            )
            assert request.executable_digest.startswith("sha256:")
            assert request.schema == (
                f"{primitive.name}_{request.executable_digest.removeprefix('sha256:')}"
            )
            policy = fragment.response_policy(snapshot, operation, primitive.name)
            resolved, digest = _compile_response(
                _DEFINITION,
                snapshot,
                operation,
                policy,
                version=version,
                responses=responses,
                scalar_responses=scalars,
            )
            if policy.schema is not None:
                assert digest is not None
                assert (
                    resolved.schema
                    == f"{policy.schema}_{digest.removeprefix('sha256:')}"
                )
            codecs[primitive.name] = resolved.codec
            total += 1
        recipe = fragment.recipe_policy(codecs)
        if fragment is schedules:
            assert recipe == _SCHEDULE_RECIPES[version]
        elif version == "1.3.9":
            assert recipe == "absent"
        else:
            assert recipe == ("canonical" if version in _CANONICAL else "legacy_direct")
    assert total == coordinate_count
    assert len(requests) < coordinate_count
    _assert_request_models_have_distinct_bound_aliases(requests)
    # Equal exact closures are deduplicated by actual executable content, while
    # distinct names/fields/defaults retain independent identities.
    assert len(responses) < coordinate_count
    assert all(
        response.executable_digest.removeprefix("sha256:") in name
        for name, (_, response) in responses.items()
    )


def _assert_request_models_have_distinct_bound_aliases(
    requests: dict[str, tuple[str, CompiledRequest]],
) -> None:
    # Same role class names must bind at each definition/import, before another
    # exact request epoch can replace that name in the aggregate module.
    plan = CompiledDomainPlan(
        definition=_DEFINITION,
        profiles=(),
        requests=tuple(request for _, request in requests.values()),
        responses=(),
        scalar_responses=(),
        codecs=(),
    )
    tree = ast.parse(_render_domain_module(plan))
    aliases: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            aliases.update(
                item.asname
                for item in node.names
                if item.asname is not None and item.asname.endswith("_request_model")
            )
        elif isinstance(node, ast.Assign):
            aliases.update(
                target.id
                for target in node.targets
                if isinstance(target, ast.Name) and target.id.endswith("_request_model")
            )
    expected_by_schema = {}
    for schema, (_, request) in requests.items():
        digest = request.executable_digest.removeprefix("sha256:")
        semantic = schema.removesuffix(f"_{digest}")
        expected_by_schema[schema] = f"_{semantic}_{digest[:12]}_request_model"
    assert aliases == set(expected_by_schema.values())
    entries = next(
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "REQUEST_SCHEMAS"
            for target in node.targets
        )
    )
    assert isinstance(entries, ast.Dict)
    assert len(entries.values) == len(requests)
    for key, value in zip(entries.keys, entries.values, strict=True):
        assert isinstance(key, ast.Constant)
        assert isinstance(key.value, str)
        assert isinstance(value, ast.Call)
        assert isinstance(value.args[0], ast.Name)
        assert value.args[0].id == expected_by_schema[key.value]


@pytest.mark.parametrize("version", ["1.3.9", "3.2.0", "3.2.1", "3.2.2", "3.4.2"])
@pytest.mark.parametrize(
    "drift",
    [
        "request_required",
        "request_default",
        "request_type",
        "response_type",
        "response_projection",
        "entity_field",
        "enum_value",
    ],
)
def test_schedule_policy_rejects_source_drift_before_rendering(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    source = (
        "SchedulerController.queryScheduleListPaging"
        if drift == "entity_field"
        else "SchedulerController.createSchedule"
    )
    operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == source
    )
    if drift.startswith("request_"):
        field = next(
            parameter
            for parameter in operation.parameters
            if parameter.wire_name == "schedule"
        )
        if drift == "request_required":
            changed = replace(field, required=False)
        elif drift == "request_default":
            changed = replace(field, default_value="unexpected")
        else:
            changed = replace(field, java_type="Object")
        operation = replace(
            operation,
            parameters=[
                changed if parameter == field else parameter
                for parameter in operation.parameters
            ],
        )
    elif drift == "response_type":
        operation = replace(operation, logical_return_type="Map<String, Object>")
    elif drift == "response_projection":
        operation = replace(operation, response_projection="status_data")
    elif drift == "entity_field":
        model_name = operation.logical_return_type.split("<", 1)[1][:-1]
        model = next(
            model for model in snapshot.models if model.import_path == model_name
        )
        changed_model = replace(
            model,
            fields=[field for field in model.fields if field.wire_name != "crontab"],
        )
        snapshot = replace(
            snapshot,
            models=[
                changed_model if candidate == model else candidate
                for candidate in snapshot.models
            ],
        )
    else:
        enum = next(enum for enum in snapshot.enums if enum.name == "WarningType")
        snapshot = replace(
            snapshot,
            enums=[
                replace(enum, values=enum.values[:-1])
                if candidate == enum
                else candidate
                for candidate in snapshot.enums
            ],
        )
    primitive = schedules.classify(operation)
    assert primitive is not None
    with pytest.raises(ValueError, match="compiled schedule"):
        schedules.response_policy(snapshot, operation, primitive)


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "3.2.2", "3.4.2"])
@pytest.mark.parametrize(
    "drift", ["request_alias", "response_projection", "response_type", "graph_field"]
)
def test_lineage_policy_rejects_projection_and_native_name_drift(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        operation
        for operation in snapshot.operations
        if operation.method_name == "queryWorkFlowLineageByCode"
    )
    if drift == "request_alias":
        operation = replace(
            operation,
            parameters=[
                replace(parameter, wire_name="workflowCode")
                if parameter.wire_name == "workFlowCode"
                else parameter
                for parameter in operation.parameters
            ],
        )
    elif drift == "response_projection":
        operation = replace(
            operation,
            response_projection="direct" if version in _CANONICAL else "single_data",
        )
    elif drift == "response_type":
        operation = replace(operation, logical_return_type="Map<String, Object>")
    else:
        model = next(
            model for model in snapshot.models if model.name == "WorkFlowLineage"
        )
        snapshot = replace(
            snapshot,
            models=[
                replace(model, fields=model.fields[:-1])
                if candidate == model
                else candidate
                for candidate in snapshot.models
            ],
        )
    with pytest.raises(ValueError, match="compiled lineage"):
        lineage.response_policy(snapshot, operation, "lineage_get")


@pytest.mark.parametrize("fragment", [schedules, lineage])
def test_fragment_recipes_reject_mixed_exact_programs(fragment: ModuleType) -> None:
    codecs = {
        primitive.name: f"{primitive.name}_3_4_2" for primitive in fragment.PRIMITIVES
    }
    first = fragment.PRIMITIVES[0].name
    codecs[first] = f"{first}_3_4_1"
    with pytest.raises(ValueError, match="incomplete or mixed"):
        fragment.recipe_policy(codecs)
