from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_queues import QUEUE_COMPILED_DOMAIN

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_DEPENDENCIES = {
    "tenant.create": ("queue.page",),
    "tenant.update": ("queue.page",),
    "user.create": ("tenant.page",),
    "user.update": ("tenant.page",),
}
_QUEUE = "org.apache.dolphinscheduler.dao.entity.Queue"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"


def _compile(bundles: tuple[RuntimeBundle, ...]) -> CompiledDomainSet:
    return compile_domains(
        bundles,
        (QUEUE_COMPILED_DOMAIN,),
        operation_dependencies=_DEPENDENCIES,
    )


@pytest.fixture(scope="module")
def compiled_queues(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return _compile(exact_runtime_bundles)


def test_queue_materializes_every_exact_recipe_and_delete_absence(
    compiled_queues: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_queues.plan("queue")
    epochs = (
        (
            ("1.3.9",),
            "legacy",
            ("page_legacy", "create_legacy_void", "update_legacy_void", None),
        ),
        (
            ("2.0.0", "2.0.1"),
            "void",
            ("page_required_id_strict", "create_void", "update_void", None),
        ),
        (
            (
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
            ),
            "create_entity",
            (
                "page_required_id_strict",
                "create_entity_required_id",
                "update_void",
                None,
            ),
        ),
        (
            (
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
            ),
            "entity",
            (
                "page_nullable_id_strict",
                "create_entity_nullable_id",
                "update_entity_nullable_id",
                None,
            ),
        ),
        (
            ("3.2.0",),
            "void_delete",
            (
                "page_nullable_id",
                "create_entity_nullable_id",
                "update_entity_nullable_id",
                "delete_void",
            ),
        ),
        (
            ("3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
            "boolean",
            (
                "page_nullable_id",
                "create_entity_nullable_id",
                "update_entity_nullable_id",
                "delete_bool",
            ),
        ),
    )
    expected = {
        version: (recipe, codecs)
        for versions, recipe, codecs in epochs
        for version in versions
    }
    assert tuple(profile.version for profile in plan.profiles) == tuple(expected)
    assert QUEUE_COMPILED_DOMAIN.semantic_absent_versions == {
        "queue.delete": frozenset(
            {
                "1.3.9",
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
            }
        )
    }
    for bundle, profile in zip(exact_runtime_bundles, plan.profiles, strict=True):
        recipe, codecs = expected[profile.version]
        programs = dict(profile.programs)
        assert profile.status == "supported"
        assert profile.recipe_id == profile.record()["recipe_id"] == recipe
        assert profile.source_contract_digest == bundle.metadata.source_contract_digest
        assert (
            tuple(
                programs[name].codec if name in programs else None
                for name in ("page", "create", "update", "delete")
            )
            == codecs
        )
        assert programs["page"].result_envelope == "optional"
        assert all(
            program.result_envelope == "required"
            for name, program in profile.programs
            if name != "page"
        )
        assert {program.source_operation for program in programs.values()} == {
            "QueueController.queryQueueListPaging",
            "QueueController.createQueue",
            "QueueController.updateQueue",
        } | ({"QueueController.deleteQueueById"} if "delete" in programs else set())
    assert sum(len(profile.programs) for profile in plan.profiles) == 120


def test_queue_reuses_request_schemas_without_merging_field_ownership(
    compiled_queues: CompiledDomainSet,
) -> None:
    plan = compiled_queues.plan("queue")
    assert {request.schema for request in plan.requests} == {
        "page",
        "create",
        "update",
        "id",
    }
    codecs = dict(plan.codecs)
    assert {
        (record["method"], record["path"], record["channel"], record["params"])
        for record in codecs.values()
    } == {
        ("GET", "queue/list-paging", "query", "page"),
        ("GET", "queues", "query", "page"),
        ("POST", "queue/create", "form", "create"),
        ("POST", "queues", "form", "create"),
        ("POST", "queue/update", "form", "update"),
        ("PUT", "queues/{id}", "path_form", "update"),
        ("DELETE", "queues/{id}", "path", "id"),
    }
    for name, binding, path_fields in (
        ("update_legacy_void", "request_param", []),
        ("update_entity_nullable_id", "path_variable", ["id"]),
    ):
        assert codecs[name]["fields"] == [
            {"name": "id", "binding": binding},
            {"name": "queue", "binding": "request_param"},
            {"name": "queueName", "binding": "request_param"},
        ]
        assert codecs[name]["path_fields"] == path_fields
    assert (
        codecs["update_legacy_void"]["request_schema_digest"]
        == codecs["update_entity_nullable_id"]["request_schema_digest"]
    )


@pytest.mark.parametrize(
    "codecs",
    [
        {"page": "page_legacy", "create": "create_void", "update": "update_void"},
        {
            "page": "page_required_id_strict",
            "create": "create_entity_nullable_id",
            "update": "update_void",
        },
        {
            "page": "page_nullable_id_strict",
            "create": "create_entity_nullable_id",
            "update": "update_entity_nullable_id",
            "delete": "delete_void",
        },
        {
            "page": "page_nullable_id",
            "create": "create_entity_nullable_id",
            "update": "update_entity_nullable_id",
        },
    ],
)
def test_queue_rejects_crossed_recipe_epochs(codecs: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="recipe is unsupported"):
        QUEUE_COMPILED_DOMAIN.recipe_policy(codecs)


@pytest.mark.parametrize(
    ("model_path", "field_name", "message"),
    [
        (_QUEUE, "id", "entity response epoch changed"),
        (_PAGE, "totalList", "page response epoch changed"),
    ],
)
def test_queue_rejects_response_nullability_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    model_path: str,
    field_name: str,
    message: str,
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                models=[
                    replace(
                        model,
                        fields=[
                            replace(field, nullable=not field.nullable)
                            if field.wire_name == field_name
                            else field
                            for field in model.fields
                        ],
                    )
                    if model.import_path == model_path
                    else model
                    for model in bundle.snapshot.models
                ],
            ),
        )
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match=message):
        _compile(bundles)


@pytest.mark.parametrize("drift", ["path_form_binding", "request_type"])
def test_queue_rejects_request_ownership_and_schema_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    bundles = []
    for bundle in exact_runtime_bundles:
        if bundle.spec.version != "3.4.1":
            bundles.append(bundle)
            continue
        operations = []
        for operation in bundle.snapshot.operations:
            if operation.operation_id == "QueueController.updateQueue":
                parameters = []
                for parameter in operation.parameters:
                    if parameter.wire_name == "id" and drift == "path_form_binding":
                        parameters.append(replace(parameter, binding="request_param"))
                    elif parameter.wire_name == "queueName" and drift == "request_type":
                        parameters.append(replace(parameter, java_type="Integer"))
                    else:
                        parameters.append(parameter)
                operations.append(replace(operation, parameters=parameters))
            else:
                operations.append(operation)
        bundles.append(
            replace(bundle, snapshot=replace(bundle.snapshot, operations=operations))
        )
    message = (
        "implicit path arguments are unsupported"
        if drift == "path_form_binding"
        else "request schema update differs"
    )
    with pytest.raises(ValueError, match=message):
        _compile(tuple(bundles))


def test_queue_rejects_delete_in_an_explicitly_absent_source_coordinate(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    modern = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    delete = next(
        operation
        for operation in modern.snapshot.operations
        if operation.operation_id == "QueueController.deleteQueueById"
    )
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot, operations=[*bundle.snapshot.operations, delete]
            ),
        )
        if bundle.spec.version == "1.3.9"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="unowned classified operations"):
        _compile(bundles)


def test_queue_rejects_a_missing_supported_delete_source(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    operation
                    for operation in bundle.snapshot.operations
                    if operation.operation_id != "QueueController.deleteQueueById"
                ],
            ),
        )
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="snapshot is missing operations"):
        _compile(bundles)
