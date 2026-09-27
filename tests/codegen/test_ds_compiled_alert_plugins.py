from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from tests.codegen.compiled_support import replace_operation, response_adapter

from ds_codegen.compiled_alert_plugins import ALERT_PLUGIN_COMPILED_DOMAIN
from ds_codegen.compiled_domains import compile_domains
from ds_codegen.extract.return_type_reviews import REVIEWED_RETURN_TYPE_RULES
from ds_codegen.runtime_contract import runtime_operation_bindings

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_INSTANCE = "org.apache.dolphinscheduler.dao.entity.AlertPluginInstance"
_VO = "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO"
_DEFINITION = "org.apache.dolphinscheduler.dao.entity.PluginDefine"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_LIST_OPERATION = (
    "AlertPluginInstanceController."
    "getAlertPluginInstance__get_alert_plugin_instances_list"
)
_NULLABLE_HELPER_PAGE_VERSIONS = frozenset(
    next(
        review.versions
        for review in REVIEWED_RETURN_TYPE_RULES
        if review.rule_name == "alert_plugin_page_nullable_total_list"
    )
)


@pytest.fixture(scope="module")
def compiled_alert_plugins(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(exact_runtime_bundles, (ALERT_PLUGIN_COMPILED_DOMAIN,))


def test_alert_plugin_exact_profiles_own_the_complete_reviewed_closure(
    compiled_alert_plugins: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_alert_plugins.plan("alert_plugin")
    epochs = (
        (("1.3.9",), None, 0),
        (("2.0.0",), "void_local_search", 7),
        (
            (
                "2.0.1",
                "2.0.2",
                "2.0.3",
                "2.0.4",
                "2.0.5",
                "2.0.6",
                "2.0.7",
                "2.0.8",
                "2.0.9",
            ),
            "void",
            7,
        ),
        (
            (
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
            ),
            "create_entity",
            7,
        ),
        (("3.2.1", "3.2.2"), "transient_entity", 9),
        (("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"), "entity", 8),
    )
    expected = {
        version: (recipe, count)
        for versions, recipe, count in epochs
        for version in versions
    }
    assert tuple(profile.version for profile in plan.profiles) == tuple(expected)
    for bundle, profile, legacy in zip(
        exact_runtime_bundles,
        plan.profiles,
        compiled_alert_plugins.legacy_bundles,
        strict=True,
    ):
        recipe, count = expected[profile.version]
        assert profile.recipe_id == recipe
        assert profile.status == ("upstream_absent" if recipe is None else "supported")
        assert len(profile.programs) == count
        assert profile.source_contract_digest == bundle.metadata.source_contract_digest
        bindings = runtime_operation_bindings(profile.version)
        source_ids = {
            source
            for name, binding in bindings.items()
            if name.startswith("alert-plugin.")
            for source in binding.source_operations
        }
        assert {
            program.source_operation for _, program in profile.programs
        } == source_ids
        if recipe in {"transient_entity", "entity"}:
            assert dict(profile.programs)["list"].codec == "list_delivery"
            source_list = next(
                operation
                for operation in bundle.snapshot.operations
                if operation.operation_id == _LIST_OPERATION
            )
            assert source_list.logical_return_type == f"List<{_VO}>"
        page_codec = dict(profile.programs).get("page")
        if profile.version in _NULLABLE_HELPER_PAGE_VERSIONS:
            assert page_codec is not None
            assert page_codec.codec == "page_nullable_required_strict"
        elif profile.version in {"3.1.8", "3.1.9"}:
            assert page_codec is not None
            assert page_codec.codec == "page_list_strict"
        assert not source_ids.intersection(
            operation.operation_id for operation in legacy.snapshot.operations
        )
        assert not {_INSTANCE, _VO, _DEFINITION}.intersection(
            {model.import_path for model in legacy.snapshot.models}
            | {dto.import_path for dto in legacy.snapshot.dtos}
        )
        if profile.version == "3.4.3":
            source_list = next(
                operation
                for operation in bundle.snapshot.operations
                if operation.operation_id == _LIST_OPERATION
            )
            assert source_list.return_type == "Result<List<AlertPluginInstanceVO>>"
        # Shared response roots remain for other package-backed domains.
        assert any(model.import_path == _PAGE for model in legacy.snapshot.models)
        assert all(
            program.result_envelope
            == (
                "required"
                if name in {"create", "update", "delete", "test_send"}
                else "optional"
            )
            for name, program in profile.programs
        )
    assert sum(len(profile.programs) for profile in plan.profiles) == 262
    assert len(plan.codecs) == 23


def test_alert_plugin_request_shapes_keep_empty_query_and_transient_field_ownership(
    compiled_alert_plugins: CompiledDomainSet,
) -> None:
    plan = compiled_alert_plugins.plan("alert_plugin")
    requests = {request.schema: request for request in plan.requests}
    assert set(requests) == {
        "page_local_search",
        "page",
        "empty",
        "id",
        "create",
        "create_transient",
        "update",
        "update_transient",
        "test_send",
        "definition_type",
    }
    codecs = dict(plan.codecs)
    assert codecs["list"]["method"] == "GET"
    assert codecs["list"]["channel"] == "query"
    assert codecs["list"]["fields"] == []
    assert codecs["list"]["params"] == "empty"
    assert codecs["definition_get_nullable_id"]["channel"] == "path"
    assert codecs["definition_get_nullable_id"]["path_fields"] == ["id"]
    assert codecs["update_instance_transient"]["fields"] == [
        {"name": "id", "binding": "path_variable"},
        {"name": "instanceName", "binding": "request_param"},
        {"name": "warningType", "binding": "request_param"},
        {"name": "pluginInstanceParams", "binding": "request_param"},
    ]
    assert codecs["create_instance_transient"]["params"] == "create_transient"
    assert codecs["update_instance_nullable_id"]["params"] == "update"
    for schema, expected_enum in (
        ("definition_type", "PluginType"),
        ("create_transient", "WarningType"),
        ("update_transient", "WarningType"),
    ):
        request = requests[schema]
        source = request.content
        assert source is not None
        assert f"import {expected_enum} as {expected_enum}" in source
        closure = "\n".join(
            (source, *(module.content for module in request.pool_modules))
        )
        assert f"class {expected_enum}(" in closure
        assert "class PluginDefine(" not in closure
        assert "class AlertPluginInstance(" not in closure


@pytest.mark.parametrize(
    ("schema", "nullable", "row"),
    [
        ("list", False, {"id": 7, "pluginDefineId": 2}),
        (
            "list_delivery",
            False,
            {"id": 7, "pluginDefineId": 2, "warningType": "ALL"},
        ),
        ("definition_list_required_id", False, {"id": 7, "pluginName": "Email"}),
        ("definition_list_nullable_id", False, {"id": 7, "pluginName": "Email"}),
    ],
)
def test_alert_plugin_structured_list_responses_preserve_exact_nullability(
    compiled_alert_plugins: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    nullable: bool,
    row: dict[str, object],
) -> None:
    plan = compiled_alert_plugins.plan("alert_plugin")
    adapter = response_adapter(plan, schema, monkeypatch)
    assert adapter.validate_python([]) == []
    result = adapter.dump_python(adapter.validate_python([row]), exclude_none=True)
    assert isinstance(result, list)
    assert result[0]["id"] == 7
    invalid: object
    for invalid in ({}, "not a list", [3]):
        with pytest.raises(ValidationError):
            adapter.validate_python(invalid)
    if nullable:
        assert adapter.validate_python(None) is None
    else:
        with pytest.raises(ValidationError):
            adapter.validate_python(None)


def test_alert_plugin_nullable_helper_page_accepts_only_present_null_or_list(
    compiled_alert_plugins: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compiled_alert_plugins.plan("alert_plugin")
    adapter = response_adapter(
        plan,
        "page_nullable_required_strict",
        monkeypatch,
    )
    page = {"totalList": None, "total": 0, "currentPage": 1}
    assert adapter.dump_python(adapter.validate_python(page))["totalList"] is None
    assert (
        adapter.dump_python(adapter.validate_python({**page, "totalList": []}))[
            "totalList"
        ]
        == []
    )
    invalid_pages: tuple[dict[str, object], ...] = (
        {"total": 0, "currentPage": 1},
        {**page, "totalList": "not a list"},
        {**page, "totalList": {}},
        {**page, "total": True},
        {**page, "total": "1"},
        {**page, "total": 1.5},
    )
    for invalid in invalid_pages:
        with pytest.raises(ValidationError):
            adapter.validate_python(invalid)


@pytest.mark.parametrize(
    ("primitive", "replacement"),
    [
        ("page", "page_list"),
        ("list", "list"),
        ("update_baseline", None),
        ("create", "create_instance_nullable_id"),
        ("update", "update_instance_nullable_id"),
        ("definition_list", "definition_list_required_id"),
    ],
)
def test_alert_plugin_rejects_crossed_recipe_epochs(
    compiled_alert_plugins: CompiledDomainSet,
    primitive: str,
    replacement: str | None,
) -> None:
    profile = next(
        profile
        for profile in compiled_alert_plugins.plan("alert_plugin").profiles
        if profile.version == "3.2.1"
    )
    codecs = {name: program.codec for name, program in profile.programs}
    if replacement is None:
        del codecs[primitive]
    else:
        codecs[primitive] = replacement
    with pytest.raises(ValueError, match="recipe is unsupported"):
        ALERT_PLUGIN_COMPILED_DOMAIN.recipe_policy(codecs)


@pytest.mark.parametrize(
    "version",
    ["3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"],
)
def test_alert_plugin_delivery_list_rejects_nullable_payload_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], version: str
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == version
    )
    operation = next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id == _LIST_OPERATION
    )
    changed = replace(operation, logical_return_type=f"Optional<List<{_VO}>>")

    with pytest.raises(ValueError, match="list response changed"):
        compile_domains(
            replace_operation(exact_runtime_bundles, version, changed),
            (ALERT_PLUGIN_COMPILED_DOMAIN,),
        )


@pytest.mark.parametrize(
    ("model_path", "field_name", "message"),
    [
        (_VO, "warningType", "VO response epoch"),
        (_INSTANCE, "id", "instance response epoch"),
        (_DEFINITION, "id", "definition response epoch"),
        (_PAGE, "totalList", "page response epoch"),
    ],
)
def test_alert_plugin_rejects_response_field_drift(
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
        if bundle.spec.version == "3.2.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match=message):
        compile_domains(bundles, (ALERT_PLUGIN_COMPILED_DOMAIN,))


@pytest.mark.parametrize(
    "drift",
    [
        "path_type",
        "path_segments",
        "path_query",
        "empty_query",
        "form_type",
        "response_projection",
    ],
)
def test_alert_plugin_rejects_request_and_projection_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    if drift == "empty_query":
        operation_id = _LIST_OPERATION
    elif drift == "form_type":
        operation_id = "AlertPluginInstanceController.updateAlertPluginInstanceById"
    else:
        operation_id = "UiPluginController.queryUiPluginDetailById"
    operation = next(
        item for item in bundle.snapshot.operations if item.operation_id == operation_id
    )
    id_parameter = next(
        parameter
        for item in bundle.snapshot.operations
        if item.operation_id == "UiPluginController.queryUiPluginDetailById"
        for parameter in item.parameters
        if parameter.wire_name == "id"
    )
    if drift == "path_type":
        changed = replace(
            operation,
            parameters=[
                replace(parameter, java_type="String")
                if parameter.wire_name == "id"
                else parameter
                for parameter in operation.parameters
            ],
        )
        message = "reviewed integer-segment shape"
    elif drift == "path_segments":
        changed = replace(
            operation,
            path="ui-plugins/{id}/{otherId}",
            parameters=[
                *operation.parameters,
                replace(id_parameter, name="otherId", wire_name="otherId"),
            ],
        )
        # A supported transport shape still needs an exact domain declaration.
        message = "request shape has 0 matching epochs"
    elif drift in {"path_query", "empty_query"}:
        changed = replace(
            operation,
            parameters=[
                *operation.parameters,
                replace(
                    id_parameter,
                    name="extra",
                    wire_name="extra",
                    binding="request_param",
                ),
            ],
        )
        message = "request shape has 0 matching epochs"
    elif drift == "form_type":
        changed = replace(
            operation,
            parameters=[
                replace(parameter, java_type="Integer")
                if parameter.wire_name == "instanceName"
                else parameter
                for parameter in operation.parameters
            ],
        )
        message = "request schema update differs"
    else:
        changed = replace(operation, response_projection="single_data")
        message = "exchange projection changed"
    with pytest.raises(ValueError, match=message):
        compile_domains(
            replace_operation(exact_runtime_bundles, "3.4.1", changed),
            (ALERT_PLUGIN_COMPILED_DOMAIN,),
        )


def test_alert_plugin_rejects_missing_supported_source(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    item
                    for item in bundle.snapshot.operations
                    if item.operation_id != _LIST_OPERATION
                ],
            ),
        )
        if bundle.spec.version == "2.0.0"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="snapshot is missing operations"):
        compile_domains(bundles, (ALERT_PLUGIN_COMPILED_DOMAIN,))
