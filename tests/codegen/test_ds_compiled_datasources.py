"""Keep datasource compilation exact without weakening its native DTO closures."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import (
    load_schema_pool,
    replace_operation,
    response_adapter,
    single_version_bundles,
)

from ds_codegen.compiled_datasources import DATASOURCE_COMPILED_DOMAIN
from ds_codegen.compiled_domains import compile_domains
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.runtime_contract import runtime_operation_bindings
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainPlan, CompiledDomainSet
    from ds_codegen.ir import OperationSpec
    from ds_codegen.runtime_bundles import RuntimeBundle
    from dsctl.support.json_types import JsonObject, JsonValue

pytestmark = pytest.mark.source_contract
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ENTITY = "org.apache.dolphinscheduler.dao.entity.DataSource"
_COMMON_DTO = "org.apache.dolphinscheduler.common.datasource.BaseDataSourceParamDTO"
_PLUGIN_DTO = (
    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
    "BaseDataSourceParamDTO"
)
_LEGACY_DETAIL = "generated.view.DataSourceService_queryDataSource_map"


def _compile(bundles: tuple[RuntimeBundle, ...]) -> CompiledDomainSet:
    return compile_domains(
        bundles,
        (DATASOURCE_COMPILED_DOMAIN,),
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )


@pytest.fixture(scope="module")
def compiled_datasources(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return _compile(exact_runtime_bundles)


def test_all_exact_datasource_sources_are_owned_without_retiring_permission_reads(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_datasources: CompiledDomainSet,
) -> None:
    plan = compiled_datasources.plan("datasource")
    assert len(plan.requests) == 12
    assert len(plan.responses) == 21
    assert len(plan.scalar_responses) == 1
    assert len(plan.codecs) == 40
    assert len(plan.definition.primitives) == 8
    expected_recipes = (
        ("legacy_form",)
        + ("typed_body",) * 17
        + ("json_void",) * 11
        + ("json_entity",) * 8
    )
    assert tuple(profile.recipe_id for profile in plan.profiles) == expected_recipes
    codecs = dict(plan.codecs)
    for original, profile, remaining in zip(
        exact_runtime_bundles,
        plan.profiles,
        compiled_datasources.legacy_bundles,
        strict=True,
    ):
        assert profile.status == "supported"
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        programs = dict(profile.programs)
        special = (
            {"get_legacy", "delete_legacy"}
            if profile.version == "1.3.9"
            else {"get", "delete"}
        )
        assert (
            set(programs) == {"page", "create", "update", "connection_test"} | special
        )
        native = {program.source_operation for program in programs.values()}
        bindings = runtime_operation_bindings(profile.version)
        evidence = {
            source
            for name, binding in bindings.items()
            if name.startswith("datasource.")
            for source in binding.source_operations
        }
        assert native == evidence
        assert len(native) == 6
        assert not native.intersection(
            op.operation_id for op in remaining.snapshot.operations
        )
        permission_sources = {
            operation.operation_id
            for operation in original.snapshot.operations
            if operation.controller == "DataSourceController"
        } - native
        assert len(permission_sources) == 2
        user_sources = {
            source
            for name, binding in bindings.items()
            if name.startswith("user.")
            for source in binding.source_operations
        }
        assert permission_sources <= user_sources
        assert permission_sources <= {
            op.operation_id for op in remaining.snapshot.operations
        }
        model_paths = {model.import_path for model in remaining.snapshot.models}
        if profile.version == "3.4.3":
            summary = "org.apache.dolphinscheduler.api.vo.DataSourceSimpleInfoVO"
            assert permission_sources == {
                "DataSourceController.getAuthorizedDatasourceList",
                "DataSourceController.getUnauthorizedDatasourceList",
            }
            assert {
                operation.logical_return_type
                for operation in original.snapshot.operations
                if operation.operation_id in permission_sources
            } == {f"List<{summary}>"}
            assert summary in model_paths
            assert _ENTITY not in model_paths
        else:
            assert _ENTITY in model_paths
        assert not {_COMMON_DTO, _PLUGIN_DTO, _LEGACY_DETAIL}.intersection(
            model.import_path for model in remaining.snapshot.models
        )
        for program in programs.values():
            codec = codecs[program.codec]
            assert codec["response_projection"] == "direct"
            assert program.result_envelope == (
                "optional" if codec["method"] == "GET" else "required"
            )


@pytest.mark.parametrize(
    "schema",
    [
        "create_common",
        "update_common",
        "create_spi_10",
        "update_spi_10",
        "create_spi_11",
        "update_spi_11",
    ],
)
def test_typed_body_request_preserves_exact_extra_policy_and_independent_path_identity(
    compiled_datasources: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    model = _request_model(compiled_datasources.plan("datasource"), schema, monkeypatch)
    body: JsonObject = {
        "id": 91,
        "name": "native",
        "type": "MYSQL",
        "port": "3306",
        "note": None,
        "other": {"literal": "value"},
        "extension": {"nested": None},
    }
    args: JsonObject = {"dataSourceParam": body}
    updating = schema.startswith("update_")
    if updating:
        args["id"] = 7
    dumped = model.model_validate(args).model_dump(
        mode="json", by_alias=True, exclude_none=True, exclude_unset=True
    )
    expected = {**body, "port": 3306}
    expected.pop("note")
    if schema.endswith("common"):
        expected.pop("extension")
    assert dumped["dataSourceParam"] == expected
    if updating:
        assert dumped["id"] == 7
    for invalid in (
        {},
        {**args, "dataSourceParam": None},
        {**args, "dataSourceParam": "{}"},
    ):
        with pytest.raises(ValidationError):
            model.model_validate(invalid)
    with pytest.raises(ValidationError, match="type"):
        model.model_validate({**args, "dataSourceParam": {"type": "SSH"}})


@pytest.mark.parametrize("schema", ["create_text", "update_text"])
def test_raw_body_requests_keep_original_string_and_reject_objects(
    compiled_datasources: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    model = _request_model(compiled_datasources.plan("datasource"), schema, monkeypatch)
    args: JsonObject = {"id": 7} if schema == "update_text" else {}
    for text in ("", "null", " \t\n", ' {"名称":null} \n'):
        assert (
            model.model_validate({**args, "jsonStr": text}).model_dump()["jsonStr"]
            == text
        )
    invalid_bodies: tuple[JsonValue, ...] = (None, {}, 1)
    for body in invalid_bodies:
        with pytest.raises(ValidationError, match="jsonStr"):
            model.model_validate({**args, "jsonStr": body})


@pytest.mark.parametrize("schema", ["create_legacy", "update_legacy"])
def test_legacy_form_keeps_required_strings_and_native_enums(
    compiled_datasources: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    model = _request_model(compiled_datasources.plan("datasource"), schema, monkeypatch)
    args: JsonObject = {
        "name": "native",
        "type": "MYSQL",
        "host": "localhost",
        "port": "3306",
        "database": "native",
        "principal": "",
        "userName": "",
        "password": "",
        "connectType": "ORACLE_SERVICE_NAME",
        "other": "{}",
    }
    if schema == "update_legacy":
        args["id"] = 7
    dumped = model.model_validate(args).model_dump(mode="json", exclude_unset=True)
    assert dumped == args
    for field in ("principal", "password", "connectType"):
        with pytest.raises(ValidationError, match=field):
            model.model_validate(
                {key: value for key, value in args.items() if key != field}
            )
    with pytest.raises(ValidationError, match="port"):
        model.model_validate({**args, "port": 3306})


@pytest.mark.parametrize(
    ("epoch", "zero_id", "strict", "type_name"),
    [
        ("legacy", True, False, "H2"),
        ("common", True, True, "PRESTO"),
        ("spi_10", True, True, "PRESTO"),
        ("spi_11", True, True, "REDSHIFT"),
        ("spi_12", False, True, "ATHENA"),
        ("spi_24", False, False, "DORIS"),
        ("spi_26", False, False, "SAGEMAKER"),
        ("spi_26_named", False, False, "SAGEMAKER"),
        ("spi_29", False, False, "DOLPHINDB"),
    ],
)
def test_response_pages_keep_defaults_strict_integers_and_complete_type_catalog(
    compiled_datasources: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    epoch: str,
    *,
    zero_id: bool,
    strict: bool,
    type_name: str,
) -> None:
    adapter = response_adapter(
        compiled_datasources.plan("datasource"), f"page_{epoch}", monkeypatch
    )
    dumped = adapter.dump_python(
        adapter.validate_python({"totalList": [{"type": type_name}]}), mode="json"
    )
    assert isinstance(dumped, dict)
    assert dumped["totalList"][0]["id"] == (0 if zero_id else None)
    assert dumped["totalList"][0]["userId"] == 0
    assert dumped["totalList"][0]["type"] == type_name
    defaults = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(defaults, dict)
    assert defaults["totalList"] == (None if zero_id else [])
    if epoch == "legacy":
        assert "pageSize" not in defaults
    else:
        assert defaults["pageSize"] == 20
    if strict:
        with pytest.raises(ValidationError, match="total"):
            adapter.validate_python({"total": "2"})
    else:
        coerced = adapter.dump_python(adapter.validate_python({"total": "2"}))
        assert isinstance(coerced, dict)
        assert coerced["total"] == 2
    with pytest.raises(ValidationError, match="type"):
        adapter.validate_python({"totalList": [{"type": "UNKNOWN"}]})


@pytest.mark.parametrize(
    "epoch",
    [
        "legacy",
        "common",
        "spi_10",
        "spi_11",
        "spi_12",
        "spi_24",
        "spi_26",
        "spi_26_named",
        "spi_29",
    ],
)
def test_detail_models_preserve_native_extension_and_legacy_port_behavior(
    compiled_datasources: CompiledDomainSet, monkeypatch: pytest.MonkeyPatch, epoch: str
) -> None:
    adapter = response_adapter(
        compiled_datasources.plan("datasource"), f"get_{epoch}", monkeypatch
    )
    value = {"name": "native", "port": "3306", "extension": {"nested": None}}
    dumped = adapter.dump_python(
        adapter.validate_python(value),
        mode="json",
        exclude_none=True,
        exclude_unset=True,
    )
    assert isinstance(dumped, dict)
    assert dumped["port"] == ("3306" if epoch == "legacy" else 3306)
    if epoch in {"legacy", "common"}:
        assert "extension" not in dumped
    else:
        assert dumped["extension"] == {"nested": None}
    with pytest.raises(ValidationError):
        adapter.validate_python(None)


@pytest.mark.parametrize("epoch", ["spi_26", "spi_26_named", "spi_29"])
def test_entity_mutations_keep_nullable_identity(
    compiled_datasources: CompiledDomainSet, monkeypatch: pytest.MonkeyPatch, epoch: str
) -> None:
    adapter = response_adapter(
        compiled_datasources.plan("datasource"), f"entity_{epoch}", monkeypatch
    )
    dumped = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(dumped, dict)
    assert dumped["id"] is None
    assert dumped["userId"] == 0
    with pytest.raises(ValidationError):
        adapter.validate_python(None)


@pytest.mark.parametrize(
    ("version", "method", "return_type"),
    [
        ("1.3.9", "delete", "Boolean"),
        ("2.0.0", "createDataSource", _ENTITY),
        ("3.2.0", "updateDataSource", _ENTITY),
        ("3.2.1", "createDataSource", "Void"),
        ("3.2.1", "connectionTest", "Void"),
    ],
)
def test_response_epochs_cannot_be_crossed(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    method: str,
    return_type: str,
) -> None:
    bundles = single_version_bundles(exact_runtime_bundles, version)
    operation = _operation(bundles, version, method)
    changed = replace(operation, logical_return_type=return_type)
    with pytest.raises(ValueError, match=r"response (type )?changed"):
        _compile(replace_operation(bundles, version, changed))


@pytest.mark.parametrize(
    ("version", "method", "field"),
    [
        ("1.3.9", "createDataSource", "password"),
        ("2.0.0", "createDataSource", "dataSourceParam"),
        ("3.1.0", "updateDataSource", "jsonStr"),
        ("3.4.2", "createDataSource", "jsonStr"),
    ],
)
def test_request_requiredness_drift_is_not_silently_recompiled(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    method: str,
    field: str,
) -> None:
    bundles = single_version_bundles(exact_runtime_bundles, version)
    operation = _operation(bundles, version, method)
    changed = replace(
        operation,
        parameters=[
            replace(item, required=False) if item.wire_name == field else item
            for item in operation.parameters
        ],
    )
    with pytest.raises(ValueError, match=r"request shape|mandatory"):
        _compile(replace_operation(bundles, version, changed))


@pytest.mark.parametrize(
    ("version", "model_path", "field"),
    [
        ("1.3.9", _LEGACY_DETAIL, "port"),
        ("2.0.0", _COMMON_DTO, "port"),
        ("3.1.0", _PAGE, "total"),
        ("3.4.2", _ENTITY, "userId"),
    ],
)
def test_model_field_and_default_drift_fails_closed(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    model_path: str,
    field: str,
) -> None:
    changed = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                models=[
                    replace(
                        model,
                        fields=[
                            replace(item, default_value="17")
                            if item.wire_name == field
                            else item
                            for item in model.fields
                        ],
                    )
                    if model.import_path == model_path
                    else model
                    for model in bundle.snapshot.models
                ],
            ),
        )
        for bundle in single_version_bundles(exact_runtime_bundles, version)
    )
    with pytest.raises(ValueError, match=r"fields|schema .* differs"):
        _compile(changed)


@pytest.mark.parametrize(
    ("version", "enum_name"), [("1.3.9", "DbConnectType"), ("3.4.2", "DbType")]
)
def test_enum_constructor_and_symbolic_serialization_remain_reviewed(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], version: str, enum_name: str
) -> None:
    changed = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                enums=[
                    replace(enum, json_value_field="code")
                    if enum.name == enum_name
                    else enum
                    for enum in bundle.snapshot.enums
                ],
            ),
        )
        for bundle in single_version_bundles(exact_runtime_bundles, version)
    )
    with pytest.raises(ValueError, match="closure changed"):
        _compile(changed)


def test_recipe_rejects_partial_or_crossed_native_programs(
    compiled_datasources: CompiledDomainSet,
) -> None:
    plan = compiled_datasources.plan("datasource")
    modern = {name: program.codec for name, program in plan.profiles[-1].programs}
    for changed in (
        {**modern, "get": "get_common"},
        {key: value for key, value in modern.items() if key != "delete"},
    ):
        with pytest.raises(ValueError, match="recipe is unsupported"):
            plan.definition.recipe_policy(changed)


def _operation(
    bundles: tuple[RuntimeBundle, ...], version: str, method: str
) -> OperationSpec:
    bundle = next(item for item in bundles if item.spec.version == version)
    return next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id == f"DataSourceController.{method}"
    )


def _request_model(
    plan: CompiledDomainPlan, schema: str, monkeypatch: pytest.MonkeyPatch
) -> type[BaseParamsModel]:
    request = next(item for item in plan.requests if item.schema == schema)
    name = f"dsctl.generated.wire_programs._test_datasource_{schema}"
    load_schema_pool(request.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if request.module_name is not None:
        module.__package__ += f".{request.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - execute only trusted deterministic compiler output
        compile(request.content or request.source, f"<{name}>", "exec"), module.__dict__
    )
    return cast("type[BaseParamsModel]", getattr(module, request.class_name))
