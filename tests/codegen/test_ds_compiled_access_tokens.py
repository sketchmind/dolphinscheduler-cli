from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, TypeAdapter, ValidationError
from tests.codegen.compiled_support import (
    load_schema_pool,
    replace_operation,
    single_version_bundles,
)

from ds_codegen.compiled_access_tokens import ACCESS_TOKEN_COMPILED_DOMAIN
from ds_codegen.compiled_domains import compile_domains
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainPlan, CompiledDomainSet
    from ds_codegen.ir import OperationSpec
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_TOKEN_MODEL = "org.apache.dolphinscheduler.dao.entity.AccessToken"
_USER_MODEL = "org.apache.dolphinscheduler.dao.entity.User"
_PAGE_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"


def _compile(bundles: tuple[RuntimeBundle, ...]) -> CompiledDomainSet:
    return compile_domains(
        bundles,
        (ACCESS_TOKEN_COMPILED_DOMAIN,),
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )


@pytest.fixture(scope="module")
def compiled_access_tokens(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return _compile(exact_runtime_bundles)


def test_access_token_compiles_all_profiles_without_owning_user_lookup(
    compiled_access_tokens: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_access_tokens.plan("access_token")
    epochs = (
        (("1.3.9",), "legacy_139"),
        (("2.0.0", "2.0.1"), "legacy_200"),
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
            "entity_void",
        ),
        (
            ("3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
            "entity_bool",
        ),
    )
    expected = {version: recipe for versions, recipe in epochs for version in versions}
    assert tuple(profile.version for profile in plan.profiles) == tuple(expected)
    assert len(plan.requests) == 7
    assert len(plan.responses) == 6
    assert len(plan.codecs) == 17
    assert {response.annotation for response in plan.scalar_responses} == {
        "str",
        "bool",
    }
    for original, profile, legacy in zip(
        exact_runtime_bundles,
        plan.profiles,
        compiled_access_tokens.legacy_bundles,
        strict=True,
    ):
        assert profile.status == "supported"
        assert profile.recipe_id == expected[profile.version]
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        programs = dict(profile.programs)
        assert set(programs) == {"list", "create", "update", "delete", "generate"}
        bindings = {
            **runtime_operation_bindings(profile.version),
            **runtime_auxiliary_operation_bindings(profile.version),
        }
        user_lookup = "user.identity" if profile.version == "3.4.3" else "user.get"
        user_sources = set(bindings[user_lookup].source_operations)
        if profile.version == "3.4.3":
            assert user_sources == {
                "UsersController.getUserInfo",
                "UsersController.listAll",
                "UsersController.queryUserList",
            }
        source_evidence = {
            source
            for name, binding in bindings.items()
            if name.startswith("access-token.")
            for source in binding.source_operations
        }
        owned_sources = {program.source_operation for program in programs.values()}
        assert source_evidence == owned_sources | user_sources
        assert not owned_sources & user_sources
        retained_sources = {
            operation.operation_id for operation in legacy.snapshot.operations
        }
        assert not owned_sources & retained_sources
        assert user_sources <= retained_sources
        assert user_sources <= {
            operation.operation_id for operation in original.snapshot.operations
        }
        for action in ("create", "update", "generate"):
            assert user_sources <= set(
                bindings[f"access-token.{action}"].source_operations
            )
        model_paths = {model.import_path for model in legacy.snapshot.models}
        assert _TOKEN_MODEL not in model_paths
        assert {_USER_MODEL, _PAGE_MODEL} <= model_paths
        if profile.version == "3.4.3":
            assert "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO" in model_paths
        assert programs["list"].result_envelope == "optional"
        assert all(
            program.result_envelope == "required"
            for name, program in programs.items()
            if name != "list"
        )


def test_access_token_schema_epochs_preserve_route_and_field_ownership(
    compiled_access_tokens: CompiledDomainSet,
) -> None:
    plan = compiled_access_tokens.plan("access_token")
    codecs = dict(plan.codecs)
    assert {request.schema for request in plan.requests} == {
        "list",
        "create_required",
        "create",
        "update_required",
        "update",
        "id",
        "generate",
    }
    for codec in ("create_legacy_void", "create_void"):
        assert codecs[codec]["params"] == "create_required"
    assert codecs["create_entity_required_id"]["params"] == "create"
    assert codecs["update_legacy_void"]["channel"] == "form"
    assert codecs["update_void"]["channel"] == "path_form"
    assert (
        codecs["update_legacy_void"]["request_schema_digest"]
        == codecs["update_void"]["request_schema_digest"]
    )
    assert codecs["update_entity_nullable_id"]["fields"] == [
        {"name": "id", "binding": "path_variable"},
        {"name": "userId", "binding": "request_param"},
        {"name": "expireTime", "binding": "request_param"},
        {"name": "token", "binding": "request_param"},
    ]
    assert codecs["delete_legacy_void"]["method"] == "POST"
    assert codecs["delete_void"]["method"] == "DELETE"
    assert codecs["generate_legacy"]["path"] == "access-token/generate"
    assert codecs["generate"]["path"] == "access-tokens/generate"
    assert codecs["generate"]["method"] == "POST"


@pytest.mark.parametrize("primitive", ["create", "update"])
def test_access_token_generated_requests_keep_required_token_and_modern_omission(
    compiled_access_tokens: CompiledDomainSet,
    primitive: str,
) -> None:
    plan = compiled_access_tokens.plan("access_token")
    required = _request_model(plan, f"{primitive}_required")
    optional = _request_model(plan, primitive)
    fields = {"userId": "7", "expireTime": "2030-01-01 00:00:00"}
    if primitive == "update":
        fields["id"] = "11"
    for missing in (fields, {**fields, "token": None}):
        with pytest.raises(ValidationError, match="token"):
            required.model_validate(missing)
        modern = optional.model_validate(missing).model_dump(
            exclude_none=True, exclude_unset=True
        )
        assert modern == {
            name: int(value) if name in {"id", "userId"} else value
            for name, value in fields.items()
        }
    for model in (required, optional):
        supplied = model.model_validate({**fields, "token": "literal-token"})
        assert supplied.model_dump()["token"] == "literal-token"
        with pytest.raises(ValidationError, match="token"):
            model.model_validate({**fields, "token": 17})


def _request_model(plan: CompiledDomainPlan, schema: str) -> type[BaseParamsModel]:
    request = next(item for item in plan.requests if item.schema == schema)
    namespace: dict[str, object] = {"BaseParamsModel": BaseParamsModel, "Field": Field}
    exec(compile(request.source, f"<{schema}>", "exec"), namespace)  # noqa: S102
    return cast("type[BaseParamsModel]", namespace[request.class_name])


@pytest.mark.parametrize(
    ("schema", "required_id", "strict"),
    [
        ("list_legacy", True, False),
        ("list_required_id_strict", True, True),
        ("list_nullable_id_strict", False, True),
        ("list_nullable_id", False, False),
    ],
)
def test_access_token_page_defaults_and_integer_validation_remain_exact(
    compiled_access_tokens: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    required_id: bool,
    strict: bool,
) -> None:
    plan = compiled_access_tokens.plan("access_token")
    response = next(item for item in plan.responses if item.schema == schema)
    load_schema_pool(response.pool_modules, monkeypatch)
    module_name = f"dsctl.generated.wire_programs._test_{response.module_name}"
    module = ModuleType(module_name)
    module.__package__ = (
        f"dsctl.generated.wire_programs.{response.module_name.rpartition('.')[0]}"
    )
    monkeypatch.setitem(sys.modules, module_name, module)
    exec(compile(response.content, f"<{module_name}>", "exec"), module.__dict__)  # noqa: S102
    adapter: TypeAdapter[object] = TypeAdapter(module.RESPONSE_TYPE)
    result = adapter.dump_python(adapter.validate_python({"totalList": [{}]}))
    assert isinstance(result, dict)
    assert result["totalList"][0]["id"] == (0 if required_id else None)
    assert result["totalList"][0]["userId"] == 0
    assert result["totalList"][0]["token"] is None
    assert result["currentPage"] == result["total"] == 0
    defaults = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(defaults, dict)
    assert defaults["totalList"] == (None if required_id else [])
    if schema == "list_legacy":
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


@pytest.mark.parametrize(
    ("version", "primitive"),
    [
        ("2.0.0", "create"),
        ("2.0.0", "update"),
        ("2.0.9", "create"),
        ("2.0.9", "update"),
    ],
)
def test_access_token_rejects_crossed_token_requirement_and_response_epoch(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    primitive: str,
) -> None:
    bundles = single_version_bundles(exact_runtime_bundles, version)
    operation = _operation(bundles, version, f"{primitive}Token")
    changed = replace(
        operation,
        parameters=[
            replace(parameter, required=version != "2.0.0")
            if parameter.wire_name == "token"
            else parameter
            for parameter in operation.parameters
        ],
    )
    with pytest.raises(
        ValueError, match="token requirement and response epoch changed"
    ):
        _compile(replace_operation(bundles, version, changed))


@pytest.mark.parametrize(
    "drift", ["request_default", "request_type", "path_binding", "generate_response"]
)
def test_access_token_rejects_request_and_scalar_response_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    version = "2.0.9"
    operation = _operation(
        exact_runtime_bundles,
        version,
        "generateToken" if drift == "generate_response" else "updateToken",
    )
    if drift == "generate_response":
        changed = replace(operation, logical_return_type="Boolean")
        message = "generate response changed"
    elif drift == "path_binding":
        changed = replace(
            operation,
            parameters=[
                replace(parameter, binding="request_param")
                if parameter.wire_name == "id"
                else parameter
                for parameter in operation.parameters
            ],
        )
        message = "implicit path arguments are unsupported"
    else:
        token = next(
            parameter
            for parameter in operation.parameters
            if parameter.wire_name == "token"
        )
        if drift == "request_default":
            changed_token = replace(token, default_value="'placeholder'")
        else:
            changed_token = replace(token, java_type="Integer")
        changed = replace(
            operation,
            parameters=[
                changed_token if parameter.wire_name == "token" else parameter
                for parameter in operation.parameters
            ],
        )
        message = r"request type or default changed|request schema .* differs"
    with pytest.raises(ValueError, match=message):
        _compile(replace_operation(exact_runtime_bundles, version, changed))


def _operation(
    bundles: tuple[RuntimeBundle, ...], version: str, method: str
) -> OperationSpec:
    return next(
        operation
        for bundle in bundles
        if bundle.spec.version == version
        for operation in bundle.snapshot.operations
        if operation.operation_id == f"AccessTokenController.{method}"
    )


@pytest.mark.parametrize(
    ("model_path", "field_name"), [(_TOKEN_MODEL, "id"), (_PAGE_MODEL, "totalList")]
)
def test_access_token_rejects_response_field_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    model_path: str,
    field_name: str,
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
        for bundle in single_version_bundles(exact_runtime_bundles, "3.2.1")
    )
    with pytest.raises(ValueError, match="response epoch changed"):
        _compile(bundles)


@pytest.mark.parametrize(
    ("primitive", "codec"),
    [
        ("list", "list_nullable_id_strict"),
        ("create", "create_void"),
        ("generate", "generate_legacy"),
    ],
)
def test_access_token_rejects_crossed_recipe_epochs(
    compiled_access_tokens: CompiledDomainSet,
    primitive: str,
    codec: str,
) -> None:
    profile = compiled_access_tokens.plan("access_token").profiles[-1]
    codecs = {name: program.codec for name, program in profile.programs}
    codecs[primitive] = codec
    with pytest.raises(ValueError, match="recipe is unsupported"):
        ACCESS_TOKEN_COMPILED_DOMAIN.recipe_policy(codecs)
