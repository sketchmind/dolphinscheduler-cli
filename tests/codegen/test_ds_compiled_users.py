from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import (
    replace_operation,
    response_adapter,
    single_version_bundles,
)

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_users import USER_COMPILED_DOMAIN
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.runtime_contract import runtime_operation_bindings
from ds_codegen.security_contract import security_contract
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.ir import OperationSpec
    from ds_codegen.runtime_bundles import RuntimeBundle
    from ds_codegen.security_contract import SecurityVersionContract

pytestmark = pytest.mark.source_contract

_USER = "org.apache.dolphinscheduler.dao.entity.User"
_PROJECT = "org.apache.dolphinscheduler.dao.entity.Project"
_DATASOURCE = "org.apache.dolphinscheduler.dao.entity.DataSource"
_NAMESPACE = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_DB_TYPE = "org.apache.dolphinscheduler.spi.enums.DbType"
_USER_TYPE = "org.apache.dolphinscheduler.common.enums.UserType"


def _compile(bundles: tuple[RuntimeBundle, ...]) -> CompiledDomainSet:
    return compile_domains(
        bundles,
        (USER_COMPILED_DOMAIN,),
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )


@pytest.fixture(scope="module")
def compiled_users(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return _compile(exact_runtime_bundles)


def test_user_and_identity_share_one_complete_local_owner(
    compiled_users: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_users.plan("user")
    expected_recipes = (
        "legacy_139",
        *("legacy_200",) * 2,
        *("state_entity",) * 8,
        *("timezone_entity",) * 17,
        "optional_void",
        *("optional_entity",) * 7,
        "optional_entity_simple_identity",
    )
    assert tuple(profile.recipe_id for profile in plan.profiles) == expected_recipes
    assert sum(len(profile.programs) for profile in plan.profiles) == 593
    assert len(plan.requests) == 14
    assert not plan.scalar_responses
    for original, profile, legacy in zip(
        exact_runtime_bundles, plan.profiles, compiled_users.legacy_bundles, strict=True
    ):
        assert profile.status == "supported"
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        bindings = runtime_operation_bindings(profile.version)
        owned = {
            source
            for semantic, binding in bindings.items()
            if semantic.startswith("user.") or semantic == "identity.current"
            for source in binding.source_operations
            if not source.startswith("TenantController.")
        }
        programs = dict(profile.programs)
        assert owned == {program.source_operation for program in programs.values()}
        assert programs["current"].source_operation == "UsersController.getUserInfo"
        recipe = security_contract(profile.version).user
        assert ("project_revoke" in programs) == (
            recipe.project_revoke in {"by-code", "by-id"}
        )
        assert ("namespace_grant" in programs) == (
            recipe.namespace_support == "supported"
        )
        assert (
            programs["datasource_authorized"].source_operation
            == recipe.datasource_authorized_operation
        )
        assert (
            programs["datasource_unauthorized"].source_operation
            == recipe.datasource_unauthorized_operation
        )
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        assert not owned & remaining
        assert not any(source.startswith("UsersController.") for source in remaining)
        assert set(bindings["tenant.page"].source_operations) <= remaining
        assert set(bindings["datasource.get"].source_operations) <= remaining
        assert set(bindings["project.get"].source_operations) <= remaining
        for primitive, program in programs.items():
            mutation = primitive in {
                "create",
                "update",
                "delete",
                "project_revoke",
            } or primitive.endswith("_grant")
            assert program.result_envelope == ("required" if mutation else "optional")


def test_user_composes_with_tenant_access_token_and_namespace(
    compiled_users: CompiledDomainSet,
    compiled_all_domains: CompiledDomainSet,
) -> None:
    combined = compiled_all_domains
    assert combined.plan("user") == compiled_users.plan("user")
    for bundle in combined.legacy_bundles:
        assert not any(
            operation.controller in {"UsersController", "K8sNamespaceController"}
            for operation in bundle.snapshot.operations
        )
        models = {model.import_path for model in bundle.snapshot.models}
        assert {_NAMESPACE, _PROJECT, _DATASOURCE}.isdisjoint(models)


def test_user_request_models_preserve_defaults_requiredness_and_extra_rejection(
    compiled_users: CompiledDomainSet,
) -> None:
    plan = compiled_users.plan("user")
    scope = {"BaseParamsModel": BaseParamsModel, "Field": Field}
    for request in plan.requests:
        assert request.module_name is None
        exec(compile(request.source, "<user-request>", "exec"), scope)  # noqa: S102
    create = cast("type[BaseParamsModel]", scope["UserCreateStateParams"])
    params = create.model_validate(
        {
            "userName": "alice",
            "userPassword": "password",
            "tenantId": 3,
            "email": "a@example.test",
        }
    )
    fields = params.model_dump(exclude_none=True)
    assert fields == {
        "userName": "alice",
        "userPassword": "password",
        "tenantId": 3,
        "queue": "",
        "email": "a@example.test",
    }
    for field in ("userName", "userPassword", "tenantId", "email"):
        values = fields.copy()
        del values[field]
        with pytest.raises(ValidationError, match=field):
            create.model_validate(values)
    with pytest.raises(ValidationError, match="timeZone"):
        create.model_validate({**fields, "timeZone": "UTC"})
    update = cast("type[BaseParamsModel]", scope["UserUpdateTimezoneParams"])
    assert (
        update.model_validate(
            {**fields, "id": 4, "state": 1, "timeZone": "UTC"}
        ).model_dump()["timeZone"]
        == "UTC"
    )
    permission = cast("type[BaseParamsModel]", scope["UserPermissionParams"])
    assert permission.model_validate({"userId": 9}).model_dump() == {"userId": 9}
    with pytest.raises(ValidationError, match="userId"):
        permission.model_validate({})
    empty = cast("type[BaseParamsModel]", scope["UserEmptyParams"])
    assert empty.model_validate({}).model_dump() == {}
    with pytest.raises(ValidationError, match="userId"):
        empty.model_validate({"userId": 9})


@pytest.mark.parametrize(
    ("schema", "identifier", "has_state", "has_timezone"),
    [
        ("entity_legacy", 0, False, False),
        ("entity_state", 0, True, False),
        ("entity_timezone", 0, True, True),
        ("entity_timezone_nullable", None, True, True),
    ],
)
def test_user_current_and_management_preserve_full_entity_fields(
    compiled_users: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    identifier: int | None,
    *,
    has_state: bool,
    has_timezone: bool,
) -> None:
    adapter = response_adapter(compiled_users.plan("user"), schema, monkeypatch)
    data = adapter.dump_python(
        adapter.validate_python(
            {"userType": "ADMIN_USER", "userPassword": "native", "future": {"x": 1}}
        ),
        mode="json",
    )
    assert isinstance(data, dict)
    assert data["id"] == identifier
    assert data["userType"] == "ADMIN_USER"
    assert data["userPassword"] == "native"
    assert "future" not in data
    assert ("state" in data) == has_state
    assert ("timeZone" in data) == has_timezone
    assert ("tenantName" in data) == (not has_state)
    with pytest.raises(ValidationError, match="userType"):
        adapter.validate_python({"userType": "UNKNOWN_USER"})


@pytest.mark.parametrize(
    ("schema", "name_field"),
    [("user_identity", "userName"), ("datasource_identity", "name")],
)
def test_343_identity_vo_codecs_project_only_native_identity_fields(
    compiled_users: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    name_field: str,
) -> None:
    adapter = response_adapter(compiled_users.plan("user"), schema, monkeypatch)
    assert adapter.dump_python(
        adapter.validate_python(
            [{"id": 7, name_field: "selected", "email": "extra", "type": "MYSQL"}]
        ),
        mode="json",
    ) == [{"id": 7, name_field: "selected"}]
    assert adapter.dump_python(adapter.validate_python([{}]), mode="json") == [
        {"id": None, name_field: None}
    ]
    with pytest.raises(ValidationError, match="id"):
        adapter.validate_python([{"id": "not-an-id", name_field: "selected"}])


@pytest.mark.parametrize(
    ("schema", "strict", "nullable_list"),
    [
        ("page_legacy_legacy", False, True),
        ("page_state_nullable_strict", True, True),
        ("page_timezone_nullable_strict", True, True),
        ("page_timezone_nullable_list_strict", True, False),
        ("page_timezone_nullable_list", False, False),
    ],
)
def test_user_page_keeps_exact_defaults_and_integer_policy(
    compiled_users: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    strict: bool,
    nullable_list: bool,
) -> None:
    adapter = response_adapter(compiled_users.plan("user"), schema, monkeypatch)
    defaults = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(defaults, dict)
    assert defaults["totalList"] == (None if nullable_list else [])
    assert defaults["total"] == 0
    if strict:
        with pytest.raises(ValidationError, match="total"):
            adapter.validate_python({"total": "2"})
    else:
        data = adapter.dump_python(adapter.validate_python({"total": "2"}))
        assert isinstance(data, dict)
        assert data["total"] == 2


def test_user_permission_lists_and_optional_create_keep_distinct_roots(
    compiled_users: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compiled_users.plan("user")
    for schema in (
        "list_legacy",
        "list_timezone_nullable",
        "project_id",
        "project_code_compact",
        "namespace_k8s_quota",
        "namespace_cluster",
    ):
        adapter = response_adapter(plan, schema, monkeypatch)
        assert adapter.validate_python([]) == []
        invalid: object
        for invalid in (None, {}, [3]):
            with pytest.raises(ValidationError):
                adapter.validate_python(invalid)
    optional = response_adapter(plan, "optional_entity_timezone_nullable", monkeypatch)
    assert optional.validate_python(None) is None
    assert optional.validate_python({"id": 3}) is not None
    entity = response_adapter(plan, "entity_timezone_nullable", monkeypatch)
    with pytest.raises(ValidationError):
        entity.validate_python(None)


def test_user_datasource_permission_response_keeps_exact_enum_closure(
    compiled_users: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compiled_users.plan("user")
    oldest = response_adapter(plan, "datasource_legacy_required", monkeypatch)
    latest = response_adapter(plan, "datasource_spi_29_3_nullable", monkeypatch)
    with pytest.raises(ValidationError, match="type"):
        oldest.validate_python([{"type": "K8S"}])
    result = latest.dump_python(
        latest.validate_python([{"type": "K8S", "connectionParams": "native"}]),
        mode="json",
    )
    assert isinstance(result, list)
    assert result[0]["type"] == "K8S"
    assert result[0]["connectionParams"] == "native"
    assert result[0]["id"] is None
    with pytest.raises(ValidationError, match="type"):
        latest.validate_python([{"type": "FUTURE_DB"}])


@pytest.mark.parametrize(
    "drift",
    [
        "state",
        "timezone",
        "create_result",
        "update_result",
        "project_identity",
        "project_revoke",
        "datasource_alias",
    ],
)
def test_user_rejects_security_recipe_and_wire_disagreement(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    contract = security_contract("3.2.0")
    recipe = contract.user
    if drift == "state":
        recipe = replace(recipe, has_state=False)
    elif drift == "timezone":
        recipe = replace(recipe, has_time_zone=False)
    elif drift == "create_result":
        recipe = replace(recipe, create_result="none")
    elif drift == "update_result":
        recipe = replace(recipe, update_result="entity")
    elif drift == "project_identity":
        recipe = replace(recipe, project_identity="id")
    elif drift == "project_revoke":
        recipe = replace(recipe, project_revoke="by-code")
    else:
        recipe = replace(
            recipe,
            datasource_unauthorized_operation="DataSourceController.unauthDatasource",
        )

    def reviewed(version: str) -> SecurityVersionContract:
        return (
            replace(contract, user=recipe)
            if version == "3.2.0"
            else security_contract(version)
        )

    monkeypatch.setattr("ds_codegen.compiled_users.security_contract", reviewed)
    with pytest.raises(ValueError, match="compiled user"):
        _compile(single_version_bundles(exact_runtime_bundles, "3.2.0"))


@pytest.mark.parametrize(
    "drift",
    [
        "route",
        "method",
        "required",
        "default",
        "type",
        "result",
        "projection",
        "extra_field",
    ],
)
def test_user_rejects_request_and_response_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    bundles = single_version_bundles(exact_runtime_bundles, "3.2.0")
    operation = _operation(bundles, "3.2.0", "UsersController.createUser")
    if drift == "route":
        changed = replace(operation, path="users/create-new")
    elif drift == "method":
        changed = replace(operation, http_method="PUT")
    elif drift == "result":
        changed = replace(operation, logical_return_type=_USER)
    elif drift == "projection":
        changed = replace(operation, response_projection="single_data")
    else:
        parameter = next(
            item for item in operation.parameters if item.wire_name == "queue"
        )
        if drift == "required":
            modified = replace(parameter, required=True, default_value=None)
        elif drift == "default":
            modified = replace(parameter, default_value="default")
        elif drift == "type":
            modified = replace(parameter, java_type="Integer")
        else:
            modified = replace(parameter, name="future", wire_name="future")
        changed = replace(
            operation,
            parameters=[
                modified if item is parameter else item for item in operation.parameters
            ],
        )
    with pytest.raises(ValueError, match="compiled user"):
        _compile(replace_operation(bundles, "3.2.0", changed))


@pytest.mark.parametrize(
    ("model_path", "field_name"),
    [
        (_USER, "timeZone"),
        (_PROJECT, "id"),
        (_DATASOURCE, "connectionParams"),
        (_NAMESPACE, "limitsCpu"),
        (_PAGE, "totalList"),
    ],
)
def test_user_rejects_complete_permission_and_identity_model_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], model_path: str, field_name: str
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
        for bundle in single_version_bundles(exact_runtime_bundles, "3.2.0")
    )
    with pytest.raises(ValueError, match="compiled user"):
        _compile(bundles)


@pytest.mark.parametrize(
    ("import_path", "drift"),
    [
        (_USER_TYPE, "value"),
        (_DB_TYPE, "value"),
        (_DB_TYPE, "metadata"),
        (_DB_TYPE, "serialization"),
    ],
)
def test_user_rejects_enum_value_metadata_and_serialization_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], import_path: str, drift: str
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == "3.2.0"
    )
    enum = next(
        item for item in bundle.snapshot.enums if item.import_path == import_path
    )
    if drift == "value":
        changed = replace(
            enum, values=[replace(enum.values[0], name="UNKNOWN"), *enum.values[1:]]
        )
    elif drift == "metadata":
        changed = replace(
            enum, fields=[replace(enum.fields[0], name="unknown"), *enum.fields[1:]]
        )
    else:
        changed = replace(enum, json_value_field="code")
    altered = replace(
        bundle,
        snapshot=replace(
            bundle.snapshot,
            enums=[changed if item is enum else item for item in bundle.snapshot.enums],
        ),
    )
    with pytest.raises(ValueError, match="compiled user"):
        _compile((altered,))


@pytest.mark.parametrize(
    ("primitive", "codec"),
    [
        ("create", "create_legacy_none"),
        ("update", "update_timezone_none"),
        ("project_revoke", "project_revoke_by_code"),
    ],
)
def test_user_rejects_crossed_runtime_recipe_epochs(
    compiled_users: CompiledDomainSet, primitive: str, codec: str
) -> None:
    profile = compiled_users.plan("user").profiles[-1]
    codecs = {name: program.codec for name, program in profile.programs}
    codecs[primitive] = codec
    with pytest.raises(ValueError, match="recipe is unsupported"):
        USER_COMPILED_DOMAIN.recipe_policy(codecs)


def _operation(
    bundles: tuple[RuntimeBundle, ...], version: str, source: str
) -> OperationSpec:
    return next(
        operation
        for bundle in bundles
        if bundle.spec.version == version
        for operation in bundle.snapshot.operations
        if operation.operation_id == source
    )
