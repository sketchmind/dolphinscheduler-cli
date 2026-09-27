"""Project compiler contracts from all reviewed original exact source bundles."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import replace_operation, response_adapter

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_projects import PROJECT_COMPILED_DOMAIN
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.runtime_contract import runtime_operation_bindings
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledDomainSet, CompiledRequest
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract
_PROJECT = "org.apache.dolphinscheduler.dao.entity.Project"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_RECIPES = {
    "1.3.9": "legacy_id",
    "2.0.0": "id_create_void_update",
    "2.0.1": "id_create_void_update",
    "2.0.9": "project_create_void_update",
    "2.0.2": "project_create_void_update",
    "2.0.3": "project_create_void_update",
    "2.0.4": "project_create_void_update",
    "2.0.5": "project_create_void_update",
    "2.0.6": "project_create_void_update",
    "2.0.7": "project_create_void_update",
    "2.0.8": "project_create_void_update",
    "3.0.0": "project_create_void_update",
    "3.0.1": "project_create_void_update",
    "3.0.6": "project_create_void_update",
    "3.0.2": "project_create_void_update",
    "3.0.3": "project_create_void_update",
    "3.0.4": "project_create_void_update",
    "3.0.5": "project_create_void_update",
    "3.1.0": "owner_preserving_project",
    "3.1.1": "owner_preserving_project",
    "3.1.2": "owner_preserving_project",
    "3.1.9": "owner_preserving_project",
    "3.1.3": "owner_preserving_project",
    "3.1.4": "owner_preserving_project",
    "3.1.5": "owner_preserving_project",
    "3.1.6": "owner_preserving_project",
    "3.1.7": "owner_preserving_project",
    "3.1.8": "owner_preserving_project",
    "3.2.0": "owner_preserving_project",
    "3.2.1": "modern",
    "3.2.2": "modern",
    "3.3.1": "modern",
    "3.3.2": "modern",
    "3.4.0": "modern",
    "3.4.1": "modern",
    "3.4.2": "modern",
    "3.4.3": "modern",
}


@pytest.fixture(scope="module")
def compiled_projects(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(
        exact_runtime_bundles,
        (PROJECT_COMPILED_DOMAIN,),
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )


def test_project_profiles_preserve_all_native_sources_and_exact_recipes(
    compiled_projects: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_projects.plan("project")
    assert {profile.version: profile.recipe_id for profile in plan.profiles} == _RECIPES
    assert (
        len(plan.requests),
        len(plan.responses),
        len(plan.scalar_responses),
        len(plan.codecs),
    ) == (8, 10, 1, 22)
    assert sum(len(profile.programs) for profile in plan.profiles) == 187
    for original, legacy, profile in zip(
        exact_runtime_bundles,
        compiled_projects.legacy_bundles,
        plan.profiles,
        strict=True,
    ):
        version = original.spec.version
        assert profile.status == "supported"
        assert (
            profile.source_tag,
            profile.source_commit,
            profile.source_tree,
            profile.source_contract_digest,
        ) == (
            original.metadata.source_tag,
            original.metadata.source_commit,
            original.metadata.source_tree,
            original.metadata.source_contract_digest,
        )
        expected = {
            "page": "ProjectController.queryProjectListPaging",
            "get": "ProjectController.queryProjectById"
            if version == "1.3.9"
            else "ProjectController.queryProjectByCode",
            "create": "ProjectController.createProject",
            "update": "ProjectController.updateProject",
            "delete_legacy"
            if version == "1.3.9"
            else "delete": "ProjectController.deleteProject",
        }
        if version in {"2.0.0", "2.0.1"}:
            expected["created_and_authed"] = (
                "ProjectController.queryProjectCreatedAndAuthorizedByUser"
            )
        programs = dict(profile.programs)
        assert {
            key: program.source_operation for key, program in programs.items()
        } == expected
        owned = set().union(
            *(
                set(binding.source_operations)
                for name, binding in runtime_operation_bindings(version).items()
                if name.startswith("project.")
            )
        )
        assert owned == set(expected.values())
        assert {
            operation.operation_id for operation in original.snapshot.operations
        } - {
            operation.operation_id for operation in legacy.snapshot.operations
        } == owned
        remaining_project = {
            operation.operation_id
            for operation in legacy.snapshot.operations
            if operation.controller == "ProjectController"
        }
        permissions = runtime_operation_bindings(version)
        permission_sources = {
            source
            for name, binding in permissions.items()
            if name in {"user.grant.project", "user.revoke.project"}
            for source in binding.source_operations
        }
        assert remaining_project <= permission_sources
        # A single-domain slice still contains the user permission consumers;
        # their independently owned Project responses must not be deleted.
        assert {_PROJECT, _PAGE} <= {
            model.import_path for model in legacy.snapshot.models
        }
        for primitive, program in programs.items():
            assert program.result_envelope == (
                "required"
                if primitive in {"create", "update", "delete"}
                else "optional"
            )
            assert dict(plan.codecs)[program.codec]["response_projection"] == "direct"
        if version == "1.3.9":
            codec = dict(plan.codecs)[programs["delete_legacy"].codec]
            assert (codec["method"], codec["path"], codec["channel"]) == (
                "GET",
                "projects/delete",
                "query",
            )
            assert programs["delete_legacy"].response_schema_digest is None
    assert plan.scalar_responses[0].schema == "id"
    assert plan.scalar_responses[0].annotation == "int"


def test_project_request_schemas_keep_identity_owner_and_omission_rules(
    compiled_projects: CompiledDomainSet,
) -> None:
    requests = {
        request.schema: _request_model(request)
        for request in compiled_projects.plan("project").requests
    }
    values: dict[str, dict[str, object]] = {
        "empty": {},
        "page": {"pageSize": 20, "pageNo": 1},
        "id": {"projectId": 41},
        "code": {"code": 42},
        "create": {"projectName": "name"},
        "update_legacy": {"projectId": 41, "projectName": "name"},
        "update_owner": {"code": 42, "projectName": "name", "userName": "kept-owner"},
        "update_direct": {"code": 42, "projectName": "name"},
    }
    assert set(requests) == set(values)
    for schema, payload in values.items():
        model = requests[schema]
        assert (
            model.model_validate(payload).model_dump(by_alias=True, exclude_none=True)
            == payload
        )
        for required in payload:
            with pytest.raises(ValidationError):
                model.model_validate(
                    {key: value for key, value in payload.items() if key != required}
                )
        with pytest.raises(ValidationError):
            model.model_validate({**payload, "invented": True})
    for schema in ("create", "update_legacy", "update_owner", "update_direct"):
        model = requests[schema]
        for description in (None, "", "literal"):
            payload = {**values[schema], "description": description}
            assert (
                model.model_validate(payload).model_dump(
                    by_alias=True, exclude_unset=True
                )
                == payload
            )
    with pytest.raises(ValidationError, match="userName"):
        requests["update_direct"].model_validate(
            {"code": 42, "projectName": "name", "userName": "not-native"}
        )


@pytest.mark.parametrize(
    "epoch", ["legacy", "code_zero", "code_nullable", "code_compact"]
)
def test_project_entity_epochs_keep_complete_fields_and_native_defaults(
    compiled_projects: CompiledDomainSet, monkeypatch: pytest.MonkeyPatch, epoch: str
) -> None:
    adapter = response_adapter(
        compiled_projects.plan("project"), f"entity_{epoch}", monkeypatch
    )
    expected: dict[str, object] = {
        "id": 0,
        "userId": 0,
        "userName": None,
        "name": None,
        "description": None,
        "createTime": None,
        "updateTime": None,
        "perm": 0,
        "defCount": 0,
    }
    if epoch != "legacy":
        expected["code"] = 0
    if epoch != "code_compact":
        expected["instRunningCount"] = 0
    if epoch in {"code_nullable", "code_compact"}:
        expected.update(id=None, userId=None)
    assert (
        adapter.dump_python(adapter.validate_python({}), mode="json", by_alias=True)
        == expected
    )
    row = {
        **expected,
        "id": 41,
        "userId": 7,
        "name": "literal",
        "description": "",
        "userName": "owner",
    }
    assert (
        adapter.dump_python(adapter.validate_python(row), mode="json", by_alias=True)
        == row
    )
    with pytest.raises(ValidationError):
        adapter.validate_python(None)


@pytest.mark.parametrize(
    ("schema", "nullable", "strict", "has_page_size"),
    [
        ("page_legacy", True, False, False),
        ("page_code_zero_strict", True, True, True),
        ("page_code_nullable_strict", False, True, True),
        ("page_code_nullable", False, False, True),
        ("page_code_compact", False, False, True),
    ],
)
def test_project_pages_preserve_complete_envelope_and_strictness(
    compiled_projects: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    nullable: bool,
    strict: bool,
    has_page_size: bool,
) -> None:
    adapter = response_adapter(compiled_projects.plan("project"), schema, monkeypatch)
    expected: dict[str, object] = {
        "totalList": None if nullable else [],
        "total": 0,
        "totalPage": None,
        "currentPage": 0,
    }
    if has_page_size:
        expected.update(pageSize=20, pageNo=None)
    assert (
        adapter.dump_python(adapter.validate_python({}), mode="json", by_alias=True)
        == expected
    )
    parsed = adapter.dump_python(
        adapter.validate_python(
            {"totalList": [{"id": 41, "name": "literal"}], "total": 1}
        ),
        mode="json",
        by_alias=True,
    )
    assert isinstance(parsed, dict)
    assert isinstance(parsed["totalList"], list)
    assert parsed["totalList"][0]["name"] == "literal"
    if strict:
        with pytest.raises(ValidationError, match="total"):
            adapter.validate_python({"total": "1"})
    else:
        parsed = adapter.dump_python(
            adapter.validate_python({"total": "1"}), mode="json"
        )
        assert isinstance(parsed, dict)
        assert parsed["total"] == 1


def test_created_authorized_list_and_void_mutations_keep_distinct_roots(
    compiled_projects: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compiled_projects.plan("project")
    adapter = response_adapter(plan, "created_and_authed", monkeypatch)
    assert adapter.validate_python([]) == []
    assert (
        adapter.dump_python(adapter.validate_python([{}]), mode="json", by_alias=True)[
            0
        ]["code"]
        == 0
    )
    invalid: object
    for invalid in (None, {}, [3]):
        with pytest.raises(ValidationError):
            adapter.validate_python(invalid)
    for version, primitive in (
        ("1.3.9", "delete_legacy"),
        ("1.3.9", "update"),
        ("2.0.0", "update"),
        ("3.0.6", "update"),
        ("3.4.2", "delete"),
    ):
        program = dict(plan.profile(version).programs)[primitive]
        assert program.response_schema_digest is None
        assert dict(plan.codecs)[program.codec]["response"] is None


@pytest.mark.parametrize(
    "version", ["1.3.9", "2.0.0", "2.0.1", "3.1.0", "3.1.1", "3.1.2", "3.3.1"]
)
@pytest.mark.parametrize("drift", ["missing_field", "nullable", "default", "extends"])
def test_project_rejects_unreviewed_entity_closure(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    model = next(item for item in snapshot.models if item.import_path == _PROJECT)
    if drift == "missing_field":
        changed = replace(model, fields=model.fields[:-1])
    elif drift == "extends":
        changed = replace(model, extends="FutureProject")
    else:
        field = model.fields[0]
        changed_field = (
            replace(field, nullable=not field.nullable)
            if drift == "nullable"
            else replace(field, default_value="7")
        )
        changed = replace(model, fields=[changed_field, *model.fields[1:]])
    modified = replace(
        snapshot,
        models=[
            changed if item.import_path == _PROJECT else item
            for item in snapshot.models
        ],
    )
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ProjectController.queryProjectListPaging"
    )
    with pytest.raises(ValueError, match="entity fields changed"):
        PROJECT_COMPILED_DOMAIN.response_policy(modified, operation, "page")


@pytest.mark.parametrize(
    "version", ["1.3.9", "2.0.0", "2.0.1", "3.1.0", "3.1.1", "3.1.2", "3.2.0", "3.3.1"]
)
def test_project_rejects_page_shape_drift(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    page = next(item for item in snapshot.models if item.import_path == _PAGE)
    changed = replace(
        page,
        fields=[
            replace(page.fields[0], nullable=not page.fields[0].nullable),
            *page.fields[1:],
        ],
    )
    modified = replace(
        snapshot,
        models=[
            changed if item.import_path == _PAGE else item for item in snapshot.models
        ],
    )
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ProjectController.queryProjectListPaging"
    )
    with pytest.raises(ValueError, match="page fields"):
        PROJECT_COMPILED_DOMAIN.response_policy(modified, operation, "page")


@pytest.mark.parametrize(
    ("version", "primitive", "method", "drift"),
    [
        ("1.3.9", "create", "createProject", "logical"),
        ("2.0.0", "create", "createProject", "logical"),
        ("3.1.0", "update", "updateProject", "logical"),
        ("2.0.0", "update", "updateProject", "owner"),
        ("3.2.1", "update", "updateProject", "identity_type"),
        ("3.2.0", "page", "queryProjectListPaging", "requiredness"),
        ("3.4.2", "page", "queryProjectListPaging", "default"),
        ("3.4.2", "get", "queryProjectByCode", "projection"),
        ("3.4.2", "delete", "deleteProject", "consumes"),
    ],
)
def test_project_rejects_changed_source_recipe_facts(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    primitive: str,
    method: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == f"ProjectController.{method}"
    )
    if drift == "logical":
        changed = replace(operation, logical_return_type="String")
    elif drift == "projection":
        changed = replace(operation, response_projection="single_data")
    elif drift == "consumes":
        changed = replace(operation, consumes=["application/json"])
    elif drift == "owner":
        changed = replace(
            operation,
            parameters=[
                item for item in operation.parameters if item.wire_name != "userName"
            ],
        )
    else:
        field = "code" if drift == "identity_type" else "pageNo"
        changed = replace(
            operation,
            parameters=[
                replace(item, java_type="long")
                if drift == "identity_type" and item.wire_name == field
                else replace(item, required=None)
                if drift == "requiredness" and item.wire_name == field
                else replace(item, default_value="1")
                if drift == "default" and item.wire_name == field
                else item
                for item in operation.parameters
            ],
        )
    with pytest.raises(ValueError, match="changed"):
        PROJECT_COMPILED_DOMAIN.response_policy(snapshot, changed, primitive)


@pytest.mark.parametrize("drift", ["path", "requiredness", "missing_operation"])
def test_project_compilation_rejects_changed_request_or_native_inventory(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == "1.3.9"
    )
    operation = next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id == "ProjectController.queryProjectListPaging"
    )
    if drift == "path":
        changed = replace(operation, path="projects/unreviewed-page")
    elif drift == "requiredness":
        changed = replace(
            operation,
            parameters=[
                replace(item, required=True) if item.wire_name == "searchVal" else item
                for item in operation.parameters
            ],
        )
    else:
        changed = replace(operation, operation_id="ProjectController.unreviewedPage")
    if drift == "missing_operation":
        modified = replace(
            bundle.snapshot,
            operations=[
                changed if item.operation_id == operation.operation_id else item
                for item in bundle.snapshot.operations
            ],
        )
        bundles = tuple(
            replace(item, snapshot=modified) if item is bundle else item
            for item in exact_runtime_bundles
        )
    else:
        bundles = replace_operation(exact_runtime_bundles, "1.3.9", changed)
    with pytest.raises(ValueError):
        compile_domains(
            bundles,
            (PROJECT_COMPILED_DOMAIN,),
            operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
        )


def _request_model(request: CompiledRequest) -> type[BaseParamsModel]:
    namespace: dict[str, object] = {"Field": Field, "BaseParamsModel": BaseParamsModel}
    exec(compile(request.source, f"<{request.schema}>", "exec"), namespace)  # noqa: S102
    return cast("type[BaseParamsModel]", namespace[request.class_name])
