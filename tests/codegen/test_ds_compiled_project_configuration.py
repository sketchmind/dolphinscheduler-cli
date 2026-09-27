"""Exact project-configuration source policies and their compiled closures."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import replace_operation, response_adapter

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_project_parameters import PROJECT_PARAMETER_COMPILED_DOMAIN
from ds_codegen.compiled_project_preferences import PROJECT_PREFERENCE_COMPILED_DOMAIN
from ds_codegen.compiled_project_worker_groups import (
    PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
)
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import (
        CompiledDomainDefinition,
        CompiledDomainSet,
        CompiledRequest,
    )
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract
_DOMAINS = (
    PROJECT_PARAMETER_COMPILED_DOMAIN,
    PROJECT_PREFERENCE_COMPILED_DOMAIN,
    PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
)
_DEPENDENCIES = {
    consumer: providers
    for consumer, providers in _COMPILED_OPERATION_DEPENDENCIES.items()
    if consumer.startswith(
        ("project-parameter.", "project-preference.", "project-worker-group.")
    )
    or consumer
    in {
        "schedule.create",
        "schedule.explain",
        "workflow.run",
        "workflow.backfill",
        "workflow.run-task",
    }
}
_ENTITY_PREFIX = "org.apache.dolphinscheduler.dao.entity."
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ABSENT_CONFIGURATION = {
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
_ABSENT_WORKER = _ABSENT_CONFIGURATION | {"3.2.0", "3.2.1"}
_LEGACY_PARAMETER = {"3.2.0", "3.2.1", "3.2.2"}


@pytest.fixture(scope="module")
def compiled_project_configuration(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(
        exact_runtime_bundles, _DOMAINS, operation_dependencies=_DEPENDENCIES
    )


def test_project_configuration_exact_profiles_and_provider_ownership(
    compiled_project_configuration: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    expected_sources = {
        "project_parameter": {
            "page": "ProjectParameterController.queryProjectParameterListPaging",
            "get": "ProjectParameterController.queryProjectParameterByCode",
            "create": "ProjectParameterController.createProjectParameter",
            "update": "ProjectParameterController.updateProjectParameter",
            "delete": "ProjectParameterController.deleteProjectParametersByCode",
        },
        "project_preference": {
            "get": "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
            "update": "ProjectPreferenceController.updateProjectPreference",
            "set_state": "ProjectPreferenceController.enableProjectPreference",
        },
        "project_worker_group": {
            "list": "ProjectWorkerGroupController.queryAssignedWorkerGroups",
            "assign": "ProjectWorkerGroupController.assignWorkerGroups",
        },
    }
    count = 0
    for original, legacy in zip(
        exact_runtime_bundles,
        compiled_project_configuration.legacy_bundles,
        strict=True,
    ):
        version = original.spec.version
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        bindings = runtime_operation_bindings(version)
        assert set(bindings["project.get"].source_operations) <= remaining
        auxiliary = runtime_auxiliary_operation_bindings(version)
        assert "project-preference.read" not in bindings
        assert ("project-preference.read" in auxiliary) == (
            version not in _ABSENT_CONFIGURATION
        )
        if version not in _ABSENT_CONFIGURATION:
            read = auxiliary["project-preference.read"]
            assert read.source_operations == (
                "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
            )
            assert tuple((item.surface, item.key) for item in read.type_closure) == (
                ("models", _ENTITY_PREFIX + "ProjectPreference"),
            )
            assert tuple(item.resource for item in read.selector_semantics) == (
                "project-preference",
            )
            for consumer in (
                "schedule.create",
                "schedule.explain",
                "workflow.run",
                "workflow.backfill",
                "workflow.run-task",
            ):
                # The native read moves to one owner without rewriting the
                # consumers' complete reviewed source evidence.
                assert set(read.source_operations) <= set(
                    bindings[consumer].source_operations
                )
                assert (
                    set(bindings[consumer].source_operations)
                    - set(read.source_operations)
                    <= remaining
                )
        for plan in compiled_project_configuration.plans:
            profile = plan.profile(version)
            assert (
                profile.source_contract_digest
                == original.metadata.source_contract_digest
            )
            assert (profile.source_tag, profile.source_commit, profile.source_tree) == (
                original.metadata.source_tag,
                original.metadata.source_commit,
                original.metadata.source_tree,
            )
            name = plan.definition.name
            absent = (
                _ABSENT_WORKER
                if name == "project_worker_group"
                else _ABSENT_CONFIGURATION
            )
            if version in absent:
                assert profile.status == "upstream_absent"
                assert profile.recipe_id is None
                assert profile.programs == ()
                continue
            expected = dict(expected_sources[name])
            if name == "project_worker_group" and version == "3.2.2":
                expected["list"] = "ProjectWorkerGroupController.queryWorkerGroups"
            assert profile.status == "supported"
            assert {
                key: program.source_operation for key, program in profile.programs
            } == expected
            assert not set(expected.values()) & remaining
            assert profile.recipe_id == (
                ("legacy_varchar" if version in _LEGACY_PARAMETER else "data_type")
                if name == "project_parameter"
                else "singleton"
                if name == "project_preference"
                else "status_data"
            )
            codecs = dict(plan.codecs)
            for key, program in profile.programs:
                assert program.result_envelope == (
                    "optional" if key in {"page", "get", "list"} else "required"
                )
                assert codecs[program.codec]["response_projection"] == (
                    "status_data"
                    if name == "project_worker_group" and key == "list"
                    else "direct"
                )
                count += 1
        models = {model.import_path for model in legacy.snapshot.models}
        assert (
            not {
                _ENTITY_PREFIX + name
                for name in (
                    "ProjectParameter",
                    "ProjectWorkerGroup",
                )
            }
            & models
        )
        # Other schedule bindings still explicitly retain this reviewed model
        # root even after their shared singleton GET moves to its compiled owner.
        assert (_ENTITY_PREFIX + "ProjectPreference" in models) == (
            version not in _ABSENT_CONFIGURATION
        )
        assert _ENTITY_PREFIX + "Project" in models
        assert _PAGE in models
    assert count == 86
    assert {
        plan.definition.name: (
            len(plan.requests),
            len(plan.responses),
            len(plan.codecs),
        )
        for plan in compiled_project_configuration.plans
    } == {
        "project_parameter": (7, 6, 13),
        "project_preference": (3, 2, 3),
        "project_worker_group": (2, 1, 2),
    }


def test_project_configuration_plans_are_identical_in_full_registry(
    compiled_project_configuration: CompiledDomainSet,
    compiled_all_domains: CompiledDomainSet,
) -> None:
    for plan in compiled_project_configuration.plans:
        assert compiled_all_domains.plan(plan.definition.name) == plan
    # The existing user/catalog/monitor composition tests independently compare
    # the other domains with/without their original sibling ownership unions.
    for legacy in compiled_all_domains.legacy_bundles:
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        delegated = set(
            runtime_operation_bindings(legacy.spec.version)[
                "project.get"
            ].source_operations
        )
        assert delegated.isdisjoint(remaining)
        profile = next(
            profile
            for profile in compiled_all_domains.plan("project").profiles
            if profile.version == legacy.spec.version
        )
        assert delegated <= {
            program.source_operation for _, program in profile.programs
        }


def test_parameter_requests_preserve_data_type_requiredness_and_native_defaults(
    compiled_project_configuration: CompiledDomainSet,
) -> None:
    plan = compiled_project_configuration.plan("project_parameter")
    models = {request.schema: _request_model(request) for request in plan.requests}
    identity = {"projectCode": 101, "code": 202}
    assert (
        models["codes"].model_validate(identity).model_dump(by_alias=True) == identity
    )
    for missing in identity:
        with pytest.raises(ValidationError):
            models["codes"].model_validate(
                {key: value for key, value in identity.items() if key != missing}
            )
    fields = {
        "projectCode": 101,
        "projectParameterName": "name",
        "projectParameterValue": "",
    }
    for epoch in ("legacy", "data_type"):
        create = models[f"create_{epoch}"]
        parsed = create.model_validate(fields)
        assert parsed.model_dump(by_alias=True, exclude_unset=True) == fields
        assert parsed.model_dump(by_alias=True).get("projectParameterDataType") == (
            "VARCHAR" if epoch == "data_type" else None
        )
        with pytest.raises(ValidationError):
            create.model_validate({"projectCode": 101, "projectParameterName": "name"})
        update = models[f"update_{epoch}"]
        payload = {**identity, **fields}
        if epoch == "data_type":
            with pytest.raises(ValidationError, match="projectParameterDataType"):
                update.model_validate(payload)
            payload["projectParameterDataType"] = "INTEGER"
        assert update.model_validate(payload).model_dump(by_alias=True) == payload
        page = models[f"page_{epoch}"]
        paging = {
            "projectCode": 101,
            "pageNo": 1,
            "pageSize": 10,
            "searchVal": "literal",
        }
        assert (
            page.model_validate(paging).model_dump(by_alias=True, exclude_none=True)
            == paging
        )
        if epoch == "legacy":
            with pytest.raises(ValidationError, match="projectParameterDataType"):
                page.model_validate({**paging, "projectParameterDataType": "INTEGER"})
        else:
            assert (
                page.model_validate(
                    {**paging, "projectParameterDataType": "INTEGER"}
                ).model_dump(by_alias=True)["projectParameterDataType"]
                == "INTEGER"
            )


def test_preference_and_worker_requests_keep_exact_literal_wire(
    compiled_project_configuration: CompiledDomainSet,
) -> None:
    preference = {
        request.schema: _request_model(request)
        for request in compiled_project_configuration.plan(
            "project_preference"
        ).requests
    }
    payload = {"projectCode": 101, "projectPreferences": '{"tenant":"literal"}'}
    assert (
        preference["update"].model_validate(payload).model_dump(by_alias=True)
        == payload
    )
    assert preference["set_state"].model_validate(
        {"projectCode": 101, "state": 0}
    ).model_dump(by_alias=True) == {"projectCode": 101, "state": 0}
    for schema in ("update", "set_state"):
        with pytest.raises(ValidationError):
            preference[schema].model_validate({"projectCode": 101})
    request = next(
        request
        for request in compiled_project_configuration.plan(
            "project_worker_group"
        ).requests
        if request.schema == "assign"
    )
    model = _request_model(request)
    for groups in (["alpha,beta"], [""], []):
        value = {"projectCode": 101, "workerGroups": groups}
        assert model.model_validate(value).model_dump(by_alias=True) == value
    for invalid in (
        {"projectCode": 101},
        {"projectCode": 101, "workerGroups": "alpha,beta"},
    ):
        with pytest.raises(ValidationError):
            model.model_validate(invalid)


@pytest.mark.parametrize("epoch", ["basic", "operator", "data_type"])
def test_parameter_response_epochs_preserve_all_fields_and_defaults(
    compiled_project_configuration: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    epoch: str,
) -> None:
    plan = compiled_project_configuration.plan("project_parameter")
    entity = response_adapter(plan, f"entity_{epoch}", monkeypatch)
    fields = {
        "id",
        "userId",
        "code",
        "projectCode",
        "paramName",
        "paramValue",
        "createTime",
        "updateTime",
    }
    if epoch != "basic":
        fields.update({"operator", "createUser", "modifyUser"})
    if epoch == "data_type":
        fields.add("paramDataType")
    defaults = entity.dump_python(
        entity.validate_python({}), mode="json", by_alias=True
    )
    assert isinstance(defaults, dict)
    assert set(defaults) == fields
    assert defaults == dict.fromkeys(fields, None) | {"code": 0, "projectCode": 0}
    row = dict(defaults) | {
        "code": 202,
        "projectCode": 101,
        "paramName": "literal",
        "paramValue": "",
    }
    assert (
        entity.dump_python(entity.validate_python(row), mode="json", by_alias=True)
        == row
    )
    page = response_adapter(plan, f"page_{epoch}", monkeypatch)
    parsed = page.dump_python(
        page.validate_python({"totalList": [row], "total": "1"}),
        mode="json",
        by_alias=True,
    )
    assert isinstance(parsed, dict)
    assert parsed["totalList"] == [row]
    assert parsed["total"] == 1
    assert page.dump_python(page.validate_python({}), mode="json", by_alias=True) == {
        "totalList": [],
        "total": 0,
        "totalPage": None,
        "pageSize": 20,
        "currentPage": 0,
        "pageNo": None,
    }
    with pytest.raises(ValidationError):
        entity.validate_python(None)


def test_nullable_preference_and_worker_list_keep_distinct_response_roots(
    compiled_project_configuration: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preference = compiled_project_configuration.plan("project_preference")
    getter = response_adapter(preference, "get", monkeypatch)
    updater = response_adapter(preference, "update", monkeypatch)
    assert getter.validate_python(None) is None
    with pytest.raises(ValidationError):
        updater.validate_python(None)
    expected = {
        "id": None,
        "code": 0,
        "projectCode": 0,
        "preferences": None,
        "userId": None,
        "state": 0,
        "createTime": None,
        "updateTime": None,
    }
    assert (
        getter.dump_python(getter.validate_python({}), mode="json", by_alias=True)
        == expected
    )
    assert (
        updater.dump_python(updater.validate_python({}), mode="json", by_alias=True)
        == expected
    )
    workers = response_adapter(
        compiled_project_configuration.plan("project_worker_group"), "list", monkeypatch
    )
    assert workers.dump_python(
        workers.validate_python([{}]), mode="json", by_alias=True
    ) == [
        {
            "id": None,
            "projectCode": None,
            "workerGroup": None,
            "createTime": None,
            "updateTime": None,
        }
    ]
    invalid: object
    for invalid in (None, {}, [3]):
        with pytest.raises(ValidationError):
            workers.validate_python(invalid)
    for name, primitive in (
        ("project_parameter", "delete"),
        ("project_preference", "set_state"),
        ("project_worker_group", "assign"),
    ):
        plan = compiled_project_configuration.plan(name)
        program = dict(plan.profile("3.4.2").programs)[primitive]
        # Native Void wrappers ignore the decoded payload; do not add validation.
        assert program.response_schema_digest is None
        assert dict(plan.codecs)[program.codec]["response"] is None


@pytest.mark.parametrize(
    ("domain", "version", "model_name", "primitive", "source"),
    [
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "3.2.0",
            "ProjectParameter",
            "get",
            "ProjectParameterController.queryProjectParameterByCode",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "3.2.2",
            "ProjectParameter",
            "get",
            "ProjectParameterController.queryProjectParameterByCode",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "3.4.2",
            "ProjectParameter",
            "get",
            "ProjectParameterController.queryProjectParameterByCode",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "3.2.0",
            "PageInfo",
            "page",
            "ProjectParameterController.queryProjectParameterListPaging",
        ),
        (
            PROJECT_PREFERENCE_COMPILED_DOMAIN,
            "3.2.0",
            "ProjectPreference",
            "get",
            "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
        ),
        (
            PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
            "3.2.2",
            "ProjectWorkerGroup",
            "list",
            "ProjectWorkerGroupController.queryWorkerGroups",
        ),
    ],
)
@pytest.mark.parametrize("drift", ["missing_field", "default", "extends"])
def test_project_configuration_rejects_complete_source_model_drift(
    exact_contract_corpus: ExactContractCorpus,
    domain: CompiledDomainDefinition,
    version: str,
    model_name: str,
    primitive: str,
    source: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    model = next(item for item in snapshot.models if item.name == model_name)
    if drift == "missing_field":
        changed = replace(model, fields=model.fields[:-1])
    elif drift == "default":
        changed = replace(
            model,
            fields=[replace(model.fields[0], default_value="1"), *model.fields[1:]],
        )
    else:
        changed = replace(model, extends="FutureBase")
    modified = replace(
        snapshot,
        models=[
            changed if item.import_path == model.import_path else item
            for item in snapshot.models
        ],
    )
    operation = next(
        item for item in snapshot.operations if item.operation_id == source
    )
    with pytest.raises(ValueError, match="fields changed"):
        domain.response_policy(modified, operation, primitive)


@pytest.mark.parametrize(
    ("domain", "primitive", "source", "drift"),
    [
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "create",
            "ProjectParameterController.createProjectParameter",
            "varchar_default",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "update",
            "ProjectParameterController.updateProjectParameter",
            "path_type",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "delete",
            "ProjectParameterController.deleteProjectParametersByCode",
            "logical",
        ),
        (
            PROJECT_PREFERENCE_COMPILED_DOMAIN,
            "get",
            "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
            "logical",
        ),
        (
            PROJECT_PREFERENCE_COMPILED_DOMAIN,
            "set_state",
            "ProjectPreferenceController.enableProjectPreference",
            "state_type",
        ),
        (
            PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
            "list",
            "ProjectWorkerGroupController.queryAssignedWorkerGroups",
            "projection",
        ),
        (
            PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
            "assign",
            "ProjectWorkerGroupController.assignWorkerGroups",
            "array_type",
        ),
        (
            PROJECT_PARAMETER_COMPILED_DOMAIN,
            "get",
            "ProjectParameterController.queryProjectParameterByCode",
            "projection",
        ),
        (
            PROJECT_PREFERENCE_COMPILED_DOMAIN,
            "update",
            "ProjectPreferenceController.updateProjectPreference",
            "consumes",
        ),
    ],
)
def test_project_configuration_rejects_request_and_response_policy_drift(
    exact_contract_corpus: ExactContractCorpus,
    domain: CompiledDomainDefinition,
    primitive: str,
    source: str,
    drift: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = next(
        item for item in snapshot.operations if item.operation_id == source
    )
    if drift == "logical":
        changed = replace(operation, logical_return_type="String")
    elif drift == "projection":
        changed = replace(
            operation,
            response_projection="direct" if primitive == "list" else "status_data",
        )
    elif drift == "consumes":
        changed = replace(operation, consumes=["application/json"])
    else:
        field, java_type = {
            "varchar_default": ("projectParameterDataType", "String"),
            "path_type": ("code", "long"),
            "state_type": ("state", "Integer"),
            "array_type": ("workerGroups", "String"),
        }[drift]
        changed = replace(
            operation,
            parameters=[
                replace(
                    item,
                    java_type=java_type,
                    default_value="INTEGER"
                    if drift == "varchar_default"
                    else item.default_value,
                )
                if item.wire_name == field
                else item
                for item in operation.parameters
            ],
        )
    with pytest.raises(ValueError, match="changed"):
        domain.response_policy(snapshot, changed, primitive)


@pytest.mark.parametrize("drift", ["requiredness", "path", "missing_operation"])
def test_parameter_compilation_rejects_changed_exact_inventory(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == "3.2.0"
    )
    operation = next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id
        == "ProjectParameterController.queryProjectParameterListPaging"
    )
    if drift == "requiredness":
        changed = replace(
            operation,
            parameters=[
                replace(item, required=True) if item.wire_name == "searchVal" else item
                for item in operation.parameters
            ],
        )
    elif drift == "path":
        changed = replace(operation, path="projects/{projectCode}/other-parameter")
    else:
        changed = replace(
            operation, operation_id="ProjectParameterController.futurePaging"
        )
    bundles = (
        tuple(
            replace(
                item,
                snapshot=replace(
                    item.snapshot,
                    operations=[
                        changed
                        if value.operation_id == operation.operation_id
                        else value
                        for value in item.snapshot.operations
                    ],
                ),
            )
            if item is bundle
            else item
            for item in exact_runtime_bundles
        )
        if drift == "missing_operation"
        else replace_operation(exact_runtime_bundles, "3.2.0", changed)
    )
    with pytest.raises(ValueError):
        compile_domains(
            bundles,
            (PROJECT_PARAMETER_COMPILED_DOMAIN,),
            operation_dependencies=_DEPENDENCIES,
        )


def _request_model(request: CompiledRequest) -> type[BaseParamsModel]:
    namespace: dict[str, object] = {"Field": Field, "BaseParamsModel": BaseParamsModel}
    exec(compile(request.source, f"<{request.schema}>", "exec"), namespace)  # noqa: S102
    return cast("type[BaseParamsModel]", namespace[request.class_name])
