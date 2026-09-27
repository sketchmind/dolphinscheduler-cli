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
)

from ds_codegen.compiled_audit import AUDIT_COMPILED_DOMAIN
from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_task_types import TASK_TYPE_COMPILED_DOMAIN
from ds_codegen.observability_contract import observability_contract
from ds_codegen.runtime_bundles import (
    _COMPILED_DOMAINS,
    _COMPILED_OPERATION_DEPENDENCIES,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet, CompiledRequest
    from ds_codegen.ir import OperationSpec
    from ds_codegen.observability_contract import ObservabilityVersionContract
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract
_DOMAINS = (AUDIT_COMPILED_DOMAIN, TASK_TYPE_COMPILED_DOMAIN)
_AUDIT = "org.apache.dolphinscheduler.api.dto.AuditDto"
_MODEL_TYPE = "org.apache.dolphinscheduler.api.dto.auditLog.AuditModelTypeDto"
_OPERATION_TYPE = "org.apache.dolphinscheduler.api.dto.auditLog.AuditOperationTypeDto"
_FAVOURITE = "org.apache.dolphinscheduler.api.dto.FavTaskDto"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"


@pytest.fixture(scope="module")
def compiled_read_catalogs(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(exact_runtime_bundles, _DOMAINS)


def test_read_catalogs_preserve_exact_ownership_absence_and_recipes(
    compiled_read_catalogs: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    audit = compiled_read_catalogs.plan("audit")
    task_type = compiled_read_catalogs.plan("task_type")
    assert tuple(item.recipe_id for item in audit.profiles) == (
        *(None,) * 11,
        *("singular_enums",) * 19,
        *("csv_text",) * 7,
    )
    assert tuple(item.recipe_id for item in task_type.profiles) == (
        *(None,) * 18,
        *("legacy",) * 10,
        *("category",) * 9,
    )
    assert (len(audit.requests), len(audit.responses), len(audit.codecs)) == (3, 6, 6)
    assert (len(task_type.requests), len(task_type.responses)) == (1, 2)
    assert sum(len(item.programs) for item in audit.profiles) == 40
    assert sum(len(item.programs) for item in task_type.profiles) == 19
    for index, (original, legacy) in enumerate(
        zip(exact_runtime_bundles, compiled_read_catalogs.legacy_bundles, strict=True)
    ):
        expected = set()
        if index >= 11:
            expected.add("AuditLogController.queryAuditLogListPaging")
        if index >= 18:
            expected.add("FavTaskController.listTaskType")
        if index >= 30:
            expected.update(
                {
                    "AuditLogController.queryAuditModelTypeList",
                    "AuditLogController.queryAuditOperationTypeList",
                }
            )
        selected = [audit.profiles[index], task_type.profiles[index]]
        assert {
            program.source_operation
            for item in selected
            for _, program in item.programs
        } == expected
        for item in selected:
            assert (
                item.source_contract_digest == original.metadata.source_contract_digest
            )
            assert item.status == ("supported" if item.programs else "upstream_absent")
            assert all(
                program.result_envelope == "optional" for _, program in item.programs
            )
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        assert not expected & remaining
        assert not any(
            operation.controller in {"AuditLogController", "FavTaskController"}
            for operation in legacy.snapshot.operations
        )
        models = {model.import_path for model in legacy.snapshot.models}
        assert not {_AUDIT, _MODEL_TYPE, _OPERATION_TYPE, _FAVOURITE} & models
        assert _PAGE in models


def test_read_catalogs_do_not_change_other_compiled_domains(
    compiled_read_catalogs: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_all_domains: CompiledDomainSet,
) -> None:
    previous = tuple(
        item for item in _COMPILED_DOMAINS if item.name not in {"audit", "task_type"}
    )
    baseline = compile_domains(
        exact_runtime_bundles,
        previous,
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )
    combined = compiled_all_domains
    for plan in baseline.plans:
        assert combined.plan(plan.definition.name) == plan
    for plan in compiled_read_catalogs.plans:
        assert combined.plan(plan.definition.name) == plan


def test_audit_request_epochs_preserve_requiredness_enums_and_csv_text(
    compiled_read_catalogs: CompiledDomainSet, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = {
        item.schema: _request_model(item, monkeypatch)
        for item in compiled_read_catalogs.plan("audit").requests
    }
    singular = requests["singular"]
    valid = {
        "pageNo": 1,
        "pageSize": 20,
        "resourceType": "USER_MODULE",
        "operationType": "READ",
    }
    assert (
        singular.model_validate(valid).model_dump(
            mode="json", by_alias=True, exclude_none=True
        )
        == valid
    )
    with pytest.raises(ValidationError):
        singular.model_validate({"pageNo": 1})
    for field, value in (
        ("resourceType", "USER_MODULE,PROJECT_MODULE"),
        ("operationType", "OTHER"),
        ("modelName", "x"),
    ):
        with pytest.raises(ValidationError):
            singular.model_validate({**valid, field: value})
    csv = requests["csv"]
    text = {
        "pageNo": 1,
        "pageSize": 20,
        "modelTypes": "USER,PROJECT",
        "operationTypes": "READ,UPDATE",
        "modelName": "kept",
    }
    assert csv.model_validate(text).model_dump(by_alias=True, exclude_none=True) == text
    with pytest.raises(ValidationError):
        csv.model_validate({**text, "resourceType": "USER_MODULE"})
    assert requests["empty"].model_validate({}).model_dump() == {}
    with pytest.raises(ValidationError):
        requests["empty"].model_validate({"filter": "invented"})


@pytest.mark.parametrize(
    "schema",
    [
        "page_resource_nullable_strict",
        "page_resource_list_strict",
        "page_resource_list",
        "page_model_list",
    ],
)
def test_audit_response_epochs_keep_complete_rows_and_page_policy(
    compiled_read_catalogs: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    adapter = response_adapter(
        compiled_read_catalogs.plan("audit"), schema, monkeypatch
    )
    row = (
        {
            "userName": "alice",
            "resource": "USER_MODULE",
            "operation": "READ",
            "time": "2026-09-05 12:00:00",
            "resourceName": "kept",
        }
        if schema.startswith("page_resource")
        else {
            "userName": "alice",
            "modelType": "USER",
            "modelName": "kept",
            "operation": "READ",
            "createTime": "2026-09-05 12:00:00",
            "description": "desc",
            "detail": "detail",
            "latency": "5ms",
        }
    )
    parsed = adapter.validate_python({"totalList": [row], "total": 1})
    result = adapter.dump_python(parsed, mode="json", by_alias=True)
    assert isinstance(result, dict)
    assert result["totalList"] == [row]
    empty = adapter.dump_python(adapter.validate_python({}), mode="json")
    assert isinstance(empty, dict)
    assert empty["totalList"] == (None if "nullable" in schema else [])
    assert empty["total"] == 0
    assert empty["pageSize"] == 20
    if schema.endswith("strict"):
        with pytest.raises(ValidationError):
            adapter.validate_python({"total": "1"})
    else:
        coerced = adapter.dump_python(adapter.validate_python({"total": "1"}))
        assert isinstance(coerced, dict)
        assert coerced["total"] == 1


def test_audit_metadata_keeps_recursive_children_and_null_default(
    compiled_read_catalogs: CompiledDomainSet, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = compiled_read_catalogs.plan("audit")
    model_types = response_adapter(plan, "model_types", monkeypatch)
    payload = [{"name": "PROJECT", "child": [{"name": "WORKFLOW"}]}]
    value = model_types.dump_python(model_types.validate_python(payload), mode="json")
    assert value == [
        {"name": "PROJECT", "child": [{"name": "WORKFLOW", "child": None}]}
    ]
    operations = response_adapter(plan, "operation_types", monkeypatch)
    assert operations.dump_python(
        operations.validate_python([{"name": "READ"}, {}])
    ) == [{"name": "READ"}, {"name": None}]


@pytest.mark.parametrize("schema", ["list_legacy", "list_category"])
def test_task_type_response_keeps_alias_default_and_exact_catalog_fields(
    compiled_read_catalogs: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    adapter = response_adapter(
        compiled_read_catalogs.plan("task_type"), schema, monkeypatch
    )
    field = "taskName" if schema == "list_legacy" else "taskCategory"
    record = {"taskType": "SHELL", field: "name-or-category", "collection": True}
    assert adapter.dump_python(
        adapter.validate_python([record]), mode="json", by_alias=True
    ) == [record]
    empty = adapter.dump_python(
        adapter.validate_python([{}]), mode="json", by_alias=True
    )
    assert empty == [{"taskType": None, field: None, "collection": False}]
    with pytest.raises(ValidationError):
        adapter.validate_python([{"collection": None}])


@pytest.mark.parametrize(
    "drift", ["route", "required", "type", "default", "projection", "return"]
)
def test_audit_rejects_exact_request_and_response_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    operation = _operation(
        exact_runtime_bundles, "3.2.2", "AuditLogController.queryAuditLogListPaging"
    )
    if drift == "route":
        changed = replace(operation, path="projects/audit/changed")
    elif drift == "projection":
        changed = replace(operation, response_projection="single_data")
    elif drift == "return":
        changed = replace(operation, logical_return_type="List<String>")
    else:
        parameters = [
            replace(
                item,
                **(
                    {"required": False}
                    if drift == "required"
                    else {"java_type": "Long"}
                    if drift == "type"
                    else {"default_value": "1"}
                ),
            )
            if item.wire_name == "pageNo"
            else item
            for item in operation.parameters
        ]
        changed = replace(operation, parameters=parameters)
    with pytest.raises(ValueError):
        compile_domains(
            replace_operation(exact_runtime_bundles, "3.2.2", changed), _DOMAINS
        )


@pytest.mark.parametrize(
    ("version", "model_path", "field", "change"),
    [
        ("3.0.0", _AUDIT, "resource", "wire"),
        ("3.2.2", _AUDIT, "detail", "nullable"),
        ("3.2.2", _MODEL_TYPE, "child", "factory"),
        ("3.2.2", _OPERATION_TYPE, "name", "type"),
        ("3.0.0", _PAGE, "totalList", "factory"),
        ("3.1.0", _FAVOURITE, "taskName", "wire"),
        ("3.2.2", _FAVOURITE, "collection", "nullable"),
    ],
)
def test_read_catalogs_reject_complete_response_field_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    model_path: str,
    field: str,
    change: str,
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == version
    )
    model = next(
        item for item in bundle.snapshot.models if item.import_path == model_path
    )
    original = next(item for item in model.fields if item.wire_name == field)
    if change == "wire":
        changed_field = replace(original, wire_name="changed")
    elif change == "nullable":
        changed_field = replace(original, nullable=field != "detail")
    elif change == "factory":
        changed_field = replace(original, default_factory="list")
    else:
        changed_field = replace(original, java_type="Integer")
    changed = replace(
        model,
        fields=[
            changed_field if item.wire_name == field else item for item in model.fields
        ],
    )
    snapshot = replace(
        bundle.snapshot,
        models=[
            changed if item.import_path == model_path else item
            for item in bundle.snapshot.models
        ],
    )
    bundles = tuple(
        replace(item, snapshot=snapshot) if item.spec.version == version else item
        for item in exact_runtime_bundles
    )
    with pytest.raises(ValueError):
        compile_domains(bundles, _DOMAINS)


@pytest.mark.parametrize("drift", ["value", "metadata", "json_value"])
def test_audit_rejects_filter_enum_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == "3.0.0"
    )
    enum = next(
        item for item in bundle.snapshot.enums if item.name == "AuditResourceType"
    )
    if drift == "json_value":
        changed = replace(enum, json_value_field="code")
    elif drift == "metadata":
        changed = replace(
            enum, fields=[replace(enum.fields[0], java_type="long"), *enum.fields[1:]]
        )
    else:
        changed = replace(
            enum,
            values=[
                replace(enum.values[0], arguments=["0", "OTHER"]),
                *enum.values[1:],
            ],
        )
    snapshot = replace(
        bundle.snapshot,
        enums=[
            changed if item.import_path == enum.import_path else item
            for item in bundle.snapshot.enums
        ],
    )
    bundles = tuple(
        replace(item, snapshot=snapshot) if item.spec.version == "3.0.0" else item
        for item in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="filter enum"):
        compile_domains(bundles, _DOMAINS)


def test_audit_rejects_reviewed_filter_recipe_disagreement(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    def changed(version: str) -> ObservabilityVersionContract:
        contract = observability_contract(version)
        return (
            replace(contract, audit=replace(contract.audit, model_name_filter=True))
            if version == "3.0.0"
            else contract
        )

    monkeypatch.setattr("ds_codegen.compiled_audit.observability_contract", changed)
    with pytest.raises(ValueError, match="filter recipe"):
        compile_domains(exact_runtime_bundles, _DOMAINS)


def _request_model(
    request: CompiledRequest, monkeypatch: pytest.MonkeyPatch
) -> type[BaseParamsModel]:
    name = f"dsctl.generated.wire_programs._test_audit_request_{request.schema}"
    load_schema_pool(request.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if request.module_name is not None:
        module.__package__ += f".{request.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - compile only the in-memory renderer output under test
        compile(request.content or request.source, f"<{name}>", "exec"), module.__dict__
    )
    return cast("type[BaseParamsModel]", getattr(module, request.class_name))


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
