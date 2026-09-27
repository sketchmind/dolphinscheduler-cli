from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import replace_operation, response_adapter

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_task_groups import TASK_GROUP_COMPILED_DOMAIN
from ds_codegen.runtime_contract import runtime_operation_bindings
from ds_codegen.task_group_contract import task_group_contract
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.ir import ContractSnapshot
    from ds_codegen.runtime_bundles import RuntimeBundle
    from ds_codegen.task_group_contract import TaskGroupVersionContract

pytestmark = pytest.mark.source_contract

_GROUP = "org.apache.dolphinscheduler.dao.entity.TaskGroup"
_QUEUE = "org.apache.dolphinscheduler.dao.entity.TaskGroupQueue"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_QUEUE_STATUS = "org.apache.dolphinscheduler.common.enums.TaskGroupQueueStatus"


@pytest.fixture(scope="module")
def compiled_task_groups(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(exact_runtime_bundles, (TASK_GROUP_COMPILED_DOMAIN,))


def test_task_group_profiles_preserve_exact_ownership_and_reviewed_recipes(
    compiled_task_groups: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_task_groups.plan("task_group")
    epochs = (
        (
            (
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
            ),
            None,
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
            "legacy_queue_void",
        ),
        (("3.2.1",), "renamed_queue_void"),
        (("3.2.2",), "renamed_queue_entity"),
        (
            ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
            "workflow_queue_entity",
        ),
    )
    expected = {version: recipe for versions, recipe in epochs for version in versions}
    assert tuple(profile.version for profile in plan.profiles) == tuple(expected)
    assert len(plan.requests) == len(plan.responses) == 9
    assert len(plan.codecs) == 20
    assert not plan.scalar_responses
    for original, profile, legacy in zip(
        exact_runtime_bundles,
        plan.profiles,
        compiled_task_groups.legacy_bundles,
        strict=True,
    ):
        assert profile.recipe_id == expected[profile.version]
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        reviewed = task_group_contract(profile.version).task_group
        assert profile.status == (
            "supported" if reviewed.support == "supported" else "upstream_absent"
        )
        programs = dict(profile.programs)
        if reviewed.support == "absent":
            assert not programs
            continue
        assert set(programs) == {
            "page",
            "project_page",
            "create",
            "update",
            "close",
            "start",
            "queue_page",
            "force_start",
            "priority",
        }
        bindings = runtime_operation_bindings(profile.version)
        owned = {
            source
            for name, binding in bindings.items()
            if name.startswith("task-group.")
            for source in binding.source_operations
        }
        assert owned == {program.source_operation for program in programs.values()}
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        assert not any(item.startswith("TaskGroupController.") for item in remaining)
        assert {_GROUP, _QUEUE}.isdisjoint(
            model.import_path for model in legacy.snapshot.models
        )
        # The shared runtime preserves its complete exact enum inventory;
        # this domain migration retires only owned operations and model closure.
        assert {enum.import_path for enum in legacy.snapshot.enums} == {
            enum.import_path for enum in original.snapshot.enums
        }
        assert _PAGE in {model.import_path for model in legacy.snapshot.models}
        assert programs["queue_page"].source_operation == reviewed.queue_operation
        for primitive, program in programs.items():
            assert program.result_envelope == (
                "optional"
                if primitive in {"page", "project_page", "queue_page"}
                else "required"
            )
    assert sum(len(profile.programs) for profile in plan.profiles) == 234


def test_task_group_requests_keep_native_defaults_and_requiredness(
    compiled_task_groups: CompiledDomainSet,
) -> None:
    plan = compiled_task_groups.plan("task_group")
    requests: dict[str, type[BaseParamsModel]] = {}
    for request in plan.requests:
        namespace: dict[str, object] = {
            "Field": Field,
            "BaseParamsModel": BaseParamsModel,
        }
        exec(compile(request.source, f"<{request.schema}>", "exec"), namespace)  # noqa: S102
        requests[request.schema] = cast(
            "type[BaseParamsModel]", namespace[request.class_name]
        )
    payload = {"name": "batch", "description": "", "groupSize": 2}
    assert requests["create"].model_validate(payload).model_dump() == {
        **payload,
        "projectCode": 0,
    }
    with pytest.raises(ValidationError, match="description"):
        requests["create"].model_validate({"name": "batch", "groupSize": 2})
    assert requests["id"].model_validate({}).model_dump() == {"id": None}
    with pytest.raises(ValidationError, match="id"):
        requests["update"].model_validate(payload)
    with pytest.raises(ValidationError, match="priority"):
        requests["priority"].model_validate({"queueId": 4})
    for identity, other in (("process", "workflow"), ("workflow", "process")):
        queue = requests[f"queue_{identity}"]
        result = queue.model_validate({"pageNo": 1, "pageSize": 20}).model_dump()
        assert result["groupId"] == -1
        assert result[f"{identity}InstanceName"] is None
        assert f"{other}InstanceName" not in result
        with pytest.raises(ValidationError, match=f"{other}InstanceName"):
            queue.model_validate(
                {"pageNo": 1, "pageSize": 20, f"{other}InstanceName": "workflow"}
            )


@pytest.mark.parametrize(
    ("schema", "required_id", "strict", "nullable_list"),
    [
        ("page_group_required_id_nullable_strict", True, True, True),
        ("page_group_nullable_id_list_strict", False, True, False),
        ("page_group_nullable_id_list", False, False, False),
        ("page_group_flag_list", False, False, False),
        ("page_queue_required_id_nullable_strict", True, True, True),
        ("page_queue_nullable_id_list_strict", False, True, False),
        ("page_queue_nullable_id_list", False, False, False),
        ("page_queue_workflow_list", False, False, False),
    ],
)
def test_task_group_page_epochs_preserve_validation_and_defaults(
    compiled_task_groups: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    required_id: bool,
    strict: bool,
    nullable_list: bool,
) -> None:
    adapter = response_adapter(
        compiled_task_groups.plan("task_group"), schema, monkeypatch
    )
    defaults = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(defaults, dict)
    assert defaults["totalList"] == (None if nullable_list else [])
    assert defaults["total"] == defaults["currentPage"] == 0
    assert defaults["pageSize"] == 20
    if strict:
        with pytest.raises(ValidationError, match="total"):
            adapter.validate_python({"total": "2"})
    else:
        page = adapter.dump_python(adapter.validate_python({"total": "2"}))
        assert isinstance(page, dict)
        assert page["total"] == 2
    page = adapter.dump_python(adapter.validate_python({"totalList": [{}]}))
    assert isinstance(page, dict)
    row = page["totalList"][0]
    assert row["id"] == (0 if required_id else None)
    if schema.startswith("page_queue"):
        assert row["priority"] == row["forceStart"] == row["inQueue"] == 0
        assert ("processId" in row) != ("workflowInstanceId" in row)
        expected_id = "workflowInstanceId" if "workflow" in schema else "processId"
        assert row[expected_id] == (None if "workflow" in schema else 0)
        with pytest.raises(ValidationError, match="status"):
            adapter.validate_python({"totalList": [{"status": "UNKNOWN"}]})
    else:
        assert row["groupSize"] == row["useSize"] == row["projectCode"] == 0


def test_task_group_entity_response_owns_flag_enum_closure(
    compiled_task_groups: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = response_adapter(
        compiled_task_groups.plan("task_group"), "entity_group_flag", monkeypatch
    )
    row = adapter.dump_python(adapter.validate_python({"status": "YES"}), mode="json")
    assert isinstance(row, dict)
    assert row["id"] is None
    assert row["status"] == "YES"
    assert row["groupSize"] == 0
    with pytest.raises(ValidationError, match="status"):
        adapter.validate_python({"status": 1})


@pytest.mark.parametrize(
    "drift",
    [
        "queue_operation",
        "queue_filter",
        "queue_identity",
        "create_result",
        "update_result",
        "numeric_status",
        "typed_results",
    ],
)
def test_task_group_rejects_reviewed_recipe_and_wire_disagreement(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    contract = task_group_contract("3.2.2")
    recipe = contract.task_group
    if drift == "queue_operation":
        recipe = replace(
            recipe, queue_operation="TaskGroupController.queryTasksByGroupId"
        )
    elif drift == "queue_filter":
        recipe = replace(recipe, queue_workflow_filter="workflowInstanceName")
    elif drift == "queue_identity":
        recipe = replace(recipe, queue_identity="workflow")
    elif drift == "create_result":
        recipe = replace(recipe, create_result="none")
    elif drift == "update_result":
        recipe = replace(recipe, update_result="none")
    elif drift == "numeric_status":
        recipe = replace(recipe, numeric_group_status=True)
    else:
        recipe = replace(recipe, typed_results=True)

    def reviewed(version: str) -> TaskGroupVersionContract:
        return (
            replace(contract, task_group=recipe)
            if version == "3.2.2"
            else task_group_contract(version)
        )

    monkeypatch.setattr("ds_codegen.compiled_task_groups.task_group_contract", reviewed)
    with pytest.raises(ValueError, match="contradicts reviewed"):
        compile_domains(exact_runtime_bundles, (TASK_GROUP_COMPILED_DOMAIN,))


def test_task_group_queue_page_requires_reviewed_projection(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = task_group_contract("3.2.2")

    def reviewed(version: str) -> TaskGroupVersionContract:
        return (
            replace(contract, queue_page_projection=None)
            if version == "3.2.2"
            else task_group_contract(version)
        )

    monkeypatch.setattr("ds_codegen.compiled_task_groups.task_group_contract", reviewed)
    with pytest.raises(ValueError, match="no reviewed paging projection"):
        compile_domains(exact_runtime_bundles, (TASK_GROUP_COMPILED_DOMAIN,))


@pytest.mark.parametrize(
    "drift", ["project_default", "group_size_required", "queue_type", "close_method"]
)
def test_task_group_rejects_request_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    bundle = next(
        item for item in exact_runtime_bundles if item.spec.version == "3.0.0"
    )
    method = {
        "project_default": "createTaskGroup",
        "group_size_required": "createTaskGroup",
        "queue_type": "queryTasksByGroupId",
        "close_method": "closeTaskGroup",
    }[drift]
    operation = next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id == f"TaskGroupController.{method}"
    )
    if drift == "project_default":
        changed = replace(
            operation,
            parameters=[
                replace(item, default_value="1")
                if item.wire_name == "projectCode"
                else item
                for item in operation.parameters
            ],
        )
        message = "request type or default"
    elif drift == "group_size_required":
        changed = replace(
            operation,
            parameters=[
                replace(item, required=False) if item.wire_name == "groupSize" else item
                for item in operation.parameters
            ],
        )
        message = "matching epochs"
    elif drift == "queue_type":
        changed = replace(
            operation,
            parameters=[
                replace(item, java_type="String")
                if item.wire_name == "groupId"
                else item
                for item in operation.parameters
            ],
        )
        message = "request type or default"
    else:
        changed = replace(operation, http_method="GET")
        message = "matching epochs"
    with pytest.raises(ValueError, match=message):
        compile_domains(
            replace_operation(exact_runtime_bundles, "3.0.0", changed),
            (TASK_GROUP_COMPILED_DOMAIN,),
        )


@pytest.mark.parametrize(
    ("model_path", "field_name"),
    [(_GROUP, "status"), (_QUEUE, "processId"), (_PAGE, "totalList")],
)
def test_task_group_rejects_response_field_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], model_path: str, field_name: str
) -> None:
    def changed(snapshot: ContractSnapshot) -> ContractSnapshot:
        return replace(
            snapshot,
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
                for model in snapshot.models
            ],
        )

    bundles = tuple(
        replace(bundle, snapshot=changed(bundle.snapshot))
        if bundle.spec.version == "3.0.0"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="response"):
        compile_domains(bundles, (TASK_GROUP_COMPILED_DOMAIN,))


def test_task_group_rejects_queue_status_enum_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                enums=[
                    replace(enum, json_value_field="code")
                    if enum.import_path == _QUEUE_STATUS
                    else enum
                    for enum in bundle.snapshot.enums
                ],
            ),
        )
        if bundle.spec.version == "3.0.0"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="TaskGroupQueueStatus enum changed"):
        compile_domains(bundles, (TASK_GROUP_COMPILED_DOMAIN,))
