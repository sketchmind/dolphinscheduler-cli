"""Independent source guards for the instance/task fragments of one joint plan."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen import compiled_workflow_instances as instances
from ds_codegen import compiled_workflow_tasks as tasks
from ds_codegen.compiled_domains import CompiledDomainDefinition, _select_request_epoch
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)

if TYPE_CHECKING:
    from types import ModuleType

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

pytestmark = pytest.mark.source_contract


@pytest.mark.parametrize(("fragment", "count"), [(instances, 388), (tasks, 160)])
def test_all_exact_source_coordinates_match_reviewed_fragment_roles(
    exact_contract_corpus: ExactContractCorpus, fragment: ModuleType, count: int
) -> None:
    definition = CompiledDomainDefinition(
        name="workflow_fragment_probe",
        schema_constant="WORKFLOW_FRAGMENT_PROBE_SCHEMA_VERSION",
        schema_version=1,
        semantic_operations=fragment.SEMANTIC_OPERATIONS,
        absent_versions=frozenset(),
        primitives=fragment.PRIMITIVES,
        classify_operation=fragment.classify,
        response_policy=fragment.response_policy,
        recipe_policy=fragment.recipe_policy,
    )
    total = 0
    for version in exact_contract_corpus.versions:
        snapshot = exact_contract_corpus.snapshot(version)
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        source_ids = {
            source
            for binding in bindings.values()
            for source in binding.source_operations
        }
        operations = {
            fragment.classify(operation): operation
            for operation in snapshot.operations
            if operation.operation_id in source_ids
            and fragment.classify(operation) is not None
        }
        if version == "3.4.3":
            expected_sources = (
                {
                    "instance_page": (
                        "WorkflowInstanceController.queryWorkflowInstanceList"
                    ),
                    "instance_get": (
                        "WorkflowInstanceController.queryWorkflowInstanceById"
                    ),
                    "instance_trigger": (
                        "WorkflowInstanceController.queryWorkflowInstancesByTriggerCode"
                    ),
                    "instance_parent": (
                        "WorkflowInstanceController.queryParentInstanceBySubId"
                    ),
                    "instance_sub": (
                        "WorkflowInstanceController.querySubWorkflowInstanceByTaskId"
                    ),
                    "instance_update": (
                        "WorkflowInstanceController.updateWorkflowInstance"
                    ),
                    "instance_control": "ExecutorController.controlWorkflowInstance",
                    "instance_execute_task": "ExecutorController.executeTask",
                    "task_instance_page": "TaskInstanceController.queryTaskListPaging",
                    "task_instance_force_success": (
                        "TaskInstanceController.forceTaskSuccess"
                    ),
                    "task_instance_savepoint": "TaskInstanceController.taskSavePoint",
                    "task_instance_stop": "TaskInstanceController.stopTask",
                    "task_log": "LoggerController.queryLog",
                }
                if fragment is instances
                else {
                    "task_code_allocate": "TaskDefinitionController.genTaskCodeList",
                    "task_get": "TaskDefinitionController.queryTaskDefinitionDetail",
                }
            )
            assert {
                name: operation.operation_id for name, operation in operations.items()
            } == expected_sources
        codecs = {}
        for primitive in fragment.PRIMITIVES:
            if version in primitive.absent_versions:
                assert primitive.name not in operations
                continue
            operation = operations[primitive.name]
            epoch = _select_request_epoch(definition, primitive, operation, snapshot)
            assert epoch.versions == frozenset({version})
            assert epoch.content_addressed
            policy = fragment.response_policy(snapshot, operation, primitive.name)
            assert policy.content_addressed == (policy.schema is not None)
            codecs[primitive.name] = policy.codec
            total += 1
        assert fragment.recipe_policy(codecs) == version.replace(".", "_")
        if codecs:
            first = next(iter(codecs))
            with pytest.raises(ValueError, match="exact reviewed recipe"):
                fragment.recipe_policy({**codecs, first: "unreviewed_codec"})
    assert total == count


@pytest.mark.parametrize(
    ("fragment", "version", "source", "primitive", "field"),
    [
        (
            instances,
            "1.3.9",
            "ProcessInstanceController.updateProcessInstance",
            "instance_update_legacy",
            "syncDefine",
        ),
        (
            instances,
            "3.4.2",
            "ExecutorController.executeTask",
            "instance_execute_task",
            "taskDependType",
        ),
        (
            instances,
            "3.1.0",
            "TaskInstanceController.queryTaskListPaging",
            "task_instance_page",
            "taskExecuteType",
        ),
        (
            tasks,
            "3.2.1",
            "TaskDefinitionController.updateTaskWithUpstream",
            "task_update",
            "upstreamCodes",
        ),
        (
            tasks,
            "3.1.0",
            "TaskDefinitionController.queryTaskDefinitionListPaging",
            "task_cleanup_page",
            "taskExecuteType",
        ),
    ],
)
@pytest.mark.parametrize("change", ["missing", "type", "default", "required", "alias"])
def test_request_source_drift_is_not_authorized_by_rendered_schema_identity(
    exact_contract_corpus: ExactContractCorpus,
    fragment: ModuleType,
    version: str,
    source: str,
    primitive: str,
    field: str,
    change: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item for item in snapshot.operations if item.operation_id == source
    )
    parameter = next(item for item in operation.parameters if item.wire_name == field)
    changed = parameter
    if change == "type":
        changed = replace(parameter, java_type="Object")
    elif change == "default":
        changed = replace(parameter, default_value="unreviewed")
    elif change == "required":
        changed = replace(parameter, required=not parameter.required)
    elif change == "alias":
        changed = replace(parameter, wire_name="unreviewedField")
    operation = replace(
        operation,
        parameters=[
            changed if item == parameter else item
            for item in operation.parameters
            if change != "missing" or item != parameter
        ],
    )
    with pytest.raises(ValueError, match="request fields changed"):
        fragment.response_policy(snapshot, operation, primitive)


@pytest.mark.parametrize(
    ("fragment", "source", "primitive", "model_name", "field"),
    [
        (
            instances,
            "WorkflowInstanceController.queryWorkflowInstanceById",
            "instance_get",
            "WorkflowInstance",
            "id",
        ),
        (
            instances,
            "LoggerController.queryLog",
            "task_log",
            "ResponseTaskLog",
            "lineNum",
        ),
        (
            tasks,
            "TaskDefinitionController.queryTaskDefinitionDetail",
            "task_get",
            "TaskDefinitionVO",
            "projectCode",
        ),
    ],
)
@pytest.mark.parametrize("change", ["missing", "type"])
def test_consumed_response_identity_remains_a_semantic_guard(
    exact_contract_corpus: ExactContractCorpus,
    fragment: ModuleType,
    source: str,
    primitive: str,
    model_name: str,
    field: str,
    change: str,
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = next(
        item for item in snapshot.operations if item.operation_id == source
    )
    model = next(item for item in snapshot.models if item.name == model_name)
    changed = replace(
        model,
        fields=[
            replace(item, java_type="String") if item.name == field else item
            for item in model.fields
            if change != "missing" or item.name != field
        ],
    )
    snapshot = replace(
        snapshot,
        models=[changed if item == model else item for item in snapshot.models],
    )
    with pytest.raises(ValueError, match=r"consumed response fields|field policy"):
        fragment.response_policy(snapshot, operation, primitive)


def test_existing_private_cleanup_epochs_keep_their_distinct_ownership() -> None:
    primitives = {primitive.name: primitive for primitive in tasks.PRIMITIVES}
    assert len(primitives["task_cleanup_page"].requests) == 27
    assert len(primitives["task_cleanup_delete"].requests) == 20
    assert len(primitives["task_cleanup_history"].requests) == 7
    assert len(primitives["task_cleanup_release"].requests) == 3
    assert {
        version
        for request in primitives["task_cleanup_release"].requests
        for version in request.versions or ()
    } == {"2.0.1", "2.0.2", "2.0.3"}
    history_versions: set[str] = set()
    for request in primitives["task_cleanup_history"].requests:
        assert request.versions is not None
        history_versions.update(request.versions)
    assert history_versions == {
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }
    assert "3.1.9" in primitives["task_cleanup_delete"].absent_versions
    assert primitives["task_cleanup_delete"].result_envelope == "optional"
    assert primitives["task_update"].result_envelope == "optional"
    instance = {primitive.name: primitive for primitive in instances.PRIMITIVES}
    assert instance["instance_update_legacy"].result_envelope == "optional"
    assert instance["instance_update"].result_envelope == "required"
    assert instance["instance_control"].result_envelope == "required"
