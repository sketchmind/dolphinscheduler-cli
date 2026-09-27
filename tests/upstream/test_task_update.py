from __future__ import annotations

import json
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import UserInputError
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.upstream.task_update import compile_task_update
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskDefinition,
    FakeWorkflowTaskRelation,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_SCHEMA_SUGGESTION = (
    "Run `dsctl schema --command task.update` and inspect "
    "set.supported_keys. For structural definition changes, use `dsctl "
    "workflow edit --patch|--file`; for finished instance repair, use "
    "`dsctl workflow-instance edit --patch|--file`."
)


def _task_and_dag() -> tuple[FakeTaskDefinition, FakeDag]:
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        flag_value=FakeEnumValue("YES"),
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        description="Load current rows",
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        flag_value=FakeEnumValue("YES"),
    )
    dag = FakeDag(
        workflow_definition_value=None,
        task_definition_list_value=[extract, load],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=201,
                post_task_code_value=202,
            )
        ],
    )
    return load, dag


def _spec(**values: object) -> WorkflowPatchTaskSetSpec:
    return WorkflowPatchTaskSetSpec.model_validate(values)


def _replace_current_task(
    dag: FakeDag,
    current: FakeTaskDefinition,
    *,
    additional_tasks: list[FakeTaskDefinition] | None = None,
) -> FakeDag:
    tasks = dag.taskDefinitionList
    assert tasks is not None
    return replace(
        dag,
        task_definition_list_value=[
            tasks[0],
            *(additional_tasks or []),
            current,
        ],
    )


def _decoded_task_params(payload: Mapping[str, object]) -> dict[str, object]:
    task_params = payload["taskParams"]
    assert isinstance(task_params, str)
    decoded = json.loads(task_params)
    assert isinstance(decoded, dict)
    return decoded


def test_compile_preserves_opaque_task_params_and_existing_dependencies() -> None:
    current, dag = _task_and_dag()
    opaque_params = {
        "type": "MYSQL",
        "datasource": 17,
        "sql": "select 1",
        "sqlType": 0,
        "futurePluginField": {"nested": [1, "two", {"keep": True}]},
    }
    current = replace(
        current,
        task_type_value="SQL",
        task_params_value=json.dumps(opaque_params, separators=(",", ":")),
    )
    dag = _replace_current_task(dag, current)

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(description="Load future rows"),
        requested_fields=["description"],
        task_code=202,
    )

    assert _decoded_task_params(result.payload) == opaque_params
    assert result.updated_upstream_codes == (201,)
    assert result.updated_fields == ("description",)
    assert result.no_change is False


def test_compile_remote_shell_command_preserves_connection_and_unknown_fields() -> None:
    current, dag = _task_and_dag()
    current = replace(
        current,
        task_type_value="REMOTESHELL",
        task_params_value=json.dumps(
            {
                "rawScript": "echo old",
                "type": "SSH",
                "datasource": 17,
                "futureConnectionField": {"keep": True},
            },
            separators=(",", ":"),
        ),
    )
    dag = _replace_current_task(dag, current)

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(command="echo new"),
        requested_fields=["command"],
        task_code=202,
    )

    assert _decoded_task_params(result.payload) == {
        "rawScript": "echo new",
        "type": "SSH",
        "datasource": 17,
        "futureConnectionField": {"keep": True},
    }
    assert result.updated_upstream_codes == (201,)
    assert result.updated_fields == ("command",)
    assert result.no_change is False


def test_compile_maps_dependency_names_to_codes() -> None:
    current, dag = _task_and_dag()
    transform = FakeTaskDefinition(
        code=203,
        name="transform",
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo transform"}',
        flag_value=FakeEnumValue("YES"),
    )
    dag = _replace_current_task(dag, current, additional_tasks=[transform])

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(depends_on=["transform"]),
        requested_fields=["depends_on"],
        task_code=202,
    )

    assert result.current_upstream_codes == (201,)
    assert result.updated_upstream_codes == (203,)
    assert result.current_projection == {"depends_on": ["extract"]}
    assert result.expected_projection == {"depends_on": ["transform"]}
    assert result.updated_fields == ("depends_on",)
    assert result.no_change is False


@pytest.mark.parametrize(
    ("dependency", "message"),
    [
        ("missing", "Task dependency 'missing' was not found"),
        ("load", "A task cannot depend on itself"),
    ],
)
def test_compile_rejects_invalid_dependencies_with_the_cli_schema_hint(
    dependency: str,
    message: str,
) -> None:
    current, dag = _task_and_dag()

    with pytest.raises(UserInputError, match=message) as exc_info:
        compile_task_update(
            current_task=current,
            dag=dag,
            update_spec=_spec(depends_on=[dependency]),
            requested_fields=["depends_on"],
            task_code=202,
        )

    assert exc_info.value.suggestion == _SCHEMA_SUGGESTION


def test_compile_clears_nullable_fields_to_ds_sentinels() -> None:
    current, dag = _task_and_dag()
    current = replace(
        current,
        worker_group_value="analytics",
        environment_code_value=42,
        task_group_id_value=12,
        task_group_priority_value=3,
        cpu_quota_value=50,
        memory_max_value=1024,
    )
    dag = _replace_current_task(dag, current)
    requested_fields = [
        "worker_group",
        "environment_code",
        "task_group_id",
        "cpu_quota",
        "memory_max",
    ]

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(
            worker_group=None,
            environment_code=None,
            task_group_id=None,
            cpu_quota=None,
            memory_max=None,
        ),
        requested_fields=requested_fields,
        task_code=202,
    )

    assert result.updated_fields == tuple(requested_fields)
    assert result.no_change is False
    assert result.payload["workerGroup"] == "default"
    assert result.payload["environmentCode"] == -1
    assert result.payload["taskGroupId"] == 0
    assert result.payload["taskGroupPriority"] == 0
    assert result.payload["cpuQuota"] == -1
    assert result.payload["memoryMax"] == -1


def test_compile_opens_timeout_with_the_ds_default_notify_strategy() -> None:
    current, dag = _task_and_dag()

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(timeout=10),
        requested_fields=["timeout"],
        task_code=202,
    )

    assert result.updated_fields == ("timeout",)
    assert result.no_change is False
    assert result.payload["timeout"] == 10
    assert result.payload["timeoutFlag"] == "OPEN"
    assert result.payload["timeoutNotifyStrategy"] == "WARN"


def test_compile_treats_implicit_warn_timeout_strategy_as_a_no_op() -> None:
    current, dag = _task_and_dag()
    current = replace(
        current,
        timeout=15,
        timeout_flag_value=FakeEnumValue("OPEN"),
        timeout_notify_strategy_value=None,
    )
    dag = _replace_current_task(dag, current)

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(timeout_notify_strategy="WARN"),
        requested_fields=["timeout_notify_strategy"],
        task_code=202,
    )

    assert result.updated_fields == ()
    assert result.no_change is True
    assert result.payload["timeoutFlag"] == "OPEN"
    assert result.payload["timeoutNotifyStrategy"] == "WARN"


def test_compile_rejects_notify_strategy_without_a_timeout() -> None:
    current, dag = _task_and_dag()

    with pytest.raises(UserInputError, match="requires timeout > 0") as exc_info:
        compile_task_update(
            current_task=current,
            dag=dag,
            update_spec=_spec(timeout_notify_strategy="FAILED"),
            requested_fields=["timeout_notify_strategy"],
            task_code=202,
        )

    assert exc_info.value.suggestion == _SCHEMA_SUGGESTION


def test_compile_reports_semantic_no_op_without_dropping_the_payload() -> None:
    current, dag = _task_and_dag()

    result = compile_task_update(
        current_task=current,
        dag=dag,
        update_spec=_spec(description="Load current rows"),
        requested_fields=["description"],
        task_code=202,
    )

    assert result.payload["description"] == "Load current rows"
    assert result.updated_upstream_codes == (201,)
    assert result.updated_fields == ()
    assert result.no_change is True


def test_compile_rejects_dynamic_task_retry_that_cannot_reset_children() -> None:
    current, dag = _task_and_dag()
    current = replace(
        current,
        task_type_value="DYNAMIC",
        task_params_value=json.dumps(
            {
                "processDefinitionCode": 9001,
                "maxNumOfSubWorkflowInstances": 2,
                "degreeOfParallelism": 1,
                "filterCondition": "",
                "listParameters": [
                    {"name": "region", "value": "east,west", "separator": ","}
                ],
            }
        ),
    )
    dag = _replace_current_task(dag, current)

    with pytest.raises(UserInputError, match=r"DYNAMIC retry\.times must be 0"):
        compile_task_update(
            current_task=current,
            dag=dag,
            update_spec=_spec(retry={"times": 1, "interval": 1}),
            requested_fields=["retry.times", "retry.interval"],
            task_code=202,
        )
