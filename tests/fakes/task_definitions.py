"""In-memory task definitions collaborators with explicit test-owned state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, cast

from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.upstream.serialization import serialize_task
from dsctl.upstream.task_definition_wire import (
    TaskTopLevelFieldPolicy,
    TaskUpdateWirePolicy,
)
from dsctl.upstream.wire import (
    PreparedWireCallToken,
    WireExecution,
    WireRequest,
)
from tests.fake_wire import FakePreparedWireCall
from tests.fakes.values import (
    _optional_enum,
    _optional_json_value,
    _optional_string,
    _require_int,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonValue
    from dsctl.upstream.protocol import (
        TaskPayloadRecord,
        WorkflowDagRecord,
    )
    from tests.fakes.definitions import (
        FakeTaskDefinition,
    )
    from tests.fakes.workflows import (
        FakeWorkflowAdapter,
    )


@dataclass
class FakeTaskAdapter:
    workflow_tasks: dict[int, list[FakeTaskDefinition]]
    generated_codes: list[int] | None = None
    generate_codes_error: ApiResultError | ApiTransportError | None = None
    generate_code_calls: list[dict[str, int]] = field(default_factory=list)
    get_errors_by_code: dict[int, ApiResultError] | None = None
    update_errors_by_code: dict[int, ApiResultError] | None = None
    update_calls: list[dict[str, object]] = field(default_factory=list)

    def generate_codes(self, *, project_code: int, count: int) -> list[int]:
        self.generate_code_calls.append({"project_code": project_code, "count": count})
        if self.generate_codes_error is not None:
            raise self.generate_codes_error
        if self.generated_codes is None:
            start = 8_000_000_000_000_000_000 + sum(
                call["count"] for call in self.generate_code_calls[:-1]
            )
            return list(range(start, start + count))
        if len(self.generated_codes) < count:
            message = "FakeTaskAdapter has too few injected task codes"
            raise AssertionError(message)
        task_codes = self.generated_codes[:count]
        del self.generated_codes[:count]
        return task_codes

    def list(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> list[FakeTaskDefinition]:
        tasks = self.workflow_tasks.get(workflow_code, [])
        return [task for task in tasks if task.projectCode == project_code]

    def get(self, *, code: int) -> FakeTaskDefinition:
        if self.get_errors_by_code is not None and code in self.get_errors_by_code:
            raise self.get_errors_by_code[code]
        for tasks in self.workflow_tasks.values():
            for task in tasks:
                if task.code == code:
                    return task
        raise ApiResultError(
            result_code=10018,
            result_message=f"task code {code} not found",
        )

    def update(
        self,
        *,
        project_code: int,
        code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> None:
        self.update_calls.append(
            {
                "project_code": project_code,
                "code": code,
                "task_definition_json": task_definition_json,
                "upstream_codes": list(upstream_codes),
            }
        )
        if (
            self.update_errors_by_code is not None
            and code in self.update_errors_by_code
        ):
            raise self.update_errors_by_code[code]
        payload = json.loads(task_definition_json)
        if not isinstance(payload, dict):
            message = "task_definition_json must decode to a JSON object"
            raise TypeError(message)
        for workflow_code, tasks in self.workflow_tasks.items():
            for index, task in enumerate(tasks):
                if task.code != code:
                    continue
                if task.projectCode != project_code:
                    continue
                current_task_priority = task.taskPriority
                current_timeout_flag = task.timeoutFlag
                current_timeout_notify_strategy = task.timeoutNotifyStrategy
                current_task_execute_type = task.taskExecuteType
                current_flag = task.flag
                tasks[index] = replace(
                    task,
                    name=_optional_string(payload.get("name")) or task.name,
                    description=_optional_string(payload.get("description")),
                    task_type_value=_optional_string(payload.get("taskType"))
                    or task.taskType,
                    task_params_value=_optional_json_value(payload.get("taskParams")),
                    worker_group_value=_optional_string(payload.get("workerGroup")),
                    fail_retry_times_value=_require_int(
                        payload.get("failRetryTimes", task.failRetryTimes)
                    ),
                    fail_retry_interval_value=_require_int(
                        payload.get("failRetryInterval", task.failRetryInterval)
                    ),
                    timeout=_require_int(payload.get("timeout", task.timeout)),
                    delay_time_value=_require_int(
                        payload.get("delayTime", task.delayTime)
                    ),
                    resource_ids_value=_optional_string(
                        payload.get("resourceIds", task.resourceIds)
                    ),
                    environment_code_value=_require_int(
                        payload.get("environmentCode", task.environmentCode)
                    ),
                    task_group_id_value=_require_int(
                        payload.get("taskGroupId", task.taskGroupId)
                    ),
                    task_group_priority_value=_require_int(
                        payload.get("taskGroupPriority", task.taskGroupPriority)
                    ),
                    task_priority_value=(
                        _optional_enum(payload.get("taskPriority"))
                        if "taskPriority" in payload
                        else current_task_priority
                    ),
                    timeout_flag_value=(
                        _optional_enum(payload.get("timeoutFlag"))
                        if "timeoutFlag" in payload
                        else current_timeout_flag
                    ),
                    timeout_notify_strategy_value=(
                        _optional_enum(payload.get("timeoutNotifyStrategy"))
                        if "timeoutNotifyStrategy" in payload
                        else current_timeout_notify_strategy
                    ),
                    task_execute_type_value=(
                        _optional_enum(payload.get("taskExecuteType"))
                        if "taskExecuteType" in payload
                        else current_task_execute_type
                    ),
                    flag_value=(
                        _optional_enum(payload.get("flag"))
                        if "flag" in payload
                        else current_flag
                    ),
                    cpu_quota_value=(
                        None
                        if payload.get("cpuQuota", task.cpuQuota) is None
                        else _require_int(payload.get("cpuQuota", task.cpuQuota))
                    ),
                    memory_max_value=(
                        None
                        if payload.get("memoryMax", task.memoryMax) is None
                        else _require_int(payload.get("memoryMax", task.memoryMax))
                    ),
                    version=(task.version or 0) + 1,
                )
                del workflow_code, upstream_codes
                return
        raise ApiResultError(
            result_code=10018,
            result_message=f"task code {code} not found",
        )


@dataclass(frozen=True)
class _FakeTaskUpdateArgs:
    project_code: int
    task_code: int
    task_definition_json: str
    upstream_codes: tuple[int, ...]


@dataclass
class FakeTaskDefinitionWire:
    """In-memory exact-wire substitute used below the deep task module."""

    adapter: FakeTaskAdapter
    ds_version: str = "3.4.1"
    recipe_fingerprint: str = "sha256:test-task-recipe"
    workflow_adapter: FakeWorkflowAdapter | None = None

    @property
    def update_policy(self) -> TaskUpdateWirePolicy:
        """Allow every current patch field in the modern in-memory recipe."""
        return TaskUpdateWirePolicy(
            update_available=True,
            dependency_update=True,
            unsupported_patch_fields=frozenset(),
        )

    @property
    def top_level_field_policy(self) -> TaskTopLevelFieldPolicy:
        """Classify the top-level task fields emitted by this fake wire."""
        fields = frozenset(
            key
            for tasks in self.adapter.workflow_tasks.values()
            for task in tasks
            for key in serialize_task(task)
        )
        return TaskTopLevelFieldPolicy(
            request_payload=fields,
            server_managed=frozenset(),
            response_derived=frozenset(),
            relation_projection=frozenset(),
            opaque_preservation=(
                frozenset({"taskParams"}) if "taskParams" in fields else frozenset()
            ),
        )

    def get(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> WireExecution[TaskPayloadRecord]:
        task = self.adapter.get(code=task_code)
        if task.projectCode != project_code:
            message = "Fake task does not belong to the requested project"
            raise AssertionError(message)
        return WireExecution(
            payload=cast("TaskPayloadRecord", task),
            raw_payload=cast("JsonValue", serialize_task(task)),
            request=WireRequest(
                method="GET",
                path=f"/projects/{project_code}/task-definition/{task_code}",
                query=None,
                form=None,
                json=None,
                content=None,
            ),
        )

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        """Return the fake workflow DAG through the exact-wire shape."""
        if self.workflow_adapter is None:
            message = "Fake task wire requires a workflow adapter"
            raise AssertionError(message)
        dag = self.workflow_adapter.describe(
            project_code=project_code,
            code=workflow_code,
        )
        return WireExecution(
            payload=cast("WorkflowDagRecord", dag),
            raw_payload={},
            request=WireRequest(
                method="GET",
                path=(f"/projects/{project_code}/workflow-definition/{workflow_code}"),
                query=None,
                form=None,
                json=None,
                content=None,
            ),
        )

    def prepare_update(
        self,
        *,
        project_code: int,
        task_code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> FakePreparedWireCall[_FakeTaskUpdateArgs]:
        args = _FakeTaskUpdateArgs(
            project_code=project_code,
            task_code=task_code,
            task_definition_json=task_definition_json,
            upstream_codes=tuple(upstream_codes),
        )
        form: dict[str, str] = {"taskDefinitionJsonObj": task_definition_json}
        if upstream_codes:
            form["upstreamCodes"] = ",".join(str(code) for code in upstream_codes)
        return FakePreparedWireCall(
            ds_version=self.ds_version,
            program_fingerprint="sha256:test-task-update-wire",
            args=args,
            request=WireRequest(
                method="PUT",
                path=(
                    f"/projects/{project_code}/task-definition/{task_code}"
                    "/with-upstream"
                ),
                query=None,
                form=form,
                json=None,
                content=None,
            ),
        )

    def apply_update(
        self,
        prepared: PreparedWireCallToken,
    ) -> WireExecution[int | None]:
        concrete = cast("FakePreparedWireCall[_FakeTaskUpdateArgs]", prepared)
        args = concrete.args
        self.adapter.update(
            project_code=args.project_code,
            code=args.task_code,
            task_definition_json=args.task_definition_json,
            upstream_codes=args.upstream_codes,
        )
        self._sync_dag_task(args.task_code)
        return WireExecution(
            payload=args.task_code,
            raw_payload=args.task_code,
            request=prepared.request,
        )

    def _sync_dag_task(self, task_code: int) -> None:
        if self.workflow_adapter is None:
            return
        updated = self.adapter.get(code=task_code)
        version = updated.version or 0
        for dag in self.workflow_adapter.dags.values():
            tasks = dag.task_definition_list_value
            if tasks is None or all(task.code != task_code for task in tasks):
                continue
            tasks[:] = [updated if task.code == task_code else task for task in tasks]
            relations = dag.workflow_task_relation_list_value
            if relations is None:
                continue
            relations[:] = [
                replace(
                    relation,
                    pre_task_version_value=(
                        version
                        if relation.preTaskCode == task_code
                        else relation.preTaskVersion
                    ),
                    post_task_version_value=(
                        version
                        if relation.postTaskCode == task_code
                        else relation.postTaskVersion
                    ),
                )
                for relation in relations
            ]


def empty_task_adapter() -> FakeTaskAdapter:
    return FakeTaskAdapter(workflow_tasks={})
