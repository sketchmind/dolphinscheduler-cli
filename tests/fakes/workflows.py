"""In-memory workflows collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
)
from tests.fakes.definitions import (
    FakeDag,
    FakeWorkflow,
    FakeWorkflowPage,
    _fake_task_definition_from_payload,
    _fake_task_relation_from_payload,
    _updated_fake_workflow_tasks_and_relations,
)
from tests.fakes.values import (
    _json_array,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from tests.fakes.schedules import (
        FakeScheduleAdapter,
    )


@dataclass
class FakeWorkflowAdapter:
    workflows: list[FakeWorkflow]
    dags: dict[int, FakeDag]
    run_results_by_code: dict[int, Sequence[int]] | None = None
    run_errors_by_code: dict[int, ApiResultError] | None = None
    create_errors_by_name: dict[str, ApiResultError] | None = None
    update_errors_by_code: dict[int, ApiResultError] | None = None
    delete_errors_by_code: dict[int, ApiResultError] | None = None
    online_errors_by_code: dict[int, ApiResultError] | None = None
    offline_errors_by_code: dict[int, ApiResultError] | None = None
    get_errors_by_call: dict[int, Exception] = field(default_factory=dict)
    schedule_adapter: FakeScheduleAdapter | None = None
    create_calls: list[dict[str, object]] = field(default_factory=list)
    update_calls: list[dict[str, object]] = field(default_factory=list)
    run_calls: list[dict[str, object]] = field(default_factory=list)
    backfill_calls: list[dict[str, object]] = field(default_factory=list)
    release_calls: list[tuple[int, str]] = field(default_factory=list)
    get_calls: list[int] = field(default_factory=list)

    def list_refs(self, *, project_code: int) -> list[FakeWorkflow]:
        return [
            workflow
            for workflow in self.workflows
            if workflow.projectCode == project_code
        ]

    def list_page(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeWorkflowPage:
        filtered = [
            workflow
            for workflow in self.workflows
            if workflow.projectCode == project_code
        ]
        if search is not None:
            filtered = [
                workflow
                for workflow in filtered
                if workflow.name is not None and search.lower() in workflow.name.lower()
            ]
        filtered = [self._with_attached_schedule(workflow) for workflow in filtered]
        return FakeWorkflowPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, project_code: int, code: int) -> FakeWorkflow:
        self.get_calls.append(code)
        call_error = self.get_errors_by_call.get(len(self.get_calls))
        if call_error is not None:
            raise call_error
        for workflow in self.workflows:
            if workflow.code == code and workflow.projectCode == project_code:
                return workflow
        raise ApiResultError(
            result_code=50003,
            result_message=f"workflow code {code} not found",
        )

    def describe(self, *, project_code: int, code: int) -> FakeDag:
        self.get(project_code=project_code, code=code)
        dag = self.dags.get(code)
        if dag is None:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow dag {code} not found",
            )
        return dag

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> None:
        self.create_calls.append(
            {
                "project_code": project_code,
                "name": name,
                "description": description,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "execution_type": execution_type,
            }
        )
        if (
            self.create_errors_by_name is not None
            and name in self.create_errors_by_name
        ):
            raise self.create_errors_by_name[name]
        next_code = max((workflow.code for workflow in self.workflows), default=100) + 1
        created = FakeWorkflow(
            code=next_code,
            name=name,
            project_code_value=project_code,
            description=description,
            global_params_value=global_params,
            user_id_value=11,
            user_name_value="alice",
            timeout=timeout,
            execution_type_value=(
                None if execution_type is None else FakeEnumValue(execution_type)
            ),
        )
        task_definitions = [
            _fake_task_definition_from_payload(item, project_code=project_code)
            for item in _json_array(task_definition_json, label="task_definition_json")
        ]
        task_relations = [
            _fake_task_relation_from_payload(item)
            for item in _json_array(task_relation_json, label="task_relation_json")
        ]
        self.workflows.append(created)
        self.dags[next_code] = FakeDag(
            workflow_definition_value=created,
            task_definition_list_value=task_definitions,
            workflow_task_relation_list_value=task_relations,
        )

    def run(
        self,
        *,
        project_code: int,
        workflow_code: int,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        self.run_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "worker_group": worker_group,
                "tenant_code": tenant_code,
                "start_node_list": (
                    None if start_node_list is None else list(start_node_list)
                ),
                "task_scope": task_scope,
                "failure_strategy": failure_strategy,
                "warning_type": warning_type,
                "workflow_instance_priority": workflow_instance_priority,
                "warning_group_id": warning_group_id,
                "environment_code": environment_code,
                "start_params": start_params,
                "dry_run": dry_run,
            }
        )
        self.get(project_code=project_code, code=workflow_code)
        if (
            self.run_errors_by_code is not None
            and workflow_code in self.run_errors_by_code
        ):
            raise self.run_errors_by_code[workflow_code]
        if self.run_results_by_code is not None:
            return list(self.run_results_by_code.get(workflow_code, []))
        return [workflow_code * 10]

    def backfill(
        self,
        *,
        project_code: int,
        workflow_code: int,
        schedule_time: str,
        run_mode: str,
        expected_parallelism_number: int,
        complement_dependent_mode: str,
        all_level_dependent: bool,
        execution_order: str,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        self.backfill_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "schedule_time": schedule_time,
                "run_mode": run_mode,
                "expected_parallelism_number": expected_parallelism_number,
                "complement_dependent_mode": complement_dependent_mode,
                "all_level_dependent": all_level_dependent,
                "execution_order": execution_order,
                "worker_group": worker_group,
                "tenant_code": tenant_code,
                "start_node_list": (
                    None if start_node_list is None else list(start_node_list)
                ),
                "task_scope": task_scope,
                "failure_strategy": failure_strategy,
                "warning_type": warning_type,
                "workflow_instance_priority": workflow_instance_priority,
                "warning_group_id": warning_group_id,
                "environment_code": environment_code,
                "start_params": start_params,
                "dry_run": dry_run,
            }
        )
        self.get(project_code=project_code, code=workflow_code)
        if (
            self.run_errors_by_code is not None
            and workflow_code in self.run_errors_by_code
        ):
            raise self.run_errors_by_code[workflow_code]
        if self.run_results_by_code is not None:
            return list(self.run_results_by_code.get(workflow_code, []))
        return [workflow_code * 10]

    def update(
        self,
        *,
        project_code: int,
        workflow_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
    ) -> None:
        self.update_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "name": name,
                "description": description,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "execution_type": execution_type,
                "release_state": release_state,
            }
        )
        if (
            self.update_errors_by_code is not None
            and workflow_code in self.update_errors_by_code
        ):
            raise self.update_errors_by_code[workflow_code]

        workflow = self.get(project_code=project_code, code=workflow_code)

        effective_release_state = release_state or "OFFLINE"
        updated_schedule = workflow.schedule
        updated_schedule_release_state = workflow.scheduleReleaseState
        if (
            effective_release_state == "OFFLINE"
            and workflow.scheduleReleaseState is not None
        ):
            updated_schedule_release_state = FakeEnumValue("OFFLINE")
            if workflow.schedule is not None:
                updated_schedule = replace(
                    workflow.schedule,
                    release_state_value=FakeEnumValue("OFFLINE"),
                )

        updated_workflow = replace(
            workflow,
            name=name,
            version=(workflow.version or 1) + 1,
            description=description,
            global_params_value=global_params,
            timeout=timeout,
            update_time_value="2026-04-10 12:00:00",
            release_state_value=FakeEnumValue(effective_release_state),
            schedule_release_state_value=updated_schedule_release_state,
            execution_type_value=(
                None if execution_type is None else FakeEnumValue(execution_type)
            ),
            schedule_value=updated_schedule,
        )

        task_definitions, task_relations = _updated_fake_workflow_tasks_and_relations(
            dags=self.dags,
            workflow_code=workflow_code,
            project_code=project_code,
            task_definition_json=task_definition_json,
            task_relation_json=task_relation_json,
        )

        for index, existing in enumerate(self.workflows):
            if existing.code == workflow_code:
                self.workflows[index] = updated_workflow
                break
        self.dags[workflow_code] = FakeDag(
            workflow_definition_value=updated_workflow,
            task_definition_list_value=task_definitions,
            workflow_task_relation_list_value=task_relations,
        )

    def delete(self, *, project_code: int, workflow_code: int) -> None:
        if (
            self.delete_errors_by_code is not None
            and workflow_code in self.delete_errors_by_code
        ):
            raise self.delete_errors_by_code[workflow_code]
        for index, workflow in enumerate(self.workflows):
            if workflow.code != workflow_code or workflow.projectCode != project_code:
                continue
            self.workflows.pop(index)
            self.dags.pop(workflow_code, None)
            if self.schedule_adapter is not None:
                self.schedule_adapter.schedules = [
                    schedule
                    for schedule in self.schedule_adapter.schedules
                    if schedule.workflowDefinitionCode != workflow_code
                ]
            return
        raise ApiResultError(
            result_code=10018,
            result_message=f"workflow code {workflow_code} not found",
        )

    def online(self, *, project_code: int, workflow_code: int) -> None:
        self.release_calls.append((workflow_code, "ONLINE"))
        if (
            self.online_errors_by_code is not None
            and workflow_code in self.online_errors_by_code
        ):
            raise self.online_errors_by_code[workflow_code]
        self._set_release_state(
            project_code=project_code,
            workflow_code=workflow_code,
            release_state="ONLINE",
        )

    def offline(self, *, project_code: int, workflow_code: int) -> None:
        self.release_calls.append((workflow_code, "OFFLINE"))
        if (
            self.offline_errors_by_code is not None
            and workflow_code in self.offline_errors_by_code
        ):
            raise self.offline_errors_by_code[workflow_code]
        self._set_release_state(
            project_code=project_code,
            workflow_code=workflow_code,
            release_state="OFFLINE",
        )

    def _set_release_state(
        self,
        *,
        project_code: int,
        workflow_code: int,
        release_state: str,
    ) -> None:
        for index, workflow in enumerate(self.workflows):
            if workflow.code != workflow_code or workflow.projectCode != project_code:
                continue
            schedule = workflow.schedule
            updated_schedule = schedule
            updated_schedule_release_state = workflow.scheduleReleaseState
            if release_state == "OFFLINE" and workflow.scheduleReleaseState is not None:
                updated_schedule_release_state = FakeEnumValue("OFFLINE")
                if schedule is not None:
                    updated_schedule = replace(
                        schedule,
                        release_state_value=FakeEnumValue("OFFLINE"),
                    )
            updated = replace(
                workflow,
                release_state_value=FakeEnumValue(release_state),
                schedule_release_state_value=updated_schedule_release_state,
                schedule_value=updated_schedule,
            )
            self.workflows[index] = updated
            dag = self.dags.get(workflow_code)
            if dag is not None:
                self.dags[workflow_code] = replace(
                    dag,
                    workflow_definition_value=updated,
                )
            if release_state == "OFFLINE" and self.schedule_adapter is not None:
                self.schedule_adapter.schedules = [
                    replace(
                        attached,
                        release_state_value=FakeEnumValue("OFFLINE"),
                    )
                    if attached.workflowDefinitionCode == workflow_code
                    else attached
                    for attached in self.schedule_adapter.schedules
                ]
            return
        raise ApiResultError(
            result_code=10018,
            result_message=f"workflow code {workflow_code} not found",
        )

    def _with_attached_schedule(self, workflow: FakeWorkflow) -> FakeWorkflow:
        if self.schedule_adapter is None:
            return workflow
        attached = next(
            (
                schedule
                for schedule in self.schedule_adapter.schedules
                if schedule.workflowDefinitionCode == workflow.code
            ),
            None,
        )
        return replace(
            workflow,
            schedule_value=attached,
            schedule_release_state_value=(
                None if attached is None else attached.releaseState
            ),
        )


def empty_workflow_adapter() -> FakeWorkflowAdapter:
    return FakeWorkflowAdapter(workflows=[], dags={})
