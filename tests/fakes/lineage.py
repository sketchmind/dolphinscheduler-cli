"""In-memory lineage collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeWorkflowLineageRelation:
    source_work_flow_code_value: int
    target_work_flow_code_value: int

    @property
    def sourceWorkFlowCode(self) -> int:  # noqa: N802
        return self.source_work_flow_code_value

    @property
    def targetWorkFlowCode(self) -> int:  # noqa: N802
        return self.target_work_flow_code_value


@dataclass(frozen=True)
class FakeWorkflowLineageDetail:
    work_flow_code_value: int
    work_flow_name_value: str | None
    work_flow_publish_status_value: str | None = None
    schedule_start_time_value: str | None = None
    schedule_end_time_value: str | None = None
    crontab_value: str | None = None
    schedule_publish_status_value: int = 0
    source_work_flow_code_value: str | None = None

    @property
    def workFlowCode(self) -> int:  # noqa: N802
        return self.work_flow_code_value

    @property
    def workFlowName(self) -> str | None:  # noqa: N802
        return self.work_flow_name_value

    @property
    def workFlowPublishStatus(self) -> str | None:  # noqa: N802
        return self.work_flow_publish_status_value

    @property
    def scheduleStartTime(self) -> str | None:  # noqa: N802
        return self.schedule_start_time_value

    @property
    def scheduleEndTime(self) -> str | None:  # noqa: N802
        return self.schedule_end_time_value

    @property
    def crontab(self) -> str | None:
        return self.crontab_value

    @property
    def schedulePublishStatus(self) -> int:  # noqa: N802
        return self.schedule_publish_status_value

    @property
    def sourceWorkFlowCode(self) -> str | None:  # noqa: N802
        return self.source_work_flow_code_value


@dataclass(frozen=True)
class FakeWorkflowLineage:
    work_flow_relation_list_value: list[FakeWorkflowLineageRelation] | None
    work_flow_relation_detail_list_value: list[FakeWorkflowLineageDetail] | None

    @property
    def workFlowRelationList(self) -> list[FakeWorkflowLineageRelation] | None:  # noqa: N802
        return self.work_flow_relation_list_value

    @property
    def workFlowRelationDetailList(self) -> list[FakeWorkflowLineageDetail] | None:  # noqa: N802
        return self.work_flow_relation_detail_list_value


@dataclass(frozen=True)
class FakeDependentLineageTask:
    project_code_value: int
    workflow_definition_code_value: int
    workflow_definition_name_value: str | None
    task_definition_code_value: int
    task_definition_name_value: str | None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        return self.workflow_definition_name_value

    @property
    def taskDefinitionCode(self) -> int:  # noqa: N802
        return self.task_definition_code_value

    @property
    def taskDefinitionName(self) -> str | None:  # noqa: N802
        return self.task_definition_name_value


@dataclass
class FakeWorkflowLineageAdapter:
    project_lineages: dict[int, FakeWorkflowLineage]
    workflow_lineages: dict[tuple[int, int], FakeWorkflowLineage]
    dependent_tasks_by_target: dict[
        tuple[int, int, int | None],
        list[FakeDependentLineageTask],
    ] = field(default_factory=dict)

    def list(self, *, project_code: int) -> FakeWorkflowLineage | None:
        return self.project_lineages.get(
            project_code,
            FakeWorkflowLineage(
                work_flow_relation_list_value=[],
                work_flow_relation_detail_list_value=[],
            ),
        )

    def get(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> FakeWorkflowLineage | None:
        return self.workflow_lineages.get(
            (project_code, workflow_code),
            FakeWorkflowLineage(
                work_flow_relation_list_value=[],
                work_flow_relation_detail_list_value=[],
            ),
        )

    def query_dependent_tasks(
        self,
        *,
        project_code: int,
        workflow_code: int,
        task_code: int | None = None,
    ) -> Sequence[FakeDependentLineageTask]:
        return list(
            self.dependent_tasks_by_target.get(
                (project_code, workflow_code, task_code),
                [],
            )
        )


def empty_workflow_lineage_adapter() -> FakeWorkflowLineageAdapter:
    return FakeWorkflowLineageAdapter(project_lineages={}, workflow_lineages={})
