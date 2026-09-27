"""Named response corrections, independent of exact review membership."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DAO_RESOURCE_IMPORT = "org.apache.dolphinscheduler.dao.entity.Resource"


SourceEvidence = Literal[
    "alert_plugin_page_nullable_total_list",
    "legacy_datasource_page_records",
    "legacy_process_definition_page_records",
    "legacy_schedule_create_missing_data_list",
    "legacy_current_user_data_list",
    "resource_detail_record",
    "resource_page_records",
    "schedule_create_data_list",
    "task_delete_nullable_payload",
    "schedule_preview_string_stream",
]
ResponseModelProjection = Literal["page_total_list_nullable_required"]


@dataclass(frozen=True)
class ReturnTypeRule:
    """One inference gap with an optional structural proof for candidate sources."""

    name: str
    operation_id: str
    raw_return_types: tuple[str, ...]
    inferred_return_type: str | None
    logical_return_type: str
    source_evidence: SourceEvidence | None = None
    response_model_projection: ResponseModelProjection | None = None
    candidate_enabled: bool = False
    type_import_overrides: tuple[tuple[str, str], ...] = ()


# Only explicitly reusable guards apply to candidates; legacy guards and
# unguarded rules remain exact-only. A source guard proves a mechanical payload
# correction, never that the operation or its release is supported at runtime.
RETURN_TYPE_RULES = (
    # PageInfo gained an empty-list initializer in 3.1.0, but this operation's
    # service helper still overwrites totalList with null on either empty input.
    ReturnTypeRule(
        name="alert_plugin_page_nullable_total_list",
        operation_id="AlertPluginInstanceController.listPaging",
        raw_return_types=("Result",),
        inferred_return_type="PageInfo<AlertPluginInstanceVO>",
        logical_return_type=(
            "org.apache.dolphinscheduler.api.utils.PageInfo<"
            "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>"
        ),
        source_evidence="alert_plugin_page_nullable_total_list",
        response_model_projection="page_total_list_nullable_required",
    ),
    # The raw PageInfo initializer names Resource, but its records are DataSource.
    ReturnTypeRule(
        name="legacy_datasource_page_records",
        operation_id="DataSourceController.queryDataSourceListPaging",
        raw_return_types=("Result",),
        inferred_return_type="PageInfo<Resource>",
        logical_return_type="PageInfo<DataSource>",
        source_evidence="legacy_datasource_page_records",
    ),
    # A raw PageInfo local hides the ProcessData/ProcessDefinition mismatch.
    ReturnTypeRule(
        name="legacy_process_definition_page_records",
        operation_id="ProcessDefinitionController.queryProcessDefinitionListPaging",
        raw_return_types=("Result",),
        inferred_return_type="PageInfo<ProcessData>",
        logical_return_type="PageInfo<ProcessDefinition>",
        source_evidence="legacy_process_definition_page_records",
    ),
    # The legacy service writes scheduleId; returnDataList reads only DATA_LIST.
    ReturnTypeRule(
        name="legacy_schedule_create_missing_data_list",
        operation_id="SchedulerController.createSchedule",
        raw_return_types=("Result",),
        inferred_return_type="SchedulerService_insertSchedule_result",
        logical_return_type="Void",
        source_evidence="legacy_schedule_create_missing_data_list",
    ),
    # The stream maps dates to strings before the HTTP payload is serialized.
    ReturnTypeRule(
        name="schedule_preview_string_stream",
        operation_id="SchedulerController.previewSchedule",
        raw_return_types=("Result",),
        inferred_return_type="Stream<String>",
        logical_return_type="List<String>",
        source_evidence="schedule_preview_string_stream",
        candidate_enabled=True,
    ),
    # An unsupported method reference leaves no inferred DATA_LIST payload;
    # the same structural proof still establishes the serialized String stream.
    ReturnTypeRule(
        name="schedule_preview_method_reference_stream",
        operation_id="SchedulerController.previewSchedule",
        raw_return_types=("Result",),
        inferred_return_type="Void",
        logical_return_type="List<String>",
        source_evidence="schedule_preview_string_stream",
        candidate_enabled=True,
    ),
    ReturnTypeRule(
        name="legacy_current_user_data_list",
        operation_id="UsersController.getUserInfo",
        raw_return_types=("Result",),
        inferred_return_type="User",
        logical_return_type="User",
        source_evidence="legacy_current_user_data_list",
    ),
    # The controller imports Spring Resource; its service serializes DAO Resource.
    ReturnTypeRule(
        name="resource_detail_record",
        operation_id="ResourcesController.queryResource",
        raw_return_types=("Result", "Result<Object>"),
        inferred_return_type="Resource",
        logical_return_type="Resource",
        source_evidence="resource_detail_record",
        candidate_enabled=True,
        type_import_overrides=(("Resource", DAO_RESOURCE_IMPORT),),
    ),
    # The controller imports Spring Resource; serialized records are DAO Resource.
    ReturnTypeRule(
        name="resource_page_records",
        operation_id="ResourcesController.queryResourceListPaging",
        raw_return_types=("Result", "Result<Object>"),
        inferred_return_type="PageInfo<Resource>",
        logical_return_type="PageInfo<Resource>",
        source_evidence="resource_page_records",
        candidate_enabled=True,
        type_import_overrides=(("Resource", DAO_RESOURCE_IMPORT),),
    ),
    # Inherited mapper generics hide the Schedule entity written into DATA_LIST.
    ReturnTypeRule(
        name="schedule_create_data_list",
        operation_id="SchedulerController.createSchedule",
        raw_return_types=("Result",),
        inferred_return_type="SchedulerServiceImpl_insertSchedule_result",
        logical_return_type="Schedule",
        source_evidence="schedule_create_data_list",
        candidate_enabled=True,
        type_import_overrides=(
            ("Schedule", "org.apache.dolphinscheduler.dao.entity.Schedule"),
        ),
    ),
    # Deletion without an upstream relation returns success without DATA_LIST.
    ReturnTypeRule(
        name="task_delete_nullable_payload",
        operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
        raw_return_types=("Result",),
        inferred_return_type="ProcessDefinition",
        logical_return_type="Optional<ProcessDefinition>",
        source_evidence="task_delete_nullable_payload",
        candidate_enabled=True,
    ),
    # Exact 2.0.0-2.0.2 writes DATA_LIST only inside the syncDefine branch.
    ReturnTypeRule(
        name="instance_update_nullable_without_sync",
        operation_id="ProcessInstanceController.updateProcessInstance",
        raw_return_types=("Result",),
        inferred_return_type="ProcessDefinition",
        logical_return_type="Optional<ProcessDefinition>",
    ),
    # selectOne may return null; this unguarded review remains exact-only.
    ReturnTypeRule(
        name="project_preference_nullable_entity",
        operation_id="ProjectPreferenceController.queryProjectPreferenceByProjectCode",
        raw_return_types=("Result",),
        inferred_return_type="ProjectPreference",
        logical_return_type="Optional<ProjectPreference>",
    ),
    # Exact no-op success writes no DATA_LIST; keep this unguarded fact exact.
    ReturnTypeRule(
        name="task_update_nullable_success",
        operation_id="TaskDefinitionController.updateTaskWithUpstream",
        raw_return_types=("Result",),
        inferred_return_type="long",
        logical_return_type="Optional<long>",
    ),
    # status_data already selects the list inside the controller map.
    ReturnTypeRule(
        name="assigned_worker_group_list",
        operation_id="ProjectWorkerGroupController.queryAssignedWorkerGroups",
        raw_return_types=("Map<String, Object>",),
        inferred_return_type="ProjectWorkerGroupController_queryAssignedWorkerGroups_result",
        logical_return_type="List<ProjectWorkerGroup>",
    ),
)
