from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from dsctl.cli_surface import (
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    DsctlError,
    UserInputError,
)
from dsctl.output import (
    CommandResult,
    dry_run_result,
    require_json_object,
    require_json_value,
)
from dsctl.services._data_quality_authoring import preflight_data_quality_authoring
from dsctl.services._dynamic_workflow_references import (
    resolve_dynamic_authoring_workflow_refs,
    resolve_read_dynamic_workflow_refs,
)
from dsctl.services._legacy_dependent_references import (
    resolve_legacy_authoring_dependent_refs,
)
from dsctl.services._legacy_workflow_mutation import (
    LegacyWorkflowMutationPlan,
    compile_legacy_workflow_mutation_draft,
    prepare_legacy_workflow_mutation_draft,
)
from dsctl.services._legacy_workflow_references import (
    audit_legacy_authoring_workflow_graph,
    resolve_legacy_authoring_workflow_refs,
)
from dsctl.services._parameter_warnings import (
    ParameterWarningDetail,
    workflow_parameter_warnings,
)
from dsctl.services._task_datasource_refs import (
    resolve_workflow_edit_task_datasources,
    task_datasource_baseline_from_dag,
    task_datasource_baseline_from_legacy_graph,
)
from dsctl.services._task_resource_refs import (
    mr_resource_full_names_from_spec,
    resolve_dag_read_task_resource_refs,
    resolve_task_resource_refs,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.main_task_ids import (
    changed_task_names_by_code,
    read_main_task_ids,
    verify_main_task_views,
)
from dsctl.services._workflow.mutation import (
    WorkflowFileEditRiskData,
    WorkflowMutationPlan,
    load_workflow_edit_spec_or_error,
    load_workflow_patch_or_error,
    prepare_workflow_file_edit,
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
    require_supported_workflow_relation_edit,
)
from dsctl.services._workflow.patch import workflow_patch_diff_output
from dsctl.services._workflow.render import (
    serialize_workflow as _serialize_workflow,
)
from dsctl.services.confirmation import require_confirmation
from dsctl.services.runtime import (
    run_with_bound_domain_selection,
)
from dsctl.services.version_resolution import resolve_runtime_selection
from dsctl.services.workflow._outcomes import WorkflowMutationProgress
from dsctl.upstream.definition_models import (
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)
from dsctl.upstream.dynamic_workflow_references import dynamic_workflow_codes_from_dag
from dsctl.upstream.mutation_outcomes import verify_mutation
from dsctl.upstream.serialization import (
    enum_value,
)
from dsctl.upstream.workflows import WORKFLOW_DOMAIN

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import (
        WorkflowSpec,
    )
    from dsctl.services._workflow.patch import WorkflowPatchDiffData
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.legacy_workflow_graph import (
        LegacyWorkflowGraphPayload,
    )
    from dsctl.upstream.protocol import (
        ScheduleRecord,
        WorkflowDagRecord,
        WorkflowPayloadRecord,
    )
    from dsctl.upstream.resolver import (
        ResolvedProject,
        ResolvedWorkflow,
    )
    from dsctl.upstream.task_parameter_projection import (
        TaskResourceRefIndex,
        TaskWorkflowRefIndex,
    )
    from dsctl.upstream.workflow_graph import (
        WorkflowUpdatePayload,
    )
    from dsctl.upstream.workflows import (
        PreparedLegacyWorkflowUpdate,
        PreparedWorkflowUpdate,
    )

from dsctl.services.workflow._errors import (
    _raise_workflow_edit_online_error,
    _raise_workflow_release_error,
    _raise_workflow_update_error,
    _workflow_edit_online_error,
)
from dsctl.services.workflow._graph import (
    _allocate_workflow_task_codes,
    _load_legacy_workflow_graph,
    _load_workflow_dag,
)
from dsctl.services.workflow._mutation import (
    _parameter_expression_warning_json_details,
    _prepare_workflow_definition_request,
    _require_workflow_wire_without_content,
    _workflow_wire_form,
    _workflow_wire_query,
)
from dsctl.services.workflow._schedules import (
    _load_target_attached_schedule,
    _workflow_scope_data,
)
from dsctl.services.workflow._selection import (
    _identity_field,
    _legacy_native_id,
    _load_workflow_detail,
    _project_selector,
    _required_name,
    _resolve_workflow_edit_target,
    _resolved_project_selection,
    _resolved_selected_workflow_data,
    _resolved_workflow_selection,
)
from dsctl.services.workflow._types import (
    WorkflowEditConstraintData,
    WorkflowEditNoChangeWarningDetail,
    WorkflowEditScheduleImpactData,
    WorkflowServiceRuntime,
)


def edit_workflow_result(
    workflow: str | None,
    *,
    patch: Path | None = None,
    file: Path | None = None,
    project: str | None = None,
    dry_run: bool = False,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Edit one workflow definition from a YAML patch or full YAML file."""
    if (patch is None) == (file is None):
        message = "Pass exactly one of --patch or --file."
        raise UserInputError(
            message,
            details={"patch": patch is not None, "file": file is not None},
            suggestion=(
                "Use `--patch PATCH.yaml` for a delta edit or `--file "
                "workflow.yaml` for a full desired-state edit."
            ),
        )
    selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    catalog = authoring.workflow_authoring_catalog_for_version(profile.ds_version)
    if patch is not None:
        workflow_patch = load_workflow_patch_or_error(patch, catalog=catalog)
        return run_with_bound_domain_selection(
            selection,
            WORKFLOW_DOMAIN,
            _edit_workflow_result,
            workflow=workflow,
            patch_file=patch,
            patch=workflow_patch,
            file=None,
            spec=None,
            catalog=catalog,
            project=project,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
        )
    if file is None:
        message = "Workflow edit file is required."
        raise RuntimeError(message)
    if workflow is None:
        message = "WORKFLOW is required when editing from a full workflow YAML file."
        raise UserInputError(
            message,
            details={"file": str(file)},
            suggestion=(
                "Run `dsctl workflow list` in the target project, then pass the "
                "returned exact workflow name or code with this file and --dry-run."
            ),
        )
    workflow_spec = load_workflow_edit_spec_or_error(file, catalog=catalog)
    return run_with_bound_domain_selection(
        selection,
        WORKFLOW_DOMAIN,
        _edit_workflow_result,
        workflow=workflow,
        patch_file=None,
        patch=None,
        file=file,
        spec=workflow_spec,
        catalog=catalog,
        project=project,
        dry_run=dry_run,
        confirm_risk=confirm_risk,
    )


def _edit_workflow_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    patch_file: Path | None,
    patch: WorkflowPatchSpec | None,
    file: Path | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    authoring.require_task_authoring_catalog_profile(
        catalog,
        ds_version=runtime.profile.ds_version,
    )
    if runtime.domain.workflows.workflow_graph_family == "legacy-json":
        return _edit_legacy_workflow_result(
            runtime,
            workflow=workflow,
            patch_file=patch_file,
            patch=patch,
            file=file,
            spec=spec,
            catalog=catalog,
            project=project,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
        )
    target = _resolve_workflow_edit_target(
        runtime,
        workflow=workflow,
        project=project,
        spec=spec,
    )
    operations = runtime.domain.workflows
    dag = _load_workflow_dag(
        runtime,
        target=target,
        action="workflow.edit",
    )
    live_payload = _load_workflow_detail(
        runtime,
        scope=target.scope,
        action="workflow.edit",
    )
    attached_schedule = _load_target_attached_schedule(
        runtime,
        target=target,
        action="workflow.edit",
        phase="pre_mutation",
    )
    definition_spec = (
        None
        if spec is None
        else prepare_workflow_file_edit(
            spec,
            attached_schedule=attached_schedule,
            workflow_release_state=enum_value(live_payload.releaseState),
        )
    )
    datasource_baseline = task_datasource_baseline_from_dag(dag)
    patch, definition_spec, task_datasource_resolutions = (
        resolve_workflow_edit_task_datasources(
            runtime.domain.task_datasources,
            patch=patch,
            spec=definition_spec,
            catalog=catalog,
            action="workflow.edit",
            baseline=datasource_baseline,
        )
    )
    read_resource_refs = resolve_dag_read_task_resource_refs(
        runtime.domain.task_resource_resolver,
        dag,
        profile_version=runtime.profile.ds_version,
    )
    read_workflow_refs = resolve_read_dynamic_workflow_refs(
        runtime.domain.workflows,
        project=target.project,
        workflow_codes=dynamic_workflow_codes_from_dag(dag),
    )
    mutation = _prepare_workflow_edit_mutation(
        dag=dag,
        project=target.resolved_project,
        patch=patch,
        spec=definition_spec,
        catalog=catalog,
        resource_refs=read_resource_refs,
        workflow_refs=read_workflow_refs,
    )
    if mutation.has_changes:
        preflight_data_quality_authoring(
            runtime.domain.data_quality_authoring_inspector,
            spec=mutation.merged_spec,
            projection_sources=mutation.compilation.projection_sources,
            ds_version=runtime.profile.ds_version,
            action="workflow.edit",
        )
    workflow_refs = resolve_dynamic_authoring_workflow_refs(
        runtime.domain.workflows,
        project=target.project,
        child_workflow_names=mutation.compilation.required_child_workflow_names,
        # Modern definition reads already proved the target code-native.
        containing_workflow_code=(
            target.workflow.native.value if mutation.has_changes else None
        ),
        audit_descendants=mutation.has_changes,
        action="workflow.edit",
    )
    resource_refs = resolve_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mutation.compilation.required_resource_full_names,
        boundary_resource=WORKFLOW_RESOURCE,
        action="workflow.edit",
    )
    main_task_ids = (
        read_main_task_ids(
            runtime.domain.task_definitions,
            project_code=target.resolved_project.code,
            task_codes=mutation.compilation.existing_task_codes,
            resource=WORKFLOW_RESOURCE,
        )
        if runtime.profile.ds_version == "3.1.0" and mutation.has_changes
        else None
    )
    preview_payload = mutation.compilation.preview(
        main_task_ids=main_task_ids,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    desired_release_state = preview_payload["releaseState"]
    merged_spec = mutation.merged_spec
    diff = mutation.diff
    has_changes = mutation.has_changes
    parameter_warnings, parameter_warning_details = workflow_parameter_warnings(
        merged_spec,
        parameter_semantics=catalog.parameter_semantics,
    )
    workflow_state_constraint_details = _workflow_edit_state_constraint_details(
        live_payload,
        attached_schedule=attached_schedule,
        has_changes=has_changes,
    )
    workflow_state_constraints = _workflow_edit_constraint_messages(
        workflow_state_constraint_details
    )
    schedule_impact_details = _workflow_edit_schedule_impact_details(
        attached_schedule,
        desired_release_state=desired_release_state,
    )
    schedule_impacts = _workflow_edit_schedule_impact_messages(schedule_impact_details)
    resolved_data = require_json_object(
        {
            "project": _resolved_project_selection(
                target.resolved_project,
                target.selected_project,
            ),
            "workflow": _resolved_selected_workflow_data(
                code=target.resolved_workflow.code,
                name=merged_spec.workflow.name,
                selection=target.selected_workflow,
            ),
            "input_mode": mutation.input_mode,
            "task_datasources": task_datasource_resolutions,
            **_workflow_edit_resolved_file_data(
                patch_file=patch_file,
                file=file,
            ),
        },
        label="workflow edit resolved",
    )

    if dry_run:
        prepared = _prepare_remote_workflow_update(
            runtime,
            scope=target.scope,
            payload=preview_payload,
            workflow_name=preview_payload["name"],
            workflow_description=preview_payload.get("description"),
        )
        return _workflow_edit_dry_run_result(
            prepared=prepared,
            resolved=resolved_data,
            diff=diff,
            workflow_state_constraints=workflow_state_constraints,
            workflow_state_constraint_details=workflow_state_constraint_details,
            schedule_impacts=schedule_impacts,
            schedule_impact_details=schedule_impact_details,
            no_change=not has_changes,
            parameter_warnings=parameter_warnings,
            parameter_warning_details=parameter_warning_details,
            failure=_workflow_edit_dry_run_failure(
                workflow=target.resolved_workflow,
                workflow_state_constraint_details=workflow_state_constraint_details,
            ),
        )

    if not has_changes:
        no_change_warning = _workflow_edit_no_change_message(mutation.input_mode)
        return CommandResult(
            data=require_json_object(
                _serialize_workflow(
                    live_payload,
                    attached_schedule=attached_schedule,
                ),
                label="workflow data",
            ),
            resolved=resolved_data,
            warnings=[
                no_change_warning,
                *schedule_impacts,
                *parameter_warnings,
            ],
            warning_details=[
                require_json_object(
                    WorkflowEditNoChangeWarningDetail(
                        code="workflow_edit_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="workflow edit warning detail",
                ),
                *_workflow_edit_schedule_impact_warning_details(
                    schedule_impact_details
                ),
                *_parameter_expression_warning_json_details(parameter_warning_details),
            ],
        )

    if enum_value(live_payload.releaseState) == "ONLINE":
        _raise_workflow_edit_online_error(
            workflow=target.resolved_workflow,
            workflow_state_constraint_details=workflow_state_constraint_details,
        )

    _require_workflow_file_edit_confirmation(
        mutation,
        confirmation=confirm_risk,
        project_code=target.resolved_project.code,
        workflow_code=target.resolved_workflow.code,
    )

    required_task_code_count = mutation.compilation.required_task_code_count
    allocated_task_codes = (
        _allocate_workflow_task_codes(
            runtime,
            project=target.project,
            count=required_task_code_count,
            action="workflow.edit",
        )
        if required_task_code_count
        else ()
    )
    payload = mutation.compilation.materialize(
        allocated_task_codes,
        main_task_ids=main_task_ids,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )

    progress = WorkflowMutationProgress(
        {"project": target.project.to_data(), "workflow": target.workflow.to_data()}
    )
    stage = "workflow_update"
    try:
        _update_remote_workflow(
            runtime,
            scope=target.scope,
            payload=payload,
            workflow_name=payload["name"],
            workflow_description=payload.get("description"),
        )
        progress.complete("workflow_update", mutation=True)
        stage = "workflow_readback"
        refreshed = verify_mutation(
            lambda: operations.detail(target.scope, action="workflow.edit"),
            ds_version=runtime.profile.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="edit",
        )
        refreshed_data = verify_mutation(
            lambda: require_json_object(
                _serialize_workflow(refreshed, attached_schedule=attached_schedule),
                label="workflow data",
            ),
            ds_version=runtime.profile.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="edit",
        )
        if main_task_ids is not None:
            progress.complete(stage)
            stage = "task_detail_readback"
            refreshed_dag = verify_mutation(
                lambda: operations.dag(target.scope, action="workflow.edit"),
                ds_version=runtime.profile.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation="edit",
            )
            verify_mutation(
                lambda: verify_main_task_views(
                    runtime.domain.task_definitions,
                    project_code=target.resolved_project.code,
                    dag=refreshed_dag,
                    changed_names_by_code=changed_task_names_by_code(
                        mutation, allocated_task_codes
                    ),
                    original_ids=main_task_ids,
                    resource=WORKFLOW_RESOURCE,
                ),
                ds_version=runtime.profile.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation="edit",
            )
        progress.complete(stage)
    except DsctlError as error:
        progress.annotate(error, failed_stage=stage)
        raise
    resolved_data["mutation"] = progress.to_data()
    return CommandResult(
        data=refreshed_data,
        resolved=resolved_data,
        warnings=[
            *schedule_impacts,
            *parameter_warnings,
        ],
        warning_details=[
            *_workflow_edit_schedule_impact_warning_details(schedule_impact_details),
            *_parameter_expression_warning_json_details(parameter_warning_details),
        ],
    )


def _edit_legacy_workflow_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    patch_file: Path | None,
    patch: WorkflowPatchSpec | None,
    file: Path | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    project: str | None,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    """Apply stable edit intent through the exact DS 1.3 graph dialect."""
    target = _resolve_workflow_edit_target(
        runtime,
        workflow=workflow,
        project=project,
        spec=spec,
    )
    snapshot, graph = _load_legacy_workflow_graph(
        runtime,
        target=target,
        action="workflow.edit",
        hydrate_workflow_refs=True,
        hydrate_resource_refs=True,
    )
    attached_schedule = _load_target_attached_schedule(
        runtime,
        target=target,
        action="workflow.edit",
        phase="pre_mutation",
    )
    definition_spec = (
        None
        if spec is None
        else prepare_workflow_file_edit(
            spec,
            attached_schedule=attached_schedule,
            workflow_release_state=snapshot.release_state,
        )
    )
    datasource_baseline = task_datasource_baseline_from_legacy_graph(graph)
    patch, definition_spec, task_datasource_resolutions = (
        resolve_workflow_edit_task_datasources(
            runtime.domain.task_datasources,
            patch=patch,
            spec=definition_spec,
            catalog=catalog,
            action="workflow.edit",
            baseline=datasource_baseline,
        )
    )
    mutation_input = patch if patch is not None else definition_spec
    if mutation_input is None:
        message = "Workflow edit requires either a patch or full workflow spec."
        raise RuntimeError(message)
    workflow_name = _required_name(snapshot.name, label="workflow")
    project_name = _required_name(target.project.name, label="project")
    mutation_draft = prepare_legacy_workflow_mutation_draft(
        graph,
        workflow_name=workflow_name,
        project_name=project_name,
        description=snapshot.description,
        release_state=snapshot.release_state,
        mutation=mutation_input,
        catalog=catalog,
    )
    resource_refs = resolve_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mr_resource_full_names_from_spec(
            mutation_draft.merged_spec,
            profile_version=runtime.profile.ds_version,
        ),
        boundary_resource=WORKFLOW_RESOURCE,
        action="workflow.edit",
    )
    containing_workflow_id = _legacy_native_id(
        target.workflow,
        label="workflow",
    )
    resolution = resolve_legacy_authoring_workflow_refs(
        runtime.domain.workflows,
        project=target.project,
        spec=mutation_draft.merged_spec,
        containing_workflow_id=containing_workflow_id,
        action="workflow.edit",
    )
    dependent_refs = resolve_legacy_authoring_dependent_refs(
        runtime.domain.workflows,
        spec=mutation_draft.merged_spec,
        action="workflow.edit",
    )
    mutation = compile_legacy_workflow_mutation_draft(
        mutation_draft,
        workflow_refs=resolution.refs,
        dependent_refs=dependent_refs,
        resource_refs=resource_refs,
    )
    audit_legacy_authoring_workflow_graph(
        runtime.domain.workflows,
        project=target.project,
        compilation=mutation.compilation,
        containing_workflow_id=containing_workflow_id,
        resolution=resolution,
        action="workflow.edit",
    )
    merged_spec = mutation.merged_spec
    desired_release_state = merged_spec.workflow.release_state.value
    parameter_warnings, parameter_warning_details = workflow_parameter_warnings(
        merged_spec,
        parameter_semantics=catalog.parameter_semantics,
    )
    workflow_state_constraint_details = (
        _workflow_edit_state_constraint_details_for_release_state(
            snapshot.release_state,
            attached_schedule=attached_schedule,
            has_changes=mutation.has_changes,
        )
    )
    workflow_state_constraints = _workflow_edit_constraint_messages(
        workflow_state_constraint_details
    )
    schedule_impact_details = _workflow_edit_schedule_impact_details(
        attached_schedule,
        desired_release_state=desired_release_state,
    )
    schedule_impacts = _workflow_edit_schedule_impact_messages(schedule_impact_details)
    resolved_workflow = WorkflowRef(
        native=target.workflow.native,
        name=merged_spec.workflow.name,
        version=target.workflow.version,
    )
    resolved_data = require_json_object(
        {
            "project": _resolved_project_selection(
                target.project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                resolved_workflow,
                target.selected_workflow,
            ),
            "input_mode": mutation.input_mode,
            "task_datasources": task_datasource_resolutions,
            **_workflow_edit_resolved_file_data(
                patch_file=patch_file,
                file=file,
            ),
        },
        label="workflow edit resolved",
    )
    payload = mutation.compilation.preview()

    if dry_run:
        prepared = _prepare_remote_workflow_update(
            runtime,
            scope=target.scope,
            payload=payload,
            workflow_name=merged_spec.workflow.name,
            workflow_description=merged_spec.workflow.description,
        )
        return _workflow_edit_dry_run_result(
            prepared=prepared,
            resolved=resolved_data,
            diff=mutation.diff,
            workflow_state_constraints=workflow_state_constraints,
            workflow_state_constraint_details=workflow_state_constraint_details,
            schedule_impacts=schedule_impacts,
            schedule_impact_details=schedule_impact_details,
            no_change=not mutation.has_changes,
            parameter_warnings=parameter_warnings,
            parameter_warning_details=parameter_warning_details,
            failure=_workflow_edit_dry_run_failure(
                workflow=target.workflow,
                workflow_state_constraint_details=workflow_state_constraint_details,
            ),
        )

    if not mutation.has_changes:
        no_change_warning = _workflow_edit_no_change_message(mutation.input_mode)
        return CommandResult(
            data=_workflow_scope_data(target.scope, attached_schedule),
            resolved=resolved_data,
            warnings=[no_change_warning, *schedule_impacts, *parameter_warnings],
            warning_details=[
                require_json_object(
                    WorkflowEditNoChangeWarningDetail(
                        code="workflow_edit_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="workflow edit warning detail",
                ),
                *_workflow_edit_schedule_impact_warning_details(
                    schedule_impact_details
                ),
                *_parameter_expression_warning_json_details(parameter_warning_details),
            ],
        )

    if snapshot.release_state == "ONLINE":
        _raise_workflow_edit_online_error(
            workflow=target.workflow,
            workflow_state_constraint_details=workflow_state_constraint_details,
        )
    _require_legacy_workflow_file_edit_confirmation(
        mutation,
        confirmation=confirm_risk,
        project=target.project,
        workflow=target.workflow,
    )
    progress = WorkflowMutationProgress(
        {"project": target.project.to_data(), "workflow": target.workflow.to_data()}
    )
    stage = "workflow_update"
    try:
        _update_remote_workflow(
            runtime,
            scope=target.scope,
            payload=mutation.compilation.materialize(),
            workflow_name=merged_spec.workflow.name,
            workflow_description=merged_spec.workflow.description,
        )
        progress.complete("workflow_update", mutation=True)
        stage = "workflow_identity_readback"
        refreshed_scope = verify_mutation(
            lambda: runtime.domain.workflows.resolve_workflow(
                _project_selector(target.project), merged_spec.workflow.name
            ),
            ds_version=runtime.profile.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="edit",
        )
        progress.complete(stage)
        if desired_release_state == "ONLINE":
            stage = "workflow_online"
            try:
                runtime.domain.workflows.release(refreshed_scope, state="ONLINE")
            except ApiResultError as error:
                _raise_workflow_release_error(
                    error,
                    project=refreshed_scope.project,
                    workflow=refreshed_scope.workflow,
                    action="online",
                )
            progress.complete(stage, mutation=True)
            stage = "workflow_readback"
            refreshed_scope = verify_mutation(
                lambda: runtime.domain.workflows.resolve_workflow(
                    _project_selector(target.project), merged_spec.workflow.name
                ),
                ds_version=runtime.profile.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation="edit",
            )
            progress.complete(stage)
    except DsctlError as error:
        progress.annotate(error, failed_stage=stage)
        raise
    resolved_data["mutation"] = progress.to_data()
    return CommandResult(
        data=_workflow_scope_data(refreshed_scope, attached_schedule),
        resolved=resolved_data,
        warnings=[*schedule_impacts, *parameter_warnings],
        warning_details=[
            *_workflow_edit_schedule_impact_warning_details(schedule_impact_details),
            *_parameter_expression_warning_json_details(parameter_warning_details),
        ],
    )


def _prepare_workflow_edit_mutation(
    *,
    dag: WorkflowDagRecord,
    project: ResolvedProject,
    patch: WorkflowPatchSpec | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> WorkflowMutationPlan:
    if patch is not None:
        mutation = prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state=_workflow_edit_release_state(patch),
            catalog=catalog,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )
    elif spec is not None:
        mutation = prepare_workflow_file_mutation_plan(
            dag,
            project=project,
            desired=spec,
            release_state=spec.workflow.release_state.value,
            catalog=catalog,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )
    else:
        message = "Workflow edit requires either a patch or full workflow spec."
        raise RuntimeError(message)
    require_supported_workflow_relation_edit(
        mutation,
        profile_version=catalog.profile_version,
    )
    return mutation


def _workflow_edit_resolved_file_data(
    *,
    patch_file: Path | None,
    file: Path | None,
) -> JsonObject:
    if patch_file is not None:
        return {"patch_file": str(patch_file)}
    if file is not None:
        return {"file": str(file)}
    return {}


def _require_workflow_file_edit_confirmation(
    mutation: WorkflowMutationPlan,
    *,
    confirmation: str | None,
    project_code: int,
    workflow_code: int,
) -> None:
    if mutation.input_mode != "file" or mutation.confirmation is None:
        return
    confirmation_details = mutation.confirmation
    require_confirmation(
        action="workflow.edit",
        confirmation=confirmation,
        payload=_workflow_file_edit_confirmation_payload(
            project_code=project_code,
            workflow_code=workflow_code,
            confirmation_details=confirmation_details,
        ),
        message=(
            "This full workflow YAML edit deletes tasks, renames the workflow, "
            "or changes task types and requires explicit confirmation."
        ),
        details=confirmation_details,
    )


def _require_legacy_workflow_file_edit_confirmation(
    mutation: LegacyWorkflowMutationPlan,
    *,
    confirmation: str | None,
    project: ProjectRef,
    workflow: WorkflowRef,
) -> None:
    """Keep destructive full-file protection while using native id vocabulary."""
    if mutation.input_mode != "file" or mutation.confirmation is None:
        return
    confirmation_details = mutation.confirmation
    require_confirmation(
        action="workflow.edit",
        confirmation=confirmation,
        payload=require_json_object(
            {
                _identity_field(project, prefix="project_"): project.native.value,
                _identity_field(workflow, prefix="workflow_"): workflow.native.value,
                "risk_type": confirmation_details["risk_type"],
                "deleted_tasks": confirmation_details["deleted_tasks"],
                "renamed_workflow": confirmation_details["renamed_workflow"],
                "old_workflow_name": confirmation_details["old_workflow_name"],
                "new_workflow_name": confirmation_details["new_workflow_name"],
                "task_type_changes": confirmation_details["task_type_changes"],
            },
            label="legacy workflow full edit confirmation payload",
        ),
        message=(
            "This full workflow YAML edit deletes tasks, renames the workflow, "
            "or changes task types and requires explicit confirmation."
        ),
        details=confirmation_details,
    )


def _workflow_file_edit_confirmation_payload(
    *,
    project_code: int,
    workflow_code: int,
    confirmation_details: WorkflowFileEditRiskData,
) -> JsonObject:
    return require_json_object(
        {
            "project_code": project_code,
            "workflow_code": workflow_code,
            "risk_type": confirmation_details["risk_type"],
            "deleted_tasks": confirmation_details["deleted_tasks"],
            "renamed_workflow": confirmation_details["renamed_workflow"],
            "old_workflow_name": confirmation_details["old_workflow_name"],
            "new_workflow_name": confirmation_details["new_workflow_name"],
            "task_type_changes": confirmation_details["task_type_changes"],
        },
        label="workflow full edit confirmation payload",
    )


def _prepare_remote_workflow_update(
    runtime: WorkflowServiceRuntime,
    *,
    scope: WorkflowScope,
    payload: WorkflowUpdatePayload | LegacyWorkflowGraphPayload,
    workflow_name: str,
    workflow_description: str | None,
) -> PreparedWorkflowUpdate | PreparedLegacyWorkflowUpdate:
    return _prepare_workflow_definition_request(
        prepare=lambda: runtime.domain.prepare_definition_update(
            scope,
            payload=payload,
            name=workflow_name,
            description=workflow_description,
        ),
        translate_error=lambda error: _raise_workflow_update_error(
            error,
            project=scope.project,
            workflow=scope.workflow,
            workflow_name=workflow_name,
        ),
    )


def _update_remote_workflow(
    runtime: WorkflowServiceRuntime,
    *,
    scope: WorkflowScope,
    payload: WorkflowUpdatePayload | LegacyWorkflowGraphPayload,
    workflow_name: str,
    workflow_description: str | None,
) -> None:
    prepared = _prepare_remote_workflow_update(
        runtime,
        scope=scope,
        payload=payload,
        workflow_name=workflow_name,
        workflow_description=workflow_description,
    )
    try:
        runtime.domain.workflows.apply_update(prepared)
    except ApiResultError as error:
        _raise_workflow_update_error(
            error,
            project=scope.project,
            workflow=scope.workflow,
            workflow_name=workflow_name,
        )


def _workflow_edit_release_state(patch: WorkflowPatchSpec) -> str | None:
    if patch.workflow is None:
        return None
    if "release_state" not in patch.workflow.set.model_fields_set:
        return None
    release_state = patch.workflow.set.release_state
    return None if release_state is None else release_state.value


def _workflow_edit_dry_run_result(
    *,
    prepared: PreparedWorkflowUpdate | PreparedLegacyWorkflowUpdate,
    resolved: JsonObject,
    diff: WorkflowPatchDiffData,
    workflow_state_constraints: list[str],
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
    schedule_impacts: list[str],
    schedule_impact_details: list[WorkflowEditScheduleImpactData],
    no_change: bool,
    parameter_warnings: list[str],
    parameter_warning_details: list[ParameterWarningDetail],
    failure: DsctlError | None,
) -> CommandResult:
    request = prepared.request
    _require_workflow_wire_without_content(
        request,
        label="workflow update request",
    )
    return replace(
        dry_run_result(
            method=request.method,
            path=request.path,
            params=_workflow_wire_query(request, label="workflow update query"),
            json_body=request.json,
            form_data=_workflow_wire_form(request, label="workflow update form"),
            requests=[] if no_change else None,
            resolved=resolved,
            extra_data=require_json_object(
                {
                    "diff": require_json_value(
                        workflow_patch_diff_output(diff),
                        label="workflow edit diff",
                    ),
                    "workflow_state_constraints": workflow_state_constraints,
                    "workflow_state_constraint_details": (
                        workflow_state_constraint_details
                    ),
                    "schedule_impacts": schedule_impacts,
                    "schedule_impact_details": schedule_impact_details,
                    "no_change": no_change,
                },
                label="workflow edit dry-run data",
            ),
            warnings=parameter_warnings,
            warning_details=_parameter_expression_warning_json_details(
                parameter_warning_details
            ),
        ),
        failure=failure,
    )


def _workflow_edit_dry_run_failure(
    *,
    workflow: ResolvedWorkflow | WorkflowRef,
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
) -> DsctlError | None:
    if not any(item["blocking"] for item in workflow_state_constraint_details):
        return None
    return _workflow_edit_online_error(
        workflow=workflow,
        workflow_state_constraint_details=workflow_state_constraint_details,
    )


def _workflow_edit_state_constraint_details(
    workflow: WorkflowPayloadRecord,
    *,
    attached_schedule: ScheduleRecord | None,
    has_changes: bool,
) -> list[WorkflowEditConstraintData]:
    return _workflow_edit_state_constraint_details_for_release_state(
        enum_value(workflow.releaseState),
        attached_schedule=attached_schedule,
        has_changes=has_changes,
    )


def _workflow_edit_state_constraint_details_for_release_state(
    release_state: str | None,
    *,
    attached_schedule: ScheduleRecord | None,
    has_changes: bool,
) -> list[WorkflowEditConstraintData]:
    if not has_changes:
        return []
    if release_state != "ONLINE":
        return []
    current_schedule_release_state = (
        None
        if attached_schedule is None
        else enum_value(attached_schedule.releaseState)
    )
    constraints: list[WorkflowEditConstraintData] = [
        {
            "code": "workflow_must_be_offline",
            "message": (
                "workflow is currently online; DolphinScheduler only allows "
                "whole-definition edits while offline"
            ),
            "blocking": True,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": current_schedule_release_state,
        }
    ]
    if current_schedule_release_state == "ONLINE":
        constraints.append(
            {
                "code": "offline_also_offlines_attached_schedule",
                "message": (
                    "taking this workflow offline before apply will also take the "
                    "attached schedule offline"
                ),
                "blocking": False,
                "current_release_state": "ONLINE",
                "required_release_state": "OFFLINE",
                "current_schedule_release_state": current_schedule_release_state,
            }
        )
    return constraints


def _workflow_edit_schedule_impact_details(
    attached_schedule: ScheduleRecord | None,
    *,
    desired_release_state: str | None,
) -> list[WorkflowEditScheduleImpactData]:
    if attached_schedule is None:
        return []
    current_schedule_release_state = enum_value(attached_schedule.releaseState)
    impacts: list[WorkflowEditScheduleImpactData] = [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": desired_release_state,
            "current_schedule_release_state": current_schedule_release_state,
        }
    ]
    if desired_release_state == "ONLINE" and current_schedule_release_state != "ONLINE":
        impacts.append(
            {
                "code": "workflow_online_leaves_schedule_offline",
                "message": (
                    "this edit can bring the workflow back online, but any attached "
                    "schedule remains offline until `schedule online` is requested"
                ),
                "desired_workflow_release_state": desired_release_state,
                "current_schedule_release_state": current_schedule_release_state,
            }
        )
    return impacts


def _workflow_edit_constraint_messages(
    constraints: list[WorkflowEditConstraintData],
) -> list[str]:
    return [constraint["message"] for constraint in constraints]


def _workflow_edit_schedule_impact_messages(
    impacts: list[WorkflowEditScheduleImpactData],
) -> list[str]:
    return [impact["message"] for impact in impacts]


def _workflow_edit_schedule_impact_warning_details(
    impacts: list[WorkflowEditScheduleImpactData],
) -> list[JsonObject]:
    return [
        require_json_object(
            impact,
            label="workflow edit schedule impact detail",
        )
        for impact in impacts
    ]


def _workflow_edit_no_change_message(input_mode: str) -> str:
    source = "workflow file" if input_mode == "file" else "patch"
    return (
        f"{source} produced no persistent workflow change; no update request was sent"
    )
