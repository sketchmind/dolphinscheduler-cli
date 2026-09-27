from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    DsctlError,
    InvalidStateError,
    UserInputError,
)
from dsctl.output import CommandResult, dry_run_result, require_json_object
from dsctl.services._dynamic_workflow_references import (
    resolve_dynamic_authoring_workflow_refs,
    resolve_read_dynamic_workflow_refs,
)
from dsctl.services._legacy_dependent_references import (
    resolve_legacy_authoring_dependent_refs,
)
from dsctl.services._legacy_workflow_mutation import (
    LegacyWorkflowMutationDraft,
    LegacyWorkflowMutationPlan,
    compile_legacy_workflow_mutation_draft,
    prepare_legacy_workflow_mutation_draft,
)
from dsctl.services._legacy_workflow_references import (
    audit_legacy_authoring_workflow_graph,
    requires_legacy_authoring_workflow_audit,
    resolve_legacy_authoring_workflow_refs,
)
from dsctl.services._task_datasource_refs import (
    resolve_workflow_edit_task_datasources,
    task_datasource_baseline_from_dag,
    task_datasource_baseline_from_legacy_graph,
)
from dsctl.services._task_resource_refs import (
    mr_resource_full_names_from_spec,
    mr_resource_ids_from_legacy_graph,
    resolve_dag_read_task_resource_refs,
    resolve_read_task_resource_refs,
    resolve_task_resource_refs,
)
from dsctl.services._validation import (
    require_positive_int,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.main_task_ids import (
    changed_task_names_by_code,
    read_main_task_ids,
    verify_main_task_views,
)
from dsctl.services._workflow.mutation import (
    WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS,
    WorkflowFileEditRiskData,
    WorkflowMutationPlan,
    load_workflow_instance_edit_spec_or_error,
    load_workflow_patch_or_error,
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.patch import workflow_patch_diff_output
from dsctl.services.confirmation import require_confirmation
from dsctl.services.runtime import (
    run_with_bound_domain_selection,
)
from dsctl.services.version_resolution import (
    resolve_runtime_selection,
    selected_target_globals,
)
from dsctl.upstream.definition_models import (
    NativeId,
    ProjectRef,
    WorkflowRef,
)
from dsctl.upstream.dynamic_workflow_references import dynamic_workflow_codes_from_dag
from dsctl.upstream.legacy_workflow_graph import (
    LegacyDependentRefIndex,
)
from dsctl.upstream.legacy_workflow_references import (
    LegacyWorkflowAuthoringResolution,
)
from dsctl.upstream.mutation_outcomes import verify_mutation
from dsctl.upstream.runtime_instances import (
    RUNTIME_INSTANCE_DOMAIN,
    LocatedWorkflowInstance,
    WorkflowInstanceSnapshot,
    runtime_instance_contract_features,
)
from dsctl.upstream.serialization import (
    enum_value,
    optional_text,
)
from dsctl.upstream.workflow_graph_requests import (
    legacy_workflow_instance_update_arguments,
    workflow_instance_update_arguments,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services._task_datasource_refs import TaskDatasourceResolutionData
    from dsctl.services._workflow.patch import WorkflowPatchDiffData
    from dsctl.services.selection import (
        SelectedValue,
    )
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        WorkflowDagRecord,
    )
    from dsctl.upstream.resolver import (
        ResolvedProject,
    )
    from dsctl.upstream.task_parameter_projection import (
        TaskResourceRefIndex,
        TaskWorkflowRefIndex,
    )
    from dsctl.upstream.wire import WireRequest
    from dsctl.upstream.workflows import WorkflowOperations


from dsctl.services.workflow_instance._commands import (
    _wait_for_final_state_suggestion,
    _workflow_instance_command,
    _workflow_instance_edit_retry_command,
)
from dsctl.services.workflow_instance._errors import (
    _raise_workflow_instance_edit_error,
)
from dsctl.services.workflow_instance._selection import (
    _is_legacy_workflow_instance,
    _legacy_workflow_instance_graph,
    _legacy_workflow_instance_name,
    _legacy_workflow_instance_project_name,
    _require_instance_workflow_code,
    _resolved_project,
    _selected_project_data,
    _selected_workflow_instance,
    _workflow_execution_status,
    get_workflow_instance,
)
from dsctl.services.workflow_instance._types import (
    RuntimeInstanceServiceRuntime,
    WorkflowInstanceEditInputMode,
    WorkflowInstanceEditNoChangeWarningDetail,
    WorkflowInstanceEditResolved,
    WorkflowInstanceSelectionData,
)


def edit_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    patch: Path | None = None,
    file: Path | None = None,
    sync_definition: bool = False,
    dry_run: bool = False,
    confirm_risk: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Edit one finished workflow instance DAG from a YAML patch or full file."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    if (patch is None) == (file is None):
        message = "Pass exactly one of --patch or --file."
        raise UserInputError(
            message,
            details={"patch": patch is not None, "file": file is not None},
            suggestion=(
                "Use `--patch PATCH.yaml` for a delta repair or `--file "
                "workflow.yaml` for a full desired-state instance DAG edit."
            ),
        )
    selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    catalog = authoring.workflow_authoring_catalog_for_version(profile.ds_version)
    if patch is not None:
        workflow_patch = load_workflow_patch_or_error(
            patch,
            catalog=catalog,
            workflow_instance=True,
        )
        return run_with_bound_domain_selection(
            selection,
            RUNTIME_INSTANCE_DOMAIN,
            _edit_workflow_instance_result,
            workflow_instance_id=normalized_workflow_instance_id,
            project=optional_text(project),
            patch_file=patch,
            patch=workflow_patch,
            file=None,
            spec=None,
            catalog=catalog,
            sync_definition=sync_definition,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
        )
    if file is None:
        message = "Workflow instance edit file is required."
        raise RuntimeError(message)
    workflow_spec = load_workflow_instance_edit_spec_or_error(
        file,
        catalog=catalog,
    )
    return run_with_bound_domain_selection(
        selection,
        RUNTIME_INSTANCE_DOMAIN,
        _edit_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
        patch_file=None,
        patch=None,
        file=file,
        spec=workflow_spec,
        catalog=catalog,
        sync_definition=sync_definition,
        dry_run=dry_run,
        confirm_risk=confirm_risk,
    )


def _edit_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
    patch_file: Path | None,
    patch: WorkflowPatchSpec | None,
    file: Path | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    sync_definition: bool,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    authoring.require_task_authoring_catalog_profile(
        catalog,
        ds_version=runtime.profile.ds_version,
    )
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    input_mode: WorkflowInstanceEditInputMode = "file" if spec is not None else "patch"
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        input_path = file if input_mode == "file" else patch_file
        if input_path is None:
            message = "Workflow instance edit input path is required."
            raise RuntimeError(message)
        message = "This workflow instance must be in a final state before edit."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(
                _workflow_instance_edit_retry_command(
                    input_mode,
                    workflow_instance_id=workflow_instance_id,
                    project_selector=selected_project.value,
                    input_path=input_path,
                )
            ),
        )
    if _is_legacy_workflow_instance(payload):
        return _edit_legacy_workflow_instance_result(
            runtime,
            workflow_instance_id=workflow_instance_id,
            selected_project=selected_project,
            located=located,
            patch_file=patch_file,
            patch=patch,
            file=file,
            spec=spec,
            catalog=catalog,
            sync_definition=sync_definition,
            dry_run=dry_run,
            confirm_risk=confirm_risk,
        )
    dag = payload.dagData
    if dag is None:
        message = "Workflow instance payload was missing dagData"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
        )
    resolved_project = _resolved_project(payload.project)
    project_code = resolved_project.code
    workflow_code = _require_instance_workflow_code(payload)
    _validate_workflow_instance_file_project(
        spec,
        project=resolved_project,
        workflow_instance_id=workflow_instance_id,
    )
    datasource_baseline = task_datasource_baseline_from_dag(dag)
    patch, spec, task_datasource_resolutions = resolve_workflow_edit_task_datasources(
        runtime.domain.task_datasources,
        patch=patch,
        spec=spec,
        catalog=catalog,
        action="workflow-instance.edit",
        baseline=datasource_baseline,
    )
    read_resource_refs = resolve_dag_read_task_resource_refs(
        runtime.domain.task_resource_resolver,
        dag,
        profile_version=runtime.profile.ds_version,
    )
    read_workflow_refs = resolve_read_dynamic_workflow_refs(
        runtime.domain.workflows,
        project=located.project,
        workflow_codes=dynamic_workflow_codes_from_dag(dag),
    )
    mutation = _prepare_workflow_instance_edit_mutation(
        dag=dag,
        project=resolved_project,
        patch=patch,
        spec=spec,
        catalog=catalog,
        resource_refs=read_resource_refs,
        workflow_refs=read_workflow_refs,
    )
    native_sync = runtime_instance_contract_features(
        runtime.profile.ds_version
    ).instance_dag_edit_requires_sync
    sync_failure = _workflow_instance_sync_failure(
        runtime,
        located=located,
        mutation=mutation,
        sync_definition=sync_definition,
    )
    if sync_failure is not None and not dry_run:
        raise sync_failure
    workflow_refs = resolve_dynamic_authoring_workflow_refs(
        runtime.domain.workflows,
        project=located.project,
        child_workflow_names=mutation.compilation.required_child_workflow_names,
        containing_workflow_code=workflow_code,
        audit_descendants=mutation.has_changes,
        action="workflow-instance.edit",
        boundary_resource=WORKFLOW_INSTANCE_RESOURCE,
    )
    resource_refs = resolve_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mutation.compilation.required_resource_full_names,
        boundary_resource=WORKFLOW_INSTANCE_RESOURCE,
        action="workflow-instance.edit",
    )
    main_task_ids = _sync_main_task_ids(
        runtime,
        project_code=project_code,
        mutation=mutation,
        sync_definition=sync_definition,
    )
    compiled_payload = mutation.compilation.preview(
        main_task_ids=main_task_ids,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    preserved_global_params = _instance_preserved_global_params(dag, mutation.diff)
    merged_spec = mutation.merged_spec
    diff = mutation.diff
    resolved = _workflow_instance_edit_resolved(
        workflow_instance_id=workflow_instance_id,
        project=_selected_project_data(payload.project, selected_project),
        workflow=_workflow_instance_resolved_workflow(
            workflow_code=workflow_code,
            workflow_name=merged_spec.workflow.name,
            workflow_version=payload.workflowDefinitionVersion,
        ),
        input_mode=mutation.input_mode,
        patch_file=patch_file,
        file=file,
        sync_definition=sync_definition,
        native_sync_effect=native_sync and sync_definition and mutation.has_changes,
        task_datasources=task_datasource_resolutions,
    )
    has_changes = mutation.has_changes

    if dry_run:
        try:
            prepared = runtime.domain.instances.prepare_workflow_instance_update(
                located,
                **workflow_instance_update_arguments(
                    compiled_payload,
                    sync_definition=sync_definition,
                    preserved_global_params=preserved_global_params,
                ),
            )
        except ApiResultError as exc:
            _raise_workflow_instance_edit_error(
                exc,
                workflow_instance_id=workflow_instance_id,
            )
        return _workflow_instance_edit_dry_run_request_result(
            request=prepared.request,
            resolved=resolved,
            diff=diff,
            no_change=not has_changes,
            failure=sync_failure,
        )

    if not has_changes:
        no_change_warning = _workflow_instance_edit_no_change_message(
            mutation.input_mode
        )
        return CommandResult(
            data=require_json_object(
                payload.to_data(),
                label="workflow-instance data",
            ),
            resolved=require_json_object(
                resolved,
                label="workflow-instance edit resolved",
            ),
            warnings=[no_change_warning],
            warning_details=[
                require_json_object(
                    WorkflowInstanceEditNoChangeWarningDetail(
                        code="workflow_instance_edit_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="workflow-instance edit warning detail",
                )
            ],
        )

    _require_workflow_instance_file_edit_confirmation(
        mutation,
        confirmation=confirm_risk,
        project_code=project_code,
        workflow_instance_id=workflow_instance_id,
        workflow_code=workflow_code,
        sync_definition=sync_definition,
    )

    required_task_code_count = mutation.compilation.required_task_code_count
    allocated_task_codes = (
        runtime.domain.instances.generate_task_codes(
            payload.project,
            count=required_task_code_count,
        )
        if required_task_code_count
        else ()
    )
    compiled_payload = mutation.compilation.materialize(
        allocated_task_codes,
        main_task_ids=main_task_ids,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )

    try:
        prepared = runtime.domain.instances.prepare_workflow_instance_update(
            located,
            **workflow_instance_update_arguments(
                compiled_payload,
                sync_definition=sync_definition,
                preserved_global_params=preserved_global_params,
            ),
        )
        saved_workflow = runtime.domain.instances.apply_workflow_instance_update(
            prepared
        )
    except ApiResultError as exc:
        _raise_workflow_instance_edit_error(
            exc,
            workflow_instance_id=workflow_instance_id,
        )

    refreshed_payload = get_workflow_instance(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
    )
    _verify_synced_main_task_views(
        runtime,
        project_code=project_code,
        refreshed=refreshed_payload,
        mutation=mutation,
        allocated_task_codes=allocated_task_codes,
        main_task_ids=main_task_ids,
    )
    updated_resolved = _workflow_instance_edit_resolved(
        workflow_instance_id=workflow_instance_id,
        project=_selected_project_data(
            refreshed_payload.project,
            selected_project,
        ),
        workflow=_workflow_instance_resolved_workflow(
            workflow_code=saved_workflow.code
            if saved_workflow is not None
            else _require_instance_workflow_code(refreshed_payload),
            workflow_name=saved_workflow.name
            if saved_workflow is not None
            else merged_spec.workflow.name,
            workflow_version=saved_workflow.version
            if saved_workflow is not None
            else refreshed_payload.workflowDefinitionVersion,
        ),
        input_mode=mutation.input_mode,
        patch_file=patch_file,
        file=file,
        sync_definition=sync_definition,
        native_sync_effect=native_sync and sync_definition,
        task_datasources=task_datasource_resolutions,
    )
    return CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            updated_resolved,
            label="workflow-instance edit resolved",
        ),
    )


def _sync_main_task_ids(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_code: int,
    mutation: WorkflowMutationPlan,
    sync_definition: bool,
) -> dict[int, int] | None:
    """Bind main ids only when DS 3.1.0 will synchronize a changed DAG."""
    if (
        runtime.profile.ds_version != "3.1.0"
        or not sync_definition
        or not mutation.has_changes
    ):
        return None
    return read_main_task_ids(
        runtime.domain.task_definitions,
        project_code=project_code,
        task_codes=mutation.compilation.existing_task_codes,
        resource=WORKFLOW_INSTANCE_RESOURCE,
    )


def _verify_synced_main_task_views(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    project_code: int,
    refreshed: WorkflowInstanceSnapshot,
    mutation: WorkflowMutationPlan,
    allocated_task_codes: Sequence[int],
    main_task_ids: dict[int, int] | None,
) -> None:
    if main_task_ids is None:
        return
    verify_mutation(
        lambda: verify_main_task_views(
            runtime.domain.task_definitions,
            project_code=project_code,
            dag=refreshed.dagData,
            changed_names_by_code=changed_task_names_by_code(
                mutation, allocated_task_codes
            ),
            original_ids=main_task_ids,
            resource=WORKFLOW_INSTANCE_RESOURCE,
        ),
        ds_version=runtime.profile.ds_version,
        resource=WORKFLOW_INSTANCE_RESOURCE,
        operation="edit",
    )


def _instance_preserved_global_params(
    dag: WorkflowDagRecord, diff: WorkflowPatchDiffData
) -> str | None:
    if (
        "global_params" in diff["workflow_updated_fields"]
        or dag.workflowDefinition is None
    ):
        return None
    return dag.workflowDefinition.globalParams


def _workflow_instance_sync_failure(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    located: LocatedWorkflowInstance,
    mutation: WorkflowMutationPlan,
    sync_definition: bool,
) -> DsctlError | None:
    if (
        not mutation.has_changes
        or not runtime_instance_contract_features(
            runtime.profile.ds_version
        ).instance_dag_edit_requires_sync
    ):
        return None
    diff = mutation.diff
    dag_changed = bool(
        diff["added_tasks"]
        or diff["updated_tasks"]
        or diff["renamed_tasks"]
        or diff["deleted_tasks"]
        or diff["added_edges"]
        or diff["removed_edges"]
    )
    if dag_changed and not sync_definition:
        return UserInputError(
            "This DolphinScheduler version changes an instance DAG only with "
            "definition synchronization.",
            details={
                "ds_version": runtime.profile.ds_version,
                "workflow_instance_id": located.instance.id,
                "project": located.project.to_data(),
                "required_option": "--sync-definition",
            },
            suggestion=(
                "Use --sync-definition only if the current workflow definition "
                "should also change; scalar-only instance edits do not require it."
            ),
        )
    if not sync_definition:
        return None
    workflows = runtime.domain.workflows
    if workflows is None:
        message = "Instance synchronization requires workflow reads"
        raise RuntimeError(message)
    current = workflows.resolve_workflow_by_code(
        located.project, _require_instance_workflow_code(located.instance)
    )
    if current.view.release_state == "ONLINE":
        return None
    command_values = {
        "workflow": str(current.workflow.native.value),
        "project": str(located.project.native.value),
    }
    edit_command = COMMAND_CATALOG.render(
        "workflow.edit", values=command_values, global_values=selected_target_globals()
    )
    online_command = COMMAND_CATALOG.render(
        "workflow.online",
        values=command_values,
        global_values=selected_target_globals(),
    )
    return InvalidStateError(
        "Native instance synchronization can set the current definition ONLINE; "
        "its current state must already be ONLINE.",
        details={
            "ds_version": runtime.profile.ds_version,
            "workflow_instance_id": located.instance.id,
            "project": located.project.to_data(),
            "workflow": current.workflow.to_data(),
            "release_state": current.view.release_state,
            "required_release_state": "ONLINE",
        },
        suggestion=(
            f"Use `{edit_command}` with a definition patch for an OFFLINE "
            f"definition. Only if publication is intended, explicitly run "
            f"`{online_command}`, then retry the instance edit with --sync-definition."
        ),
    )


def _has_legacy_child_workflow_names(spec: WorkflowSpec) -> bool:
    """Return whether an instance edit needs same-project reference reads."""
    return any(
        task.type == "SUB_WORKFLOW"
        and task.task_params is not None
        and isinstance(task.task_params.get("childWorkflowName"), str)
        for task in spec.tasks
    )


def _has_legacy_dependent_names(spec: WorkflowSpec) -> bool:
    """Return whether an instance edit needs DEPENDENT target reads."""
    return any(
        task.type == "DEPENDENT"
        and task.task_params is not None
        and isinstance(task.task_params.get("dependence"), Mapping)
        for task in spec.tasks
    )


def _require_legacy_workflow_operations(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
) -> WorkflowOperations:
    """Require the exact workflow read seam used by instance graph authoring."""
    operations = runtime.domain.legacy_workflows
    if operations is not None:
        return operations
    message = "Legacy workflow-instance authoring runtime is incomplete"
    raise ApiTransportError(
        message,
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": workflow_instance_id,
            "missing_dependency": "legacy_workflows",
        },
    )


def _compile_legacy_workflow_instance_mutation(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    payload: WorkflowInstanceSnapshot,
    workflow_native: NativeId,
    draft: LegacyWorkflowMutationDraft,
    resource_refs: TaskResourceRefIndex | None,
) -> LegacyWorkflowMutationPlan:
    """Bind and audit final exact 1.3.9 instance graph references."""
    resolution = LegacyWorkflowAuthoringResolution.empty()
    dependent_refs = LegacyDependentRefIndex.empty()
    operations: WorkflowOperations | None = None
    if _has_legacy_child_workflow_names(
        draft.merged_spec
    ) or _has_legacy_dependent_names(draft.merged_spec):
        operations = _require_legacy_workflow_operations(
            runtime,
            workflow_instance_id=workflow_instance_id,
        )
        if _has_legacy_child_workflow_names(draft.merged_spec):
            resolution = resolve_legacy_authoring_workflow_refs(
                operations,
                project=payload.project,
                spec=draft.merged_spec,
                containing_workflow_id=workflow_native.value,
                action="workflow-instance.edit",
            )
        if _has_legacy_dependent_names(draft.merged_spec):
            dependent_refs = resolve_legacy_authoring_dependent_refs(
                operations,
                spec=draft.merged_spec,
                action="workflow-instance.edit",
            )
    mutation = compile_legacy_workflow_mutation_draft(
        draft,
        workflow_refs=resolution.refs,
        dependent_refs=dependent_refs,
        resource_refs=resource_refs,
    )
    if not requires_legacy_authoring_workflow_audit(
        mutation.compilation,
        project=payload.project,
        action="workflow-instance.edit",
    ):
        return mutation
    if operations is None:
        operations = _require_legacy_workflow_operations(
            runtime,
            workflow_instance_id=workflow_instance_id,
        )
    audit_legacy_authoring_workflow_graph(
        operations,
        project=payload.project,
        compilation=mutation.compilation,
        containing_workflow_id=workflow_native.value,
        resolution=resolution,
        action="workflow-instance.edit",
    )
    return mutation


def _edit_legacy_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    selected_project: SelectedValue,
    located: LocatedWorkflowInstance,
    patch_file: Path | None,
    patch: WorkflowPatchSpec | None,
    file: Path | None,
    spec: WorkflowSpec | None,
    catalog: TaskAuthoringCatalog,
    sync_definition: bool,
    dry_run: bool,
    confirm_risk: str | None,
) -> CommandResult:
    payload = located.instance
    workflow_name = _legacy_workflow_instance_name(payload)
    project_name = _legacy_workflow_instance_project_name(payload)
    workflow_native = payload.workflow_native
    if not isinstance(payload.project.native, NativeId) or not isinstance(
        workflow_native, NativeId
    ):
        message = "Legacy workflow instance authoring requires id-native metadata"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "project": payload.project.to_data(),
            },
        )
    _validate_legacy_workflow_instance_file_project(
        spec,
        project=payload.project,
        workflow_instance_id=workflow_instance_id,
    )
    graph = _legacy_workflow_instance_graph(payload)
    read_resource_refs = resolve_read_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mr_resource_ids_from_legacy_graph(graph),
    )
    graph = _legacy_workflow_instance_graph(
        payload,
        resource_refs=read_resource_refs,
    )
    datasource_baseline = task_datasource_baseline_from_legacy_graph(graph)
    patch, spec, task_datasource_resolutions = resolve_workflow_edit_task_datasources(
        runtime.domain.task_datasources,
        patch=patch,
        spec=spec,
        catalog=catalog,
        action="workflow-instance.edit",
        baseline=datasource_baseline,
    )
    mutation_input: WorkflowPatchSpec | WorkflowSpec
    if patch is not None:
        mutation_input = patch
    elif spec is not None:
        mutation_input = spec
    else:
        message = (
            "Workflow instance edit requires either a patch or full workflow spec."
        )
        raise RuntimeError(message)
    mutation_draft = prepare_legacy_workflow_mutation_draft(
        graph,
        workflow_name=workflow_name,
        project_name=project_name,
        description=None,
        release_state="OFFLINE",
        mutation=mutation_input,
        catalog=catalog,
        risk_type="workflow_instance_full_edit_destructive_change",
    )
    resource_refs = resolve_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mr_resource_full_names_from_spec(
            mutation_draft.merged_spec,
            profile_version=runtime.profile.ds_version,
        ),
        boundary_resource=WORKFLOW_INSTANCE_RESOURCE,
        action="workflow-instance.edit",
    )
    mutation = _compile_legacy_workflow_instance_mutation(
        runtime,
        workflow_instance_id=workflow_instance_id,
        payload=payload,
        workflow_native=workflow_native,
        draft=mutation_draft,
        resource_refs=resource_refs,
    )
    if mutation.input_mode == "file":
        _validate_workflow_instance_file_patch_support(mutation.diff)
    resolved = _workflow_instance_edit_resolved(
        workflow_instance_id=workflow_instance_id,
        project=_selected_project_data(payload.project, selected_project),
        workflow=_legacy_workflow_instance_resolved_workflow(
            payload,
            workflow_name=mutation.merged_spec.workflow.name,
        ),
        input_mode=mutation.input_mode,
        patch_file=patch_file,
        file=file,
        sync_definition=sync_definition,
        task_datasources=task_datasource_resolutions,
    )

    if not mutation.has_changes and not dry_run:
        return _workflow_instance_edit_no_change_result(
            payload,
            resolved=resolved,
            input_mode=mutation.input_mode,
        )

    if not dry_run:
        _require_legacy_workflow_instance_file_edit_confirmation(
            mutation,
            confirmation=confirm_risk,
            project_id=payload.project.native.value,
            workflow_instance_id=workflow_instance_id,
            workflow_id=workflow_native.value,
            sync_definition=sync_definition,
        )

    compiled = mutation.compilation.preview()
    prepared = runtime.domain.instances.prepare_legacy_workflow_instance_update(
        located,
        **legacy_workflow_instance_update_arguments(
            compiled,
            sync_definition=sync_definition,
        ),
    )
    if dry_run:
        return _workflow_instance_edit_dry_run_request_result(
            request=prepared.request,
            resolved=resolved,
            diff=mutation.diff,
            no_change=not mutation.has_changes,
        )

    try:
        runtime.domain.instances.apply_legacy_workflow_instance_update(prepared)
    except ApiResultError as exc:
        _raise_workflow_instance_edit_error(
            exc,
            workflow_instance_id=workflow_instance_id,
        )

    refreshed_payload = get_workflow_instance(
        runtime,
        project_selector=selected_project.value,
        workflow_instance_id=workflow_instance_id,
    )
    updated_resolved = _workflow_instance_edit_resolved(
        workflow_instance_id=workflow_instance_id,
        project=_selected_project_data(
            refreshed_payload.project,
            selected_project,
        ),
        workflow=_legacy_workflow_instance_resolved_workflow(
            refreshed_payload,
            workflow_name=mutation.merged_spec.workflow.name,
        ),
        input_mode=mutation.input_mode,
        patch_file=patch_file,
        file=file,
        sync_definition=sync_definition,
        task_datasources=task_datasource_resolutions,
    )
    return CommandResult(
        data=require_json_object(
            refreshed_payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            updated_resolved,
            label="workflow-instance edit resolved",
        ),
    )


def _prepare_workflow_instance_edit_mutation(
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
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state=None,
            catalog=catalog,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )
    if spec is None:
        message = (
            "Workflow instance edit requires either a patch or full workflow spec."
        )
        raise RuntimeError(message)
    mutation = prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=spec,
        release_state=None,
        risk_type="workflow_instance_full_edit_destructive_change",
        catalog=catalog,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    _validate_workflow_instance_file_patch_support(mutation.diff)
    return mutation


def _validate_workflow_instance_file_project(
    spec: WorkflowSpec | None,
    *,
    project: ResolvedProject,
    workflow_instance_id: int,
) -> None:
    if spec is None or spec.workflow.project is None:
        return
    requested_project = spec.workflow.project
    if requested_project in {project.name, str(project.code)}:
        return
    message = (
        "workflow-instance edit --file project does not match the instance project"
    )
    export_command = _workflow_instance_command(
        "export",
        workflow_instance_id=workflow_instance_id,
        project_selector=str(project.code),
    )
    raise UserInputError(
        message,
        details={
            "file_project": requested_project,
            "instance_project": {
                "code": project.code,
                "name": project.name,
            },
        },
        suggestion=(
            f"Export the target instance with `{export_command}`, edit that file, "
            "and keep workflow.project unchanged."
        ),
    )


def _validate_legacy_workflow_instance_file_project(
    spec: WorkflowSpec | None,
    *,
    project: ProjectRef,
    workflow_instance_id: int,
) -> None:
    if spec is None or spec.workflow.project is None:
        return
    requested_project = spec.workflow.project
    accepted = {str(project.native.value)}
    if project.name is not None:
        accepted.add(project.name)
    if requested_project in accepted:
        return
    message = (
        "workflow-instance edit --file project does not match the instance project"
    )
    project_selector = project.name or str(project.native.value)
    export_command = _workflow_instance_command(
        "export",
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
    )
    raise UserInputError(
        message,
        details={
            "file_project": requested_project,
            "instance_project": project.to_data(),
        },
        suggestion=(
            f"Export the target instance with `{export_command}`, edit that file, "
            "and keep workflow.project unchanged."
        ),
    )


def _validate_workflow_instance_file_patch_support(
    diff: WorkflowPatchDiffData,
) -> None:
    unsupported_fields = sorted(
        field_name
        for field_name in diff["workflow_updated_fields"]
        if field_name not in WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
    )
    if not unsupported_fields:
        return
    message = (
        "workflow-instance edit --file only supports workflow.global_params and "
        "workflow.timeout changes"
    )
    raise UserInputError(
        message,
        details={
            "unsupported_fields": unsupported_fields,
            "supported_fields": sorted(
                WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
            ),
        },
        suggestion=(
            "Use `dsctl workflow edit --file ...` for definition-level fields "
            "such as name, description, execution_type, or release_state."
        ),
    )


def _workflow_instance_resolved_workflow(
    *,
    workflow_code: int,
    workflow_name: str | None,
    workflow_version: int | None,
) -> JsonObject:
    return require_json_object(
        {
            "code": workflow_code,
            "name": workflow_name,
            "version": workflow_version,
        },
        label="workflow-instance resolved workflow",
    )


def _legacy_workflow_instance_resolved_workflow(
    payload: WorkflowInstanceSnapshot,
    *,
    workflow_name: str,
) -> JsonObject:
    native = payload.workflow_native
    if not isinstance(native, NativeId):
        message = "Legacy workflow instance payload was missing its definition id"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": payload.id,
                "ds_version": payload.ds_version,
            },
        )
    return WorkflowRef(
        native=native,
        name=workflow_name,
        version=payload.workflowDefinitionVersion,
    ).to_data()


def _workflow_instance_edit_resolved(
    *,
    workflow_instance_id: int,
    project: JsonObject,
    workflow: JsonObject,
    input_mode: WorkflowInstanceEditInputMode,
    patch_file: Path | None,
    file: Path | None,
    sync_definition: bool,
    task_datasources: list[TaskDatasourceResolutionData],
    native_sync_effect: bool = False,
) -> WorkflowInstanceEditResolved:
    resolved = WorkflowInstanceEditResolved(
        workflowInstance=WorkflowInstanceSelectionData(id=workflow_instance_id),
        project=project,
        workflow=workflow,
        input_mode=input_mode,
        syncDefine=sync_definition,
        task_datasources=task_datasources,
    )
    if patch_file is not None:
        resolved["patch_file"] = str(patch_file)
    if file is not None:
        resolved["file"] = str(file)
    if native_sync_effect:
        resolved["native_definition_release_effect"] = (
            "Definition metadata changes may set the current definition ONLINE."
        )
    return resolved


def _workflow_instance_edit_dry_run_request_result(
    *,
    request: WireRequest,
    resolved: WorkflowInstanceEditResolved,
    diff: WorkflowPatchDiffData,
    no_change: bool,
    failure: DsctlError | None = None,
) -> CommandResult:
    if request.form is None:
        message = "Prepared workflow-instance edit request was missing form data"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "method": request.method,
                "path": request.path,
            },
        )
    return _workflow_instance_edit_dry_run_payload_result(
        method=request.method,
        path=request.path,
        form_data=require_json_object(
            dict(request.form),
            label="prepared workflow-instance edit form data",
        ),
        resolved=resolved,
        diff=diff,
        no_change=no_change,
        failure=failure,
    )


def _workflow_instance_edit_dry_run_payload_result(
    *,
    method: str,
    path: str,
    form_data: JsonObject,
    resolved: WorkflowInstanceEditResolved,
    diff: WorkflowPatchDiffData,
    no_change: bool,
    failure: DsctlError | None = None,
) -> CommandResult:
    warnings: list[str] = []
    warning_details: list[JsonObject] = []
    effect = resolved.get("native_definition_release_effect")
    if effect is not None:
        warnings.append(effect)
        warning_details.append(
            {
                "code": "workflow_instance_definition_sync_may_set_online",
                "message": effect,
                "condition": "native definition metadata differs",
            }
        )
    if no_change:
        no_change_warning = _workflow_instance_edit_no_change_message(
            resolved["input_mode"]
        )
        warnings.append(no_change_warning)
        warning_details.append(
            require_json_object(
                WorkflowInstanceEditNoChangeWarningDetail(
                    code="workflow_instance_edit_no_persistent_change",
                    message=no_change_warning,
                    no_change=True,
                    request_sent=False,
                ),
                label="workflow-instance edit dry-run warning detail",
            )
        )
    return replace(
        dry_run_result(
            method=method,
            path=path,
            form_data=require_json_object(
                form_data,
                label="workflow-instance edit dry-run form data",
            ),
            requests=[] if no_change else None,
            resolved=require_json_object(
                resolved,
                label="workflow-instance edit dry-run resolved",
            ),
            warnings=warnings,
            warning_details=warning_details,
            extra_data={
                "diff": require_json_object(
                    workflow_patch_diff_output(diff),
                    label="workflow-instance edit dry-run diff",
                ),
                "no_change": no_change,
                "syncDefine": resolved["syncDefine"],
                "workflow_state_constraints": [] if failure is None else [str(failure)],
            },
        ),
        failure=failure,
    )


def _workflow_instance_edit_no_change_message(
    input_mode: WorkflowInstanceEditInputMode,
) -> str:
    source = "workflow file" if input_mode == "file" else "patch"
    return (
        f"{source} produced no persistent workflow instance change; no edit "
        "request was sent"
    )


def _workflow_instance_edit_no_change_result(
    payload: WorkflowInstanceSnapshot,
    *,
    resolved: WorkflowInstanceEditResolved,
    input_mode: WorkflowInstanceEditInputMode,
) -> CommandResult:
    no_change_warning = _workflow_instance_edit_no_change_message(input_mode)
    return CommandResult(
        data=require_json_object(
            payload.to_data(),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            resolved,
            label="workflow-instance edit resolved",
        ),
        warnings=[no_change_warning],
        warning_details=[
            require_json_object(
                WorkflowInstanceEditNoChangeWarningDetail(
                    code="workflow_instance_edit_no_persistent_change",
                    message=no_change_warning,
                    no_change=True,
                    request_sent=False,
                ),
                label="workflow-instance edit warning detail",
            )
        ],
    )


def _require_workflow_instance_file_edit_confirmation(
    mutation: WorkflowMutationPlan,
    *,
    confirmation: str | None,
    project_code: int,
    workflow_instance_id: int,
    workflow_code: int,
    sync_definition: bool,
) -> None:
    if mutation.input_mode != "file" or mutation.confirmation is None:
        return
    confirmation_details = mutation.confirmation
    require_confirmation(
        action="workflow-instance.edit",
        confirmation=confirmation,
        payload=_workflow_instance_file_edit_confirmation_payload(
            project_code=project_code,
            workflow_instance_id=workflow_instance_id,
            workflow_code=workflow_code,
            sync_definition=sync_definition,
            confirmation_details=confirmation_details,
        ),
        message=(
            "This full workflow-instance YAML edit deletes tasks or changes task "
            "types and requires explicit confirmation."
        ),
        details=confirmation_details,
    )


def _require_legacy_workflow_instance_file_edit_confirmation(
    mutation: LegacyWorkflowMutationPlan,
    *,
    confirmation: str | None,
    project_id: int,
    workflow_instance_id: int,
    workflow_id: int,
    sync_definition: bool,
) -> None:
    if mutation.input_mode != "file" or mutation.confirmation is None:
        return
    confirmation_details = mutation.confirmation
    require_confirmation(
        action="workflow-instance.edit",
        confirmation=confirmation,
        payload=require_json_object(
            {
                "project_id": project_id,
                "workflow_instance_id": workflow_instance_id,
                "workflow_id": workflow_id,
                "syncDefine": sync_definition,
                "risk_type": confirmation_details["risk_type"],
                "deleted_tasks": confirmation_details["deleted_tasks"],
                "task_type_changes": confirmation_details["task_type_changes"],
            },
            label="legacy workflow-instance full edit confirmation payload",
        ),
        message=(
            "This full workflow-instance YAML edit deletes tasks or changes task "
            "types and requires explicit confirmation."
        ),
        details=confirmation_details,
    )


def _workflow_instance_file_edit_confirmation_payload(
    *,
    project_code: int,
    workflow_instance_id: int,
    workflow_code: int,
    sync_definition: bool,
    confirmation_details: WorkflowFileEditRiskData,
) -> JsonObject:
    return require_json_object(
        {
            "project_code": project_code,
            "workflow_instance_id": workflow_instance_id,
            "workflow_code": workflow_code,
            "syncDefine": sync_definition,
            "risk_type": confirmation_details["risk_type"],
            "deleted_tasks": confirmation_details["deleted_tasks"],
            "task_type_changes": confirmation_details["task_type_changes"],
        },
        label="workflow-instance full edit confirmation payload",
    )
