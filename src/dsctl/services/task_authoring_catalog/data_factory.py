from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        DataFactoryAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

DATA_FACTORY_PIPELINE_TRIGGER_FACET = "DATA_FACTORY/pipeline_trigger"


def _data_factory_runtime_guidance(surface: DataFactoryAuthoringSurface) -> str:
    """Describe exact Azure credentials, remote identity, and replay hazards."""
    if not surface.available:
        return "DATA_FACTORY is absent from this exact DolphinScheduler profile."
    if (
        surface.wire_epoch != "literal-three-field-pipeline-trigger"
        or surface.credential_source != "worker-properties"
        or surface.query_interval_key is None
        or surface.application_id_field != "runId"
        or surface.application_id_persistence != "appIds-callback"
    ):
        message = "Available DATA_FACTORY surface lacks its exact runtime contract"
        raise ValueError(message)
    credential_keys = ", ".join(surface.credential_keys)
    logging = (
        "Upstream logs the literal task params at INFO but does not add worker "
        "credential values to those params. "
        if surface.task_params_logged and not surface.credentials_logged
        else ""
    )
    outputs = (
        "The pipeline result is not a DolphinScheduler task output. "
        if not surface.result_output_supported
        else ""
    )
    resume = (
        "After the runId is stored in appIds by the callback, worker failover can "
        "resume polling and cancel the same Azure run. "
        if surface.failover_supported
        else ""
    )
    replay = (
        "Azure createRun can succeed before the appIds callback is durable; that "
        "gap, or a retry without appIds, can resubmit a duplicate pipeline run. "
        if surface.callback_persistence_gap and surface.retry_may_resubmit
        else ""
    )
    return (
        "Configure Azure credentials on every eligible worker with "
        f"{credential_keys}. The executor uses {surface.query_interval_key} with "
        "a 10000 ms default polling interval. The three identity fields are "
        "literal: DS placeholder substitution and localParams are unsupported, "
        "and this plugin cannot pass pipeline parameters. "
        f"{logging}{outputs}{resume}{replay}Keep credentials out of task fields."
    )


def _data_factory_fields(
    surface: DataFactoryAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    runtime = _data_factory_runtime_guidance(surface)
    descriptions = {
        "factoryName": (
            f"Literal existing Azure Data Factory name selected for the run. {runtime}"
        ),
        "resourceGroupName": (
            "Literal Azure resource-group name containing the selected factory."
        ),
        "pipelineName": (
            "Literal existing pipeline name triggered without pipeline parameters."
        ),
    }
    return tuple(
        model_field(
            f"task_params.{field_name}",
            compile_path=f"taskDefinitionJson[].taskParams.{field_name}",
            description=description,
        )
        for field_name, description in descriptions.items()
    )


def _data_factory_templates(
    surface: DataFactoryAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _data_factory_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Trigger one existing Azure Data Factory pipeline.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope: factoryName, resourceGroupName, and pipelineName "
                "only. runId is runtime-only; localParams, inherited, resource, "
                "and future fields remain unchanged/export preservation only.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one Azure Data Factory pipeline trigger
name: trigger-data-factory-pipeline
type: DATA_FACTORY
description: Trigger one existing Azure Data Factory pipeline
task_params:
  factoryName: analytics-factory
  resourceGroupName: analytics-rg
  pipelineName: daily-copy
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
                )
            ),
        ),
    )


def _data_factory_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).data_factory
    if not surface.available:
        message = f"DATA_FACTORY is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DATA_FACTORY_PIPELINE_TRIGGER_FACET,
        family="data-factory-pipeline-trigger-v1",
        review="data-factory-literal-pipeline-trigger",
        params_model=_family_model("DATA_FACTORY"),
        fields=_data_factory_fields(surface),
        state_rules=(),
        templates=_data_factory_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="DATA_FACTORY",
        category="Cloud",
        kind="typed",
        default_facet=DATA_FACTORY_PIPELINE_TRIGGER_FACET,
        facets={DATA_FACTORY_PIPELINE_TRIGGER_FACET: membership},
    )
