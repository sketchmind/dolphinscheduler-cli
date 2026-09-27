from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        DmsAuthoringSurface,
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

DMS_RESUME_EXISTING_FULL_LOAD_FACET = "DMS/resume_existing_full_load"


def _dms_runtime_guidance(surface: DmsAuthoringSurface) -> str:
    """Describe exact DMS credentials, tracking, and replay hazards."""
    if not surface.available:
        return "DMS is absent from this exact DolphinScheduler profile."
    if (
        surface.wire_epoch != "explicit-resume-existing-full-load"
        or surface.credential_source is None
        or surface.poll_interval_ms != 1000
        or surface.application_id_field != "replicationTaskArn"
        or surface.application_id_persistence != "appIds-callback"
        or surface.declared_migration_type != "full-load"
    ):
        message = "Available DMS surface lacks its exact resume runtime contract"
        raise ValueError(message)
    provider_types = ", ".join(surface.credential_provider_types)
    if surface.credential_source == "worker-properties":
        credential_setup = (
            "Configure static credentials on every eligible worker with "
            + ", ".join(surface.credential_keys)
            + ". "
        )
    else:
        if (
            len(surface.credential_keys) != 5
            or len(surface.credential_provider_types) != 2
        ):
            message = "AWS-authentication DMS surface lacks its exact credentials"
            raise ValueError(message)
        provider_key, access_key, secret_key, region_key, endpoint_key = (
            surface.credential_keys
        )
        static_provider, instance_provider = surface.credential_provider_types
        credential_setup = (
            f"Configure {provider_key} and {region_key} on every eligible worker. "
            f"{static_provider} additionally uses {access_key} and {secret_key}; "
            f"{instance_provider} uses the worker instance profile, and "
            f"{endpoint_key} is optional. "
        )
    logging = (
        "Upstream logs full task parameters and replicationTaskArn plus remote "
        "resource identifiers at INFO; worker credential values are not logged "
        "as task parameters. "
        if surface.task_params_logged
        and surface.resource_identifiers_logged
        and not surface.credentials_logged
        else ""
    )
    tracking = (
        "The replicationTaskArn is persisted to appIds by a callback, after which "
        "worker failover can resume 1000 ms polling and cancellation can stop the "
        "same remote task. "
        if surface.failover_supported and surface.cancel_supported
        else ""
    )
    replay = (
        "A crash before the appIds callback is durable leaves a persistence gap; "
        "retry may resubmit resume-processing. "
        if surface.callback_persistence_gap and surface.retry_may_resubmit
        else ""
    )
    timeout = (
        "Polling has no explicit internal timeout, so use the task timeout as the "
        "caller-owned execution limit. "
        if not surface.explicit_internal_timeout
        else ""
    )
    output = (
        "No DMS result is exposed as a DolphinScheduler task output. "
        if not surface.result_output_supported
        else ""
    )
    literal_inputs = (
        "This explicit five-field resume facet performs no DolphinScheduler "
        "parameter substitution and accepts no resource files. "
        if not surface.parameter_substitution and not surface.resource_files_supported
        else ""
    )
    remote_validation = (
        "DolphinScheduler cannot verify its remote migration type or task state "
        "before sending resume-processing. "
        if not surface.remote_migration_type_verified
        and not surface.remote_task_state_verified
        else ""
    )
    incomplete_tables = (
        "Resuming may reload partially completed and not-yet-loaded tables. "
        if surface.resume_full_load_may_reload_incomplete_tables
        else ""
    )
    reload_target = (
        "The destructive reload-target alternative, which can truncate or drop "
        "target tables, is not exposed by this typed facet. "
        if not surface.reload_target_exposed
        else ""
    )
    cdc = (
        "A CDC task without a stop position can report success after start while "
        "remote replication continues. "
        if surface.cdc_without_stop_position_reports_success_after_start
        else ""
    )
    return (
        "WARNING: the caller must supply a previously executed and then stopped "
        "full-load AWS DMS "
        f"task. {remote_validation}{incomplete_tables}{reload_target}{cdc}"
        f"{literal_inputs}{credential_setup}The credential source is "
        f"{surface.credential_source}; supported providers are {provider_types}. "
        f"{logging}{tracking}{replay}{timeout}"
        f"{output}Keep credentials out of task fields."
    )


def _dms_fields(surface: DmsAuthoringSurface) -> tuple[TaskAuthoringField, ...]:
    runtime = _dms_runtime_guidance(surface)
    return (
        model_field(
            "task_params.isRestartTask",
            compile_path="taskDefinitionJson[].taskParams.isRestartTask",
            description=f"Strict constant true selecting restart. {runtime}",
        ),
        model_field(
            "task_params.isJsonFormat",
            compile_path="taskDefinitionJson[].taskParams.isJsonFormat",
            description="Strict constant false; JSON create-task mode is excluded.",
        ),
        model_field(
            "task_params.migrationType",
            "enum",
            choices=("full-load",),
            compile_path="taskDefinitionJson[].taskParams.migrationType",
            description=(
                "Required caller declaration full-load; upstream does not verify "
                "that declaration against the remote task before resuming it."
            ),
        ),
        model_field(
            "task_params.startReplicationTaskType",
            "enum",
            choices=("resume-processing",),
            compile_path=("taskDefinitionJson[].taskParams.startReplicationTaskType"),
            description=(
                "Exact resume-processing operation; reload-target is deliberately "
                "not exposed."
            ),
        ),
        model_field(
            "task_params.replicationTaskArn",
            compile_path="taskDefinitionJson[].taskParams.replicationTaskArn",
            description=(
                "Literal AWS DMS replication-task ARN used as the remote identity "
                "and persisted application id."
            ),
        ),
    )


def _dms_templates(surface: DmsAuthoringSurface) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _dms_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Resume one previously stopped AWS DMS full-load task.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope: the exact five explicit fields below. Create, "
                "JSON, CDC, reload-target, inherited, resource, and future native "
                "state remain unchanged/export preservation only.\n"
                + task_template_with_runtime_controls(
                    """# Task template for resuming one existing full-load DMS task
name: resume-existing-full-load
type: DMS
description: Resume one previously stopped full-load replication task
task_params:
  isRestartTask: true
  isJsonFormat: false
  migrationType: full-load
  startReplicationTaskType: resume-processing
  replicationTaskArn: arn:aws:dms:us-east-1:123456789012:task:REPLACE-ME
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
"""
                )
            ),
        ),
    )


def _dms_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).dms
    if not surface.available:
        message = f"DMS is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DMS_RESUME_EXISTING_FULL_LOAD_FACET,
        family="dms-existing-full-load-resume-v1",
        review="dms-resume-existing-full-load-exact-subset",
        params_model=_family_model("DMS"),
        fields=_dms_fields(surface),
        state_rules=(),
        templates=_dms_templates(surface),
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
        task_type="DMS",
        category="Cloud",
        kind="typed",
        default_facet=DMS_RESUME_EXISTING_FULL_LOAD_FACET,
        facets={DMS_RESUME_EXISTING_FULL_LOAD_FACET: membership},
    )
