from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        DinkyAuthoringSurface,
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

DINKY_JOB_TRIGGER_FACET = "DINKY/job_trigger"


def _dinky_runtime_guidance(surface: DinkyAuthoringSurface) -> str:
    """Describe exact implicit forwarding and recovery hazards for Dinky."""
    if not surface.available:
        return "DINKY is absent from this exact DolphinScheduler profile."
    if surface.variable_forwarding == "none":
        if surface.version_negotiation:
            message = "No-forwarding DINKY surface cannot negotiate the v1 API"
            raise ValueError(message)
        forwarding = "This exact executor does not forward workflow globals. "
    else:
        if not surface.version_negotiation:
            message = "Forwarding DINKY surface must negotiate the v1 API"
            raise ValueError(message)
        if surface.variable_forwarding == "workflow_globals_and_local_params":
            forwarded_values = "workflow global parameters and DINKY localParams"
        elif surface.variable_forwarding == ("workflow_globals_and_local_placeholders"):
            forwarded_values = (
                "workflow global parameters and DINKY localParams after local "
                "placeholder expansion against the prepared parameter map"
            )
        elif surface.variable_forwarding == "all_prepared_params":
            forwarded_values = (
                "all prepared parameter values, including built-in, project, "
                "workflow-global, task, command, varPool, and business values"
            )
        else:
            message = "Available DINKY surface must declare variable forwarding"
            raise ValueError(message)
        variable_logging = (
            " The complete outgoing map is logged at INFO; dsctl does not redact "
            "secrets."
            if surface.variables_logged
            else ""
        )
        forwarding = (
            "When version negotiation selects the Dinky 1.x path, "
            f"{forwarded_values} are sent to Dinky; the legacy API path does not "
            f"forward them.{variable_logging} Never place secrets in those values. "
        )
    request_security = (
        "authenticated" if surface.authenticated_request else "unauthenticated"
    )
    request_timeout = (
        "an explicit HTTP timeout"
        if surface.explicit_http_timeout
        else "no explicit HTTP timeout"
    )
    task_logging = (
        "Task parameters are logged upstream together with request URLs and response "
        "content. "
        if surface.task_params_logged
        else ""
    )
    failover = (
        "The remote job identity supports failover resume. "
        if surface.failover_supported
        else "The remote job id is not durably persisted for failover. "
    )
    retry = (
        "Retry may resubmit the Dinky job."
        if surface.retry_may_resubmit
        else "Retry does not resubmit the Dinky job."
    )
    return (
        forwarding
        + task_logging
        + f"The worker uses {request_security} HTTP requests with {request_timeout}. "
        + failover
        + retry
    )


def _dinky_fields(
    surface: DinkyAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    """Return the exact core Dinky job-trigger authoring surface."""
    runtime = _dinky_runtime_guidance(surface)
    return (
        model_field(
            "task_params.address",
            compile_path="taskDefinitionJson[].taskParams.address",
            description=(
                "Literal absolute HTTP(S) Dinky base address without credentials, "
                f"query, fragment, or DS placeholders. {runtime}"
            ),
        ),
        model_field(
            "task_params.taskId",
            compile_path="taskDefinitionJson[].taskParams.taskId",
            description=(
                "Literal non-secret Dinky task identifier sent as a request value."
            ),
        ),
        model_field(
            "task_params.online",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.online",
            description=(
                "Strict boolean selecting Dinky online-task submission; false uses "
                "ordinary job submission."
            ),
        ),
    )


def _dinky_template_body(
    surface: DinkyAuthoringSurface,
    body: str,
) -> str:
    comments = (
        f"# Runtime prerequisite: {_dinky_runtime_guidance(surface)}\n"
        "# Typed scope: only address, taskId, and online. localParams, varPool, and "
        "future native state remain unchanged/export preservation only.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _dinky_templates(
    surface: DinkyAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Trigger one existing Dinky job through its HTTP API.",
            payload_modes=("task_params",),
            yaml=_dinky_template_body(
                surface,
                """# Task template for one Dinky job trigger
name: trigger-dinky-job
type: DINKY
description: Trigger one existing Dinky job
task_params:
  address: https://dinky.example.com
  taskId: "1842"
  online: false
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
            ),
        ),
    )


def _dinky_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).dinky
    if not surface.available:
        message = f"DINKY is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DINKY_JOB_TRIGGER_FACET,
        family="dinky-job-trigger-v1",
        review="dinky-job-trigger-literal-endpoint-subset",
        params_model=_family_model("DINKY"),
        fields=_dinky_fields(surface),
        state_rules=(),
        templates=_dinky_templates(surface),
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
        task_type="DINKY",
        category="Universal",
        kind="typed",
        default_facet=DINKY_JOB_TRIGGER_FACET,
        facets={DINKY_JOB_TRIGGER_FACET: membership},
    )
