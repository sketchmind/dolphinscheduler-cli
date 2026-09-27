from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        SeatunnelAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringStateRule,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

SEATUNNEL_LITERAL_LOCAL_CONFIG_JOB_FACET = "SEATUNNEL/literal_local_config_job"


def _seatunnel_runtime_guidance(
    surface: SeatunnelAuthoringSurface,
    *,
    profile_version: str,
) -> str:
    """Describe exact local launcher, disclosure, and recovery boundaries."""
    if not surface.available or surface.wire_epoch is None:
        return "SEATUNNEL is absent from this exact DolphinScheduler profile."
    launcher = (
        "The 3.0.x REST-only compiler wrapper writes the literal config with "
        "POSIX printf, then starts start-seatunnel-spark.sh in client/local mode. "
        if surface.wire_epoch == "legacy-raw-shell"
        else (
            "The worker writes a UTF-8 custom config and starts "
            "start-seatunnel-spark.sh in client/local mode. "
            if surface.wire_epoch
            in {"engine-custom-config", "engine-explicit-local-master"}
            else (
                "The worker writes a UTF-8 custom config and starts seatunnel.sh "
                "with deploy mode local. "
            )
        )
    )
    parameter_gate = (
        "This exact executor forwards workflow globals through shell command "
        "arguments outside the facet; typed compilation therefore requires a "
        "parameter-free workflow. "
        if surface.workflow_parameter_forwarding != "none"
        else ""
    )
    return (
        "Route to a Unix-like worker whose exported SEATUNNEL_HOME contains the "
        "selected launcher and whose Java, SeaTunnel connectors/catalogs, network "
        "access, tenant permissions, and data permissions match the config. "
        f"{launcher}{parameter_gate}The literal ASCII config rejects DS "
        "placeholders and is INFO-logged with task parameters and commands, so it "
        "is not secret storage. HOCON/JSON validity and connector semantics remain "
        "operator prerequisites. Resources, parameters, output declarations, "
        "remote engines, startup options, and arbitrary shell are outside typed "
        "authoring. Cancellation is worker-local best effort; there is no durable "
        "application id or failover reattachment, and retry reruns the complete "
        f"job. Exact profile: {profile_version}."
    )


def _seatunnel_fields(
    surface: SeatunnelAuthoringSurface,
    *,
    profile_version: str,
) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.rawScript",
            compile_path=(
                "taskDefinitionJson[].taskParams.rawScript or compiler-owned "
                "3.0.x POSIX wrapper"
            ),
            description=(
                "Preserved literal ASCII SeaTunnel HOCON or JSON configuration "
                "with LF/tab only and no DolphinScheduler placeholders. "
                + _seatunnel_runtime_guidance(
                    surface,
                    profile_version=profile_version,
                )
            ),
        ),
    )


def _seatunnel_templates(
    surface: SeatunnelAuthoringSurface,
    *,
    profile_version: str,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _seatunnel_runtime_guidance(
        surface,
        profile_version=profile_version,
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal worker-local SeaTunnel configuration.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisites and limits: {guidance}\n"
                "# Typed scope: one literal local config; no resources or params.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one worker-local SeaTunnel config
name: run-seatunnel-config
type: SEATUNNEL
description: Execute one literal SeaTunnel configuration
task_params:
  rawScript: |
    env {
      execution.parallelism = 1
    }
    source {
      FakeSource {}
    }
    sink {
      Console {}
    }
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


def _seatunnel_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    """Materialize the twelve exact reviewed SeaTunnel memberships."""
    surface = get_task_authoring_surface(profile_version).seatunnel
    if not surface.available:
        message = f"SEATUNNEL is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=SEATUNNEL_LITERAL_LOCAL_CONFIG_JOB_FACET,
        family="seatunnel-literal-local-config-v1",
        review="seatunnel-literal-local-config-exact-subset",
        params_model=_family_model("SEATUNNEL"),
        fields=_seatunnel_fields(surface, profile_version=profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed SEATUNNEL literal worker-local config authoring",
                condition_paths=("workflow.global_params",),
                active_paths=("task_params.rawScript",),
                compile_policy=(
                    (
                        "task_params.rawScript",
                        (
                            "project by exact raw-shell, SPARK/custom, or "
                            "seatunnel.sh epoch"
                        ),
                    ),
                    ("task_params.localParams", "send compiler-owned empty list"),
                    ("task_params.resourceList", "send compiler-owned empty list"),
                    (
                        "task_params.others",
                        "send compiler-owned empty string when native",
                    ),
                    (
                        "workflow.global_params",
                        "require empty when the exact executor forwards CLI variables",
                    ),
                ),
                description=(
                    "The facet owns only one literal local configuration. Richer "
                    "native state is unchanged/export preserve-only."
                ),
            ),
        ),
        templates=_seatunnel_templates(surface, profile_version=profile_version),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "SEATUNNEL/literal_local_config_job closes raw opaque create/edit; "
            "engine, startup, resources, parameters, options, outputs, and future "
            "state are unchanged/export preservation only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="SEATUNNEL",
        category="DataIntegration",
        kind="typed",
        default_facet=SEATUNNEL_LITERAL_LOCAL_CONFIG_JOB_FACET,
        facets={SEATUNNEL_LITERAL_LOCAL_CONFIG_JOB_FACET: membership},
    )
