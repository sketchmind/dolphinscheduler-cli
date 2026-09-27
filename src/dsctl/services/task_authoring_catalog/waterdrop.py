from __future__ import annotations

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
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

WATERDROP_LITERAL_LOCAL_CONFIG_JOB_FACET = "WATERDROP/literal_local_config_job"
_WATERDROP_RUNTIME_EXCLUSION_FACET = "WATERDROP/runtime_exclusion"


def _waterdrop_runtime_guidance(profile_version: str) -> str:
    """Describe the exact Shell alias prerequisites and replay boundary."""
    surface = get_task_authoring_surface(profile_version).waterdrop
    if not surface.available:
        return f"WATERDROP is not executable in DolphinScheduler {profile_version}."
    if not surface.channel_registered or surface.script_encoding is None:
        message = "Available WATERDROP surface lacks its Shell alias facts"
        raise ValueError(message)
    return (
        "Route to a POSIX worker with WATERDROP_HOME, a compatible Waterdrop/Spark "
        "runtime, tenant file permissions, and target connectivity. The worker "
        "writes the generated script with its platform-default charset and logs "
        "task params, resolved script, command, and child output at INFO. Values "
        "are not secret storage. Cancellation is worker-local best effort; no "
        "durable application id or failover reattachment exists, and retry can "
        "rerun the whole job."
    )


def _waterdrop_fields(profile_version: str) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.configResource",
            choice_source="dsctl resource list",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path=(
                "taskDefinitionJson[].taskParams.resourceList[].id and "
                "taskDefinitionJson[].taskParams.rawScript --config"
            ),
            description=(
                "Absolute ASCII shell-safe FILE resource fullName. The compiler "
                "resolves its positive id, stages it, and removes exactly one "
                "leading slash for the worker-relative --config path. "
                + _waterdrop_runtime_guidance(profile_version)
            ),
        ),
    )


def _waterdrop_templates(profile_version: str) -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one worker-local Waterdrop configuration job.",
            payload_modes=("task_params",),
            resource_fields=("task_params.configResource",),
            yaml=(
                "# Runtime prerequisite: "
                f"{_waterdrop_runtime_guidance(profile_version)}\n"
                "# Typed scope fixes local/client execution and queue=default.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one worker-local Waterdrop config
name: run-waterdrop-config
type: WATERDROP
description: Execute one staged Waterdrop configuration
task_params:
  configResource: /waterdrop/orders.conf
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


def _waterdrop_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).waterdrop
    if not surface.available:
        if surface.exclusion_reason is not None:
            return _waterdrop_runtime_exclusion_profile(profile_version)
        reason = surface.exclusion_reason or "upstream-absent"
        message = (
            f"WATERDROP typed authoring is unavailable in DolphinScheduler "
            f"{profile_version}: {reason}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=WATERDROP_LITERAL_LOCAL_CONFIG_JOB_FACET,
        family="waterdrop-literal-local-config-job-v1",
        review="waterdrop-literal-local-config-job-exact-subset",
        params_model=_family_model("WATERDROP"),
        fields=_waterdrop_fields(profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed WATERDROP worker-local config authoring",
                condition_paths=(),
                active_paths=("task_params.configResource",),
                compile_policy=(
                    (
                        "task_params.configResource",
                        "resolve to one positive FILE id and a staged relative path",
                    ),
                    ("task_params.localParams", "send compiler-owned empty list"),
                    (
                        "task_params.rawScript",
                        "send one compiler-owned local/client/default launcher line",
                    ),
                ),
                description=(
                    "Remote modes, variables, multiple configs, arbitrary scripts, "
                    "and future native state are outside this typed facet."
                ),
            ),
        ),
        templates=_waterdrop_templates(profile_version),
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
            "WATERDROP/literal_local_config_job exposes only the closed typed "
            "configResource intent; native scripts and richer state are "
            "unchanged/export preservation only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="WATERDROP",
        category="DataIntegration",
        kind="typed",
        default_facet=WATERDROP_LITERAL_LOCAL_CONFIG_JOB_FACET,
        facets={WATERDROP_LITERAL_LOCAL_CONFIG_JOB_FACET: membership},
    )


def _waterdrop_runtime_exclusion_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    """Materialize exact 2.0.0 as preserve-only missing-channel state."""
    surface = get_task_authoring_surface(profile_version).waterdrop
    reason = surface.exclusion_reason
    if (
        profile_version != "2.0.0"
        or surface.available
        or surface.channel_registered
        or reason != "waterdrop-channel-not-registered-to-shell"
    ):
        message = f"WATERDROP {profile_version} is not the reviewed runtime hole"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=_WATERDROP_RUNTIME_EXCLUSION_FACET,
        family="waterdrop-runtime-exclusion-v1",
        review="waterdrop-shell-channel-runtime-exclusion",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when="existing exact 2.0.0 WATERDROP server state",
                condition_paths=(),
                active_paths=("task_params",),
                compile_policy=(("task_params", "preserve unchanged only"),),
                description=(
                    "The stock worker has no WATERDROP-to-SHELL channel alias. "
                    "Typed and raw opaque create/edit are closed because no "
                    "payload can repair that runtime registration defect."
                ),
            ),
        ),
        templates=(),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=False,
        typed_edit=False,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "Exact 2.0.0 registers no WATERDROP worker channel; existing native "
            "state is unchanged/export preservation only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="WATERDROP",
        category="DataIntegration",
        kind="generic",
        default_facet=_WATERDROP_RUNTIME_EXCLUSION_FACET,
        facets={_WATERDROP_RUNTIME_EXCLUSION_FACET: membership},
    )
