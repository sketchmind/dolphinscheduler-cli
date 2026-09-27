from __future__ import annotations

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

PIGEON_TARGET_JOB_FACET = "PIGEON/target_job"


_PIGEON_FIELDS = (
    model_field(
        "task_params.targetJobName",
        compile_path="taskDefinitionJson[].taskParams.targetJobName",
        description=(
            "Non-empty Pigeon/TIS job name sent as appname. The DS worker also "
            "requires the resolved parameter p_host at execution time."
        ),
    ),
)

_PIGEON_TEMPLATES = (
    TaskAuthoringTemplate(
        name="minimal",
        summary="Trigger one named Pigeon/TIS job.",
        payload_modes=("task_params",),
        yaml=task_template_with_runtime_controls(
            """# Task template for PIGEON
# Set p_host in the enclosing workflow.global_params;
# this task does not declare localParams.
# The DS worker must resolve p_host before execution.
name: pigeon-task
type: PIGEON
description: Trigger one Pigeon/TIS job
task_params:
  targetJobName: daily-orders
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
)

_PIGEON_TARGET_JOB_CONTRACT = TaskAuthoringFacetContract(
    facet_id=PIGEON_TARGET_JOB_FACET,
    family="pigeon-target-job-v1",
    review="pigeon-exact-target-job",
    params_model=_family_model("PIGEON"),
    fields=_PIGEON_FIELDS,
    state_rules=(),
    templates=_PIGEON_TEMPLATES,
)


def _pigeon_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=_PIGEON_TARGET_JOB_CONTRACT,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="PIGEON",
        category="Upstream",
        kind="typed",
        default_facet=PIGEON_TARGET_JOB_FACET,
        facets={PIGEON_TARGET_JOB_FACET: membership},
    )
