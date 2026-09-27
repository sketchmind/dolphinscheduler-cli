from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    DatasyncTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        DatasyncAuthoringSurface,
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
)

DATASYNC_CREATE_AND_EXECUTE_FACET = "DATASYNC/create_and_execute"


def _datasync_runtime_guidance(surface: DatasyncAuthoringSurface) -> str:
    """Describe the exact public modes, credentials, defects, and lifecycle."""
    if not surface.available:
        return "DATASYNC is absent from this exact DolphinScheduler profile."
    if (
        surface.wire_epoch != "create-and-execute-normal-and-raw-json"
        or surface.credential_source is None
        or surface.raw_json_model != "DatasyncParameters"
        or surface.application_id_field != "taskExecutionArn"
        or surface.application_id_persistence != "appIds-callback"
    ):
        message = "Available DATASYNC surface lacks its exact runtime contract"
        raise ValueError(message)
    credentials = ", ".join(surface.credential_keys)
    return (
        "Normal fixes jsonFormat=false and sends name, "
        "sourceLocationArn, destinationLocationArn, and optional "
        "cloudWatchLogGroupArn. Raw JSON needs jsonFormat=true. Worker maps "
        "known UpperCamelCase DatasyncParameters; unknown fields are ignored. Only "
        "unknown enum values in inherited LocalParams/VarPool Property Direct/Type "
        "become null. Enum-like strings are not enum-validated or null-converted; "
        "FilterType reaches SDK unchanged for AWS validation. "
        "This is not arbitrary AWS CreateTask JSON. The whole Options copy is "
        "ineffective. Includes overwrite Excludes; Schedule leaves a recurring AWS "
        "Task. localParams are runtime-dead; placeholders and resources are "
        "unsupported, with no structured output. Upstream INFO-logs task params and "
        "converted task params. Fields are not secret storage; dsctl does not detect "
        "or redact secrets. Workers need "
        f"{credentials} ({surface.credential_source}). A fresh run calls CreateTask "
        "then StartTaskExecution, does not delete the Task, and does not close the "
        "client. A callback stores taskExecutionArn in appIds for failover resume "
        "and cancel. Pre-callback failure or retry without appIds can duplicate and "
        "leak Tasks. Polling has no deadline. The upstream UI uses the same UI model "
        "for outer task name and inner name on create/edit; REST can keep them "
        "distinct."
    )


def _datasync_fields(
    surface: DatasyncAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    guidance = _datasync_runtime_guidance(surface)
    return (
        TaskAuthoringField(
            "task_params.jsonFormat",
            "boolean",
            required=False,
            default=False,
            compile_path="taskDefinitionJson[].taskParams.jsonFormat",
            description=(
                "Strict public-mode selector: false selects normal fields and "
                f"true selects raw JSON. {guidance}"
            ),
        ),
        TaskAuthoringField(
            "task_params.name",
            "string",
            required=True,
            active_when="task_params.jsonFormat == false",
            compile_path="taskDefinitionJson[].taskParams.name",
            description=(
                "Nonblank literal AWS DataSync task name used by normal mode. "
                "INFO-logged and not secret storage."
            ),
        ),
        TaskAuthoringField(
            "task_params.sourceLocationArn",
            "string",
            required=True,
            active_when="task_params.jsonFormat == false",
            compile_path="taskDefinitionJson[].taskParams.sourceLocationArn",
            description="Nonblank literal source location ARN for normal mode.",
        ),
        TaskAuthoringField(
            "task_params.destinationLocationArn",
            "string",
            required=True,
            active_when="task_params.jsonFormat == false",
            compile_path="taskDefinitionJson[].taskParams.destinationLocationArn",
            description="Nonblank literal destination location ARN for normal mode.",
        ),
        TaskAuthoringField(
            "task_params.cloudWatchLogGroupArn",
            "string",
            required=False,
            active_when="task_params.jsonFormat == false",
            compile_path="taskDefinitionJson[].taskParams.cloudWatchLogGroupArn",
            description="Optional nonblank literal CloudWatch log-group ARN.",
        ),
        TaskAuthoringField(
            "task_params.json",
            "string",
            required=True,
            active_when="task_params.jsonFormat == true",
            compile_path="taskDefinitionJson[].taskParams.json",
            description=(
                "Nonblank syntactically valid JSON object preserved byte-for-byte "
                "for the explicit raw JSON public mode."
            ),
        ),
    )


_DATASYNC_STATE_RULES = (
    TaskAuthoringStateRule(
        when="task_params.jsonFormat == false",
        condition_paths=("task_params.jsonFormat",),
        active_paths=(
            "task_params.name",
            "task_params.sourceLocationArn",
            "task_params.destinationLocationArn",
            "task_params.cloudWatchLogGroupArn",
        ),
        inactive_paths=("task_params.json",),
        compile_policy=(("task_params.jsonFormat", "literal false"),),
        description="Normal public mode owns exactly the four normal fields.",
    ),
    TaskAuthoringStateRule(
        when="task_params.jsonFormat == true",
        condition_paths=("task_params.jsonFormat",),
        active_paths=("task_params.json",),
        inactive_paths=(
            "task_params.name",
            "task_params.sourceLocationArn",
            "task_params.destinationLocationArn",
            "task_params.cloudWatchLogGroupArn",
        ),
        compile_policy=(("task_params.jsonFormat", "literal true"),),
        description="Raw JSON public mode owns only its explicit wrapper.",
    ),
)


def _datasync_templates(
    surface: DatasyncAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _datasync_runtime_guidance(surface)
    comments = (
        f"# Runtime prerequisite: {guidance}\n"
        "# Preservation: richer native state remains unchanged/export opaque "
        "preservation only.\n"
    )
    normal = task_template_with_runtime_controls(
        """# Task template for one normal AWS DataSync create-and-execute run
name: nightly-transfer
type: DATASYNC
description: Create and execute one AWS DataSync task
task_params:
  jsonFormat: false
  name: nightly-transfer
  sourceLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/source
  destinationLocationArn: arn:aws:datasync:cn-north-1:123456789012:location/destination
  cloudWatchLogGroupArn: arn:aws:logs:cn-north-1:123456789012:log-group:datasync
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
"""
    )
    raw_json = task_template_with_runtime_controls(
        """# Task template for selector-restricted opaque raw JSON DataSync authoring
# This is not the normal typed field contract or arbitrary AWS passthrough.
name: raw-datasync
type: DATASYNC
description: Create and execute from known DatasyncParameters JSON
task_params:
  jsonFormat: true
  json: |
    {
      "Name": "raw-transfer",
      "SourceLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/source",
      "DestinationLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/dst"
    }
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
"""
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Create and execute one DataSync task from normal fields.",
            payload_modes=("task_params",),
            yaml=f"{comments}{normal}",
        ),
        TaskAuthoringTemplate(
            name="raw-json",
            summary="Create and execute from the explicit raw JSON public mode.",
            payload_modes=("task_params",),
            yaml=f"{comments}{raw_json}",
        ),
    )


def _is_reviewed_datasync_raw_json_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Select only one fully valid explicit raw-JSON public wrapper."""
    try:
        validated = DatasyncTaskParamsSpec.model_validate(task_params)
    except (TypeError, ValueError):
        return False
    return validated.json_format is True


def _datasync_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).datasync
    if not surface.available:
        message = f"DATASYNC is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DATASYNC_CREATE_AND_EXECUTE_FACET,
        family="datasync-create-and-execute-v1",
        review="datasync-normal-and-raw-json-exact-public-modes",
        params_model=_family_model("DATASYNC"),
        fields=_datasync_fields(surface),
        state_rules=_DATASYNC_STATE_RULES,
        templates=_datasync_templates(surface),
        opaque_authoring_selector=_is_reviewed_datasync_raw_json_mode,
        restrict_opaque_authoring_to_selector=True,
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="DATASYNC",
        category="Other",
        kind="typed",
        default_facet=DATASYNC_CREATE_AND_EXECUTE_FACET,
        facets={DATASYNC_CREATE_AND_EXECUTE_FACET: membership},
    )
