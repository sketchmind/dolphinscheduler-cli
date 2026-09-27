from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.task_spec import (
    emr_local_param_placeholder_names,
    validate_emr_json_text,
)
from dsctl.upstream.task_authoring_surface import (
    EmrAuthoringSurface,
    get_task_authoring_surface,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject

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

EMR_OPERATION_FACET = "EMR/operation"


def _emr_local_param_values(task_params: YamlObject) -> dict[str, str | None]:
    raw_params = task_params.get("localParams")
    values: dict[str, str | None] = {}
    if not isinstance(raw_params, list):
        return values
    for raw_param in raw_params:
        if not isinstance(raw_param, Mapping):
            continue
        raw_prop = raw_param.get("prop")
        raw_value = raw_param.get("value")
        if isinstance(raw_prop, str) and (
            isinstance(raw_value, str) or raw_value is None
        ):
            values[raw_prop] = raw_value
    return values


def _emr_bound_parameter_placeholders(task_params: YamlObject) -> set[str]:
    placeholders: set[str] = set()
    for field_name in ("jobFlowDefineJson", "stepsDefineJson"):
        value = task_params.get(field_name)
        if isinstance(value, str):
            placeholders.update(emr_local_param_placeholder_names(value))
    return placeholders


def _validate_emr_json_fields(
    task_params: YamlObject,
    *,
    allow_unresolved_placeholders: bool,
    placeholder_values: Mapping[str, str | None] | None = None,
) -> dict[str, YamlObject]:
    parsed_fields: dict[str, YamlObject] = {}
    for field_name in ("jobFlowDefineJson", "stepsDefineJson"):
        value = task_params.get(field_name)
        if isinstance(value, str):
            parsed_fields[field_name] = validate_emr_json_text(
                value,
                field=field_name,
                allow_unresolved_placeholders=allow_unresolved_placeholders,
                placeholder_values=placeholder_values,
            )
    return parsed_fields


def _validate_emr_step_count(
    task_params: YamlObject,
    parsed_fields: Mapping[str, YamlObject],
) -> None:
    if task_params.get("programType") != "ADD_JOB_FLOW_STEPS":
        return
    steps_document = parsed_fields.get("stepsDefineJson", {})
    steps = steps_document.get("Steps")
    if not isinstance(steps, list) or len(steps) != 1:
        message = "stepsDefineJson must contain exactly one Steps entry"
        raise ValueError(message)


def _emr_fields(surface: EmrAuthoringSurface) -> tuple[TaskAuthoringField, ...]:
    substitution = (
        "DS substitutes ${...} and $[...] before AWS parses this raw JSON text."
        if surface.parameter_substitution
        else (
            "DS placeholder substitution is not supported in this release; "
            "the raw JSON text is sent unchanged."
        )
    )
    program_description = (
        "Amazon EMR operation selected by this task. "
        f"{_emr_runtime_prerequisite(surface)}"
    )
    if not surface.native_program_type:
        program_description += (
            " The exact 3.0.x wire omits this discriminator and implies RUN_JOB_FLOW."
        )
    fields = [
        model_field(
            "task_params.programType",
            choices=surface.program_types,
            compile_path=(
                "taskDefinitionJson[].taskParams.programType"
                if surface.native_program_type
                else None
            ),
            description=program_description,
        ),
        model_field(
            "task_params.jobFlowDefineJson",
            required=True,
            active_when="task_params.programType == RUN_JOB_FLOW",
            compile_path="taskDefinitionJson[].taskParams.jobFlowDefineJson",
            description=f"Raw AWS RunJobFlowRequest JSON text. {substitution}",
        ),
    ]
    if "ADD_JOB_FLOW_STEPS" in surface.program_types:
        fields.append(
            model_field(
                "task_params.stepsDefineJson",
                required=True,
                active_when="task_params.programType == ADD_JOB_FLOW_STEPS",
                compile_path="taskDefinitionJson[].taskParams.stepsDefineJson",
                description=(
                    "Raw AWS AddJobFlowStepsRequest JSON text with exactly one "
                    f"Steps entry. {substitution}"
                ),
            )
        )
    return tuple(fields)


def _emr_runtime_prerequisite(surface: EmrAuthoringSurface) -> str:
    keys = ", ".join(surface.credential_keys)
    if surface.credential_source == "aws-authentication":
        configuration = (
            "Configure the DolphinScheduler AWS authentication aws.emr.* section "
            f"before execution ({keys}); static credentials require the access-key "
            "entries, while an instance-profile provider does not."
        )
    else:
        configuration = (
            "Configure the DolphinScheduler worker credentials before execution "
            f"using {keys}."
        )
    failover = (
        " Upstream EMR task failover is not implemented."
        if not surface.failover_supported
        else ""
    )
    return (
        f"{configuration}{failover} RUN_JOB_FLOW creates a new cluster, "
        "not a step on an existing cluster. "
        "WAITING is a successful cluster state and may leave the cluster "
        "running; configure its lifetime and termination deliberately. "
        "ADD_JOB_FLOW_STEPS submits one step to an existing cluster; "
        "retries may repeat side effects."
    )


def _emr_template_body(surface: EmrAuthoringSurface, body: str) -> str:
    guidance = _emr_runtime_prerequisite(surface)
    comments = f"# Runtime prerequisite: {guidance}".replace(
        " Upstream",
        "\n# Upstream",
    )
    return f"{comments}\n{task_template_with_runtime_controls(body)}"


def _emr_state_rules(
    surface: EmrAuthoringSurface,
) -> tuple[TaskAuthoringStateRule, ...]:
    run_compile_policy = (
        (
            "task_params.programType",
            (
                "send RUN_JOB_FLOW"
                if surface.native_program_type
                else "omit; the native task class implies RUN_JOB_FLOW"
            ),
        ),
    )
    rules = [
        TaskAuthoringStateRule(
            when="task_params.programType == RUN_JOB_FLOW",
            condition_paths=("task_params.programType",),
            active_paths=("task_params.jobFlowDefineJson",),
            inactive_paths=("task_params.stepsDefineJson",),
            compile_policy=run_compile_policy,
            description="Create one EMR cluster from RunJobFlowRequest JSON.",
        )
    ]
    if "ADD_JOB_FLOW_STEPS" in surface.program_types:
        rules.append(
            TaskAuthoringStateRule(
                when="task_params.programType == ADD_JOB_FLOW_STEPS",
                condition_paths=("task_params.programType",),
                active_paths=("task_params.stepsDefineJson",),
                inactive_paths=("task_params.jobFlowDefineJson",),
                compile_policy=(
                    ("task_params.programType", "send ADD_JOB_FLOW_STEPS"),
                ),
                description="Add exactly one step to an existing EMR cluster.",
            )
        )
    return tuple(rules)


def _emr_templates(
    surface: EmrAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    templates = [
        TaskAuthoringTemplate(
            name="minimal",
            summary="Create one Amazon EMR job flow from raw AWS request JSON.",
            payload_modes=("task_params",),
            yaml=_emr_template_body(
                surface,
                """# Task template for EMR RUN_JOB_FLOW
name: emr-job-flow
type: EMR
description: Create one Amazon EMR job flow
task_params:
  programType: RUN_JOB_FLOW
  jobFlowDefineJson: |-
    {
      "Name": "dsctl-emr-job",
      "ReleaseLabel": "emr-6.15.0",
      "Instances": {"KeepJobFlowAliveWhenNoSteps": true}
    }
  localParams: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
            ),
        )
    ]
    if "ADD_JOB_FLOW_STEPS" in surface.program_types:
        templates.append(
            TaskAuthoringTemplate(
                name="add-steps",
                summary="Add exactly one step to an existing Amazon EMR cluster.",
                payload_modes=("task_params",),
                yaml=_emr_template_body(
                    surface,
                    """# Task template for EMR ADD_JOB_FLOW_STEPS
name: emr-add-step
type: EMR
description: Add one step to an existing Amazon EMR cluster
task_params:
  programType: ADD_JOB_FLOW_STEPS
  stepsDefineJson: |-
    {
      "JobFlowId": "j-EXAMPLE",
      "Steps": [
        {
          "Name": "echo-ok",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "ok"]}
        }
      ]
    }
  localParams: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
                ),
            )
        )
    if surface.parameter_substitution:
        templates.append(
            TaskAuthoringTemplate(
                name="params",
                purpose="option",
                summary="RunJobFlow JSON with one DS-substituted IN parameter.",
                payload_modes=("task_params",),
                parameter_fields=("task_params.localParams[]",),
                yaml="""task_params:
  localParams:
  - prop: cluster_name
    direct: IN
    type: VARCHAR
    value: nightly-cluster
""",
            )
        )
    return tuple(templates)


def _emr_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).emr
    if not surface.available:
        message = f"EMR is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=EMR_OPERATION_FACET,
        family="emr-operation-v1",
        review="emr-exact-program-mode-json",
        params_model=_family_model("EMR"),
        fields=_emr_fields(surface),
        state_rules=_emr_state_rules(surface),
        templates=_emr_templates(surface),
        parameter_data_types=("VARCHAR",),
        parameter_directions=("IN",),
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
        task_type="EMR",
        category="Cloud",
        kind="typed",
        default_facet=EMR_OPERATION_FACET,
        facets={EMR_OPERATION_FACET: membership},
    )


def validate_semantics(
    task_params: YamlObject,
    *,
    version: str,
    surface: EmrAuthoringSurface,
) -> None:
    """Validate the exact literal or substitution-capable EMR JSON epoch."""
    local_values = _emr_local_param_values(task_params)
    if surface.parameter_substitution:
        parsed_fields = _validate_emr_json_fields(
            task_params,
            allow_unresolved_placeholders=True,
            placeholder_values=local_values,
        )
        _validate_emr_step_count(task_params, parsed_fields)
        return
    bound_placeholders = sorted(
        _emr_bound_parameter_placeholders(task_params).intersection(local_values)
    )
    if bound_placeholders:
        message = (
            "EMR JSON placeholder substitution is unsupported for "
            f"DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "EMR",
                "field": "tasks[].task_params.*DefineJson",
                "placeholders": bound_placeholders,
                "reason": "upstream_capability_absent",
            },
            suggestion=(
                "Remove matching localParams and send the text literally, or use "
                "DolphinScheduler 3.2.2 or newer for DS placeholder substitution."
            ),
        )
    parsed_fields = _validate_emr_json_fields(
        task_params,
        allow_unresolved_placeholders=False,
    )
    _validate_emr_step_count(task_params, parsed_fields)
