from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.upstream.task_authoring_surface import (
        SagemakerAuthoringSurface,
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

SAGEMAKER_START_PIPELINE_EXECUTION_FACET = "SAGEMAKER/start_pipeline_execution"


def validate_semantics(
    task_params: YamlObject,
    *,
    version: str,
    surface: SagemakerAuthoringSurface,
) -> None:
    """Require the canonical datasource reference only on its exact epochs."""
    if surface.datasource_required and task_params.get("datasource") is None:
        message = (
            "SAGEMAKER task_params.datasource requires one positive integer id "
            f"or nonblank exact name on DolphinScheduler {version}."
        )
        raise ValueError(message)


def _sagemaker_runtime_guidance(surface: SagemakerAuthoringSurface) -> str:
    """Describe exact SageMaker request, credential, and recovery behavior."""
    if surface.exclusion_reason is not None:
        return f"SAGEMAKER typed authoring is disabled: {surface.exclusion_reason}."
    if not surface.available:
        return "SAGEMAKER is absent from this exact DolphinScheduler profile."
    if (
        surface.execution_epoch is None
        or surface.credential_source is None
        or surface.raw_request_model != "StartPipelineExecutionRequest"
        or surface.raw_request_property_naming != "UpperCamelCase"
        or surface.local_param_forwarding is None
        or surface.poll_interval_ms != 5000
        or surface.polling_statuses != ("Executing",)
        or surface.success_statuses != ("Succeeded",)
    ):
        message = "Available SAGEMAKER surface lacks its exact runtime contract"
        raise ValueError(message)

    credentials = ", ".join(surface.credential_keys)
    if not surface.datasource_required:
        datasource = (
            "This epoch has no datasource field; eligible workers need "
            f"{credentials} ({surface.credential_source}). "
        )
    elif surface.datasource_credentials_used:
        datasource = (
            "A positive SAGEMAKER datasource is required; the compiler owns "
            "native type=SAGEMAKER, and the worker uses that datasource's "
            "userName, password, and awsRegion through a static AWS provider. "
            "Initialization INFO-logs the resolved task parameters, including "
            "those datasource credential values. "
        )
    else:
        providers = ", ".join(surface.credential_provider_types)
        datasource = (
            "A positive SAGEMAKER datasource and compiler-owned type=SAGEMAKER "
            "remain required and are initialized. The resolved datasource "
            "credentials are INFO-logged but then ignored by the AWS client, "
            f"which instead reads {credentials} ({surface.credential_source}; "
            f"{providers}, with optional endpoint). "
        )

    if surface.application_id_persistence == "appIds-callback":
        recovery = (
            "After StartPipelineExecution returns, an appIds callback stores "
            "pipelineExecutionArn plus clientRequestToken for failover polling and "
            "cancel. Failure before callback persistence, or retry without appIds, "
            "can start another execution. "
        )
    else:
        recovery = (
            "The legacy synchronous worker calls setAppIds with a null ARN before "
            "StartPipelineExecution, never corrects it after submit, and therefore "
            "has no durable application id or failover resume. Cancel depends on "
            "the same in-memory worker, and retry can start another execution. "
        )

    if surface.local_param_forwarding == "input-only":
        parameter_forwarding = (
            "IN and OUT localParams are both authorable, but this epoch builds the "
            "prepared map from IN declarations only. An OUT declaration therefore "
            "does not participate in request substitution, and SAGEMAKER never "
            "publishes it as output. "
        )
    else:
        parameter_forwarding = (
            "IN and OUT localParams are both authorable and this epoch places both "
            "directions in the prepared map, so an OUT value can replace request "
            "text. SAGEMAKER still never handles or publishes output, so OUT is not "
            "an output channel. "
        )

    return (
        "The public field is preserved raw AWS StartPipelineExecutionRequest JSON. "
        "The reviewed known shape is PipelineName, PipelineExecutionDisplayName, "
        "PipelineParameters entries with Name and Value, "
        "PipelineExecutionDescription, ClientRequestToken, and "
        "ParallelismConfiguration.MaxParallelExecutionSteps. Jackson recognizes "
        "those exact UpperCamelCase properties; unknown or incorrectly cased fields "
        "are ignored rather than forwarded to AWS. Its mapper is configured to map "
        "unknown enum values to null, but the reviewed public "
        "StartPipelineExecutionRequest "
        "shape has no enum member, so that setting is inert here. DolphinScheduler "
        "substitutes ${...} "
        "and $[...] across the whole JSON text before parsing and performs no JSON "
        "escaping, so every substituted value must already be safe in its exact JSON "
        "position. Author an explicit, stable ClientRequestToken as the caller's "
        "idempotency key for one logical submission, reused on its retries "
        "and changed for a new intended execution; when omitted, an "
        "SDK-generated token may be transient and "
        "the plugin's subsequently read or persisted token may remain null. Resources "
        "and structured output are unsupported. Upstream "
        "INFO-logs task parameters, the resolved request, pipeline identifiers, "
        "statuses, and up to 100 reversed pipeline steps. Task fields are not secret "
        f"storage. {parameter_forwarding}{datasource}Polling sleeps 5000 ms only "
        "while status is Executing; "
        "only Succeeded is success, and every other observed status fails. There is "
        "no internal deadline and the AWS client is not closed. "
        f"{recovery}The CLI timeout remains an outer orchestration control."
    )


def _sagemaker_fields(
    profile_version: str,
    surface: SagemakerAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    """Return raw request plus exact-profile parameter and datasource fields."""
    execution_epoch = surface.execution_epoch
    if execution_epoch is None:
        message = "Available SAGEMAKER surface lacks its execution epoch"
        raise ValueError(message)
    if surface.local_param_forwarding == "input-only":
        request_forwarding = (
            "Only IN localParams enter request substitution on this exact profile; "
            "OUT remains authorable but is neither substituted nor published."
        )
        direction_description = (
            "IN and OUT are authorable, but only IN enters request substitution on "
            "this exact profile; OUT is not published."
        )
    elif surface.local_param_forwarding == "all-local-params":
        request_forwarding = (
            "IN and OUT localParams both enter request substitution on this exact "
            "profile, but SAGEMAKER never publishes OUT values."
        )
        direction_description = (
            "IN and OUT both enter request substitution on this exact profile, but "
            "SAGEMAKER does not publish OUT values."
        )
    else:
        message = "Available SAGEMAKER surface lacks local-parameter forwarding"
        raise ValueError(message)
    credentials = ", ".join(surface.credential_keys)
    if not surface.datasource_required:
        credential_guidance = (
            f"The {execution_epoch.replace('-', ' ')} uses {credentials}."
        )
    elif surface.datasource_credentials_used:
        credential_guidance = (
            "A positive SAGEMAKER datasource supplies userName, password, and "
            "awsRegion; initialization INFO-logs those values."
        )
    else:
        credential_guidance = (
            "A positive SAGEMAKER datasource remains required and INFO-logged but "
            f"is ignored by the AWS client, which uses {credentials} with an "
            "optional endpoint."
        )
    recovery_guidance = (
        "An appIds callback enables post-callback tracking and cancel; retry or a "
        "pre-callback failure can submit again."
        if surface.application_id_persistence == "appIds-callback"
        else (
            "The legacy synchronous worker has no durable application id or "
            "failover resume, and retry can submit again."
        )
    )
    request_guidance = (
        "Preserved raw StartPipelineExecutionRequest JSON object. Known exact "
        "UpperCamelCase fields are PipelineName, PipelineExecutionDisplayName, "
        "PipelineParameters[{Name,Value}], PipelineExecutionDescription, "
        "ClientRequestToken, and "
        "ParallelismConfiguration.MaxParallelExecutionSteps. Unknown or "
        "incorrectly cased fields are ignored, not passed through. DS substitutes "
        "${...} and $[...] across the whole JSON text and performs no JSON escaping; "
        "values must already be safe at their JSON position. "
        f"{request_forwarding} Author an "
        "explicit stable ClientRequestToken per logical submission, reused "
        "for retries and changed for the next intended execution. Upstream "
        "INFO-logs "
        "authored and resolved request data; fields are not secret storage. Polling "
        "sleeps 5000 ms only while status is Executing, and only Succeeded is success. "
        "There is no internal deadline, the AWS client is not closed, and structured "
        f"output are unsupported. {credential_guidance} {recovery_guidance}"
    )
    parameter_types = tuple(
        sorted(get_parameter_semantics(profile_version).allowed_property_types)
    )
    fields: list[TaskAuthoringField] = [
        model_field(
            "task_params.sagemakerRequestJson",
            compile_path="taskDefinitionJson[].taskParams.sagemakerRequestJson",
            description=request_guidance,
        ),
        model_field(
            "task_params.localParams",
            default=[],
            compile_path="taskDefinitionJson[].taskParams.localParams",
            description=(
                "Ordered unique DolphinScheduler parameter declarations used for "
                "whole-text request substitution."
            ),
        ),
        model_field(
            "task_params.localParams[]",
            compile_path="taskDefinitionJson[].taskParams.localParams",
            description="One exact-profile DolphinScheduler Property declaration.",
        ),
        model_field(
            "task_params.localParams[].prop",
            compile_path="taskDefinitionJson[].taskParams.localParams[].prop",
            description="Unique nonblank parameter name referenced by placeholders.",
        ),
        model_field(
            "task_params.localParams[].direct",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.localParams[].direct",
            description=direction_description,
        ),
        model_field(
            "task_params.localParams[].type",
            model_default=True,
            choices=parameter_types,
            choice_source="dsctl enum list data-type",
            related_commands=("dsctl enum list data-type",),
            compile_path="taskDefinitionJson[].taskParams.localParams[].type",
            description="Exact Property data type accepted by this profile.",
        ),
        model_field(
            "task_params.localParams[].value",
            compile_path="taskDefinitionJson[].taskParams.localParams[].value",
            description=(
                "Optional string value. Substitution is textual and does not JSON-"
                "escape this value."
            ),
        ),
    ]
    if surface.datasource_required:
        fields.append(
            model_field(
                "task_params.datasource",
                "integer|string",
                required=True,
                choices=(),
                choice_source="dsctl datasource list",
                related_commands=(
                    "dsctl datasource list",
                    "dsctl datasource get DATASOURCE",
                    "dsctl datasource test DATASOURCE",
                ),
                compile_path="taskDefinitionJson[].taskParams.datasource",
                description=(
                    "Positive SAGEMAKER datasource id or exact name required by "
                    "this exact wire. "
                    "The compiler separately owns native type=SAGEMAKER."
                ),
            )
        )
    return tuple(fields)


def _sagemaker_state_rules(
    surface: SagemakerAuthoringSurface,
) -> tuple[TaskAuthoringStateRule, ...]:
    active_paths = [
        "task_params.sagemakerRequestJson",
        "task_params.localParams",
    ]
    type_policy = "omit; native parameter model has no datasource type"
    if surface.datasource_required:
        active_paths.append("task_params.datasource")
        type_policy = "send compiler-owned SAGEMAKER"
    return (
        TaskAuthoringStateRule(
            when="typed SAGEMAKER StartPipelineExecution authoring",
            condition_paths=(),
            active_paths=tuple(active_paths),
            compile_policy=(
                ("task_params.type", type_policy),
                ("task_params.resourceList", "send []"),
                ("task_params.localParams", "send [] when absent"),
            ),
            description=(
                "The compiler projects one exact native SageMaker parameter wire; "
                "credentials and runtime application ids are never authored."
            ),
        ),
    )


def _sagemaker_templates(
    surface: SagemakerAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _sagemaker_runtime_guidance(surface)
    datasource = "  datasource: 1\n" if surface.datasource_required else ""
    comments = (
        f"# Runtime prerequisite: {guidance}\n"
        '# Optional request member: "ClientRequestToken": "LOGICAL-RUN-TOKEN"\n'
        "# Replace the token for a new logical run; reuse it only for retries.\n"
        "# Preservation: inherited/future native state is unchanged/export opaque "
        "preservation only; raw opaque create/edit is closed.\n"
    )
    minimal = task_template_with_runtime_controls(
        """# Task template for one AWS SageMaker pipeline execution
name: start-sagemaker-pipeline
type: SAGEMAKER
description: Start one existing SageMaker pipeline
task_params:
  sagemakerRequestJson: |-
    {
      "PipelineName": "nightly-training"
    }
__SAGEMAKER_DATASOURCE__  localParams: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
""".replace("__SAGEMAKER_DATASOURCE__", datasource)
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Start one existing SageMaker pipeline from raw AWS request JSON.",
            payload_modes=("task_params",),
            yaml=f"{comments}{minimal}",
        ),
        TaskAuthoringTemplate(
            name="params",
            purpose="option",
            summary="Start a SageMaker pipeline after whole-text DS substitution.",
            payload_modes=("task_params",),
            parameter_fields=("task_params.localParams[]",),
            yaml="""task_params:
  localParams:
  - prop: training_job_name
    direct: IN
    type: VARCHAR
    value: nightly-training
""",
        ),
    )


def _sagemaker_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).sagemaker
    if surface.exclusion_reason is not None:
        if (
            profile_version not in {"3.1.1", "3.1.2"}
            or surface.available
            or surface.exclusion_reason != "pipeline-polling-status-never-refreshed"
        ):
            message = f"SAGEMAKER {profile_version} is not the reviewed runtime hole"
            raise ValueError(message)
        facet = "SAGEMAKER/runtime_exclusion"
        membership = TaskAuthoringFacetMembership(
            profile_version=profile_version,
            contract=TaskAuthoringFacetContract(
                facet_id=facet,
                family="sagemaker-runtime-exclusion-v1",
                review="sagemaker-polling-status-runtime-exclusion",
                params_model=None,
                fields=(),
                state_rules=(),
                templates=(),
            ),
            typed_create=False,
            typed_edit=False,
            opaque_create=False,
            opaque_edit=False,
            opaque_preserve=True,
            constraint=(
                f"Exact {profile_version} SAGEMAKER never refreshes the local "
                "polling status; an initially Executing pipeline cannot finish "
                "normally. Existing state is unchanged/export preservation only."
            ),
        )
        return TaskTypeAuthoringProfile(
            task_type="SAGEMAKER",
            category="MachineLearning",
            kind="generic",
            default_facet=facet,
            facets={facet: membership},
        )
    if not surface.available:
        message = f"SAGEMAKER is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    parameter_types = tuple(
        sorted(get_parameter_semantics(profile_version).allowed_property_types)
    )
    contract = TaskAuthoringFacetContract(
        facet_id=SAGEMAKER_START_PIPELINE_EXECUTION_FACET,
        family="sagemaker-start-pipeline-execution-v1",
        review="sagemaker-start-pipeline-execution-exact-public-template",
        params_model=_family_model("SAGEMAKER"),
        fields=_sagemaker_fields(profile_version, surface),
        state_rules=_sagemaker_state_rules(surface),
        templates=_sagemaker_templates(surface),
        parameter_data_types=parameter_types,
        parameter_directions=("IN", "OUT"),
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
        task_type="SAGEMAKER",
        category="MachineLearning",
        kind="typed",
        default_facet=SAGEMAKER_START_PIPELINE_EXECUTION_FACET,
        facets={SAGEMAKER_START_PIPELINE_EXECUTION_FACET: membership},
    )
