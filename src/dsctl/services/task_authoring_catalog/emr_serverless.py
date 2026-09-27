from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import validate_emr_json_text
from dsctl.services.task_authoring_catalog.emr import _emr_local_param_values
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

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject

EMR_SERVERLESS_START_JOB_RUN_FACET = "EMR_SERVERLESS/start_job_run"


def _emr_serverless_runtime_guidance() -> str:
    return (
        "Configure worker aws.emr.* credentials and region, or a usable AWS SDK "
        "default credential chain; the application must already be STARTED or "
        "CREATED and the execution role needs EMR Serverless plus job data "
        "permissions. The executor overrides JSON ApplicationId, "
        "ExecutionRoleArn, Name, and ClientToken from the top-level task/runtime "
        "values. Once the callback persists jobRunId through appIds, "
        "failover can resume polling. A failure between submission and "
        "persistence has no durable id and may submit another job on retry."
    )


def _emr_serverless_fields() -> tuple[TaskAuthoringField, ...]:
    literal_description = (
        "This top-level field is sent literally and does not support DS "
        "placeholder substitution."
    )
    return (
        model_field(
            "task_params.applicationId",
            compile_path="taskDefinitionJson[].taskParams.applicationId",
            description=(
                "Nonblank existing EMR Serverless application id. "
                f"{literal_description} {_emr_serverless_runtime_guidance()}"
            ),
        ),
        model_field(
            "task_params.executionRoleArn",
            compile_path="taskDefinitionJson[].taskParams.executionRoleArn",
            description=(
                "Nonblank IAM execution-role ARN. "
                f"{literal_description} The top-level value overrides JSON "
                "ExecutionRoleArn."
            ),
        ),
        model_field(
            "task_params.jobName",
            compile_path="taskDefinitionJson[].taskParams.jobName",
            description=(
                "Optional job name; empty uses the DolphinScheduler task name. "
                f"{literal_description} A nonempty value overrides JSON Name."
            ),
        ),
        model_field(
            "task_params.startJobRunRequestJson",
            compile_path=("taskDefinitionJson[].taskParams.startJobRunRequestJson"),
            description=(
                "Raw AWS StartJobRunRequest JSON object. DS substitutes ${...} "
                "and $[...] before AWS parsing; unresolved runtime placeholders "
                "must stay inside quoted JSON strings, while bound localParams "
                "are validated with their exact raw values. Top-level "
                "applicationId and "
                "executionRoleArn override JSON ApplicationId and "
                "ExecutionRoleArn. The CLI intentionally does not own the AWS DTO."
            ),
        ),
    )


def _emr_serverless_template_body(body: str) -> str:
    guidance = _emr_serverless_runtime_guidance()
    comments = (
        f"# Runtime prerequisite: {guidance}\n"
        "# Override rule: top-level applicationId/executionRoleArn and runtime "
        "Name/ClientToken override those JSON members.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _emr_serverless_templates() -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Submit one raw AWS EMR Serverless StartJobRun request.",
            payload_modes=("task_params",),
            yaml=_emr_serverless_template_body(
                """# Task template for EMR_SERVERLESS
name: emr-serverless-job
type: EMR_SERVERLESS
description: Submit one EMR Serverless Spark job
task_params:
  applicationId: 00fkht2eodujab09
  executionRoleArn: arn:aws:iam::123456789012:role/EMRServerlessExecutionRole
  jobName: dsctl-serverless-job
  startJobRunRequestJson: |-
    {
      "JobDriver": {
        "SparkSubmit": {
          "EntryPoint": "s3://example-bucket/jobs/example.py"
        }
      },
      "ConfigurationOverrides": {}
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
        ),
        TaskAuthoringTemplate(
            name="params",
            purpose="option",
            summary="Submit raw StartJobRun JSON with one DS-bound parameter.",
            payload_modes=("task_params",),
            parameter_fields=("task_params.localParams[]",),
            yaml="""task_params:
  localParams:
  - prop: entry_point
    direct: IN
    type: VARCHAR
    value: s3://example-bucket/jobs/example.py
""",
        ),
    )


def _emr_serverless_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    if profile_version not in {"3.4.2", "3.4.3"}:
        message = f"EMR_SERVERLESS is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=EMR_SERVERLESS_START_JOB_RUN_FACET,
        family="emr-serverless-start-job-run-v1",
        review="emr-serverless-3.4.2-raw-start-job-run",
        params_model=_family_model("EMR_SERVERLESS"),
        fields=_emr_serverless_fields(),
        state_rules=(),
        templates=_emr_serverless_templates(),
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
        task_type="EMR_SERVERLESS",
        category="Cloud",
        kind="typed",
        default_facet=EMR_SERVERLESS_START_JOB_RUN_FACET,
        facets={EMR_SERVERLESS_START_JOB_RUN_FACET: membership},
    )


def validate_semantics(
    task_params: YamlObject,
) -> None:
    """Validate the substituted raw StartJobRun request without owning AWS."""
    request_text = task_params.get("startJobRunRequestJson")
    if not isinstance(request_text, str):
        message = "startJobRunRequestJson must be a string"
        raise TypeError(message)
    validate_emr_json_text(
        request_text,
        field="startJobRunRequestJson",
        allow_unresolved_placeholders=True,
        placeholder_values=_emr_local_param_values(task_params),
        require_quoted_unresolved_placeholders=True,
    )
