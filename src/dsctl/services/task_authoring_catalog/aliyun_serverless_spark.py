from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        AliyunServerlessSparkAuthoringSurface,
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

ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET = (
    "ALIYUN_SERVERLESS_SPARK/literal_jar_submit"
)


def _aliyun_serverless_spark_runtime_guidance(
    surface: AliyunServerlessSparkAuthoringSurface,
) -> str:
    """Describe exact Aliyun template, retry, trust, and recovery behavior."""
    if not surface.available or surface.runtime_epoch is None:
        return (
            "ALIYUN_SERVERLESS_SPARK is absent from this exact "
            "DolphinScheduler profile."
        )
    if (
        surface.credential_source != "datasource"
        or not surface.template_fetch_always
        or not surface.template_may_mutate_submit_parameters
        or not surface.template_may_derive_release_and_fusion
        or not surface.task_params_logged
        or surface.retry_attempts != 11
        or surface.retry_interval_ms != 1000
        or surface.poll_interval_seconds != 10
        or surface.default_endpoint_template is None
    ):
        message = (
            "Available ALIYUN_SERVERLESS_SPARK surface lacks its exact runtime contract"
        )
        raise ValueError(message)
    credentials = ", ".join(surface.credential_keys)
    common = (
        "Configure one Aliyun ALIYUN_SERVERLESS_SPARK datasource with "
        f"{credentials}; it may use a custom endpoint or the default endpoint "
        f"template {surface.default_endpoint_template}. Datasource credential "
        "and connectivity validity are not verified by typed authoring. The "
        "executor always performs a template fetch. Returned template "
        "configuration may mutate the Spark submit parameters and, because the "
        "typed facet omits engineReleaseVersion, may derive display release and "
        "fusion values for the SDK request. Upstream logs the complete task "
        "params, jobRunId, and state at INFO. Authored fields are not secret "
        "storage, and the CLI does not detect or redact secrets; keep secrets out "
        "of every typed field. Datasource-managed accessKeyId/accessKeySecret are "
        "not task params, and task code does not explicitly log them through task "
        "params. There is no result output, no "
        "durable callback-backed application id, and no failover resume. Retry "
        "may submit a duplicate remote job. Cancel requires the in-memory "
        "jobRunId. Status polling waits 10 seconds and has no internal deadline. "
        "This facet refreshes no live evidence and makes no profile promotion. "
    )
    if surface.runtime_epoch == "legacy-single-submit":
        exact = (
            "Exact 3.3.1 status poll calls can retry for up to 11 attempts, but "
            "template fetch and start are not retried. Cancel failure is logged "
            "and swallowed. A Failed remote state maps to KILL. The executor does "
            "not set a client token."
        )
    elif surface.runtime_epoch == "retry-client-token":
        exact = (
            "Template, start, status, and cancel calls can retry for up to 11 "
            "attempts with a 1000 ms retry interval. One client token is reused "
            "within one DS attempt; a DS retry creates a new token and can "
            "duplicate submission. The start and cancel exception cause is not "
            "preserved."
        )
    else:
        exact = (
            "Template, start, status, and cancel calls can retry for up to 11 "
            "attempts with a 1000 ms retry interval. One client token is reused "
            "within one DS attempt; a DS retry creates a new token and can "
            "duplicate submission. The start and cancel exception cause is "
            "preserved."
        )
    return common + exact


def _aliyun_serverless_spark_fields(
    surface: AliyunServerlessSparkAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    runtime = _aliyun_serverless_spark_runtime_guidance(surface)
    return (
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
                "Positive ALIYUN_SERVERLESS_SPARK datasource id or exact name. "
                + runtime
            ),
        ),
        model_field(
            "task_params.workspaceId",
            compile_path="taskDefinitionJson[].taskParams.workspaceId",
            description=(
                "Literal Aliyun Serverless Spark workspace identity. INFO-logged; "
                "not secret storage, and the CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.resourceQueueId",
            compile_path="taskDefinitionJson[].taskParams.resourceQueueId",
            description=(
                "Literal resource queue identity in the selected workspace. "
                "INFO-logged; not secret storage, and the CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.jobName",
            compile_path="taskDefinitionJson[].taskParams.jobName",
            description=(
                "Literal non-secret remote job name. INFO-logged; not secret "
                "storage, and the CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.entryPoint",
            compile_path="taskDefinitionJson[].taskParams.entryPoint",
            description=(
                "Safe absolute oss:// object URI used as the JAR entry point. "
                "INFO-logged; not secret storage, and the CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.entryPointArguments",
            compile_path="taskDefinitionJson[].taskParams.entryPointArguments",
            description=(
                "Non-empty literal argument list projected to the exact native "
                "hash-delimited string. INFO-logged; not secret storage, and the "
                "CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.entryPointArguments[]",
            required=False,
            compile_path="taskDefinitionJson[].taskParams.entryPointArguments[]",
            description=(
                "One literal argument without the native # delimiter or DS "
                "placeholder syntax. INFO-logged; not secret storage, and the CLI "
                "does not redact it."
            ),
        ),
        model_field(
            "task_params.sparkSubmitParameters",
            compile_path="taskDefinitionJson[].taskParams.sparkSubmitParameters",
            description=(
                "Literal Spark submit parameters sent to Aliyun. INFO-logged; not "
                "secret storage, and the CLI does not redact it."
            ),
        ),
        model_field(
            "task_params.isProduction",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.isProduction",
            description=(
                "Strict environment flag; false selects the upstream development "
                "environment."
            ),
        ),
    )


def _aliyun_serverless_spark_templates(
    surface: AliyunServerlessSparkAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _aliyun_serverless_spark_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Submit one literal JAR to Aliyun EMR Serverless Spark.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope: exactly the eight task_params fields below. "
                "codeType=JAR and type=ALIYUN_SERVERLESS_SPARK are fixed by the "
                "projector. Template, engine release, inherited parameter, "
                "resource, and future state remain unchanged/export preservation "
                "only.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one literal Aliyun Serverless Spark JAR
name: submit-aliyun-serverless-spark-jar
type: ALIYUN_SERVERLESS_SPARK
description: Submit one literal JAR to Aliyun EMR Serverless Spark
task_params:
  datasource: 17
  workspaceId: w-analytics-prod
  resourceQueueId: root.analytics
  jobName: daily-orders
  entryPoint: oss://analytics-jobs/jars/orders.jar
  entryPointArguments:
    - --date
    - "2026-08-20"
  sparkSubmitParameters: >-
    --class com.example.Orders --conf spark.sql.shuffle.partitions=64
  isProduction: false
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


def _is_reviewed_aliyun_serverless_spark_opaque_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize only exact native PYTHON/SQL modes for explicit opaque work."""
    return task_params.get("type") == "ALIYUN_SERVERLESS_SPARK" and task_params.get(
        "codeType"
    ) in {"PYTHON", "SQL"}


def _aliyun_serverless_spark_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).aliyun_serverless_spark
    if not surface.available:
        message = (
            f"ALIYUN_SERVERLESS_SPARK is absent from DolphinScheduler {profile_version}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET,
        family="aliyun-serverless-spark-literal-jar-v1",
        review="aliyun-serverless-spark-literal-jar-exact-subset",
        params_model=_family_model("ALIYUN_SERVERLESS_SPARK"),
        fields=_aliyun_serverless_spark_fields(surface),
        state_rules=(),
        templates=_aliyun_serverless_spark_templates(surface),
        opaque_authoring_selector=_is_reviewed_aliyun_serverless_spark_opaque_mode,
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
        task_type="ALIYUN_SERVERLESS_SPARK",
        category="Cloud",
        kind="typed",
        default_facet=ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET,
        facets={ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET: membership},
    )
