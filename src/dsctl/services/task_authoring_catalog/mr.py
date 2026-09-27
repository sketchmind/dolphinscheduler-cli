from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue

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

MR_LITERAL_JAVA_JAR_JOB_FACET = "MR/literal_java_jar_job"


def _mr_runtime_guidance(profile_version: str) -> str:
    """Describe the closed Java MapReduce JAR subset and its exact binding."""
    surface = get_task_authoring_surface(profile_version).mr
    resource_wire = (
        "The compiler resolves mainJar fullName to a positive resource id before "
        "writing the legacy mainJar object. "
        if surface.wire_epoch != "resource-name"
        else ("The compiler writes mainJar fullName as ResourceInfo.resourceName. ")
    )
    return (
        "Eligible workers need a compatible hadoop CLI on PATH, Hadoop/YARN "
        "configuration, tenant execution permission, readable DS resource "
        "storage, and queue plus data access. "
        + resource_wire
        + "The compiler fixes programType=JAVA, appName='', others='', "
        "localParams=[], and resourceList=[]; MapReduceParameters adds mainJar "
        "to its runtime resource list. mainClass and each mainArgs item are "
        "restricted to literal unquoted shell-safe tokens, and mainArgs are "
        "joined with one space. Queue selection remains runtime-owned. Upstream "
        "INFO-logs the complete task params, final command, command file, and "
        "child output; typed fields are not secret storage. The facet publishes "
        "no MR-owned DS output, cannot reattach after master or worker failover, "
        "and retry/failover can rerun the whole JAR with duplicate side effects. "
        "No live evidence is refreshed and no profile is promoted."
    )


def _mr_fields(profile_version: str) -> tuple[TaskAuthoringField, ...]:
    guidance = _mr_runtime_guidance(profile_version)
    return (
        model_field(
            "task_params.mainJar",
            choice_source="dsctl resource list",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.mainJar",
            description=(
                "Absolute shell-safe DS resource fullName ending in .jar. " + guidance
            ),
        ),
        model_field(
            "task_params.mainClass",
            compile_path="taskDefinitionJson[].taskParams.mainClass",
            description=(
                "Literal ASCII Java entry class written to the upstream unquoted "
                "mainClass slot."
            ),
        ),
        model_field(
            "task_params.mainArgs",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.mainArgs",
            description="Ordered application-argument tokens joined with one space.",
        ),
        model_field(
            "task_params.mainArgs[]",
            compile_path="taskDefinitionJson[].taskParams.mainArgs",
            description=(
                "One nonblank unquoted shell-safe token without whitespace, shell "
                "expansion, or DolphinScheduler placeholders."
            ),
        ),
    )


def _mr_templates(profile_version: str) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _mr_runtime_guidance(profile_version)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal Java MapReduce JAR job.",
            payload_modes=("task_params",),
            resource_fields=("task_params.mainJar",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope: one Java JAR, entry class, and shell-safe "
                "application arguments.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one literal Java MapReduce JAR job
name: run-mapreduce-jar
type: MR
description: Run one Java MapReduce JAR job
task_params:
  mainJar: /jobs/wordcount.jar
  mainClass: com.example.WordCount
  mainArgs:
    - hdfs:///input
    - hdfs:///output
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


def _is_reviewed_mr_opaque_mode(task_params: Mapping[str, YamlValue]) -> bool:
    """Allow only the executable alternate native MR program mode opaquely."""
    return task_params.get("programType") == "SCALA"


def _mr_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    contract = TaskAuthoringFacetContract(
        facet_id=MR_LITERAL_JAVA_JAR_JOB_FACET,
        family="mr-literal-java-jar-job-v1",
        review="mr-literal-java-jar-shell-safe-subset",
        params_model=_family_model("MR"),
        fields=_mr_fields(profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed MR literal Java JAR authoring",
                condition_paths=(),
                active_paths=(
                    "task_params.mainJar",
                    "task_params.mainClass",
                    "task_params.mainArgs",
                ),
                compile_policy=(
                    (
                        "task_params.mainJar",
                        "bind exact positive id before 3.2.0; send resourceName later",
                    ),
                    ("task_params.programType", "send compiler-owned JAVA"),
                    ("task_params.appName", "send compiler-owned empty string"),
                    ("task_params.others", "send compiler-owned empty string"),
                    ("task_params.localParams", "send compiler-owned empty list"),
                    ("task_params.resourceList", "send compiler-owned empty list"),
                ),
                description=(
                    "The portable facet owns one Java MapReduce JAR, entry class, "
                    "and literal application arguments only."
                ),
            ),
        ),
        templates=_mr_templates(profile_version),
        opaque_authoring_selector=_is_reviewed_mr_opaque_mode,
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
        task_type="MR",
        category="Universal",
        kind="typed",
        default_facet=MR_LITERAL_JAVA_JAR_JOB_FACET,
        facets={MR_LITERAL_JAVA_JAR_JOB_FACET: membership},
    )
