from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    SqoopLiteralCommandAsciiTaskParamsSpec,
    SqoopLiteralCommandTaskParamsSpec,
)
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
    model_field,
)

SQOOP_LITERAL_COMMAND_FACET = "SQOOP/literal_command"


def _sqoop_runtime_guidance(profile_version: str) -> str:
    """Describe the closed custom-command subset shared by all exact releases."""
    surface = get_task_authoring_surface(profile_version).sqoop
    line_endings = (
        "CRLF is normalized to LF before execution. "
        if surface.line_separator == "lf"
        else "CRLF is normalized to the worker operating-system line separator. "
    )
    encoding = (
        "The worker writes the generated script as UTF-8. "
        if surface.script_encoding == "utf-8"
        else "The worker uses its platform-default script charset, so typed "
        "arguments are restricted to ASCII. "
    )
    logging = (
        "Upstream INFO-logs full task params and the final command, so typed "
        "fields are not secret storage. "
        if not surface.late_password_mask
        else "Upstream INFO-logs serialized task params, including `customShell`, "
        "before this task init registers its narrow password mask, and directly "
        "logs the final generated command. Typed fields are not secret storage. "
    )
    return (
        "Eligible workers need a POSIX shell, a compatible sqoop executable on "
        "PATH, Hadoop/YARN configuration, JDBC drivers, endpoint connectivity, "
        "and data permissions. "
        "The compiler POSIX-quotes each literal argument and writes exactly one "
        "`sqoop import` or `sqoop export` command into upstream CUSTOM mode. "
        + line_endings
        + encoding
        + "Parameters, resources, and DS output are excluded. Inline `--password`, "
        "`--password=...`, and interactive `-P` are rejected; use `--password-file` "
        "with a worker-accessible credential instead. "
        + logging
        + "The task has no durable application id or failover reattachment; retry or "
        "failover can rerun the whole transfer and duplicate effects. Native "
        "TEMPLATE jobs and arbitrary CUSTOM shell remain explicit opaque authoring. "
        "No live evidence is refreshed and no profile is promoted."
    )


def _sqoop_fields(profile_version: str) -> tuple[TaskAuthoringField, ...]:
    guidance = _sqoop_runtime_guidance(profile_version)
    encoding_constraint = (
        " Arguments may contain safe Unicode because this worker epoch writes "
        "the script as UTF-8."
        if get_task_authoring_surface(profile_version).sqoop.script_encoding == "utf-8"
        else " Arguments must be ASCII-only because this worker epoch uses its "
        "platform-default script charset."
    )
    return (
        model_field(
            "task_params.subcommand",
            "string",
            compile_path="taskDefinitionJson[].taskParams.customShell",
            description="Literal Sqoop transfer direction. " + guidance,
        ),
        model_field(
            "task_params.args",
            compile_path="taskDefinitionJson[].taskParams.customShell",
            description="Ordered arguments for the selected Sqoop subcommand.",
        ),
        model_field(
            "task_params.args[]",
            required=False,
            compile_path="taskDefinitionJson[].taskParams.customShell",
            description=(
                "One literal argument. Internal spaces are preserved through "
                "compiler-owned POSIX quoting; edge whitespace, controls, DS "
                "placeholders, and password-bearing switches are rejected."
                + encoding_constraint
            ),
        ),
    )


def _sqoop_templates(profile_version: str) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _sqoop_runtime_guidance(profile_version)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal Sqoop import command.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# The password file must already be readable by the selected "
                "worker/tenant.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one literal Sqoop import
name: import-orders
type: SQOOP
description: Import one relational table into HDFS
task_params:
  subcommand: import
  args:
    - --connect
    - jdbc:mysql://db.example.invalid:3306/source
    - --username
    - sqoop_reader
    - --password-file
    - file:///run/secrets/sqoop-password
    - --table
    - orders
    - --target-dir
    - hdfs:///warehouse/orders
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


def _is_reviewed_sqoop_opaque_mode(task_params: Mapping[str, YamlValue]) -> bool:
    """Allow only the two upstream executable SQOOP modes opaquely."""
    job_type = task_params.get("jobType")
    return isinstance(job_type, str) and job_type in {"CUSTOM", "TEMPLATE"}


def _sqoop_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).sqoop
    contract = TaskAuthoringFacetContract(
        facet_id=SQOOP_LITERAL_COMMAND_FACET,
        family="sqoop-literal-command-v1",
        review="sqoop-literal-custom-command-exact-subset",
        params_model=(
            SqoopLiteralCommandAsciiTaskParamsSpec
            if surface.script_encoding == "platform-default"
            else SqoopLiteralCommandTaskParamsSpec
        ),
        fields=_sqoop_fields(profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed SQOOP literal import/export command authoring",
                condition_paths=(),
                active_paths=("task_params.subcommand", "task_params.args"),
                compile_policy=(
                    ("task_params.jobType", "send compiler-owned CUSTOM"),
                    (
                        "task_params.customShell",
                        "send one compiler-quoted sqoop command",
                    ),
                    ("task_params.localParams", "send compiler-owned empty list"),
                ),
                description=(
                    "The portable facet owns one literal Sqoop import or export "
                    "command and no native datasource-template state."
                ),
            ),
        ),
        templates=_sqoop_templates(profile_version),
        opaque_authoring_selector=_is_reviewed_sqoop_opaque_mode,
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
        task_type="SQOOP",
        category="DataIntegration",
        kind="typed",
        default_facet=SQOOP_LITERAL_COMMAND_FACET,
        facets={SQOOP_LITERAL_COMMAND_FACET: membership},
    )
