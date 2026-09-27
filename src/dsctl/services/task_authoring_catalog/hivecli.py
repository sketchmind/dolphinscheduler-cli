from __future__ import annotations

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
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

HIVECLI_INLINE_SCRIPT_FACET = "HIVECLI/inline_script"


def _hive_cli_execution_guidance(profile_version: str) -> str:
    execution = get_task_authoring_surface(profile_version).hive_cli.script_execution
    if execution == "hive-e-whole-command":
        behavior = (
            "The worker runs the Hive CLI with hive -e after DS substitutes "
            "placeholders across the whole command."
        )
    elif execution == "generated-file-sql-only":
        behavior = (
            "The worker substitutes SQL text only, writes a temporary SQL file, "
            "and runs the Hive CLI with hive -f."
        )
    else:
        message = f"HIVECLI is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    prerequisite = (
        " Every eligible worker needs the Hive CLI in PATH, Hive/HDFS client "
        "configuration, and access to HDFS and the Hive Metastore; this task does "
        "not use a DS datasource."
    )
    return f"{behavior}{prerequisite}"


def _hive_cli_fields(profile_version: str) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.hiveCliTaskExecutionType",
            "enum",
            choices=("SCRIPT",),
            compile_path=("taskDefinitionJson[].taskParams.hiveCliTaskExecutionType"),
            description=_hive_cli_execution_guidance(profile_version),
        ),
        model_field(
            "task_params.hiveSqlScript",
            compile_path="taskDefinitionJson[].taskParams.hiveSqlScript",
            description=(
                "Non-empty inline Hive SQL text. DS placeholders are supported in "
                "this SQL field for every reviewed HIVECLI version."
            ),
        ),
        model_field(
            "task_params.hiveCliOptions",
            compile_path="taskDefinitionJson[].taskParams.hiveCliOptions",
            description=(
                "Optional literal Hive CLI options. DS placeholder syntax is "
                "forbidden here so one YAML document behaves identically before "
                "and after the 3.2 execution change."
            ),
        ),
    )


def _hive_cli_template_body(profile_version: str, body: str) -> str:
    guidance = _hive_cli_execution_guidance(profile_version)
    return task_template_with_runtime_controls(
        f"# Runtime prerequisite: {guidance}\n{body}"
    )


def _hive_cli_templates(
    profile_version: str,
) -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one inline Hive SQL script through the worker Hive CLI.",
            payload_modes=("task_params",),
            yaml=_hive_cli_template_body(
                profile_version,
                """# Task template for HIVECLI inline SCRIPT
name: hivecli-inline-query
type: HIVECLI
description: Run one inline Hive SQL statement
task_params:
  hiveCliTaskExecutionType: SCRIPT
  hiveSqlScript: |-
    SHOW DATABASES;
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
            summary="Run inline Hive SQL with one portable IN/VARCHAR parameter.",
            payload_modes=("task_params",),
            parameter_fields=("task_params.localParams[]",),
            yaml="""task_params:
  localParams:
  - prop: bizdate
    direct: IN
    type: VARCHAR
    value: '2026-08-20'
""",
        ),
    )


def _hive_cli_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).hive_cli
    if not surface.available:
        message = f"HIVECLI is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=HIVECLI_INLINE_SCRIPT_FACET,
        family="hivecli-inline-script-v1",
        review="hivecli-inline-script-exact-subset",
        params_model=_family_model("HIVECLI"),
        fields=_hive_cli_fields(profile_version),
        state_rules=(),
        templates=_hive_cli_templates(profile_version),
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
        task_type="HIVECLI",
        category="Universal",
        kind="typed",
        default_facet=HIVECLI_INLINE_SCRIPT_FACET,
        facets={HIVECLI_INLINE_SCRIPT_FACET: membership},
    )
