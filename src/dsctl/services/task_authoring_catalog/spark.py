from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        SparkInlineSqlAuthoringSurface,
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

SPARK_INLINE_LOCAL_SQL_FACET = "SPARK/inline_local_sql"


def _spark_inline_sql_runtime_guidance(
    surface: SparkInlineSqlAuthoringSurface,
) -> str:
    """Describe exact local Spark SQL execution without exposing wire epochs."""
    if not surface.available:
        return "SPARK inline local SQL is absent from this exact profile."
    if surface.wire_epoch is None or surface.spark_home is None:
        message = "Available SPARK inline SQL surface lacks one exact wire epoch"
        raise ValueError(message)
    expected_home = (
        "SPARK_HOME2" if surface.wire_epoch == "legacy-spark2" else "SPARK_HOME"
    )
    if surface.spark_home != expected_home:
        message = "SPARK inline SQL wire epoch and home variable disagree"
        raise ValueError(message)
    if surface.parameter_substitution:
        substitution = (
            "Upstream expands SQL from the prepared parameter map before writing "
            "the worker file; this stable literal subset still rejects DS "
            "placeholders for cross-version consistency. "
        )
    else:
        substitution = (
            "Upstream writes the SQL file before final-command parameter "
            "substitution, so DS placeholders inside SQL are not replaced. "
        )
    logging = (
        "The raw or expanded SQL text is logged upstream. "
        if surface.sql_logged
        else ""
    )
    line_endings = (
        "Canonical and wire spelling is preserved, but upstream normalizes CRLF "
        "before writing the worker SQL file. "
        if surface.line_endings_normalized
        else ""
    )
    failover = (
        "The local Spark application can resume after worker failover. "
        if surface.failover_supported
        else (
            "The synchronous local process exposes no durable application id or "
            "worker-failover resume. "
        )
    )
    retry = (
        "Retry reexecutes the complete SQL and may repeat side effects. "
        if surface.retry_reexecutes
        else ""
    )
    return (
        f"Route to a worker with {surface.spark_home}, Spark SQL, Java, Hadoop, "
        "Hive catalog configuration, and target-data permissions. "
        f"{substitution}{logging}{line_endings}{failover}{retry}"
        "This facet is not live evidence and makes no confidentiality guarantee."
    )


def _spark_inline_sql_fields(
    surface: SparkInlineSqlAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.rawScript",
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description=(
                "Nonblank literal Spark SQL preserved in canonical and wire form; "
                "DS placeholders and unsafe control characters are rejected. "
                f"{_spark_inline_sql_runtime_guidance(surface)}"
            ),
        ),
    )


def _spark_inline_sql_template_body(
    surface: SparkInlineSqlAuthoringSurface,
    body: str,
) -> str:
    opaque_modes = "Application programs remain explicit native opaque modes. "
    if surface.wire_epoch in {"script", "script-master"}:
        opaque_modes += "Resource-file SQL is also an explicit native opaque mode."
    else:
        opaque_modes += "Resource-file SQL is absent from this exact profile."
    comments = (
        f"# Runtime prerequisite: {_spark_inline_sql_runtime_guidance(surface)}\n"
        f"# Typed scope: inline SQL on local Spark only. {opaque_modes}\n"
        "# Treat retries as whole-SQL reexecution and keep secrets out of logged SQL.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _spark_inline_sql_templates(
    surface: SparkInlineSqlAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal SQL script through worker-local Spark SQL.",
            payload_modes=("task_params",),
            yaml=_spark_inline_sql_template_body(
                surface,
                """# Task template for one worker-local Spark SQL script
name: run-inline-local-sql
type: SPARK
description: Run one literal Spark SQL script
task_params:
  rawScript: SELECT 1 AS answer
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
            ),
        ),
    )


def _is_reviewed_spark_opaque_mode(
    surface: SparkInlineSqlAuthoringSurface,
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize exact native SPARK modes outside inline local SQL."""
    program_type = task_params.get("programType")
    if program_type in {"JAVA", "SCALA", "PYTHON"}:
        return True
    return bool(
        surface.wire_epoch in {"script", "script-master"}
        and program_type == "SQL"
        and task_params.get("sqlExecutionType") == "FILE"
    )


def _spark_inline_sql_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).spark_inline_sql
    if not surface.available:
        message = (
            f"SPARK does not expose inline SQL in DolphinScheduler {profile_version}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=SPARK_INLINE_LOCAL_SQL_FACET,
        family="spark-inline-local-sql-v1",
        review="spark-inline-local-sql-literal-subset",
        params_model=_family_model("SPARK"),
        fields=_spark_inline_sql_fields(surface),
        state_rules=(),
        templates=_spark_inline_sql_templates(surface),
        opaque_authoring_selector=partial(
            _is_reviewed_spark_opaque_mode,
            surface,
        ),
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
        task_type="SPARK",
        category="Universal",
        kind="typed",
        default_facet=SPARK_INLINE_LOCAL_SQL_FACET,
        facets={SPARK_INLINE_LOCAL_SQL_FACET: membership},
    )
