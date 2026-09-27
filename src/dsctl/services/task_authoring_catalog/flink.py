from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    FlinkInlineLocalSqlAsciiTaskParamsSpec,
    FlinkInlineLocalSqlTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        FlinkInlineSqlAuthoringSurface,
        FlinkStreamInlineSqlAuthoringSurface,
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
    model_field,
)

FLINK_INLINE_LOCAL_SQL_FACET = "FLINK/inline_local_sql"
FLINK_STREAM_INLINE_LOCAL_SQL_FACET = "FLINK_STREAM/inline_local_sql"


def _flink_inline_sql_runtime_guidance(
    surface: FlinkInlineSqlAuthoringSurface,
) -> str:
    """Describe exact local Flink SQL execution and its observable hazards."""
    if not surface.available:
        reason = surface.exclusion_reason or "typed facet absent"
        return f"FLINK inline local SQL is unavailable: {reason}."
    if surface.script_encoding is None or surface.sql_command is None:
        message = "Available FLINK inline SQL surface lacks an executor encoding"
        raise ValueError(message)
    if surface.script_encoding == "platform-default":
        encoding = (
            "The 3.0.x worker writes SQL with the platform-default charset, so "
            "the selected typed schema accepts ASCII only. "
        )
    else:
        encoding = "The worker writes the SQL script as UTF-8. "
    command = (
        "The worker resolves sql-client.sh from PATH. "
        if surface.sql_command == "PATH"
        else "The worker runs FLINK_HOME/bin/sql-client.sh. "
    )
    substitution = (
        "Upstream performs prepared-map parameter substitution before writing "
        "SQL; this typed literal facet still rejects every DS placeholder. "
        if surface.parameter_substitution
        else "Upstream does not perform parameter substitution inside the SQL. "
    )
    logging = (
        "Upstream logs task parameters, raw SQL script content and file paths, "
        "and the execution command at INFO. "
        if surface.task_params_logged
        and surface.script_logged
        and surface.command_logged
        else ""
    )
    output = (
        "SQL client result output is not surfaced as a DS task output. "
        if not surface.result_output_supported
        else "SQL client result output is surfaced as a DS task output. "
    )
    durable_identity = (
        "The local process exposes a durable application id. "
        if surface.durable_application_id
        else "The local process exposes no durable application id. "
    )
    failover = (
        "Worker failover can resume the execution. "
        if surface.failover_supported
        else "Worker failover cannot resume the execution. "
    )
    retry = (
        "Retry reexecutes the complete SQL and may repeat side effects. "
        if surface.retry_reexecutes
        else ""
    )
    return (
        "Route to a worker with the Flink SQL client, Java, connector and catalog "
        "configuration, and target-data permissions. "
        f"{encoding}{command}{substitution}{logging}{output}{durable_identity}"
        f"{failover}{retry}This facet is not live evidence or secret storage."
    )


def _flink_stream_inline_sql_runtime_guidance(
    surface: FlinkStreamInlineSqlAuthoringSurface,
) -> str:
    """Add exact FLINK_STREAM stop-path limits to the shared executor facts."""
    guidance = _flink_inline_sql_runtime_guidance(surface)
    application_id = (
        "Worker-local SQL is expected to publish an application id. "
        if surface.local_sql_application_id_expected
        else "Worker-local SQL is not expected to publish an application id. "
    )
    if surface.cancel_requires_application_id:
        cancel = "Plugin cancel requires an application id. "
    else:
        cancel = "Plugin cancel does not require an application id. "
    if surface.savepoint_requires_application_id:
        savepoint = "Savepoint requests require an application id. "
    else:
        savepoint = "Savepoint requests do not require an application id. "
    stop_sequence = {
        "none": "No plugin stop sequence is available. ",
        "plugin-cancel-then-pid-tree": (
            "Plugin cancel runs before the worker PID tree is killed. "
        ),
        "pid-tree-then-plugin-cancel": (
            "The worker PID tree is killed before plugin cancel runs. "
        ),
        "plugin-cancel-only": (
            "The stop sequence runs plugin cancel only, with no process-tree kill. "
        ),
    }[surface.stop_sequence]
    reliable_stop = (
        "Reliable stop is supported for an unbounded SQL stream. "
        if surface.reliable_stop_supported
        else (
            "Reliable stop is not supported, so an unbounded SQL stream may "
            "continue after the DS task stops. "
        )
    )
    return (
        f"{guidance} {application_id}{cancel}{savepoint}{stop_sequence}{reliable_stop}"
    )


def _flink_inline_sql_fields(
    surface: FlinkInlineSqlAuthoringSurface,
    *,
    guidance: str | None = None,
) -> tuple[TaskAuthoringField, ...]:
    runtime_guidance = guidance or _flink_inline_sql_runtime_guidance(surface)
    return (
        model_field(
            "task_params.rawScript",
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description=(
                "Nonblank literal local Flink SQL preserved in canonical and wire "
                "spelling; DS placeholders, carriage returns, and C0/C1 control "
                "or unpaired Unicode surrogate characters are rejected. "
                f"{runtime_guidance}"
            ),
        ),
    )


def _flink_inline_sql_templates(
    surface: FlinkInlineSqlAuthoringSurface,
    *,
    task_type: str = "FLINK",
    guidance: str | None = None,
) -> tuple[TaskAuthoringTemplate, ...]:
    runtime_guidance = guidance or _flink_inline_sql_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal SQL script through worker-local Flink SQL.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {runtime_guidance}\n"
                "# Typed scope: rawScript only; the projector supplies programType "
                "SQL, deployMode local, and an explicit empty initScript.\n"
                "# JAVA, SCALA, PYTHON, and exact non-local SQL modes remain native "
                "opaque authoring. Local SQL extras remain unchanged/export "
                "preservation only.\n"
                + task_template_with_runtime_controls(
                    f"""# Task template for one worker-local Flink SQL script
name: run-inline-local-sql
type: {task_type}
description: Run one literal Flink SQL script
task_params:
  rawScript: SELECT 1 AS answer
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


def _flink_nonlocal_sql_modes(profile_version: str) -> frozenset[str]:
    if profile_version in {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return frozenset({"cluster"})
    if profile_version in {
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }:
        return frozenset({"cluster", "application"})
    return frozenset({"cluster", "application", "standalone"})


def _is_reviewed_flink_opaque_mode(
    profile_version: str,
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize exact native FLINK modes excluded from local inline SQL."""
    program_type = task_params.get("programType")
    if isinstance(program_type, str) and program_type in {
        "JAVA",
        "SCALA",
        "PYTHON",
    }:
        return True
    deploy_mode = task_params.get("deployMode")
    return (
        program_type == "SQL"
        and isinstance(deploy_mode, str)
        and deploy_mode in _flink_nonlocal_sql_modes(profile_version)
    )


def _flink_inline_sql_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).flink_inline_sql
    return _flink_family_inline_sql_authoring_profile(
        profile_version,
        task_type="FLINK",
        facet_id=FLINK_INLINE_LOCAL_SQL_FACET,
        family="flink-inline-local-sql-v1",
        review="flink-inline-local-sql-literal-subset",
        surface=surface,
        guidance=_flink_inline_sql_runtime_guidance(surface),
    )


def _flink_stream_inline_sql_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).flink_stream_inline_sql
    return _flink_family_inline_sql_authoring_profile(
        profile_version,
        task_type="FLINK_STREAM",
        facet_id=FLINK_STREAM_INLINE_LOCAL_SQL_FACET,
        family="flink-stream-inline-local-sql-v1",
        review="flink-stream-inline-local-sql-literal-subset",
        surface=surface,
        guidance=_flink_stream_inline_sql_runtime_guidance(surface),
    )


def _flink_family_inline_sql_authoring_profile(
    profile_version: str,
    *,
    task_type: str,
    facet_id: str,
    family: str,
    review: str,
    surface: FlinkInlineSqlAuthoringSurface,
    guidance: str,
) -> TaskTypeAuthoringProfile:
    """Materialize one Flink-family literal-SQL facet from shared invariants."""
    if not surface.available:
        reason = surface.exclusion_reason or "typed facet absent"
        reviewed_hole = (
            task_type == "FLINK"
            and profile_version == "3.1.1"
            and reason == "local-cluster-sql-execution-target-inverted"
        ) or (
            task_type == "FLINK_STREAM"
            and profile_version in {"3.1.1", "3.1.2", "3.1.3", "3.1.4"}
            and reason == "broken-unconditional-main-jar-no-review"
        )
        if reviewed_hole:
            excluded_facet = f"{task_type}/runtime_exclusion"
            membership = TaskAuthoringFacetMembership(
                profile_version=profile_version,
                contract=TaskAuthoringFacetContract(
                    facet_id=excluded_facet,
                    family=f"{task_type.lower()}-runtime-exclusion-v1",
                    review=f"{task_type.lower()}-local-sql-runtime-exclusion",
                    params_model=None,
                    fields=(),
                    state_rules=(),
                    templates=(
                        TaskAuthoringTemplate(
                            name="minimal",
                            summary=(
                                "Review an explicit native JAR payload "
                                "for this SQL runtime hole."
                            ),
                            payload_modes=("task_params",),
                            yaml=task_template_with_runtime_controls(
                                f"# Exact {profile_version}: local SQL is excluded "
                                f"({reason}).\n"
                                "# Native opaque scaffold: replace the example "
                                "resource id, class and execution settings "
                                "with a verified native JAR job.\n"
                                f"name: run-native-{task_type.lower()}\n"
                                f"type: {task_type}\n"
                                "task_params:\n"
                                "  programType: JAVA\n"
                                "  mainJar: {id: 1}\n"
                                "  mainClass: example.Main\n"
                                "  deployMode: local\n"
                            ),
                        ),
                    ),
                    opaque_authoring_selector=partial(
                        _is_reviewed_flink_opaque_mode, profile_version
                    ),
                    restrict_opaque_authoring_to_selector=True,
                ),
                typed_create=False,
                typed_edit=False,
                opaque_create=True,
                opaque_edit=True,
                opaque_preserve=True,
                constraint=(
                    f"Exact {profile_version} {task_type} local SQL is disabled: "
                    f"{reason}. Explicit native jar/nonlocal modes remain opaque; "
                    "canonical local SQL cannot fall back to opaque authoring."
                ),
            )
            return TaskTypeAuthoringProfile(
                task_type=task_type,
                category="Universal",
                kind="generic",
                default_facet=excluded_facet,
                facets={excluded_facet: membership},
            )
        message = (
            f"{task_type} inline local SQL is unavailable in DolphinScheduler "
            f"{profile_version}: {reason}"
        )
        raise ValueError(message)
    params_model = (
        FlinkInlineLocalSqlAsciiTaskParamsSpec
        if surface.script_encoding == "platform-default"
        else FlinkInlineLocalSqlTaskParamsSpec
    )
    contract = TaskAuthoringFacetContract(
        facet_id=facet_id,
        family=family,
        review=review,
        params_model=params_model,
        fields=_flink_inline_sql_fields(surface, guidance=guidance),
        state_rules=(),
        templates=_flink_inline_sql_templates(
            surface,
            task_type=task_type,
            guidance=guidance,
        ),
        opaque_authoring_selector=partial(
            _is_reviewed_flink_opaque_mode,
            profile_version,
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
        task_type=task_type,
        category="Universal",
        kind="typed",
        default_facet=facet_id,
        facets={facet_id: membership},
    )
