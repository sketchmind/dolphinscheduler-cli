from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec,
    DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        DataQualityAuthoringSurface,
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

DATA_QUALITY_LOCAL_MYSQL_TABLE_ROW_COUNT_EQUALS_FACET = (
    "DATA_QUALITY/local_mysql_table_row_count_equals"
)


def _data_quality_runtime_guidance(surface: DataQualityAuthoringSurface) -> str:
    """Describe the fixed stock rule and its exact comparison epoch."""
    if not surface.available:
        return "DATA_QUALITY is absent from this exact DolphinScheduler profile."
    if (
        surface.rule_id != 10
        or surface.main_class
        != "org.apache.dolphinscheduler.data.quality.DataQualityApplication"
        or not surface.fixed_value_comparison
        or not surface.blocking_failure_strategy
        or surface.result_operator not in {"EQ", "NE"}
    ):
        message = "Available DATA_QUALITY surface lacks its exact fixed-rule contract"
        raise ValueError(message)
    database_wire = (
        "The compiler emits the literal database into the rule-input wire. "
        if surface.database_wire_required
        else (
            "The legacy runtime obtains the database directly from the selected "
            "datasource, so the canonical task has no database field. "
        )
    )
    return (
        "Before typed create or a changed edit carrying this exact fixed-point "
        "projection, including dry-run, dsctl performs mandatory live attestation "
        "of the permission-visible MYSQL datasource and the stock ruleId=10 page "
        "and form fingerprint; modern releases also require the datasource "
        "database to equal the authored database. No-op edits and unchanged richer "
        "opaque-preserve tasks skip this authoring preflight. The compiler selects "
        "local Spark, fixed-value equality, and blocking failure behavior; the exact "
        f"native result operator is {surface.result_operator}. {database_wire}"
        "Use a parameter-free workflow and shell-safe hidden datasource/config "
        "values: legacy execution can place raw database passwords in the Spark "
        "command. Eligible workers need the data-quality application, Spark, Java, "
        "a MySQL JDBC driver, network reachability, SELECT permission on the table, "
        "and writable DolphinScheduler metadata tables. Upstream INFO-logs the "
        "complete task parameters, command, and resolved credentials; task fields "
        "are not secret storage. If the expected data-quality result row is missing, "
        "the master can leave the task successful without evaluating the rule. The "
        "task has no structured output, durable application id, or failover "
        "reattachment; cancellation is best-effort, and retry can duplicate result "
        "rows or alerts."
    )


def _data_quality_fields(
    surface: DataQualityAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    runtime = _data_quality_runtime_guidance(surface)
    fields = [
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
            compile_path=(
                "taskDefinitionJson[].taskParams.ruleInputParameter[src_datasource_id]"
            ),
            description=(
                "Positive id or exact name of an existing MYSQL datasource. " + runtime
            ),
        ),
        model_field(
            "task_params.table",
            compile_path=(
                "taskDefinitionJson[].taskParams.ruleInputParameter[src_table]"
            ),
            description="Literal conservative MySQL table identifier.",
        ),
        model_field(
            "task_params.expectedRowCount",
            compile_path=(
                "taskDefinitionJson[].taskParams.ruleInputParameter[comparison_name]"
            ),
            description=(
                "Expected nonnegative table row count, limited to 2^53-1 because "
                "upstream persists and compares the value through double precision."
            ),
        ),
    ]
    if surface.database_field_required:
        fields.insert(
            1,
            model_field(
                "task_params.database",
                compile_path=(
                    "taskDefinitionJson[].taskParams.ruleInputParameter[src_database]"
                ),
                description="Literal conservative MySQL database identifier.",
            ),
        )
    return tuple(fields)


def _data_quality_templates(
    surface: DataQualityAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _data_quality_runtime_guidance(surface)
    database_line = "  database: analytics\n" if surface.database_field_required else ""
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Block when one MySQL table row count differs from a fixed value.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope is closed; richer rule, Spark, inherited, or future "
                "state remains unchanged/export preservation only.\n"
                + task_template_with_runtime_controls(
                    f"""# Task template for one MySQL table row-count equality check
name: verify-orders-row-count
type: DATA_QUALITY
description: Require the orders table to contain the expected number of rows
task_params:
  datasource: 1
{database_line}\
  table: orders
  expectedRowCount: 1000
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
"""
                )
            ),
        ),
    )


def _data_quality_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).data_quality
    if not surface.available:
        message = f"DATA_QUALITY is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DATA_QUALITY_LOCAL_MYSQL_TABLE_ROW_COUNT_EQUALS_FACET,
        family="data-quality-local-mysql-table-row-count-equals-v1",
        review="data-quality-local-mysql-table-row-count-equality",
        params_model=(
            DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec
            if surface.database_field_required
            else DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec
        ),
        fields=_data_quality_fields(surface),
        state_rules=(),
        templates=_data_quality_templates(surface),
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
        task_type="DATA_QUALITY",
        category="DataQuality",
        kind="typed",
        default_facet=DATA_QUALITY_LOCAL_MYSQL_TABLE_ROW_COUNT_EQUALS_FACET,
        facets={
            DATA_QUALITY_LOCAL_MYSQL_TABLE_ROW_COUNT_EQUALS_FACET: membership,
        },
    )
