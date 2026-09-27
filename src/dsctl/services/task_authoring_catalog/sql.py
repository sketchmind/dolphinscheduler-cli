from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    Sql139InlineTaskParamsSpec,
)
from dsctl.upstream.task_parameter_projection import (
    is_sql_139_complete_native_package,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringStateRule,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

SQL_INLINE_FACET = "SQL/inline_script"
SQL_RESOURCE_FILE_FACET = "SQL/resource_file"


def _uses_sql_resource_fields(task_params: Mapping[str, YamlValue]) -> bool:
    return "sqlSource" in task_params or "sqlResource" in task_params


_SQL_FIELDS = (
    model_field(
        "task_params.type",
        "enum",
        default="MYSQL",
        choice_source="dsctl enum list db-type",
        related_commands=(
            "dsctl enum list db-type",
            "dsctl template datasource",
            "dsctl template datasource --type TYPE",
        ),
        compile_path="taskDefinitionJson[].taskParams.type",
        description="Datasource type used by the SQL plugin.",
    ),
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
        description="Positive datasource id or exact datasource name.",
    ),
    model_field(
        "task_params.sql",
        compile_path="taskDefinitionJson[].taskParams.sql",
        description="SQL text.",
    ),
    model_field(
        "task_params.sqlType",
        default=0,
        choices=("0", "1"),
        compile_path="taskDefinitionJson[].taskParams.sqlType",
        description="0=query statements that return rows; 1=non-query statements.",
    ),
    model_field(
        "task_params.sendEmail",
        default=False,
        active_when="normally only meaningful when sqlType=0",
        compile_path="taskDefinitionJson[].taskParams.sendEmail",
        description="Ask DS to email query results.",
    ),
    model_field(
        "task_params.displayRows",
        default=10,
        active_when="normally only meaningful when sqlType=0",
        compile_path="taskDefinitionJson[].taskParams.displayRows",
        description="Maximum displayed result rows for query SQL.",
    ),
    model_field(
        "task_params.showType",
        default="TABLE",
        active_when="normally only meaningful when sqlType=0",
        compile_path="taskDefinitionJson[].taskParams.showType",
        description="Result display type.",
    ),
    model_field(
        "task_params.connParams",
        default="",
        compile_path="taskDefinitionJson[].taskParams.connParams",
        description="Datasource connection parameter override.",
    ),
    model_field(
        "task_params.preStatements[]",
        default=[],
        compile_path="taskDefinitionJson[].taskParams.preStatements",
        description="Statements run before the main SQL.",
    ),
    model_field(
        "task_params.postStatements[]",
        default=[],
        compile_path="taskDefinitionJson[].taskParams.postStatements",
        description="Statements run after the main SQL.",
    ),
    model_field(
        "task_params.groupId",
        default=0,
        active_when="required by DS email setup when sendEmail=true",
        choice_source="dsctl alert-group list",
        related_commands=(
            "dsctl alert-group list",
            "dsctl alert-group create --name NAME --instance-id ID",
        ),
        compile_path="taskDefinitionJson[].taskParams.groupId",
        description="Alert group id used for SQL result email.",
    ),
    model_field(
        "task_params.title",
        default="",
        active_when="required by DS email setup when sendEmail=true",
        compile_path="taskDefinitionJson[].taskParams.title",
        description="Email title for SQL result notifications.",
    ),
    model_field(
        "task_params.limit",
        default=0,
        compile_path="taskDefinitionJson[].taskParams.limit",
        description="Optional DS SQL result limit.",
    ),
)

_SQL_STATE_RULES = (
    TaskAuthoringStateRule(
        when="task_params.sqlType == 0",
        condition_paths=("task_params.sqlType",),
        active_paths=(
            "task_params.sendEmail",
            "task_params.displayRows",
            "task_params.showType",
            "task_params.groupId",
            "task_params.title",
        ),
        description="Query SQL may produce displayable rows and OUT params.",
    ),
    TaskAuthoringStateRule(
        when="task_params.sqlType == 1",
        condition_paths=("task_params.sqlType",),
        active_paths=(
            "task_params.preStatements",
            "task_params.postStatements",
        ),
        inactive_paths=(
            "task_params.displayRows",
            "task_params.showType",
            "task_params.groupId",
            "task_params.title",
        ),
        compile_policy=(
            ("task_params.sendEmail", "prefer false"),
            ("task_params.localParams", "send [] when absent"),
            ("task_params.varPool", "send [] when absent"),
            ("task_params.preStatements", "send [] when absent"),
            ("task_params.postStatements", "send [] when absent"),
        ),
        description=(
            "Non-query SQL is for DDL/DML statements; do not model it as a "
            "result-set query."
        ),
    ),
)

_SQL_PARAMETER_FIELDS = (
    "task_params.localParams[]",
    "task_params.varPool[]",
)

_SQL_TEMPLATES = (
    TaskAuthoringTemplate(
        name="minimal",
        summary="SQL task with datasource, sqlType, and result display fields.",
        payload_modes=("task_params",),
        yaml=task_template_with_runtime_controls(
            """# Task template for SQL
name: sql-task
type: SQL
description: Example SQL task
task_params:
  type: MYSQL
  datasource: 1
  sql: |
    select 1;
  sqlType: 0
  sendEmail: false
  displayRows: 10
  showType: TABLE
  connParams: ""
  preStatements: []
  postStatements: []
  groupId: 0
  title: ""
  limit: 0
  localParams: []
  varPool: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
    TaskAuthoringTemplate(
        name="output",
        summary="SQL example with IN localParams and one OUT result column.",
        payload_modes=("task_params",),
        parameter_fields=_SQL_PARAMETER_FIELDS,
        yaml=task_template_with_runtime_controls(
            """# Task template for SQL with dynamic parameters
name: sql-params-task
type: SQL
description: Use one IN param and publish one OUT param from result rows
task_params:
  type: MYSQL
  datasource: 1
  sql: |
    select count(*) as row_count
    from source_table
    where bizdate = '${bizdate}';
  sqlType: 0
  sendEmail: false
  displayRows: 10
  showType: TABLE
  connParams: ""
  preStatements: []
  postStatements: []
  groupId: 0
  title: ""
  limit: 0
  localParams:
    - prop: bizdate
      direct: IN
      type: VARCHAR
      value: ${system.biz.date}
    - prop: row_count
      direct: OUT
      type: INTEGER
      value: "0"
  varPool: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
    TaskAuthoringTemplate(
        name="pre-post-statements",
        purpose="option",
        summary="SQL task with preStatements and postStatements.",
        payload_modes=("task_params",),
        yaml="""task_params:
  preStatements:
  - set session sql_mode = 'STRICT_TRANS_TABLES'
  postStatements:
  - analyze table target_table
""",
    ),
)

_SQL_INLINE_CONTRACT = TaskAuthoringFacetContract(
    facet_id=SQL_INLINE_FACET,
    family="sql-inline-script-v1",
    review="sql-3.4.1-to-3.4.2",
    params_model=_family_model("SQL"),
    fields=_SQL_FIELDS,
    state_rules=_SQL_STATE_RULES,
    templates=_SQL_TEMPLATES,
)

_SQL_139_DATASOURCE_TYPES = (
    "MYSQL",
    "POSTGRESQL",
    "HIVE",
    "SPARK",
    "CLICKHOUSE",
    "ORACLE",
    "SQLSERVER",
    "DB2",
)
_SQL_139_PARAMETER_DATA_TYPES = (
    "VARCHAR",
    "INTEGER",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "BOOLEAN",
)
_SQL_139_FIELDS = (
    model_field(
        "task_params.type",
        default="MYSQL",
        choice_source="dsctl enum list db-type",
        related_commands=(
            "dsctl enum list db-type",
            "dsctl datasource list",
            "dsctl datasource get DATASOURCE",
        ),
        compile_path="processDefinitionJson.tasks[].params.type",
        description="Exact 1.3.9 datasource type implemented by the SQL worker.",
    ),
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
        compile_path="processDefinitionJson.tasks[].params.datasource",
        description=(
            "Positive datasource id or exact name. The 1.3.9 workflow API does "
            "not attest datasource permission before the master resolves it."
        ),
    ),
    model_field(
        "task_params.sql",
        compile_path="processDefinitionJson.tasks[].params.sql",
        description=(
            "Literal SQL text. Upstream logs original and prepared SQL and bound "
            "values at INFO; do not place secrets here."
        ),
    ),
    model_field(
        "task_params.sqlType",
        compile_path="processDefinitionJson.tasks[].params.sqlType",
        description="0 executes a query; 1 executes an update statement.",
    ),
    model_field(
        "task_params.displayRows",
        model_default=True,
        active_when="task_params.sqlType == 0",
        compile_path="processDefinitionJson.tasks[].params.displayRows",
        description="Maximum number of query rows rendered in worker diagnostics.",
    ),
    model_field(
        "task_params.connParams",
        model_default=True,
        active_when="task_params.type == HIVE",
        compile_path="processDefinitionJson.tasks[].params.connParams",
        description=(
            "Optional HIVE-only key=value entries separated by semicolons. "
            "Values cannot contain semicolons or equals signs."
        ),
    ),
    model_field(
        "task_params.preStatements[]",
        default=[],
        compile_path="processDefinitionJson.tasks[].params.preStatements",
        description="Nonblank statements executed before the main SQL.",
    ),
    model_field(
        "task_params.postStatements[]",
        default=[],
        compile_path="processDefinitionJson.tasks[].params.postStatements",
        description="Nonblank statements executed after the main SQL.",
    ),
    model_field(
        "task_params.limit",
        model_default=True,
        compile_path="processDefinitionJson.tasks[].params.limit",
        description="Nonnegative exact native SQL result limit.",
    ),
)
_SQL_139_STATE_RULES = (
    TaskAuthoringStateRule(
        when="all exact 1.3.9 SQL typed authoring",
        condition_paths=(),
        active_paths=(),
        compile_policy=(
            ("task_params.sendEmail", "send compiler-owned false"),
            ("task_params.udfs", "send compiler-owned empty text"),
            ("task_params.showType", "send compiler-owned TABLE"),
            ("task_params.title", "send compiler-owned empty text"),
            ("task_params.receivers", "send compiler-owned empty text"),
            ("task_params.receiversCc", "send compiler-owned empty text"),
        ),
        description=(
            "Every typed datasource closes email, UDF, and richer display state "
            "while keeping the dangerous nullable sendEmail default disabled."
        ),
    ),
    TaskAuthoringStateRule(
        when="task_params.type == HIVE",
        condition_paths=("task_params.type",),
        active_paths=("task_params.connParams",),
        compile_policy=(("task_params.connParams", "send empty text when absent"),),
        description="Only exact 1.3.9 HIVE consumes connection overrides.",
    ),
    TaskAuthoringStateRule(
        when="all datasource types except HIVE",
        condition_paths=("task_params.type",),
        active_paths=(),
        inactive_paths=("task_params.connParams",),
        compile_policy=(("task_params.connParams", "send compiler-owned empty text"),),
        description="Non-HIVE typed authoring fixes connection overrides empty.",
    ),
)
_SQL_139_TEMPLATES = (
    TaskAuthoringTemplate(
        name="minimal",
        summary="Execute one exact 1.3.9 SQL query without email or UDF state.",
        payload_modes=("task_params",),
        yaml=task_template_with_runtime_controls(
            """# Exact 1.3.9 SQL task; upstream INFO-logs SQL and bound values.
name: sql-task
type: SQL
description: Execute one legacy SQL query
task_params:
  type: MYSQL
  datasource: 1
  sql: |-
    select 1;
  sqlType: 0
  displayRows: 10
  connParams: ""
  preStatements: []
  postStatements: []
  limit: 0
  localParams: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
    TaskAuthoringTemplate(
        name="params",
        purpose="option",
        summary="Execute exact 1.3.9 SQL with one IN-only scalar parameter.",
        payload_modes=("task_params",),
        parameter_fields=("task_params.localParams[]",),
        yaml="""task_params:
  localParams:
  - prop: bizdate
    direct: IN
    type: VARCHAR
    value: ${system.biz.date}
""",
    ),
)
_SQL_139_INLINE_CONTRACT = TaskAuthoringFacetContract(
    facet_id=SQL_INLINE_FACET,
    family="sql-inline-script-v1",
    review="legacy-1.3.9-sql-safe-inline-subset",
    params_model=Sql139InlineTaskParamsSpec,
    fields=_SQL_139_FIELDS,
    state_rules=_SQL_139_STATE_RULES,
    templates=_SQL_139_TEMPLATES,
    parameter_data_types=_SQL_139_PARAMETER_DATA_TYPES,
    parameter_directions=("IN",),
    opaque_authoring_selector=is_sql_139_complete_native_package,
    restrict_opaque_authoring_to_selector=True,
)

_SQL_RESOURCE_FILE_CONTRACT = TaskAuthoringFacetContract(
    facet_id=SQL_RESOURCE_FILE_FACET,
    family="sql-resource-file-3.4.2-v1",
    review="sql-3.4.1-to-3.4.2",
    params_model=None,
    fields=(),
    state_rules=(),
    templates=(),
)


def _sql_inline_membership(profile_version: str) -> TaskAuthoringFacetMembership:
    contract = (
        _SQL_139_INLINE_CONTRACT if profile_version == "1.3.9" else _SQL_INLINE_CONTRACT
    )
    return TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )


def _sql_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    facets: dict[str, TaskAuthoringFacetMembership] = {
        SQL_INLINE_FACET: _sql_inline_membership(profile_version),
    }
    if profile_version in {"3.4.2", "3.4.3"}:
        facets[SQL_RESOURCE_FILE_FACET] = TaskAuthoringFacetMembership(
            profile_version=profile_version,
            contract=_SQL_RESOURCE_FILE_CONTRACT,
            typed_create=False,
            typed_edit=False,
            opaque_create=False,
            opaque_edit=False,
            opaque_preserve=True,
            constraint=(
                "SQL resource-file authoring has not passed the "
                f"{profile_version} profile gates."
            ),
        )
    return TaskTypeAuthoringProfile(
        task_type="SQL",
        category="Universal",
        kind="typed",
        default_facet=SQL_INLINE_FACET,
        facets=facets,
    )
