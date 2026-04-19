from __future__ import annotations

import json
from textwrap import dedent
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.errors import UserInputError
from dsctl.models.common import DataType, Direct
from dsctl.models.task_spec import canonical_task_type
from dsctl.output import CommandResult, require_json_object
from dsctl.services import _task_templates
from dsctl.services.datasource_payload import (
    datasource_template_data,
    datasource_template_index_data,
    require_datasource_payload_type,
    supported_datasource_template_types,
)

if TYPE_CHECKING:
    from dsctl.services._task_templates import TaskTemplateMetadata


class TaskTemplateTypesData(TypedDict):
    """Stable discovery payload for `dsctl template task --list`."""

    task_types: list[str]
    count: int
    typed_task_types: list[str]
    generic_task_types: list[str]
    task_types_by_category: dict[str, list[str]]
    task_templates: dict[str, TaskTemplateMetadata]
    rows: list[TaskTemplateTypeRowData]


class ParameterFieldData(TypedDict):
    """One DS Property field accepted in authored YAML."""

    name: str
    required: bool
    value_type: str
    description: str


class ParameterReferenceData(TypedDict):
    """One supported parameter reference syntax."""

    syntax: str
    description: str


class ParameterOutputData(TypedDict):
    """One supported parameter output publication syntax."""

    task_types: list[str]
    syntax: str
    description: str


class BuiltInParameterData(TypedDict):
    """One DS built-in parameter reference."""

    name: str
    syntax: str
    description: str


class ParameterTimeFormData(TypedDict):
    """One DS time placeholder expression form."""

    form: str
    description: str


class ParameterPropertyTopicDetails(TypedDict):
    """Detailed payload for the parameter property topic."""

    ds_model: str
    property_fields: list[ParameterFieldData]
    direct_values: list[str]
    type_values: list[str]
    scopes: dict[str, str]
    yaml: str


class ParameterBuiltInTopicDetails(TypedDict):
    """Detailed payload for the built-in parameter topic."""

    reference_syntax: list[ParameterReferenceData]
    built_in_variables: list[BuiltInParameterData]
    yaml: str


class ParameterTimeTopicDetails(TypedDict):
    """Detailed payload for the time placeholder topic."""

    syntax: str
    behavior: str
    cautions: list[str]
    examples: list[str]
    forms: list[ParameterTimeFormData]
    yaml: str


class ParameterContextTopicDetails(TypedDict):
    """Detailed payload for the parameter context topic."""

    scopes: dict[str, str]
    priority: list[str]
    rules: list[str]


class ParameterOutputTopicDetails(TypedDict):
    """Detailed payload for the parameter output topic."""

    output_syntax: list[ParameterOutputData]
    sql_rules: list[str]
    examples: dict[str, str]


ParameterTopicDetails: TypeAlias = (
    ParameterPropertyTopicDetails
    | ParameterBuiltInTopicDetails
    | ParameterTimeTopicDetails
    | ParameterContextTopicDetails
    | ParameterOutputTopicDetails
)


class ParameterAllTopicDetails(TypedDict):
    """Detailed payload for the all parameter topic."""

    topics: dict[str, ParameterTopicDetails]


class ParameterTopicData(TypedDict):
    """One parameter syntax topic discoverable by AI clients."""

    topic: str
    command: str
    summary: str


class EnvironmentConfigLineData(TypedDict):
    """One line in the DS environment config shell template."""

    line: str
    purpose: str


class ClusterConfigFieldData(TypedDict):
    """One field in the DS cluster config JSON object."""

    name: str
    required: bool
    value_type: str
    description: str


class TextLineData(TypedDict):
    """One rendered text-template line for table and tsv output."""

    line_no: int
    line: str


class EnvironmentConfigTemplateData(TypedDict):
    """Stable discovery payload for `dsctl template environment`."""

    filename: str
    config: str
    lines: list[EnvironmentConfigLineData]
    target_commands: list[str]
    source_options: list[str]
    upstream_request_shape: str
    rules: list[str]


class ClusterConfigTemplateData(TypedDict):
    """Stable discovery payload for `dsctl template cluster`."""

    filename: str
    config: str
    payload: dict[str, str]
    fields: list[ClusterConfigFieldData]
    rows: list[ClusterConfigFieldData]
    target_commands: list[str]
    source_options: list[str]
    upstream_request_shape: str
    upstream_ui_shape: str
    rules: list[str]


class ClusterConfigTemplateCapabilityData(TypedDict):
    """Compact capability metadata for cluster config templates."""

    command: str
    source_options: list[str]
    target_commands: list[str]


class TaskTemplateTypeRowData(TypedDict):
    """One compact task-template type row."""

    task_type: str
    kind: str
    category: str
    default_variant: str
    variants: str


class ParameterSyntaxIndexData(TypedDict):
    """Compact discovery payload for `dsctl template params`."""

    default_topic: str
    topics: list[ParameterTopicData]
    recommended_flow: list[str]
    rules: list[str]


class ParameterSyntaxTopicData(TypedDict):
    """Detailed payload for one `dsctl template params --topic ...` topic."""

    topic: str
    summary: str
    next_topics: list[str]
    details: ParameterTopicDetails | ParameterAllTopicDetails


def supported_parameter_syntax_topics() -> tuple[str, ...]:
    """Return supported parameter syntax topic names."""
    return _PARAMETER_SYNTAX_TOPICS


def parameter_syntax_index_data() -> ParameterSyntaxIndexData:
    """Return compact parameter syntax discovery metadata."""
    return {
        "default_topic": "overview",
        "topics": [
            {
                "topic": "overview",
                "command": "dsctl template params",
                "summary": "Compact index for progressive parameter discovery.",
            },
            {
                "topic": "property",
                "command": "dsctl template params --topic property",
                "summary": (
                    "DS Property fields, directions, data types, and YAML shape."
                ),
            },
            {
                "topic": "built-in",
                "command": "dsctl template params --topic built-in",
                "summary": "Built-in ${system.*} variables and ${name} references.",
            },
            {
                "topic": "time",
                "command": "dsctl template params --topic time",
                "summary": "DS $[...] time placeholder expressions.",
            },
            {
                "topic": "context",
                "command": "dsctl template params --topic context",
                "summary": "Parameter scopes, precedence, and upstream passing rules.",
            },
            {
                "topic": "output",
                "command": "dsctl template params --topic output",
                "summary": "OUT parameter publication through logs and SQL results.",
            },
            {
                "topic": "all",
                "command": "dsctl template params --topic all",
                "summary": "All parameter syntax topics for offline reference.",
            },
        ],
        "recommended_flow": [
            "Run `dsctl template params` first and select only the needed topic.",
            "Run `dsctl template task TYPE --variant params` for task-specific YAML.",
            "Run `dsctl lint workflow FILE` before sending the workflow to DS.",
            "Run `dsctl workflow create --file FILE --dry-run` before mutation.",
        ],
        "rules": [
            "The CLI preserves DS parameter expressions as strings.",
            "DS evaluates ${...}, $[...], and output parameters at runtime.",
            "Use topic-specific output to avoid filling AI context unnecessarily.",
        ],
    }


def parameter_syntax_data(
    topic: str = "overview",
) -> ParameterSyntaxIndexData | ParameterSyntaxTopicData:
    """Return DS parameter syntax metadata for workflow YAML authoring."""
    normalized_topic = _normalize_parameter_syntax_topic(topic)
    if normalized_topic == "overview":
        return parameter_syntax_index_data()
    topic_data = _parameter_syntax_topics_data()
    if normalized_topic == "all":
        all_details: ParameterAllTopicDetails = {"topics": topic_data}
        return {
            "topic": "all",
            "summary": "All DS parameter syntax topics.",
            "next_topics": [],
            "details": all_details,
        }
    return {
        "topic": normalized_topic,
        "summary": _parameter_syntax_topic_summary(normalized_topic),
        "next_topics": _parameter_syntax_next_topics(normalized_topic),
        "details": topic_data[normalized_topic],
    }


def parameter_syntax_result(topic: str | None = None) -> CommandResult:
    """Return stable DS parameter syntax metadata and examples."""
    normalized_topic = _normalize_parameter_syntax_topic(topic or "overview")
    return CommandResult(
        data=require_json_object(
            parameter_syntax_data(normalized_topic),
            label="parameter syntax data",
        ),
        resolved={
            "topic": normalized_topic,
            "available_topics": list(supported_parameter_syntax_topics()),
            "ds_model": "Property",
            "template_variants": [
                task_type
                for task_type, metadata in task_template_metadata().items()
                if "params" in metadata["variants"]
            ],
        },
    )


def _normalize_parameter_syntax_topic(topic: str) -> str:
    normalized = topic.strip().lower().replace("_", "-")
    if normalized in _PARAMETER_SYNTAX_TOPICS:
        return normalized
    supported = ", ".join(_PARAMETER_SYNTAX_TOPICS)
    message = f"Unsupported parameter syntax topic '{topic}'. Supported: {supported}"
    raise UserInputError(
        message,
        details={"topic": topic},
        suggestion="Run `dsctl template params` to inspect available topics.",
    )


def _parameter_syntax_topic_summary(topic: str) -> str:
    for item in parameter_syntax_index_data()["topics"]:
        if item["topic"] == topic:
            return item["summary"]
    return "DS parameter syntax topic."


def _parameter_syntax_next_topics(topic: str) -> list[str]:
    if topic == "property":
        return ["built-in", "time", "output"]
    if topic == "built-in":
        return ["time", "context"]
    if topic == "time":
        return ["property", "context"]
    if topic == "context":
        return ["output", "property"]
    if topic == "output":
        return ["context", "property"]
    return []


def _parameter_syntax_topics_data() -> dict[str, ParameterTopicDetails]:
    return {
        "property": _parameter_property_topic_data(),
        "built-in": _parameter_built_in_topic_data(),
        "time": _parameter_time_topic_data(),
        "context": _parameter_context_topic_data(),
        "output": _parameter_output_topic_data(),
    }


def _parameter_property_topic_data() -> ParameterPropertyTopicDetails:
    return {
        "ds_model": "Property",
        "property_fields": _parameter_property_fields(),
        "direct_values": [value.value for value in Direct],
        "type_values": [value.value for value in DataType],
        "scopes": {
            "workflow.global_params": (
                "Workflow-level parameters. A mapping is shorthand for IN "
                "VARCHAR properties."
            ),
            "task_params.localParams": (
                "Task-level DS Property entries consumed by the task plugin."
            ),
            "task_params.varPool": (
                "Runtime output pool. Keep it empty in new authored YAML unless "
                "preserving an exported DS shape."
            ),
        },
        "yaml": _parameter_property_yaml(),
    }


def _parameter_built_in_topic_data() -> ParameterBuiltInTopicDetails:
    return {
        "reference_syntax": _parameter_reference_syntax(),
        "built_in_variables": [
            {
                "name": "system.biz.date",
                "syntax": "${system.biz.date}",
                "description": "Day before the schedule time, formatted yyyyMMdd.",
            },
            {
                "name": "system.biz.curdate",
                "syntax": "${system.biz.curdate}",
                "description": "Schedule time date, formatted yyyyMMdd.",
            },
            {
                "name": "system.datetime",
                "syntax": "${system.datetime}",
                "description": "Schedule time datetime, formatted yyyyMMddHHmmss.",
            },
            {
                "name": "system.task.execute.path",
                "syntax": "${system.task.execute.path}",
                "description": "Absolute execution path of the current task.",
            },
            {
                "name": "system.task.instance.id",
                "syntax": "${system.task.instance.id}",
                "description": "Current task instance id.",
            },
            {
                "name": "system.task.definition.name",
                "syntax": "${system.task.definition.name}",
                "description": "Current task definition name.",
            },
            {
                "name": "system.task.definition.code",
                "syntax": "${system.task.definition.code}",
                "description": "Current task definition code.",
            },
            {
                "name": "system.workflow.instance.id",
                "syntax": "${system.workflow.instance.id}",
                "description": "Workflow instance id for the current task.",
            },
            {
                "name": "system.workflow.definition.name",
                "syntax": "${system.workflow.definition.name}",
                "description": "Workflow definition name for the current task.",
            },
            {
                "name": "system.workflow.definition.code",
                "syntax": "${system.workflow.definition.code}",
                "description": "Workflow definition code for the current task.",
            },
            {
                "name": "system.project.name",
                "syntax": "${system.project.name}",
                "description": "Current project name.",
            },
            {
                "name": "system.project.code",
                "syntax": "${system.project.code}",
                "description": "Current project code.",
            },
        ],
        "yaml": _parameter_built_in_yaml(),
    }


def _parameter_time_topic_data() -> ParameterTimeTopicDetails:
    return {
        "syntax": "$[expression]",
        "behavior": "DS evaluates time placeholders at workflow runtime.",
        "cautions": [
            (
                "DS uses Java-style date patterns: yyyy is calendar year, while "
                "YYYY is week-based year."
            ),
            (
                "Expressions such as $[yyyyww] mix calendar year and week number; "
                "use year_week(...) when week-of-year output is intended."
            ),
        ],
        "examples": [
            "$[yyyyMMdd]",
            "$[yyyyMMdd-1]",
            "$[yyyy-MM-dd]",
            "$[HHmmss+1/24]",
            "$[add_months(yyyyMMdd,-1)]",
            "$[this_day(yyyy-MM-dd)]",
            "$[last_day(yyyy-MM-dd)]",
            "$[year_week(yyyy-MM-dd)]",
            "$[year_week(yyyy-MM-dd,5)]",
            "$[month_first_day(yyyy-MM-dd,-1)]",
            "$[month_last_day(yyyy-MM-dd,-1)]",
            "$[week_first_day(yyyy-MM-dd,-1)]",
            "$[week_last_day(yyyy-MM-dd,-1)]",
        ],
        "forms": [
            {
                "form": "$[yyyyMMdd]",
                "description": ("Format schedule time with a Java-style date pattern."),
            },
            {
                "form": "$[yyyyMMdd+N]",
                "description": "Add N days; use -N for days before.",
            },
            {
                "form": "$[yyyyMMdd+7*N]",
                "description": "Add N weeks; use -7*N for weeks before.",
            },
            {
                "form": "$[HHmmss+N/24]",
                "description": "Add N hours; use -N/24 for hours before.",
            },
            {
                "form": "$[HHmmss+N/24/60]",
                "description": "Add N minutes; use -N/24/60 for minutes before.",
            },
            {
                "form": "$[add_months(yyyyMMdd,N)]",
                "description": "Add N months; use 12*N for years.",
            },
            {
                "form": "$[this_day(format)] / $[last_day(format)]",
                "description": "Current day or previous day with the selected format.",
            },
            {
                "form": "$[year_week(format)] / $[year_week(format,N)]",
                "description": "Week of year, optionally choosing week start day N.",
            },
            {
                "form": "$[month_first_day(format,N)] / $[month_last_day(format,N)]",
                "description": "First or last day of a month offset by N months.",
            },
            {
                "form": "$[week_first_day(format,N)] / $[week_last_day(format,N)]",
                "description": "First or last day of a week offset by N weeks.",
            },
        ],
        "yaml": _parameter_time_yaml(),
    }


def _parameter_context_topic_data() -> ParameterContextTopicDetails:
    return {
        "scopes": {
            "project": "Project parameters are managed by `project-parameter`.",
            "workflow.global_params": "Workflow-wide parameters.",
            "task_params.localParams": "Task-local parameters.",
            "upstream_output": "OUT parameters passed from upstream dependencies.",
            "startup": "Runtime parameters passed when starting a workflow.",
        },
        "priority": [
            "Startup Parameter",
            "Local Parameter",
            "Parameter Context",
            "Global Parameter",
        ],
        "rules": [
            "Upstream-to-downstream passing is one-way along dependencies.",
            (
                "For DS 3.3+ behavior, downstream tasks should declare an IN "
                "parameter with the same prop to consume an upstream OUT value."
            ),
            ("If no dependency path exists, local parameters are not passed upstream."),
            (
                "A downstream local parameter with the same name overrides "
                "upstream context."
            ),
        ],
    }


def _parameter_output_topic_data() -> ParameterOutputTopicDetails:
    return {
        "output_syntax": _parameter_output_syntax(),
        "sql_rules": [
            (
                "For one-row SQL results, OUT prop values are matched by result "
                "column name."
            ),
            "For multi-row SQL results, use LIST to capture result column values.",
        ],
        "examples": {
            "shell_constant": "echo '${setValue(row_count=42)}'",
            "shell_variable": 'echo "#{setValue(row_count=${lines_num})}"',
            "python": "print('${setValue(row_count=%s)}' % value)",
            "sql": "select count(*) as row_count from source_table",
        },
    }


def _parameter_property_fields() -> list[ParameterFieldData]:
    return [
        {
            "name": "prop",
            "required": True,
            "value_type": "string",
            "description": "Parameter name referenced as ${prop}.",
        },
        {
            "name": "direct",
            "required": False,
            "value_type": "enum",
            "description": "Direction of the parameter: IN or OUT.",
        },
        {
            "name": "type",
            "required": False,
            "value_type": "enum",
            "description": "DS parameter data type.",
        },
        {
            "name": "value",
            "required": False,
            "value_type": "string|null",
            "description": "Initial value or expression text.",
        },
    ]


def _parameter_reference_syntax() -> list[ParameterReferenceData]:
    return [
        {
            "syntax": "${name}",
            "description": (
                "Reference a workflow, project, upstream, or local parameter "
                "named name where DS parameter substitution is supported."
            ),
        },
        {
            "syntax": "${system.biz.date}",
            "description": "Reference a DS built-in system parameter.",
        },
        {
            "syntax": "$[yyyyMMdd-1]",
            "description": "Reference a DS time placeholder expression.",
        },
    ]


def _parameter_output_syntax() -> list[ParameterOutputData]:
    return [
        {
            "task_types": ["SHELL", "PYTHON", "REMOTESHELL"],
            "syntax": "${setValue(name=value)}",
            "description": (
                "Write this token to task logs to publish one OUT parameter."
            ),
        },
        {
            "task_types": ["SHELL", "PYTHON", "REMOTESHELL"],
            "syntax": "#{setValue(name=value)}",
            "description": (
                "Alternative output token parsed from script-like task logs."
            ),
        },
        {
            "task_types": ["SQL"],
            "syntax": "result column named like an OUT prop",
            "description": (
                "SQL tasks can publish result columns whose names match OUT "
                "parameter prop values."
            ),
        },
    ]


def supported_task_template_variants() -> tuple[str, ...]:
    """Return every supported task template variant name."""
    return _task_templates.all_task_template_variants()


def supported_task_template_types() -> tuple[str, ...]:
    """Return the supported stable task template types."""
    return _task_templates.supported_task_template_types()


def supported_datasource_types() -> tuple[str, ...]:
    """Return datasource types supported by local payload templates."""
    return supported_datasource_template_types()


def typed_task_template_types() -> tuple[str, ...]:
    """Return task types backed by typed `task_params` models."""
    return _task_templates.typed_task_template_types()


def generic_task_template_types() -> tuple[str, ...]:
    """Return task types that currently emit generic raw `task_params` templates."""
    return _task_templates.generic_task_template_types()


def task_template_metadata() -> dict[str, TaskTemplateMetadata]:
    """Return task template metadata for all supported task types."""
    return _task_templates.task_template_metadata()


def workflow_template_result(*, with_schedule: bool = False) -> CommandResult:
    """Return the stable workflow YAML template."""
    yaml_text = _workflow_template_yaml(with_schedule=with_schedule)
    return CommandResult(
        data=require_json_object(
            {
                "yaml": yaml_text,
                "lines": _text_lines(yaml_text),
            },
            label="workflow template data",
        ),
        resolved={"with_schedule": with_schedule},
    )


def environment_config_template_result() -> CommandResult:
    """Return one DS environment shell/export config template."""
    lines = [
        EnvironmentConfigLineData(
            line="export JAVA_HOME=/opt/java",
            purpose="Java runtime used by shell, Java, and JVM-based task types.",
        ),
        EnvironmentConfigLineData(
            line="export HADOOP_HOME=/opt/hadoop",
            purpose="Hadoop client installation used by Hadoop ecosystem tasks.",
        ),
        EnvironmentConfigLineData(
            line="export HADOOP_CONF_DIR=/etc/hadoop/conf",
            purpose="Hadoop/YARN configuration directory visible to workers.",
        ),
        EnvironmentConfigLineData(
            line="export SPARK_HOME=/opt/spark",
            purpose="Spark client installation used by Spark tasks.",
        ),
        EnvironmentConfigLineData(
            line="export PYTHON_LAUNCHER=/opt/python/bin/python3",
            purpose="Python interpreter path used by Python-style tasks.",
        ),
        EnvironmentConfigLineData(
            line=("export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"),
            purpose="Expose selected runtimes on PATH without replacing worker PATH.",
        ),
    ]
    config = "\n".join(item["line"] for item in lines) + "\n"
    return CommandResult(
        data=require_json_object(
            EnvironmentConfigTemplateData(
                filename="env.sh",
                config=config,
                lines=lines,
                target_commands=[
                    "dsctl environment create --name NAME --config-file env.sh",
                    "dsctl environment update ENVIRONMENT --config-file env.sh",
                ],
                source_options=["--config TEXT", "--config-file PATH"],
                upstream_request_shape=(
                    "EnvironmentController form field `config` stores raw "
                    "shell/export text."
                ),
                rules=[
                    "Use shell/export syntax, not JSON.",
                    "Prefer --config-file for multiline environment configs.",
                    "The paths must exist on DolphinScheduler worker hosts.",
                    "Keep secrets out of environment configs when possible.",
                    "Bind worker groups with repeated --worker-group values.",
                ],
            ),
            label="environment config template data",
        ),
        resolved={"template": "environment.config"},
    )


def cluster_config_template_result() -> CommandResult:
    """Return one DS cluster config JSON template."""
    payload = {
        "k8s": _cluster_k8s_config_placeholder(),
        "yarn": "",
    }
    fields = [
        ClusterConfigFieldData(
            name="k8s",
            required=True,
            value_type="string",
            description=(
                "Kubernetes kubeconfig content. DS currently reads this field "
                "when resolving a cluster's Kubernetes config."
            ),
        ),
        ClusterConfigFieldData(
            name="yarn",
            required=False,
            value_type="string",
            description=(
                "Reserved by the DS UI shape; DS 3.4.1 does not actively use "
                "this field."
            ),
        ),
    ]
    return CommandResult(
        data=require_json_object(
            ClusterConfigTemplateData(
                filename="cluster-config.json",
                config=_cluster_config_json(payload),
                payload=payload,
                fields=fields,
                rows=fields,
                target_commands=[
                    (
                        "dsctl cluster create --name NAME "
                        "--config-file cluster-config.json"
                    ),
                    "dsctl cluster update CLUSTER --config-file cluster-config.json",
                ],
                source_options=["--config TEXT", "--config-file PATH"],
                upstream_request_shape=(
                    "ClusterController form field `config` stores a raw string; "
                    "DS 3.4.1 expects a JSON object for cluster config usage."
                ),
                upstream_ui_shape=(
                    "The DS 3.4.1 UI submits JSON.stringify({k8s, yarn})."
                ),
                rules=[
                    "Use JSON object syntax, not a bare kubeconfig string.",
                    "Prefer --config-file for multiline Kubernetes kubeconfigs.",
                    "Keep the k8s value as the full kubeconfig text.",
                    "Keep yarn as an empty string unless your DS deployment uses it.",
                    "The kubeconfig must be usable from DolphinScheduler API/workers.",
                ],
            ),
            label="cluster config template data",
        ),
        resolved={"template": "cluster.config"},
    )


def cluster_config_template_capability_data() -> ClusterConfigTemplateCapabilityData:
    """Return compact capability metadata for cluster config templates."""
    return {
        "command": "dsctl template cluster",
        "source_options": ["--config TEXT", "--config-file PATH"],
        "target_commands": [
            "dsctl cluster create --name NAME --config-file cluster-config.json",
            "dsctl cluster update CLUSTER --config-file cluster-config.json",
        ],
    }


def datasource_template_result(datasource_type: str | None = None) -> CommandResult:
    """Return datasource payload-template discovery or one JSON template."""
    if datasource_type is None:
        index_data = datasource_template_index_data()
        data = dict(index_data)
        data["rows"] = [
            {
                "type": datasource_type_name,
                "template_command": (
                    f"dsctl template datasource --type {datasource_type_name}"
                ),
            }
            for datasource_type_name in index_data["supported_types"]
        ]
        return CommandResult(
            data=require_json_object(
                data,
                label="datasource template index data",
            ),
            resolved={"view": "list"},
        )
    normalized_type = require_datasource_payload_type(datasource_type)
    template_data = datasource_template_data(normalized_type)
    data = dict(template_data)
    data["rows"] = template_data["fields"]
    return CommandResult(
        data=require_json_object(
            data,
            label="datasource template data",
        ),
        resolved={
            "view": "template",
            "datasource_type": normalized_type,
        },
    )


def task_template_result(
    task_type: str,
    *,
    variant: str | None = None,
) -> CommandResult:
    """Return one task YAML template for the requested task type."""
    normalized = _normalize_task_type(task_type)
    normalized_variant = _normalize_task_template_variant(normalized, variant)
    template_kind = _task_templates.task_template_kind(normalized)
    yaml_text = _task_templates.task_template_yaml(
        normalized,
        variant=normalized_variant,
    )
    return CommandResult(
        data=require_json_object(
            {
                "yaml": yaml_text,
                "rows": _text_lines(yaml_text),
            },
            label="task template data",
        ),
        resolved={
            "task_type": normalized,
            "task_category": _task_templates.task_template_category(normalized),
            "template_kind": template_kind,
            "variant": normalized_variant,
            "available_variants": list(
                _task_templates.task_template_variants(normalized)
            ),
        },
    )


def task_template_types_result() -> CommandResult:
    """Return the supported stable task template types."""
    task_types = list(supported_task_template_types())
    typed_task_types = list(typed_task_template_types())
    generic_task_types = list(generic_task_template_types())
    task_types_by_category = {
        category: list(task_types)
        for category, task_types in _task_template_types_by_category().items()
    }
    return CommandResult(
        data=require_json_object(
            _task_template_types_data(
                task_types=task_types,
                typed_task_types=typed_task_types,
                generic_task_types=generic_task_types,
                task_types_by_category=task_types_by_category,
                task_templates=task_template_metadata(),
                rows=_task_template_type_rows(),
            ),
            label="task template types data",
        ),
        resolved={"mode": "list"},
    )


def _normalize_task_type(task_type: str) -> str:
    normalized = canonical_task_type(task_type)
    suggestion = "Run `dsctl template task --list` to inspect supported task types."
    if not normalized:
        message = "TASK_TYPE is required."
        raise UserInputError(
            message,
            details={
                "available_task_types_count": len(_SUPPORTED_TASK_TEMPLATE_TYPES),
                "discovery_command": "dsctl template task --list",
            },
            suggestion=suggestion,
        )
    if normalized in _SUPPORTED_TASK_TEMPLATE_TYPES:
        return normalized
    message = f"Unsupported task template type '{task_type}'."
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "available_task_types_count": len(_SUPPORTED_TASK_TEMPLATE_TYPES),
            "discovery_command": "dsctl template task --list",
        },
        suggestion=suggestion,
    )


def _normalize_task_template_variant(task_type: str, variant: str | None) -> str:
    if variant is None:
        return _task_templates.task_template_variants(task_type)[0]
    normalized = variant.strip().lower().replace("_", "-")
    supported_variants = _task_templates.task_template_variants(task_type)
    if normalized in supported_variants:
        return normalized
    message = (
        f"Unsupported task template variant '{variant}' for task type '{task_type}'."
    )
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "variant": variant,
            "available_variants": list(supported_variants),
            "discovery_command": "dsctl template task --list",
        },
        suggestion="Run `dsctl template task --list` to inspect supported variants.",
    )


def _parameter_property_yaml() -> str:
    return dedent(
        """\
        # DS Property shape used by workflow.global_params and task_params.localParams.
        workflow:
          name: property-example-workflow
          global_params:
            bizdate: "${system.biz.date}"
        tasks:
          - name: shell-property-task
            type: SHELL
            task_params:
              rawScript: |
                echo "bizdate=${bizdate}"
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
              resourceList: []
              varPool: []
        """
    )


def _parameter_built_in_yaml() -> str:
    return dedent(
        """\
        # Built-in and named parameter references are preserved for DS runtime.
        workflow:
          name: built-in-parameter-workflow
          global_params:
            bizdate: "${system.biz.date}"
            curdate: "${system.biz.curdate}"
        tasks:
          - name: echo-built-ins
            type: SHELL
            command: |
              echo "bizdate=${bizdate}"
              echo "curdate=${curdate}"
              echo "task=${system.task.definition.name}"
        """
    )


def _parameter_time_yaml() -> str:
    return dedent(
        """\
        # DS evaluates $[...] time placeholders at workflow runtime.
        workflow:
          name: time-placeholder-workflow
          global_params:
            bizdate: "$[yyyyMMdd-1]"
            month_start: "$[month_first_day(yyyy-MM-dd,-1)]"
        tasks:
          - name: echo-time-placeholders
            type: SHELL
            command: |
              echo "bizdate=${bizdate}"
              echo "month_start=${month_start}"
        """
    )


def _workflow_template_yaml(*, with_schedule: bool) -> str:
    base = dedent(
        """\
        # Workflow YAML template for `dsctl workflow create --file ...`
        workflow:
          name: example-workflow
          project: example-project
          description: Example workflow definition
          timeout: 0
          global_params:
            bizdate: "${system.biz.date}"
          execution_type: PARALLEL
          release_state: OFFLINE
        tasks:
          - name: extract
            type: SHELL
            command: |
              echo "extract step"
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            # Optional task runtime controls:
            # flag: NO
            # environment_code: 42
            # task_group_id: 12
            # task_group_priority: 0
            # timeout_notify_strategy: WARN
            # cpu_quota: 50
            # memory_max: 1024
            delay: 0
            depends_on: []
          - name: load
            type: SHELL
            command: |
              echo "load step"
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            # Optional task runtime controls:
            # flag: NO
            # environment_code: 42
            # task_group_id: 12
            # task_group_priority: 0
            # timeout_notify_strategy: WARN
            # cpu_quota: 50
            # memory_max: 1024
            delay: 0
            depends_on:
              - extract
        """
    )
    if not with_schedule:
        return base
    schedule = dedent(
        """\
        schedule:
          cron: "0 0 2 * * ?"
          timezone: Asia/Shanghai
          start: "2026-01-01 00:00:00"
          end: "2026-12-31 23:59:59"
          enabled: false
        """
    )
    return f"{base}{schedule}"


def _cluster_k8s_config_placeholder() -> str:
    return dedent(
        """\
        apiVersion: v1
        kind: Config
        clusters:
          - cluster:
              certificate-authority-data: CHANGE_ME_BASE64_CA
              server: https://KUBERNETES_API_SERVER:6443
            name: kubernetes
        contexts:
          - context:
              cluster: kubernetes
              user: kubernetes-admin
            name: kubernetes-admin@kubernetes
        current-context: kubernetes-admin@kubernetes
        users:
          - name: kubernetes-admin
            user:
              client-certificate-data: CHANGE_ME_BASE64_CERT
              client-key-data: CHANGE_ME_BASE64_KEY
        """
    )


def _cluster_config_json(payload: dict[str, str]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def _task_template_types_data(
    *,
    task_types: list[str],
    typed_task_types: list[str],
    generic_task_types: list[str],
    task_types_by_category: dict[str, list[str]],
    task_templates: dict[str, TaskTemplateMetadata],
    rows: list[TaskTemplateTypeRowData],
) -> TaskTemplateTypesData:
    return {
        "task_types": task_types,
        "count": len(task_types),
        "typed_task_types": typed_task_types,
        "generic_task_types": generic_task_types,
        "task_types_by_category": task_types_by_category,
        "task_templates": task_templates,
        "rows": rows,
    }


def _task_template_type_rows() -> list[TaskTemplateTypeRowData]:
    metadata = task_template_metadata()
    return [
        TaskTemplateTypeRowData(
            task_type=task_type,
            kind=metadata[task_type]["kind"],
            category=metadata[task_type]["category"],
            default_variant=metadata[task_type]["default_variant"],
            variants=",".join(metadata[task_type]["variants"]),
        )
        for task_type in supported_task_template_types()
    ]


def _task_template_types_by_category() -> dict[str, tuple[str, ...]]:
    metadata = task_template_metadata()
    categories: dict[str, list[str]] = {}
    for task_type in supported_task_template_types():
        category = metadata[task_type]["category"]
        categories.setdefault(category, []).append(task_type)
    return {category: tuple(task_types) for category, task_types in categories.items()}


def _text_lines(text: str) -> list[TextLineData]:
    return [
        TextLineData(line_no=index, line=line)
        for index, line in enumerate(text.splitlines(), start=1)
    ]


_PARAMETER_SYNTAX_TOPICS = (
    "overview",
    "property",
    "built-in",
    "time",
    "context",
    "output",
    "all",
)
_SUPPORTED_TASK_TEMPLATE_TYPES = supported_task_template_types()
