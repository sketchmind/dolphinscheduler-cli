from __future__ import annotations

import json
from shlex import quote, split
from textwrap import dedent
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.command_contract import COMMAND_CATALOG
from dsctl.command_references import project_command_references
from dsctl.errors import UserInputError
from dsctl.models.task_spec import canonical_task_type
from dsctl.models.workflow_spec import WorkflowMetadataSpec
from dsctl.output import CommandResult, require_json_object
from dsctl.services import _task_templates
from dsctl.services._discovery_commands import render_discovery_command
from dsctl.services._workflow.authoring import (
    load_selected_task_authoring_catalog,
)
from dsctl.services.datasource_payload import (
    datasource_template_data,
    datasource_template_index_data,
    require_datasource_payload_type,
    supported_datasource_template_types,
)
from dsctl.services.enums import supported_enum_member_values
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog.parameter_guidance import (
    nested_workflow_parameter_rules,
)
from dsctl.services.version_resolution import resolve_version, selected_target_globals
from dsctl.services.workflow_examples import (
    normalize_workflow_example,
    workflow_example_yaml,
)
from dsctl.upstream.parameter_semantics import (
    ParameterSemanticsProfile,
    get_parameter_semantics,
)
from dsctl.upstream.schedules import schedule_contract_features
from dsctl.upstream.task_profiles import task_authoring_profile
from dsctl.upstream.workflows import supports_workflow_execution_type
from dsctl.versioning import DEFAULT_DS_VERSION

if TYPE_CHECKING:
    from dsctl.services._task_templates import TaskTemplateMetadata
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.support.yaml_io import JsonObject, JsonValue


def _template_result_data(value: object, *, label: str) -> JsonObject:
    """Project canonical command-pattern fields into template metadata."""
    data = project_command_references(value)
    return require_json_object(_target_template_references(data), label=label)


def _target_template_references(
    value: JsonValue, *, command_field: bool = False
) -> JsonValue:
    """Bind structured command references without rewriting artifacts or prose."""
    if isinstance(value, dict):
        return {
            key: _target_template_references(
                item,
                command_field=key == "command"
                or key.endswith(
                    ("_command", "_commands", "_command_pattern", "_command_patterns")
                ),
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _target_template_references(item, command_field=command_field)
            for item in value
        ]
    if command_field and isinstance(value, str) and value.startswith("dsctl "):
        return _targeted_template_hint(value)
    return value


class TaskTemplateTypesData(TypedDict):
    """Stable discovery payload for `dsctl template task`."""

    task_types: list[str]
    count: int
    typed_task_types: list[str]
    generic_task_types: list[str]
    task_types_by_category: dict[str, list[str]]
    default_task_type: str
    next_command: str
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
    rules: list[str]


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


class TextTemplateArtifactData(TypedDict):
    """Stable metadata for one emitted text template."""

    kind: str
    format: str
    raw_command: str
    target_command: str


class WorkflowPatchTemplateData(TypedDict):
    """Stable discovery payload for workflow patch YAML templates."""

    artifact: TextTemplateArtifactData
    yaml: str
    related_command_patterns: list[str]
    rules: list[str]


class EnvironmentConfigTemplateData(TypedDict):
    """Stable discovery payload for `dsctl template environment`."""

    filename: str
    config: str
    lines: list[EnvironmentConfigLineData]
    target_command_patterns: list[str]
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
    target_command_patterns: list[str]
    source_options: list[str]
    upstream_request_shape: str
    upstream_ui_shape: str
    rules: list[str]


class ClusterConfigTemplateCapabilityData(TypedDict):
    """Compact capability metadata for cluster config templates."""

    command: str
    source_options: list[str]
    target_command_patterns: list[str]


class TaskTemplateTypeRowData(TypedDict):
    """One compact task-template type row."""

    task_type: str
    kind: str
    category: str
    variants: list[str]
    next_command: str


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
            (
                "Run `dsctl template task TYPE` for the main task YAML "
                "and optional parameter examples."
            ),
            "Run `dsctl task-type schema TYPE` for bounded task field rules.",
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
    *,
    ds_version: str = DEFAULT_DS_VERSION,
) -> ParameterSyntaxIndexData | ParameterSyntaxTopicData:
    """Return DS parameter syntax metadata for workflow YAML authoring."""
    normalized_topic = _normalize_parameter_syntax_topic(topic)
    if normalized_topic == "overview":
        return parameter_syntax_index_data()
    topic_data = _parameter_syntax_topics_data(ds_version=ds_version)
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


def parameter_syntax_result(
    topic: str | None = None,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return stable DS parameter syntax metadata and examples."""
    selected_catalog = _selected_task_catalog(catalog, env_file=env_file)
    normalized_topic = _normalize_parameter_syntax_topic(topic or "overview")
    return CommandResult(
        data=_template_result_data(
            parameter_syntax_data(
                normalized_topic,
                ds_version=selected_catalog.profile_version,
            ),
            label="parameter syntax data",
        ),
        resolved={
            "topic": normalized_topic,
            "ds_version": selected_catalog.profile_version,
            "available_topics": list(supported_parameter_syntax_topics()),
            "ds_model": "Property",
            "template_variants": [
                task_type
                for task_type, metadata in task_template_metadata(
                    catalog=selected_catalog
                ).items()
                if metadata["parameter_fields"]
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


def _parameter_syntax_topics_data(
    *,
    ds_version: str,
) -> dict[str, ParameterTopicDetails]:
    semantics = get_parameter_semantics(ds_version)
    return {
        "property": _parameter_property_topic_data(ds_version=ds_version),
        "built-in": _parameter_built_in_topic_data(),
        "time": _parameter_time_topic_data(),
        "context": _parameter_context_topic_data(semantics=semantics),
        "output": _parameter_output_topic_data(semantics=semantics),
    }


def _parameter_property_topic_data(
    *,
    ds_version: str,
) -> ParameterPropertyTopicDetails:
    return {
        "ds_model": "Property",
        "property_fields": _parameter_property_fields(),
        "direct_values": list(
            supported_enum_member_values("direct", ds_version=ds_version)
        ),
        "type_values": list(
            supported_enum_member_values("data-type", ds_version=ds_version)
        ),
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


def _parameter_context_topic_data(
    *,
    semantics: ParameterSemanticsProfile,
) -> ParameterContextTopicDetails:
    scopes = {
        "workflow.global_params": "Workflow-wide parameters.",
        "task_params.localParams": "Task-local parameters.",
    }
    if semantics.startup.wire != "absent":
        scopes["startup"] = "Runtime parameters passed when starting a workflow."
    if semantics.resolution.project_parameters:
        scopes["project"] = "Project parameters are managed by `project-parameter`."
    if semantics.output.var_pool_transport:
        scopes["upstream_output"] = "OUT parameters passed from upstream dependencies."

    priority_names = {
        "context": "Upstream Output / VarPool",
        "startup": "Startup Parameter",
        "local": "Local Parameter",
        "global": "Global Parameter",
        "project": "Project Parameter",
        "builtin": "Built-in Parameter",
    }
    rules: list[str] = []
    if semantics.output.var_pool_transport:
        rules.append("Upstream-to-downstream passing is one-way along dependencies.")
        rules.append(
            "If no dependency path exists, local parameters are not passed upstream."
        )
    if semantics.output.downstream_binding == "declared-in-only":
        rules.append(
            "Downstream tasks must declare an IN parameter with the same prop "
            "to consume an upstream OUT value; the upstream varPool value "
            "then overrides its local fallback."
        )
    rules.append(
        "A self-referential local value such as prop=label and value=${label} "
        "shadows the same-name global and can create a circular placeholder; "
        "omit it to consume the global directly."
    )
    rules.extend(nested_workflow_parameter_rules(semantics))
    return {
        "scopes": scopes,
        "priority": [
            priority_names[source]
            for source in semantics.resolution.effective_precedence
        ],
        "rules": rules,
    }


def _parameter_output_topic_data(
    *,
    semantics: ParameterSemanticsProfile,
) -> ParameterOutputTopicDetails:
    output = semantics.output
    if output.set_value_parser == "absent":
        return {
            "output_syntax": [],
            "sql_rules": [],
            "examples": {},
            "rules": [
                (
                    f"DolphinScheduler {semantics.version} has no varPool-backed "
                    "task output publication for script-like or SQL tasks."
                )
            ],
        }

    supports_hash_syntax = output.set_value_parser != "dollar-line-start"
    accepts_inline_tokens = output.set_value_parser == "dollar-or-hash-stream"
    task_types = task_authoring_profile(semantics.version)["task_types"]
    supports_remote_shell = "REMOTESHELL" in task_types
    return {
        "output_syntax": _parameter_output_syntax(
            supports_hash_syntax=supports_hash_syntax,
            accepts_inline_tokens=accepts_inline_tokens,
            supports_remote_shell=supports_remote_shell,
        ),
        "sql_rules": [
            (
                "For one-row SQL results, OUT prop values are matched by result "
                "column name."
            ),
            "For multi-row SQL results, use LIST to capture result column values.",
        ],
        "examples": {
            "shell_constant": "echo '${setValue(row_count=42)}'",
            "shell_variable": (
                'echo "#{setValue(row_count=${lines_num})}"'
                if supports_hash_syntax
                else 'echo "${setValue(row_count=${lines_num})}"'
            ),
            "python": "print('${setValue(row_count=%s)}' % value)",
            "sql": "select count(*) as row_count from source_table",
        },
        "rules": [
            (
                "Script-like output tokens may appear anywhere in a task log line."
                if accepts_inline_tokens
                else "Script-like output tokens must begin a task log line."
            )
        ],
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


def _parameter_output_syntax(
    *,
    supports_hash_syntax: bool,
    accepts_inline_tokens: bool,
    supports_remote_shell: bool,
) -> list[ParameterOutputData]:
    task_types = ["SHELL", "PYTHON"]
    if supports_remote_shell:
        task_types.append("REMOTESHELL")
    placement = (
        "anywhere in a task log line"
        if accepts_inline_tokens
        else "at the beginning of a task log line"
    )
    output_syntax: list[ParameterOutputData] = [
        {
            "task_types": task_types,
            "syntax": "${setValue(name=value)}",
            "description": (
                f"Write this token {placement} to publish one OUT parameter."
            ),
        },
    ]
    if supports_hash_syntax:
        output_syntax.append(
            {
                "task_types": task_types,
                "syntax": "#{setValue(name=value)}",
                "description": (
                    f"Write this alternative token {placement} to publish one "
                    "OUT parameter."
                ),
            }
        )
    output_syntax.append(
        {
            "task_types": ["SQL"],
            "syntax": "result column named like an OUT prop",
            "description": (
                "SQL tasks can publish result columns whose names match OUT "
                "parameter prop values."
            ),
        },
    )
    return output_syntax


def supported_task_template_variants(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return every supported task template variant name."""
    return _task_templates.all_task_template_variants(catalog=catalog)


def supported_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return task types present in one exact profile."""
    return _task_templates.supported_task_template_types(catalog=catalog)


def supported_datasource_types(
    ds_version: str = DEFAULT_DS_VERSION,
) -> tuple[str, ...]:
    """Return datasource types supported by local payload templates."""
    return supported_datasource_template_types(ds_version)


def typed_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return source-reviewed typed task types for one exact profile."""
    return _task_templates.typed_task_template_types(catalog=catalog)


def generic_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return exact task types using unvalidated opaque templates."""
    return _task_templates.generic_task_template_types(catalog=catalog)


def task_template_metadata(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> dict[str, TaskTemplateMetadata]:
    """Return exact-profile task template metadata."""
    return _task_templates.task_template_metadata(catalog=catalog)


def workflow_template_result(
    *,
    with_schedule: bool = False,
    example: str | None = None,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return a workflow YAML template for the selected exact profile."""
    selected_catalog = _selected_task_catalog(catalog, env_file=env_file)
    selected_example = normalize_workflow_example(example)
    commands = _workflow_template_commands(
        with_schedule=with_schedule, env_file=env_file, example=selected_example
    )
    yaml_text = _workflow_template_yaml(
        with_schedule=with_schedule, catalog=selected_catalog, commands=commands
    )
    if selected_example != "basic":
        yaml_text = workflow_example_yaml(
            selected_example, yaml_text, catalog=selected_catalog, commands=commands
        )
    resolved: JsonObject = {
        "with_schedule": with_schedule,
        "ds_version": selected_catalog.profile_version,
    }
    if selected_example != "basic":
        resolved["example"] = selected_example
    return CommandResult(
        data=_template_result_data(
            {
                "artifact": {
                    "kind": "workflow-template",
                    "format": "yaml",
                    "raw_command": commands["raw"],
                    "target_command": commands["target"],
                },
                "yaml": yaml_text,
                "related_command_patterns": [
                    command
                    for name, command in commands.items()
                    if name not in {"raw", "target"}
                ],
            },
            label="workflow template data",
        ),
        resolved=resolved,
    )


def _targeted_template_hint(command: str) -> str:
    tokens = split(command)
    if any(
        token in {"--context", "--env-file"}
        or token.startswith(("--context=", "--env-file="))
        for token in tokens
    ):
        return command
    target_options = "".join(
        f" --{name} {quote(value)}" for name, value in selected_target_globals().items()
    )
    return command.replace("dsctl ", f"dsctl{target_options} ", 1)


def workflow_patch_template_result() -> CommandResult:
    """Return the stable workflow edit patch YAML template."""
    yaml_text = _workflow_patch_template_yaml()
    return CommandResult(
        data=_template_result_data(
            WorkflowPatchTemplateData(
                artifact=TextTemplateArtifactData(
                    kind="workflow-patch-template",
                    format="yaml",
                    raw_command=_targeted_template_hint(
                        "dsctl template workflow-patch --raw"
                    ),
                    target_command=_targeted_template_hint(
                        "dsctl workflow edit WORKFLOW --patch FILE"
                    ),
                ),
                yaml=yaml_text,
                related_command_patterns=[
                    _targeted_template_hint(command)
                    for command in [
                        "dsctl workflow edit WORKFLOW --patch FILE --dry-run",
                        "dsctl template task TYPE --raw",
                        "dsctl task-type schema TYPE",
                    ]
                ],
                rules=[
                    "Patch YAML is rooted at `patch:`.",
                    "workflow.set accepts definition-level metadata fields.",
                    (
                        "tasks.create[] uses full task fragments from "
                        "`dsctl template task`."
                    ),
                    (
                        "tasks.update[].set uses partial fields from "
                        "`dsctl task-type schema TYPE`."
                    ),
                    "tasks.rename[] preserves DS task identity across a name change.",
                    "Run --dry-run before mutating the workflow definition.",
                ],
            ),
            label="workflow patch template data",
        ),
        resolved={"template": "workflow.patch"},
    )


def workflow_instance_patch_template_result() -> CommandResult:
    """Return the stable workflow-instance edit patch YAML template."""
    yaml_text = _workflow_instance_patch_template_yaml()
    return CommandResult(
        data=_template_result_data(
            WorkflowPatchTemplateData(
                artifact=TextTemplateArtifactData(
                    kind="workflow-instance-patch-template",
                    format="yaml",
                    raw_command=_targeted_template_hint(
                        "dsctl template workflow-instance-patch --raw"
                    ),
                    target_command=_targeted_template_hint(
                        "dsctl workflow-instance edit WORKFLOW_INSTANCE --project "
                        "PROJECT --patch FILE"
                    ),
                ),
                yaml=yaml_text,
                related_command_patterns=[
                    _targeted_template_hint(command)
                    for command in [
                        (
                            "dsctl workflow-instance edit WORKFLOW_INSTANCE "
                            "--project PROJECT --patch FILE --dry-run"
                        ),
                        (
                            "dsctl workflow-instance edit WORKFLOW_INSTANCE "
                            "--project PROJECT --patch FILE --sync-definition"
                        ),
                        "dsctl template task TYPE --raw",
                        "dsctl task-type schema TYPE",
                    ]
                ],
                rules=[
                    "Patch YAML is rooted at `patch:`.",
                    (
                        "workflow-instance edit only accepts "
                        "workflow.set.global_params and workflow.set.timeout."
                    ),
                    (
                        "tasks.create[] uses full task fragments from "
                        "`dsctl template task`."
                    ),
                    (
                        "tasks.update[].set uses partial fields from "
                        "`dsctl task-type schema TYPE`."
                    ),
                    (
                        "Use --sync-definition only when the repaired instance "
                        "DAG should be written back to the workflow definition."
                    ),
                    "Run --dry-run before mutating the workflow instance.",
                ],
            ),
            label="workflow-instance patch template data",
        ),
        resolved={"template": "workflow-instance.patch"},
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
        data=_template_result_data(
            EnvironmentConfigTemplateData(
                filename="env.sh",
                config=config,
                lines=lines,
                target_command_patterns=[
                    "dsctl environment create --name NAME --config-file env.sh",
                    "dsctl environment update ENVIRONMENT --config-file env.sh",
                ],
                source_options=["--config CONFIG", "--config-file CONFIG_FILE"],
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


def _workflow_template_commands(
    *, with_schedule: bool, env_file: str | None, example: str = "basic"
) -> dict[str, str]:
    requests: dict[str, tuple[str, dict[str, str | bool]]] = {
        "raw": ("template.workflow", {"with-schedule": with_schedule, "raw": True}),
        "target": ("workflow.create", {"file": "FILE"}),
        "fields": ("schema", {"command": "workflow.create"}),
        "tasks": ("template.task", {"task_type": "TYPE", "raw": True}),
        "parameters": ("template.params", {"topic": "context"}),
        "lint": ("lint.workflow", {"file": "FILE"}),
        "preview": ("workflow.create", {"file": "FILE", "dry-run": True}),
    }
    if example != "basic":
        requests["raw"][1]["example"] = example
        requests.update(
            {
                "outputs": ("template.params", {"topic": "output"}),
                "basic": ("template.workflow", {"example": "basic", "raw": True}),
                "workflows": ("workflow.list", {"project": "PROJECT"}),
                "dependent_schema": ("task-type.schema", {"task_type": "DEPENDENT"}),
            }
        )
    return {
        name: COMMAND_CATALOG.render(
            action,
            values=values,
            global_values=selected_target_globals(env_file),
        )
        for name, (action, values) in requests.items()
    }


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
        data=_template_result_data(
            ClusterConfigTemplateData(
                filename="cluster-config.json",
                config=_cluster_config_json(payload),
                payload=payload,
                fields=fields,
                rows=fields,
                target_command_patterns=[
                    (
                        "dsctl cluster create --name NAME "
                        "--config-file cluster-config.json"
                    ),
                    "dsctl cluster update CLUSTER --config-file cluster-config.json",
                ],
                source_options=["--config CONFIG", "--config-file CONFIG_FILE"],
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
        "source_options": ["--config CONFIG", "--config-file CONFIG_FILE"],
        "target_command_patterns": [
            "dsctl cluster create --name NAME --config-file cluster-config.json",
            "dsctl cluster update CLUSTER --config-file cluster-config.json",
        ],
    }


def datasource_template_result(
    datasource_type: str | None = None,
    *,
    ds_version: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return datasource payload-template discovery or one JSON template."""
    selected_version = (
        resolve_version(env_file, mode="local").version
        if ds_version is None
        else ds_version
    )
    if datasource_type is None:
        index_data = datasource_template_index_data(version=selected_version)
        data = dict(index_data)
        data["rows"] = [
            {
                "type": datasource_type_name,
                "template_command": (
                    _datasource_template_command(
                        datasource_type_name,
                        selected_version,
                    )
                ),
            }
            for datasource_type_name in index_data["supported_types"]
        ]
        return CommandResult(
            data=_template_result_data(
                data,
                label="datasource template index data",
            ),
            resolved={"view": "list"},
        )
    normalized_type = require_datasource_payload_type(
        datasource_type,
        version=selected_version,
    )
    template_data = datasource_template_data(
        normalized_type,
        version=selected_version,
    )
    data = dict(template_data)
    data["rows"] = template_data["fields"]
    return CommandResult(
        data=_template_result_data(
            data,
            label="datasource template data",
        ),
        resolved={
            "view": "template",
            "datasource_type": normalized_type,
        },
    )


def _datasource_template_command(datasource_type: str, ds_version: str) -> str:
    return (
        f"dsctl template datasource --ds-version {ds_version} --type {datasource_type}"
    )


def _task_template_commands(
    task_type: str, variant: str | None, *, env_file: str | None
) -> dict[str, str]:
    raw_values: dict[str, str | bool] = {"task_type": task_type, "raw": True}
    if variant is not None:
        raw_values["variant"] = variant
    requests: dict[str, tuple[str, dict[str, str | bool]]] = {
        "raw": (
            "template.task",
            raw_values,
        ),
        "schema": ("task-type.schema", {"task_type": task_type}),
        "summary": ("task-type.get", {"task_type": task_type}),
        "index": ("template.task", {}),
        "parameters": ("template.params", {"topic": "context"}),
        "outputs": ("template.params", {"topic": "output"}),
    }
    return {
        name: COMMAND_CATALOG.render(
            action,
            values=values,
            global_values=selected_target_globals(env_file),
        )
        for name, (action, values) in requests.items()
    }


def _metadata_has_outputs(task_type: str, catalog: TaskAuthoringCatalog) -> bool:
    """Choose the installed output combination only where output transport exists."""
    return task_type in {"SHELL", "PYTHON", "REMOTESHELL", "SQL"} and bool(
        catalog.parameter_semantics.output.var_pool_transport
    )


def _task_template_navigation(
    task_type: str,
    yaml_text: str,
    *,
    catalog: TaskAuthoringCatalog,
    commands: dict[str, str],
    env_file: str | None,
) -> str:
    header = f"# Schema and value discovery: {commands['schema']}\n"
    example = {
        "SWITCH": "branch",
        "CONDITIONS": "branch",
        "SUB_WORKFLOW": "child",
        "DEPENDENT": "dependent",
    }.get(task_type, "output" if _metadata_has_outputs(task_type, catalog) else "basic")
    if example == "branch" and not catalog.supports_typed_authoring("SWITCH"):
        example = "basic"
    combination = COMMAND_CATALOG.render(
        "template.workflow",
        values={"example": example, "raw": True},
        global_values=selected_target_globals(env_file),
    )
    header += f"# Complete workflow example: {combination}\n"
    metadata = _task_templates.task_template_metadata(catalog=catalog)[task_type]
    if metadata["parameter_fields"] or task_type == "SUB_WORKFLOW":
        header += (
            f"# Parameter scopes: {commands['parameters']}\n"
            f"# OUT to downstream IN: {commands['outputs']}\n"
        )
    data = task_type_schema_result(task_type, catalog=catalog).data
    fields = data.get("fields", []) if isinstance(data, dict) else []
    choices: dict[str, list[str]] = {}
    for field in fields if isinstance(fields, list) else []:
        if not isinstance(field, dict):
            continue
        if not str(field.get("path", "")).startswith(("task_params.", "resources")):
            continue
        source = field.get("choice_source")
        if (
            not isinstance(source, str)
            or not source.startswith("dsctl ")
            or "enum list" in source
        ):
            continue
        # Keep the schema's reference chain; fields identify id/code/name/fullName.
        value = field.get("choice_value", "value")
        choices.setdefault(source, []).append(f"{field['path']}={value}")
    target_options = "".join(
        f" --{name} {quote(value)}"
        for name, value in selected_target_globals(env_file).items()
    )
    for source, paths in choices.items():
        command = (
            source
            if {"--context", "--env-file"} & set(split(source))
            else source.replace("dsctl ", f"dsctl{target_options} ", 1)
        )
        header += f"# Discover {', '.join(paths)}: {command}\n"
    return header + yaml_text


def task_template_result(
    task_type: str,
    *,
    variant: str | None = None,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return one task YAML template for the requested task type."""
    selected_catalog = _selected_task_catalog(catalog, env_file=env_file)
    normalized = _normalize_task_type(task_type, catalog=selected_catalog)
    normalized_variant = _normalize_task_template_variant(
        normalized,
        variant,
        catalog=selected_catalog,
        env_file=env_file,
    )
    template_kind = _task_templates.task_template_kind(
        normalized,
        catalog=selected_catalog,
    )
    yaml_text = _task_templates.task_template_yaml(
        normalized,
        variant=normalized_variant,
        catalog=selected_catalog,
    )
    metadata = _task_templates.task_template_metadata(catalog=selected_catalog)[
        normalized
    ]
    commands = _task_template_commands(
        normalized, normalized_variant, env_file=env_file
    )
    yaml_text = _task_template_navigation(
        normalized,
        yaml_text,
        catalog=selected_catalog,
        commands=commands,
        env_file=env_file,
    )
    return CommandResult(
        data=_template_result_data(
            {
                "artifact": {
                    "kind": "task-fragment",
                    "format": "yaml",
                    "paste_into": "workflow YAML tasks[]",
                    "raw_command": commands["raw"],
                },
                "yaml": yaml_text,
                "template": {
                    "task_type": normalized,
                    "category": _task_templates.task_template_category(
                        normalized,
                        catalog=selected_catalog,
                    ),
                    "kind": template_kind,
                    **(
                        {"variant": normalized_variant}
                        if normalized_variant is not None
                        else {}
                    ),
                    "variants": metadata["variants"],
                    "schema_command": commands["schema"],
                    "summary_command": commands["summary"],
                    "index_command": commands["index"],
                },
            },
            label="task template data",
        ),
        resolved={
            "task_type": normalized,
            "task_category": _task_templates.task_template_category(
                normalized,
                catalog=selected_catalog,
            ),
            "template_kind": template_kind,
            **(
                {"variant": normalized_variant}
                if normalized_variant is not None
                else {}
            ),
        },
    )


def task_template_types_result(
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return exact-profile typed and opaque task template types."""
    selected_catalog = _selected_task_catalog(catalog, env_file=env_file)
    task_types = list(supported_task_template_types(catalog=selected_catalog))
    typed_task_types = list(typed_task_template_types(catalog=selected_catalog))
    generic_task_types = list(generic_task_template_types(catalog=selected_catalog))
    task_types_by_category = {
        category: list(task_types)
        for category, task_types in _task_template_types_by_category(
            catalog=selected_catalog
        ).items()
    }
    return CommandResult(
        data=_template_result_data(
            _task_template_types_data(
                task_types=task_types,
                typed_task_types=typed_task_types,
                generic_task_types=generic_task_types,
                task_types_by_category=task_types_by_category,
                rows=_task_template_type_rows(
                    catalog=selected_catalog, env_file=env_file
                ),
            ),
            label="task template types data",
        ),
        resolved={"mode": "index"},
    )


def _normalize_task_type(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> str:
    normalized = canonical_task_type(task_type)
    supported_task_types = supported_task_template_types(catalog=catalog)
    suggestion = "Run `dsctl template task` to inspect supported task types."
    if not normalized:
        message = "TASK_TYPE is required."
        raise UserInputError(
            message,
            details={
                "available_task_types_count": len(supported_task_types),
                "discovery_command": "dsctl template task",
            },
            suggestion=suggestion,
        )
    if normalized in supported_task_types:
        return normalized
    message = f"Unsupported task template type '{task_type}'."
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "available_task_types_count": len(supported_task_types),
            "discovery_command": "dsctl template task",
        },
        suggestion=suggestion,
    )


def _normalize_task_template_variant(
    task_type: str,
    variant: str | None,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> str | None:
    if variant is None:
        return None
    normalized = variant.strip().lower().replace("_", "-")
    supported_variants = _task_templates.task_template_variants(
        task_type,
        catalog=catalog,
    )
    if normalized in supported_variants:
        return normalized
    message = (
        f"Unsupported task template variant '{variant}' for task type '{task_type}'."
    )
    discovery = render_discovery_command(
        "task-type.get", values={"task_type": task_type}, env_file=env_file
    )
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "variant": variant,
            "available_variants": _task_templates.task_template_metadata(
                catalog=catalog
            )[task_type]["variants"],
            "discovery_command": discovery,
        },
        suggestion=(
            f"Omit --variant for the default template, or run `{discovery}` "
            "to inspect independently useful scenarios."
        ),
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


def _workflow_patch_template_yaml() -> str:
    command = _targeted_template_hint("dsctl workflow edit WORKFLOW --patch FILE")
    header = f"# Workflow patch YAML template for `{command}`\n"
    return header + dedent(
        """\
        # Keep the root key as `patch:`. Remove unused operation blocks before use.
        patch:
          workflow:
            set:
              description: Updated workflow description
              # timeout: 60  # Optional workflow timeout in minutes.

          # Uncomment task operations as needed.
          #
          # tasks:
          #   create:
          #     - name: transform
          #       type: SHELL
          #       command: |
          #         echo "transform step"
          #       depends_on:
          #         - extract
          #
          #   update:
          #     - match:
          #         name: load
          #       set:
          #         depends_on:
          #           - transform
          #
          #   rename:
          #     - from: old-load
          #       to: load
          #
          #   delete:
          #     - obsolete
        """
    )


def _workflow_instance_patch_template_yaml() -> str:
    command = _targeted_template_hint(
        "dsctl workflow-instance edit WORKFLOW_INSTANCE --project PROJECT --patch FILE"
    )
    header = f"# Workflow-instance patch YAML template for:\n# `{command}`\n"
    return header + dedent(
        """\
        # Keep the root key as `patch:`. Remove unused operation blocks before use.
        # Only workflow.set.global_params and workflow.set.timeout are accepted.
        patch:
          # workflow:
          #   set:
          #     timeout: 60  # Optional minutes; do not change unrelated fields.
          #     global_params:
          #       repair_note: manual-repair
          tasks:
            update:
              - match:
                  name: failed-step
                set:
                  command: |
                    echo "patched failed step"

          # Uncomment task operations as needed.
          #
          # tasks:
          #   create:
          #     - name: repair-step
          #       type: SHELL
          #       command: |
          #         echo "repair step"
          #       depends_on:
          #         - failed-step
          #
          #   update:
          #     - match:
          #         name: failed-step
          #       set:
          #         command: |
          #           echo "patched failed step"
          #
          #   rename:
          #     - from: old-task
          #       to: repaired-task
          #
          #   delete:
          #     - obsolete-task
        """
    )


def _workflow_template_yaml(
    *,
    with_schedule: bool,
    catalog: TaskAuthoringCatalog,
    commands: dict[str, str],
) -> str:
    metadata = WorkflowMetadataSpec(name="example-workflow", project="example-project")
    release_state = "ONLINE" if with_schedule else metadata.release_state.value
    workflow = dedent(
        f"""\
        # Workflow YAML template for `{commands["target"]}`
        # Replace names/project; the project must exist. Task names must be unique.
        # depends_on lists upstream task names in this file; extract -> load below.
        # YAML fields and tenant/run/schedule settings: {commands["fields"]}
        # Task fragments and runtime controls (replace TYPE): {commands["tasks"]}
        # Parameter scope and precedence: {commands["parameters"]}
        # Check locally: {commands["lint"]}
        # Preview against the target without creating: {commands["preview"]}
        workflow:
          name: {metadata.name}
          project: {metadata.project}
          # description: Example workflow definition
          # timeout: {metadata.timeout}  # Minutes; 0 disables the workflow timeout.
          # Shared by both tasks; task-only parameters belong in task_params.localParams
          # For localParams, replace command with the task's native task_params form.
          # Remove example globals for tasks that require a parameter-free workflow.
          global_params:
            bizdate: "${{system.biz.date}}"
        """
    )
    if supports_workflow_execution_type(catalog.profile_version):
        workflow += f"  # execution_type: {metadata.execution_type.value}\n"
    if with_schedule:
        workflow += "  # ONLINE is required before creating the attached schedule.\n"
    workflow += f"  release_state: {release_state}\ntasks:\n"
    for name, dependencies in (("extract", "[]"), ("load", "[extract]")):
        workflow += (
            f"  - name: {name}\n"
            "    type: SHELL\n"
            "    command: |\n"
            f'      echo "{name} ${{bizdate}}"\n'
            f"    depends_on: {dependencies}\n"
        )
    if not with_schedule:
        return workflow
    schedule_features = schedule_contract_features(catalog.profile_version)
    timezone_line = (
        "  timezone: Asia/Shanghai\n"
        if schedule_features.timezone
        else "  # Uses the DolphinScheduler server-local timezone.\n"
    )
    missed_fire_line = (
        "  # Missed fires: omission uses the native create default; "
        "update omission preserves it.\n"
        f"  # missed_fire_policy: {schedule_features.missed_fire_policy_default}\n"
        if schedule_features.missed_fire_policy
        else ""
    )
    schedule = (
        "schedule:\n"
        '  cron: "0 0 2 * * ?"  # Quartz cron: daily at 02:00.\n'
        f"{timezone_line}"
        f"{missed_fire_line}"
        "  # Replace this example date range with the required scheduling window.\n"
        '  start: "2026-01-01 00:00:00"\n'
        '  end: "2026-12-31 23:59:59"\n'
        "  enabled: false  # Keep the schedule offline until ready.\n"
    )
    return f"{workflow}{schedule}"


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
    rows: list[TaskTemplateTypeRowData],
) -> TaskTemplateTypesData:
    return {
        "task_types": task_types,
        "count": len(task_types),
        "typed_task_types": typed_task_types,
        "generic_task_types": generic_task_types,
        "task_types_by_category": task_types_by_category,
        "default_task_type": "SHELL",
        "next_command": "dsctl task-type get SHELL",
        "rows": rows,
    }


def _task_template_type_rows(
    *,
    catalog: TaskAuthoringCatalog,
    env_file: str | None = None,
) -> list[TaskTemplateTypeRowData]:
    metadata = task_template_metadata(catalog=catalog)
    return [
        TaskTemplateTypeRowData(
            task_type=task_type,
            kind=metadata[task_type]["kind"],
            category=metadata[task_type]["category"],
            variants=metadata[task_type]["variants"],
            next_command=render_discovery_command(
                "task-type.get", values={"task_type": task_type}, env_file=env_file
            ),
        )
        for task_type in supported_task_template_types(catalog=catalog)
    ]


def _task_template_types_by_category(
    *,
    catalog: TaskAuthoringCatalog,
) -> dict[str, tuple[str, ...]]:
    metadata = task_template_metadata(catalog=catalog)
    categories: dict[str, list[str]] = {}
    for task_type in supported_task_template_types(catalog=catalog):
        category = metadata[task_type]["category"]
        categories.setdefault(category, []).append(task_type)
    return {category: tuple(task_types) for category, task_types in categories.items()}


def _selected_task_catalog(
    catalog: TaskAuthoringCatalog | None,
    *,
    env_file: str | None,
) -> TaskAuthoringCatalog:
    if catalog is not None:
        return catalog
    if env_file is not None:
        return load_selected_task_authoring_catalog(env_file)
    return load_selected_task_authoring_catalog(None)


_PARAMETER_SYNTAX_TOPICS = (
    "overview",
    "property",
    "built-in",
    "time",
    "context",
    "output",
    "all",
)
