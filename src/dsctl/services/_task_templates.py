from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent, indent
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.models.task_spec import supported_typed_task_types
from dsctl.upstream import (
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)

if TYPE_CHECKING:
    from collections.abc import Callable

TaskTemplateKind = Literal["generic", "typed"]


class TaskTemplateMetadata(TypedDict):
    """Machine-readable authoring metadata for one task template type."""

    kind: TaskTemplateKind
    category: str
    default_variant: str
    variants: list[str]
    variant_summaries: dict[str, str]
    payload_modes: list[str]
    parameter_fields: list[str]
    resource_fields: list[str]


@dataclass(frozen=True)
class TaskTemplateVariant:
    """One renderable task template scenario."""

    name: str
    summary: str
    builder: Callable[[], str]
    payload_modes: tuple[str, ...]
    parameter_fields: tuple[str, ...] = ()
    resource_fields: tuple[str, ...] = ()


def supported_task_template_types() -> tuple[str, ...]:
    """Return the supported stable task template types."""
    return _SUPPORTED_TASK_TEMPLATE_TYPES


def typed_task_template_types() -> tuple[str, ...]:
    """Return task types backed by typed `task_params` models."""
    return _TYPED_TASK_TEMPLATE_TYPES


def generic_task_template_types() -> tuple[str, ...]:
    """Return task types that currently emit generic raw `task_params` templates."""
    return _GENERIC_TASK_TEMPLATE_TYPES


def all_task_template_variants() -> tuple[str, ...]:
    """Return every known task template variant name."""
    variant_names = {
        variant.name for variants in _VARIANTS.values() for variant in variants
    }
    return tuple(sorted(variant_names))


def task_template_variants(task_type: str) -> tuple[str, ...]:
    """Return variant names supported by one normalized task type."""
    return tuple(variant.name for variant in _variants_for(task_type))


def task_template_kind(task_type: str) -> TaskTemplateKind:
    """Return the template kind for one normalized task type."""
    return "typed" if task_type in _TYPED_TASK_TEMPLATE_TYPES else "generic"


def task_template_category(task_type: str) -> str:
    """Return the upstream default category for one normalized task type."""
    return _TASK_TYPE_TO_CATEGORY[task_type]


def task_template_metadata() -> dict[str, TaskTemplateMetadata]:
    """Return task template metadata for all supported task types."""
    return {
        task_type: _metadata_for(task_type)
        for task_type in _SUPPORTED_TASK_TEMPLATE_TYPES
    }


def task_template_yaml(task_type: str, *, variant: str = "minimal") -> str:
    """Render one task template for a normalized type and variant."""
    for candidate in _variants_for(task_type):
        if candidate.name == variant:
            return candidate.builder()
    message = (
        f"Unsupported task template variant '{variant}' for task type '{task_type}'"
    )
    raise KeyError(message)


def _metadata_for(task_type: str) -> TaskTemplateMetadata:
    variants = _variants_for(task_type)
    return {
        "kind": task_template_kind(task_type),
        "category": task_template_category(task_type),
        "default_variant": variants[0].name,
        "variants": [variant.name for variant in variants],
        "variant_summaries": {variant.name: variant.summary for variant in variants},
        "payload_modes": sorted(
            {mode for variant in variants for mode in variant.payload_modes}
        ),
        "parameter_fields": sorted(
            {field for variant in variants for field in variant.parameter_fields}
        ),
        "resource_fields": sorted(
            {field for variant in variants for field in variant.resource_fields}
        ),
    }


def _variants_for(task_type: str) -> tuple[TaskTemplateVariant, ...]:
    variants = _VARIANTS.get(task_type)
    if variants is not None:
        return variants
    return (
        TaskTemplateVariant(
            name="minimal",
            summary="Generic DS-native task_params placeholder.",
            builder=lambda: _generic_task_template_yaml(task_type),
            payload_modes=("task_params",),
        ),
    )


def _workflow_script_resource_fields() -> tuple[str, ...]:
    return ("task_params.resourceList[].resourceName",)


def _task_parameter_fields() -> tuple[str, ...]:
    return ("task_params.localParams[]", "task_params.varPool[]")


def _shell_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SHELL
            name: shell-task
            type: SHELL
            description: Example shell task
            command: |
              echo "hello from DolphinScheduler"
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _shell_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SHELL with dynamic parameters
            name: shell-params-task
            type: SHELL
            description: Use IN params and emit one OUT param
            task_params:
              rawScript: |
                echo "bizdate=${bizdate}"
                echo '${setValue(row_count=42)}'
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
                - prop: row_count
                  direct: OUT
                  type: INTEGER
                  value: "0"
              resourceList: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _shell_resource_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SHELL with attached DS resources
            name: shell-resource-task
            type: SHELL
            description: Run a shell script attached from DS resources
            task_params:
              rawScript: |
                bash scripts/job.sh
              resourceList:
                - resourceName: /tenant/resources/scripts/job.sh
              localParams: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _python_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for PYTHON
            name: python-task
            type: PYTHON
            description: Example python task
            command: |
              print("hello from DolphinScheduler")
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _python_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for PYTHON with dynamic parameters
            name: python-params-task
            type: PYTHON
            description: Use IN params and emit one OUT param
            task_params:
              rawScript: |
                print("bizdate=${bizdate}")
                print("${setValue(row_count=42)}")
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
                - prop: row_count
                  direct: OUT
                  type: INTEGER
                  value: "0"
              resourceList: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _python_resource_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for PYTHON with attached DS resources
            name: python-resource-task
            type: PYTHON
            description: Run a python script attached from DS resources
            task_params:
              rawScript: |
                python scripts/job.py
              resourceList:
                - resourceName: /tenant/resources/scripts/job.py
              localParams: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _remote_shell_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for REMOTESHELL
            name: remote-shell-task
            type: REMOTESHELL
            description: Example remote shell task
            task_params:
              rawScript: |
                echo "hello from remote shell"
              type: SSH
              datasource: 1
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _remote_shell_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for REMOTESHELL with dynamic parameters
            name: remote-shell-params-task
            type: REMOTESHELL
            description: Use IN params and emit one OUT param on a remote host
            task_params:
              rawScript: |
                echo "bizdate=${bizdate}"
                echo '${setValue(row_count=42)}'
              type: SSH
              datasource: 1
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
        )
    )


def _sql_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SQL
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
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _sql_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SQL with dynamic parameters
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
        )
    )


def _sql_pre_post_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SQL with pre/post statements
            name: sql-pre-post-task
            type: SQL
            description: Run SQL with setup and cleanup statements
            task_params:
              type: MYSQL
              datasource: 1
              sql: |
                insert into target_table
                select * from staging_table;
              sqlType: 1
              sendEmail: false
              displayRows: 10
              showType: TABLE
              connParams: ""
              preStatements:
                - set session sql_mode = 'STRICT_TRANS_TABLES'
              postStatements:
                - analyze table target_table
              groupId: 0
              title: ""
              limit: 0
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _http_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for HTTP
            name: http-task
            type: HTTP
            description: Example HTTP task
            task_params:
              url: https://example.test/health
              httpMethod: GET
              httpParams: []
              httpBody: ""
              httpCheckCondition: STATUS_CODE_DEFAULT
              condition: ""
              connectTimeout: 10000
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _http_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for HTTP with dynamic parameters
            name: http-params-task
            type: HTTP
            description: Call an HTTP endpoint with one IN param
            task_params:
              url: https://example.test/jobs/${bizdate}
              httpMethod: GET
              httpParams:
                - prop: X-Bizdate
                  httpParametersType: HEADERS
                  value: ${bizdate}
              httpBody: ""
              httpCheckCondition: STATUS_CODE_DEFAULT
              condition: ""
              connectTimeout: 10000
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _http_post_json_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for HTTP POST with JSON body
            name: http-post-json-task
            type: HTTP
            description: Call an HTTP JSON endpoint
            task_params:
              url: https://example.test/jobs
              httpMethod: POST
              httpParams:
                - prop: Content-Type
                  httpParametersType: HEADERS
                  value: application/json
              httpBody: |
                {"job": "daily-etl"}
              httpCheckCondition: STATUS_CODE_DEFAULT
              condition: ""
              connectTimeout: 10000
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _sub_workflow_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SUB_WORKFLOW
            name: child-workflow-task
            type: SUB_WORKFLOW
            description: Example sub-workflow task
            task_params:
              workflowDefinitionCode: 1000000000001
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _sub_workflow_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SUB_WORKFLOW with dynamic parameters
            name: child-workflow-params-task
            type: SUB_WORKFLOW
            description: Run one child workflow and pass local parameters
            task_params:
              workflowDefinitionCode: 1000000000001
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
              resourceList: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _dependent_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for DEPENDENT
            name: dependent-task
            type: DEPENDENT
            description: Example dependent task
            task_params:
              dependence:
                relation: AND
                checkInterval: 10
                failurePolicy: DEPENDENT_FAILURE_FAILURE
                dependTaskList:
                  - relation: AND
                    dependItemList:
                      - dependentType: DEPENDENT_ON_WORKFLOW
                        projectCode: 1
                        definitionCode: 1000000000001
                        depTaskCode: 0
                        cycle: day
                        dateValue: last1Days
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _dependent_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for DEPENDENT with dynamic parameters
            name: dependent-params-task
            type: DEPENDENT
            description: Wait for an upstream workflow with a parameterized date window
            task_params:
              dependence:
                relation: AND
                checkInterval: 10
                failurePolicy: DEPENDENT_FAILURE_FAILURE
                dependTaskList:
                  - relation: AND
                    dependItemList:
                      - dependentType: DEPENDENT_ON_WORKFLOW
                        projectCode: 1
                        definitionCode: 1000000000001
                        depTaskCode: 0
                        cycle: day
                        dateValue: ${date_window}
              localParams:
                - prop: date_window
                  direct: IN
                  type: VARCHAR
                  value: last1Days
              resourceList: []
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _switch_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SWITCH
            name: switch-task
            type: SWITCH
            description: Example switch task
            task_params:
              switchResult:
                dependTaskList:
                  - condition: ${route} == "A"
                    nextNode: task-a
                  - condition: ${route} == "B"
                    nextNode: task-b
                nextNode: task-default
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _switch_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for SWITCH with dynamic parameters
            name: switch-params-task
            type: SWITCH
            description: Route branches from a parameter value
            task_params:
              switchResult:
                dependTaskList:
                  - condition: ${route} == "A"
                    nextNode: task-a
                  - condition: ${route} == "B"
                    nextNode: task-b
                nextNode: task-default
              localParams:
                - prop: route
                  direct: IN
                  type: VARCHAR
                  value: A
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _conditions_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for CONDITIONS
            name: conditions-task
            type: CONDITIONS
            description: Example conditions task
            task_params:
              dependence:
                relation: AND
                dependTaskList:
                  - relation: AND
                    dependItemList:
                      - dependentType: DEPENDENT_ON_TASK
                        projectCode: 1
                        definitionCode: 1000000000001
                        depTaskCode: 1000000000002
                        cycle: day
                        dateValue: today
                        status: SUCCESS
              conditionResult:
                successNode:
                  - on-success
                failedNode:
                  - on-failed
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _conditions_params_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for CONDITIONS with dynamic parameters
            name: conditions-params-task
            type: CONDITIONS
            description: Route downstream branches and expose task params explicitly
            task_params:
              dependence:
                relation: AND
                dependTaskList:
                  - relation: AND
                    dependItemList:
                      - dependentType: DEPENDENT_ON_TASK
                        projectCode: 1
                        definitionCode: 1000000000001
                        depTaskCode: 1000000000002
                        cycle: day
                        dateValue: today
                        status: SUCCESS
              conditionResult:
                successNode:
                  - on-success
                failedNode:
                  - on-failed
              localParams:
                - prop: bizdate
                  direct: IN
                  type: VARCHAR
                  value: ${system.biz.date}
              varPool: []
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _generic_task_template_yaml(task_type: str) -> str:
    task_name = _task_template_name(task_type)
    return _task_template_with_runtime_controls(
        dedent(
            f"""\
            # Task template for {task_type}
            # Replace task_params with the DS-native plugin payload for this task type.
            name: {task_name}
            type: {task_type}
            description: Example {task_type} task
            task_params: {{}}
            worker_group: default
            priority: MEDIUM
            retry:
              times: 0
              interval: 0
            timeout: 0
            """
        )
    )


def _task_template_with_runtime_controls(base: str) -> str:
    """Append the shared task-runtime comment block to one task template."""
    return f"{base}{_task_runtime_controls_comment_block()}delay: 0\ndepends_on: []\n"


def _task_runtime_controls_comment_block(*, indent_level: int = 0) -> str:
    """Return one shared commented task-runtime block for authoring templates."""
    block = dedent(
        """\
        # Optional task runtime controls:
        # flag: NO
        # environment_code: 42
        # task_group_id: 12
        # task_group_priority: 0
        # timeout_notify_strategy: WARN
        # cpu_quota: 50
        # memory_max: 1024
        """
    )
    return indent(block, " " * indent_level)


def _task_template_name(task_type: str) -> str:
    return f"{task_type.lower().replace('_', '-')}-task"


def _validated_typed_task_template_types() -> tuple[str, ...]:
    expected = supported_typed_task_types()
    actual = tuple(sorted(_VARIANTS))
    if actual == expected:
        return expected

    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    reasons: list[str] = []
    if missing:
        reasons.append(f"missing builders: {', '.join(missing)}")
    if unexpected:
        reasons.append(f"unexpected builders: {', '.join(unexpected)}")
    details = "; ".join(reasons)
    message = "Task template builders must stay aligned with typed task specs"
    if details:
        message = f"{message} ({details})"
    raise RuntimeError(message)


def _validated_supported_task_template_types() -> tuple[str, ...]:
    supported = upstream_default_task_types()
    missing_typed = sorted(set(_TYPED_TASK_TEMPLATE_TYPES) - set(supported))
    if missing_typed:
        message = (
            "Task template support must include every typed task spec in the "
            f"upstream default task-type set (missing: {', '.join(missing_typed)})"
        )
        raise RuntimeError(message)
    return supported


_VARIANTS: dict[str, tuple[TaskTemplateVariant, ...]] = {
    "CONDITIONS": (
        TaskTemplateVariant(
            name="minimal",
            summary="Route downstream branches from upstream task states.",
            builder=_conditions_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="CONDITIONS example with explicit localParams and varPool fields.",
            builder=_conditions_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="condition-routing",
            summary="Explicit CONDITIONS example with success and failure targets.",
            builder=_conditions_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "DEPENDENT": (
        TaskTemplateVariant(
            name="minimal",
            summary="Wait for an upstream workflow or task dependency.",
            builder=_dependent_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="DEPENDENT example with a parameterized dateValue.",
            builder=_dependent_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="workflow-dependency",
            summary="DEPENDENT example targeting an upstream workflow.",
            builder=_dependent_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "HTTP": (
        TaskTemplateVariant(
            name="minimal",
            summary="HTTP GET health-check style task.",
            builder=_http_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="HTTP example with localParams used in URL and headers.",
            builder=_http_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="post-json",
            summary="HTTP POST task with JSON body and Content-Type header.",
            builder=_http_post_json_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "PYTHON": (
        TaskTemplateVariant(
            name="minimal",
            summary="Inline python command shorthand.",
            builder=_python_template_yaml,
            payload_modes=("command",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="PYTHON example with IN localParams and one OUT varPool value.",
            builder=_python_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="resource",
            summary="Run a python file attached through DS resourceList.",
            builder=_python_resource_template_yaml,
            payload_modes=("task_params",),
            resource_fields=_workflow_script_resource_fields(),
        ),
    ),
    "REMOTESHELL": (
        TaskTemplateVariant(
            name="minimal",
            summary="Remote shell task using datasource-backed SSH settings.",
            builder=_remote_shell_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary=(
                "REMOTESHELL example with IN localParams and one OUT varPool value."
            ),
            builder=_remote_shell_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="datasource",
            summary="REMOTESHELL example showing type and datasource fields.",
            builder=_remote_shell_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "SHELL": (
        TaskTemplateVariant(
            name="minimal",
            summary="Inline shell command shorthand.",
            builder=_shell_template_yaml,
            payload_modes=("command",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="SHELL example with IN localParams and one OUT varPool value.",
            builder=_shell_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="resource",
            summary="Run a shell file attached through DS resourceList.",
            builder=_shell_resource_template_yaml,
            payload_modes=("task_params",),
            resource_fields=_workflow_script_resource_fields(),
        ),
    ),
    "SQL": (
        TaskTemplateVariant(
            name="minimal",
            summary="SQL task with datasource, sqlType, and result display fields.",
            builder=_sql_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="SQL example with IN localParams and one OUT result column.",
            builder=_sql_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="pre-post-statements",
            summary="SQL task with preStatements and postStatements.",
            builder=_sql_pre_post_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "SUB_WORKFLOW": (
        TaskTemplateVariant(
            name="minimal",
            summary="Run one child workflow definition by code.",
            builder=_sub_workflow_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary=(
                "SUB_WORKFLOW example with explicit localParams and varPool fields."
            ),
            builder=_sub_workflow_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="child-workflow",
            summary="SUB_WORKFLOW example showing workflowDefinitionCode.",
            builder=_sub_workflow_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
    "SWITCH": (
        TaskTemplateVariant(
            name="minimal",
            summary="Route to the first matching branch or a default branch.",
            builder=_switch_template_yaml,
            payload_modes=("task_params",),
        ),
        TaskTemplateVariant(
            name="params",
            summary="SWITCH example with branch routing from localParams.",
            builder=_switch_params_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
        TaskTemplateVariant(
            name="branching",
            summary="SWITCH example with named branch targets.",
            builder=_switch_template_yaml,
            payload_modes=("task_params",),
        ),
    ),
}

_TYPED_TASK_TEMPLATE_TYPES = _validated_typed_task_template_types()
_TASK_TEMPLATE_TYPES_BY_CATEGORY = upstream_default_task_types_by_category()
_SUPPORTED_TASK_TEMPLATE_TYPES = _validated_supported_task_template_types()
_GENERIC_TASK_TEMPLATE_TYPES = tuple(
    task_type
    for task_type in _SUPPORTED_TASK_TEMPLATE_TYPES
    if task_type not in _TYPED_TASK_TEMPLATE_TYPES
)
_TASK_TYPE_TO_CATEGORY = {
    task_type: category
    for category, task_types in _TASK_TEMPLATE_TYPES_BY_CATEGORY.items()
    for task_type in task_types
}
