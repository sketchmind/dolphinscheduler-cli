from __future__ import annotations

from dataclasses import dataclass, replace
from functools import cache
from textwrap import dedent
from typing import TYPE_CHECKING, Literal, TypedDict, cast

import yaml

from dsctl.models.common import is_yaml_object
from dsctl.models.task_spec import supported_typed_task_types
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringCatalog,
    default_task_authoring_catalog,
)
from dsctl.services.task_authoring_catalog import (
    task_template_with_runtime_controls as _task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.parameter_guidance import (
    nested_workflow_parameter_rules,
    switch_parameter_guidance,
    switch_uses_local_params,
)
from dsctl.services.task_authoring_catalog.templates import (
    generic_task_runtime_limitation,
    project_task_runtime_template,
)
from dsctl.upstream import (
    upstream_default_task_types,
    upstream_default_task_types_by_category,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.models.common import YamlObject, YamlValue

TaskTemplateKind = Literal["generic", "typed"]


class TaskTemplateMetadata(TypedDict):
    """Machine-readable authoring metadata for one task template type."""

    kind: TaskTemplateKind
    category: str
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
    purpose: Literal["scenario", "option"] = "scenario"


@dataclass(frozen=True)
class _DefaultTaskTemplateIndex:
    typed: tuple[str, ...]
    supported: tuple[str, ...]
    generic: tuple[str, ...]


@cache
def _default_task_template_index() -> _DefaultTaskTemplateIndex:
    typed = _validated_typed_task_template_types()
    supported = _validated_supported_task_template_types(typed)
    return _DefaultTaskTemplateIndex(
        typed=typed,
        supported=supported,
        generic=tuple(task_type for task_type in supported if task_type not in typed),
    )


def _ordered_authorable_task_types(
    catalog: TaskAuthoringCatalog,
    *,
    baseline: tuple[str, ...],
) -> tuple[str, ...]:
    """Keep baseline ordering while admitting exact-profile-only types."""
    authorable = frozenset(catalog.authorable_task_types)
    stable = tuple(task_type for task_type in baseline if task_type in authorable)
    extra = tuple(sorted(authorable - set(baseline)))
    return (*stable, *extra)


def supported_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return exact-profile task types, retaining stable default ordering."""
    if catalog is None:
        return _default_task_template_index().supported
    return _ordered_authorable_task_types(
        catalog,
        baseline=upstream_default_task_types(),
    )


def typed_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return task types whose exact profile passed typed authoring review."""
    if catalog is None:
        return _default_task_template_index().typed
    return tuple(sorted(catalog.reviewed_typed_task_types))


def generic_task_template_types(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return exact task types emitted as opaque task_params templates."""
    if catalog is None:
        return _default_task_template_index().generic
    typed = frozenset(typed_task_template_types(catalog=catalog))
    return tuple(
        task_type
        for task_type in supported_task_template_types(catalog=catalog)
        if task_type not in typed
    )


def all_task_template_variants(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return every known task template variant name."""
    selected_catalog = _selected_catalog(catalog)
    variant_names = {
        variant.name
        for task_type in supported_task_template_types(catalog=selected_catalog)
        for variant in _variants_for(task_type, catalog=selected_catalog)
        if variant.name != "minimal"
    }
    return tuple(sorted(variant_names))


def task_template_variants(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[str, ...]:
    """Return variant names supported by one normalized task type."""
    return tuple(
        variant.name
        for variant in _variants_for(task_type, catalog=_selected_catalog(catalog))
        if variant.name != "minimal"
    )


def task_template_kind(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> TaskTemplateKind:
    """Return the template kind for one normalized task type."""
    selected_catalog = _selected_catalog(catalog)
    entry = selected_catalog.entries.get(task_type)
    if entry is not None:
        return cast("TaskTemplateKind", entry.kind)
    return (
        "typed" if selected_catalog.supports_typed_authoring(task_type) else "generic"
    )


def task_template_category(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> str:
    """Return the upstream default category for one normalized task type."""
    entry = _selected_catalog(catalog).entries.get(task_type)
    if entry is not None:
        return entry.category
    return _TASK_TYPE_TO_CATEGORY.get(task_type, "Upstream")


def task_template_metadata(
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> dict[str, TaskTemplateMetadata]:
    """Return task template metadata for all supported task types."""
    selected_catalog = _selected_catalog(catalog)
    return {
        task_type: _metadata_for(task_type, catalog=selected_catalog)
        for task_type in supported_task_template_types(catalog=selected_catalog)
    }


def task_template_yaml(
    task_type: str,
    *,
    variant: str | None = None,
    catalog: TaskAuthoringCatalog | None = None,
) -> str:
    """Render one task template for a normalized type and variant."""
    selected_catalog = _selected_catalog(catalog)
    candidates = _variants_for(task_type, catalog=selected_catalog)
    for candidate in candidates:
        if candidate.name == variant or (
            variant is None and candidate == candidates[0]
        ):
            rendered = candidate.builder()
            if candidate.name == candidates[0].name:
                rendered = _main_template_options(
                    task_type, rendered, catalog=selected_catalog
                )
            return rendered
    message = (
        f"Unsupported task template variant '{variant}' for task type '{task_type}'"
    )
    raise KeyError(message)


def _main_template_options(
    task_type: str, rendered: str, *, catalog: TaskAuthoringCatalog
) -> str:
    """Show short input and field hints without duplicating complete scenarios."""
    active = yaml.safe_load(rendered)
    if not is_yaml_object(active):
        return rendered
    seen: set[str] = set()
    for variant in _examples_for(task_type, catalog=catalog)[1:]:
        if variant.purpose != "option" and variant.name != "output":
            continue
        example_text = variant.builder()
        example = yaml.safe_load(example_text)
        if not is_yaml_object(example):
            continue
        example_params = example.get("task_params")
        if not is_yaml_object(example_params):
            continue
        active_params = active.get("task_params", {})
        if not is_yaml_object(active_params):
            active_params = {}
        changed = _optional_field_delta(example_params, active_params)
        if not changed:
            continue
        block = yaml.safe_dump(changed, sort_keys=False).rstrip()
        if block in seen:
            continue
        seen.add(block)
        rendered += (
            "\n# Optional task_params fields: replace the fields above; "
            "do not append duplicate keys.\n"
        )
        rendered += _optional_field_notes(task_type, changed, example_params, active)
        rendered += "".join("# " + line + "\n" for line in block.splitlines())
    return rendered


def _optional_field_delta(example: YamlObject, active: YamlObject) -> YamlObject:
    changed: YamlObject = {
        key: example[key]
        for key in ("parameters", "preStatements", "postStatements")
        if key in example and example[key] != active.get(key)
    }
    local_params = example.get("localParams")
    if isinstance(local_params, list):
        inputs: list[YamlValue] = [
            parameter
            for parameter in local_params
            if isinstance(parameter, dict) and parameter.get("direct") == "IN"
        ]
        if inputs:
            changed["localParams"] = inputs[:1]
    return changed


def _optional_field_notes(
    task_type: str,
    changed: YamlObject,
    example: YamlObject,
    active: YamlObject,
) -> str:
    notes = ""
    if "localParams" in changed:
        if "command" in active:
            notes += (
                "# Move command to task_params.rawScript before adding localParams.\n"
            )
        if task_type == "SWITCH":
            notes += (
                "# Pair route with ${route} in switchResult branch conditions; "
                "the first matching branch wins.\n"
            )
        elif task_type != "PROCEDURE":
            notes += (
                "# Use ${prop} in the task's script/request field; "
                "the declaration alone does not substitute a value.\n"
            )
        if task_type == "PROCEDURE":
            notes += (
                f"# Pair localParams with method: {example['method']!r}; "
                "one ordered binding per ?.\n"
            )
    if "preStatements" in changed or "postStatements" in changed:
        notes += (
            "# Pre/main/post statements are not an automatic transaction "
            "or rollback; retry may repeat their effects.\n"
        )
    return notes


def _metadata_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> TaskTemplateMetadata:
    variants = _variants_for(task_type, catalog=catalog)
    examples = _examples_for(task_type, catalog=catalog)
    return {
        "kind": task_template_kind(task_type, catalog=catalog),
        "category": task_template_category(task_type, catalog=catalog),
        "variants": [variant.name for variant in variants if variant.name != "minimal"],
        "variant_summaries": {
            variant.name: variant.summary
            for variant in variants
            if variant.name != "minimal"
        },
        "payload_modes": sorted(
            {mode for variant in examples for mode in variant.payload_modes}
        ),
        "parameter_fields": sorted(
            {field for variant in examples for field in variant.parameter_fields}
        ),
        "resource_fields": sorted(
            {field for variant in examples for field in variant.resource_fields}
        ),
    }


def _variants_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskTemplateVariant, ...]:
    """Return only independently useful, exact-supported public scenarios."""
    return tuple(
        example
        for example in _examples_for(task_type, catalog=catalog)
        if example.purpose == "scenario"
    )


def _examples_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskTemplateVariant, ...]:
    entry = catalog.entries.get(task_type)
    if entry is not None:
        entry_variants = tuple(
            TaskTemplateVariant(
                name=template.name,
                summary=template.summary,
                builder=template.render,
                payload_modes=template.payload_modes,
                parameter_fields=template.parameter_fields,
                resource_fields=template.resource_fields,
                purpose=template.purpose,
            )
            for template in entry.default.contract.templates
        )
        return _project_typed_variants(
            task_type,
            variants=entry_variants,
            catalog=catalog,
        )
    variants = (
        _VARIANTS.get(task_type)
        if catalog.supports_typed_authoring(task_type)
        else None
    )
    if variants is not None:
        return _project_typed_variants(
            task_type,
            variants=variants,
            catalog=catalog,
        )
    return (
        TaskTemplateVariant(
            name="minimal",
            summary="Generic DS-native task_params placeholder.",
            builder=lambda: _project_task_template_yaml(
                task_type, _generic_task_template_yaml(task_type), catalog=catalog
            ),
            payload_modes=("task_params",),
        ),
    )


def _project_typed_variants(
    task_type: str,
    *,
    variants: tuple[TaskTemplateVariant, ...],
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskTemplateVariant, ...]:
    """Bind static examples to one exact profile's reviewed authoring surface."""
    projected: list[TaskTemplateVariant] = []
    legacy_sub_workflow = (
        task_type == "SUB_WORKFLOW" and catalog.profile_version == "1.3.9"
    )
    for source_variant in variants:
        variant = source_variant
        if (
            variant.name == "output"
            and not catalog.parameter_semantics.output.var_pool_transport
        ):
            variant = replace(variant, name="params", purpose="option")
        if (
            task_type in {"PYTHON", "SHELL"}
            and catalog.profile_version == "1.3.9"
            and variant.name == "resource"
        ):
            continue
        if (
            task_type == "HTTP"
            and variant.name == "post-json"
            and not catalog.authoring_surface.http.request_body
        ):
            continue
        summary = variant.summary
        if legacy_sub_workflow:
            summary = (
                "Resolve one same-project child workflow name to its exact "
                "DolphinScheduler 1.3.9 processDefinitionId."
            )
        if (
            task_type in {"PYTHON", "SHELL"}
            and variant.name == "params"
            and not catalog.parameter_semantics.output.var_pool_transport
        ):
            summary = f"{task_type} example with one IN localParam."
        projected.append(
            TaskTemplateVariant(
                name=variant.name,
                summary=summary,
                builder=_projected_template_builder(
                    task_type=task_type,
                    variant=variant,
                    catalog=catalog,
                ),
                payload_modes=variant.payload_modes,
                parameter_fields=(
                    ()
                    if legacy_sub_workflow
                    else tuple(
                        field
                        for field in variant.parameter_fields
                        if field != "task_params.varPool[]"
                        or catalog.parameter_semantics.output.var_pool_transport
                    )
                ),
                resource_fields=(
                    () if legacy_sub_workflow else variant.resource_fields
                ),
                purpose=variant.purpose,
            )
        )
    return tuple(projected)


def _projected_template_builder(
    *,
    task_type: str,
    variant: TaskTemplateVariant,
    catalog: TaskAuthoringCatalog,
) -> Callable[[], str]:
    def render() -> str:
        return _project_task_template_yaml(
            task_type,
            variant.builder(),
            catalog=catalog,
        )

    return render


def _project_task_template_yaml(
    task_type: str,
    yaml_text: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> str:
    yaml_text = project_task_runtime_template(yaml_text, catalog=catalog)
    if catalog.authoring_surface.task_node.wire == "legacy-process-json":
        yaml_text = _project_legacy_task_node_template(yaml_text, catalog=catalog)
    if task_type == "PYTHON" and catalog.profile_version == "1.3.9":
        yaml_text = _python_139_runtime_guidance() + yaml_text
    if task_type == "CONDITIONS":
        yaml_text = _conditions_graph_guidance() + yaml_text
        if catalog.profile_version == "1.3.9":
            yaml_text = _conditions_139_runtime_guidance() + yaml_text
    if task_type == "HTTP" and not catalog.authoring_surface.http.request_body:
        projected = yaml_text.replace('  httpBody: ""\n', "")
        if catalog.profile_version == "1.3.9":
            projected = _http_139_runtime_guidance() + projected
        return projected
    yaml_text = _project_special_runtime_guidance(task_type, yaml_text, catalog=catalog)
    if task_type == "SUB_WORKFLOW":
        return _project_sub_workflow_template(yaml_text, catalog=catalog)
    return yaml_text


def _project_special_runtime_guidance(
    task_type: str, yaml_text: str, *, catalog: TaskAuthoringCatalog
) -> str:
    if task_type == "SWITCH":
        if not switch_uses_local_params(catalog.profile_version):
            yaml_text = yaml_text.replace(
                "  localParams:\n    - prop: route\n      direct: IN\n"
                "      type: VARCHAR\n      value: A\n",
                "",
            )
        return (
            "# "
            + switch_parameter_guidance(catalog.profile_version)
            + "\n# Define task-a, task-b and task-default in this workflow; "
            "branch edges are added automatically.\n" + yaml_text
        )
    reason = generic_task_runtime_limitation(task_type, catalog=catalog)
    if reason is not None:
        return (
            f"# Runtime limitation in DS {catalog.profile_version}: {reason}\n"
            "# Saving opaque state does not establish a runnable stream task.\n"
            + yaml_text
        )
    return yaml_text


def _project_sub_workflow_template(
    yaml_text: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> str:
    """Bind one SUB_WORKFLOW template to its exact identity and input epoch."""
    if catalog.profile_version == "1.3.9":
        yaml_text = _project_sub_workflow_139_identity(yaml_text)
    rules = "".join(f"# {rule}\n" for rule in _nested_workflow_template_rules(catalog))
    return rules + yaml_text


def _project_sub_workflow_139_identity(yaml_text: str) -> str:
    """Project the canonical name selector onto the reviewed 1.3.9 subset."""
    projected = yaml_text.replace(
        "  workflowDefinitionCode: 1000000000001\n",
        "  childWorkflowName: child-daily\n",
    )
    projected = projected.replace("  localParams: []\n", "")
    projected = projected.replace("  resourceList: []\n", "")
    projected = projected.replace("  varPool: []\n", "")
    return (
        "# Identity boundary: childWorkflowName is resolved within the same project "
        "to native processDefinitionId.\n"
        "# Compile path: processDefinitionJson.tasks[].params.processDefinitionId.\n"
        f"{projected}"
    )


def _http_139_runtime_guidance() -> str:
    """Return exact execution warnings for the legacy HTTP worker task."""
    return (
        "# Runtime boundary: DolphinScheduler applies prepared placeholder "
        "substitution to the URL and each HTTP property.\n"
        "# Logging warning: upstream logs complete task params, substituted request "
        "params and properties, the configured URL, status, and the full response "
        "body at INFO.\n"
        "# Security boundary: task fields are not secret storage; dsctl does not "
        "redact them.\n"
        "# Recovery boundary: there is no reliable cancel or worker-failover resume.\n"
        "# Retry reissues the whole HTTP request; POST, PUT, and DELETE side effects "
        "can duplicate.\n"
    )


def _python_139_runtime_guidance() -> str:
    """Return exact execution warnings for the legacy Python worker task."""
    return (
        "# Runtime boundary: DolphinScheduler performs placeholder substitution "
        "after CRLF-to-LF normalization, writes the substituted script as UTF-8, "
        "and selects PYTHON_HOME with a python fallback.\n"
        "# Logging warning: upstream logs full task params, the original and "
        "substituted script, its command, and stdout/stderr at INFO.\n"
        "# Security boundary: task fields are not secret storage; dsctl does not "
        "redact them.\n"
        "# Resource boundary: typed resourceList must stay empty because the "
        "legacy full-name path bypasses positive-ID permission checks; nonempty "
        "native resource state remains unchanged/export opaque.\n"
        "# Recovery boundary: this is a local process with best-effort cancel, no "
        "structured output, and no worker-failover resume.\n"
        "# Retry reruns the whole script; database, file, and remote side effects "
        "can repeat.\n"
    )


def _conditions_graph_guidance() -> str:
    """Explain how a CONDITIONS fragment connects to its surrounding tasks."""
    return (
        "# Define every referenced task name in the same workflow tasks[] list.\n"
        "# dsctl automatically adds incoming edges from predicate tasks and outgoing\n"
        "# edges to successNode/failedNode targets; "
        "no duplicate depends_on is needed.\n"
    )


def _conditions_139_runtime_guidance() -> str:
    """Return exact execution warnings for the legacy master-local router."""
    return (
        "# Runtime boundary: DolphinScheduler 1.3.9 evaluates same-process "
        "task-name predicates on the master; dependence and conditionResult are "
        "split TaskNode fields beside params.\n"
        "# Logging warning: upstream logs task names, expected and actual states, "
        "and the final condition result at INFO; these fields are not secret "
        "storage.\n"
        "# Graph boundary: successNode and failedNode each name exactly one "
        "different direct successor in this reviewed subset.\n"
        "# Recovery boundary: there is no worker process, structured output, "
        "remote application id, or durable failover resume. Retry or master "
        "failover can reevaluate persisted task state.\n"
    )


def _project_legacy_task_node_template(
    yaml_text: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> str:
    """Remove parameter examples that cannot compile into the legacy payload."""
    projected_lines: list[str] = []
    skip_output_param = False
    for line in yaml_text.splitlines(keepends=True):
        stripped = line.strip()
        if (
            stripped == "varPool: []"
            and not catalog.parameter_semantics.output.var_pool_transport
        ):
            continue
        if stripped == "- prop: row_count":
            skip_output_param = True
            continue
        if skip_output_param:
            if stripped.startswith(("direct:", "type:", "value:")):
                continue
            skip_output_param = False
        if "${setValue(row_count=42)}" in line:
            continue
        projected_lines.append(line)
    return "".join(projected_lines).replace(
        "description: Use IN params and emit one OUT param",
        "description: Use one IN parameter",
    )


def nested_workflow_parameter_guidance(catalog: TaskAuthoringCatalog) -> str:
    """Render the shared exact child-parameter rules for schema summaries."""
    return " ".join(_nested_workflow_template_rules(catalog))


def _nested_workflow_template_rules(catalog: TaskAuthoringCatalog) -> tuple[str, ...]:
    return tuple(nested_workflow_parameter_rules(catalog.parameter_semantics))


def _selected_catalog(
    catalog: TaskAuthoringCatalog | None,
) -> TaskAuthoringCatalog:
    return default_task_authoring_catalog() if catalog is None else catalog


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


def _shell_output_template_yaml() -> str:
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
                - resourceName: /scripts/job.sh
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


def _python_output_template_yaml() -> str:
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
                import runpy; runpy.run_path("scripts/job.py", run_name="__main__")
              resourceList:
                - resourceName: /scripts/job.py
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


def _remote_shell_output_template_yaml() -> str:
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


def _http_input_field_example() -> str:
    """Return one concise IN field example for the main template."""
    return """task_params:
  localParams:
  - prop: bizdate
    direct: IN
    type: VARCHAR
    value: ${system.biz.date}
"""


def _http_post_json_template_yaml() -> str:
    return _task_template_with_runtime_controls(
        dedent(
            """\
            # Task template for HTTP POST with JSON body
            # POST may change the remote system; rerunning can repeat that change.
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


def _switch_input_field_example() -> str:
    """Return one concise IN field example for the main template."""
    return dedent(
        """\
        task_params:
          localParams:
            - prop: route
              direct: IN
              type: VARCHAR
              value: A
        """
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
                      - task: upstream-task
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


def _task_template_name(task_type: str) -> str:
    return f"{task_type.lower().replace('_', '-')}-task"


def _validated_typed_task_template_types() -> tuple[str, ...]:
    expected = supported_typed_task_types()
    actual = tuple(
        sorted(
            {
                *_VARIANTS,
                *default_task_authoring_catalog().reviewed_typed_task_types,
            }
        )
    )
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


def _validated_supported_task_template_types(
    typed_task_types: tuple[str, ...],
) -> tuple[str, ...]:
    supported = _ordered_authorable_task_types(
        default_task_authoring_catalog(),
        baseline=upstream_default_task_types(),
    )
    missing_typed = sorted(set(typed_task_types) - set(supported))
    if missing_typed:
        message = (
            "Task template support must include every typed task spec authorized "
            f"by the stable exact profile (missing: {', '.join(missing_typed)})"
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
            purpose="option",
            summary="HTTP example with localParams used in URL and headers.",
            builder=_http_input_field_example,
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
            name="output",
            summary="PYTHON example with IN localParams and one OUT varPool value.",
            builder=_python_output_template_yaml,
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
            name="output",
            summary=(
                "REMOTESHELL example with IN localParams and one OUT varPool value."
            ),
            builder=_remote_shell_output_template_yaml,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
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
            name="output",
            summary="SHELL example with IN localParams and one OUT varPool value.",
            builder=_shell_output_template_yaml,
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
    "SUB_WORKFLOW": (
        TaskTemplateVariant(
            name="minimal",
            summary="Run one child workflow definition by code.",
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
            purpose="option",
            summary="SWITCH example with branch routing from localParams.",
            builder=_switch_input_field_example,
            payload_modes=("task_params",),
            parameter_fields=_task_parameter_fields(),
        ),
    ),
}

_TASK_TEMPLATE_TYPES_BY_CATEGORY = upstream_default_task_types_by_category()
_TASK_TYPE_TO_CATEGORY = {
    task_type: category
    for category, task_types in _TASK_TEMPLATE_TYPES_BY_CATEGORY.items()
    for task_type in task_types
}
