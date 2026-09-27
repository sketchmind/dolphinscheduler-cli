from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import yaml

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.common import is_yaml_object
from dsctl.services import _task_templates

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog

WORKFLOW_EXAMPLES = ("basic", "output", "branch", "child", "dependent")


def normalize_workflow_example(example: str | None) -> str:
    """Normalize one bounded workflow-composition selector."""
    normalized = "basic" if example is None else example.strip().lower()
    if normalized not in WORKFLOW_EXAMPLES:
        message = f"Unknown workflow example '{example}'."
        raise UserInputError(
            message,
            details={"available_examples": list(WORKFLOW_EXAMPLES)},
            suggestion="Run `dsctl template workflow --help` to choose an example.",
        )
    return normalized


def workflow_example_yaml(
    example: str,
    basic_yaml: str,
    *,
    catalog: TaskAuthoringCatalog,
    commands: dict[str, str],
) -> str:
    """Compose one workflow from the existing exact task-template facts."""
    required_type = {
        "output": "SHELL",
        "branch": "SWITCH",
        "child": "SUB_WORKFLOW",
        "dependent": "DEPENDENT",
    }[example]
    if not catalog.supports_typed_authoring(required_type) or (
        example == "output"
        and not catalog.parameter_semantics.output.var_pool_transport
    ):
        message = (
            f"Workflow example '{example}' is unavailable in "
            f"DS {catalog.profile_version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={"example": example, "ds_version": catalog.profile_version},
            suggestion=(
                "Choose basic, or inspect the exact task-type schema "
                "and parameter output topic."
            ),
        )
    document = _mapping(yaml.safe_load(basic_yaml))
    workflow = _mapping(document["workflow"])
    workflow["name"] = "parent-example" if example == "child" else f"{example}-example"
    guidance: list[str] = []
    if example == "output":
        tasks = _output_tasks(catalog)
        guidance = [
            (
                "Producer OUT and consumer IN are declared; "
                "depends_on carries the output."
                if catalog.parameter_semantics.output.downstream_binding
                == "declared-in-only"
                else (
                    "This exact profile binds predecessor outputs implicitly; "
                    "consumer localParams stays empty."
                )
            ),
            f"Exact output syntax and binding: {commands['outputs']}",
        ]
    elif example == "branch":
        tasks = _branch_tasks(catalog)
        _mapping(workflow["global_params"])["route"] = "A"
        guidance = [
            (
                "route is a workflow global: old SWITCH evaluators "
                "do not read task localParams."
            ),
            "SWITCH adds branch edges; join explicitly depends on every branch target.",
        ]
    elif example == "child":
        tasks = [_task("SUB_WORKFLOW", "call-child", catalog)]
        guidance = [
            (
                "This file defines only the parent. "
                "First create and release a separate child:"
            ),
            commands["basic"],
            f"Find the real same-project child identity: {commands['workflows']}",
            "Replace childWorkflowName/code from that result before dry-run.",
            _task_templates.nested_workflow_parameter_guidance(catalog),
        ]
    else:
        wait = _task("DEPENDENT", "wait-upstream", catalog)
        successor = _task("SHELL", "continue", catalog)
        successor["depends_on"] = ["wait-upstream"]
        successor["command"] = "echo upstream-ready\n"
        tasks = [wait, successor]
        guidance = [
            (
                "Replace upstream project/workflow/task definition identities; "
                "these are not instance ids."
            ),
            f"Exact names/codes and date windows: {commands['dependent_schema']}",
            (
                "DEPENDENT reads another workflow's history; "
                "it creates no edge into that workflow."
            ),
        ]
    document["tasks"] = list(tasks)
    header = basic_yaml.partition("workflow:\n")[0].replace(
        "; extract -> load below.", "."
    )
    header += "# Task timeout is in minutes; 0 disables it.\n"
    comments = "".join(f"# {line}\n" for line in guidance)
    return (
        header
        + comments
        + yaml.dump(
            document, Dumper=_ExampleDumper, sort_keys=False, allow_unicode=True
        )
    )


def _task(
    task_type: str,
    name: str,
    catalog: TaskAuthoringCatalog,
    *,
    variant: str | None = None,
) -> YamlObject:
    text = _task_templates.task_template_yaml(
        task_type, variant=variant, catalog=catalog
    )
    task = _mapping(yaml.safe_load(text))
    task["name"] = name
    return task


def _output_tasks(catalog: TaskAuthoringCatalog) -> list[YamlValue]:
    producer = _task("SHELL", "produce", catalog, variant="output")
    consumer = deepcopy(producer)
    consumer["name"] = "consume"
    consumer["depends_on"] = ["produce"]
    consumer["description"] = "Consume the predecessor output"
    params = _mapping(consumer["task_params"])
    properties = params["localParams"]
    if not isinstance(properties, list):
        message = "SHELL output template must declare localParams"
        raise TypeError(message)
    outputs = [
        _mapping(prop) for prop in properties if _mapping(prop).get("direct") == "OUT"
    ]
    if len(outputs) != 1:
        message = "SHELL output template must declare one output"
        raise ValueError(message)
    name = outputs[0]["prop"]
    outputs[0]["direct"] = "IN"
    params["localParams"] = (
        list(outputs)
        if catalog.parameter_semantics.output.downstream_binding == "declared-in-only"
        else []
    )
    params["rawScript"] = f'echo "output=${{{name}}}"\n'
    return [producer, consumer]


def _branch_tasks(catalog: TaskAuthoringCatalog) -> list[YamlValue]:
    switch = _task("SWITCH", "choose", catalog)
    branches = _mapping(_mapping(switch["task_params"])["switchResult"])
    conditions = branches["dependTaskList"]
    if not isinstance(conditions, list):
        message = "SWITCH template must have a branch list"
        raise TypeError(message)
    targets = [_mapping(branch)["nextNode"] for branch in conditions]
    targets.append(branches["nextNode"])
    tasks: list[YamlValue] = [switch]
    for target in targets:
        if not isinstance(target, str):
            message = "SWITCH template branch targets must be names"
            raise TypeError(message)
        task = _task("SHELL", target, catalog)
        task["command"] = f"echo {target}\n"
        tasks.append(task)
    join = _task("SHELL", "join", catalog)
    join["command"] = "echo joined\n"
    join["depends_on"] = targets
    tasks.append(join)
    return tasks


def _mapping(value: YamlValue) -> YamlObject:
    if not is_yaml_object(value):
        message = "Workflow template must contain YAML mappings"
        raise TypeError(message)
    return value


class _ExampleDumper(yaml.SafeDumper):
    """Retain readable scripts while serializing assembled safe YAML values."""


def _represent_example_string(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None
    )


_ExampleDumper.add_representer(str, _represent_example_string)
