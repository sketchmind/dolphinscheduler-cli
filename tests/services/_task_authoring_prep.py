from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

import yaml

from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.mutation import (
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.render import workflow_live_baseline
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.task_authoring_catalog.parameter_guidance import (
    switch_uses_local_params,
)
from dsctl.services.template import task_template_result

if TYPE_CHECKING:
    from tests.fakes import FakeDag

    from dsctl.models.common import YamlObject
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.resolver import ResolvedProject


def template_yaml(task_type: str, version: str, *, variant: str = "minimal") -> str:
    """Read the public default (the historic internal name is ``minimal``)."""
    result = task_template_result(
        task_type,
        variant=None if variant == "minimal" else variant,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    yaml_text = result.data["yaml"]
    assert isinstance(yaml_text, str)
    return yaml_text


def parameter_example_yaml(
    task_type: str, version: str, *, example: str = "params"
) -> str:
    """Compose independent field deltas for semantic tests, never a CLI selector."""
    fixture_path = (
        Path(__file__).parents[1] / "fixtures/task_authoring/parameter_examples.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    names = {
        "HTTP": "http-in",
        "PROCEDURE": "procedure-in",
        "PYTHON": "python-in-legacy",
        "SHELL": "shell-in-legacy",
        "SQL": "sql-lifecycle" if example == "pre-post-statements" else "sql-in-legacy",
        "SWITCH": "switch-local-input",
        "HIVECLI": "hivecli-in",
        "JUPYTER": "jupyter-options",
        "SAGEMAKER": "sagemaker-in",
        "ZEPPELIN": "zeppelin-literal-map",
        "EMR": "emr-in",
        "EMR_SERVERLESS": "emr-serverless-in",
    }
    delta = fixture["cases"][names[task_type]]
    if task_type == "SWITCH" and not switch_uses_local_params(version):
        delta = {}
    original = template_yaml(task_type, version)
    task = yaml.safe_load(original)
    assert isinstance(task, dict)
    params = task.setdefault("task_params", {})
    if "command" in task:
        params["rawScript"] = task.pop("command")
    params.update(delta)
    comments = "\n".join(line for line in original.splitlines() if line.startswith("#"))
    return comments + "\n" + yaml.safe_dump(task, sort_keys=False)


def task_params_schema(task_type: str, version: str) -> dict[object, object]:
    """Read the task-params definition from the public JSON schema."""
    result = task_type_schema_result(
        task_type,
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    return task_params


def resolved_field_schema(
    task_params: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    """Resolve one direct field or local definition without changing its schema."""
    properties = task_params["properties"]
    assert isinstance(properties, dict)
    field_schema = properties[field_name]
    assert isinstance(field_schema, dict)
    reference = field_schema.get("$ref")
    if not isinstance(reference, str):
        return field_schema
    definitions = task_params["$defs"]
    assert isinstance(definitions, dict)
    resolved = definitions[reference.rsplit("/", maxsplit=1)[-1]]
    assert isinstance(resolved, dict)
    return resolved


def schema_guidance(task_type: str, version: str) -> str:
    """Collect public field descriptions for facet-owned guidance assertions."""
    result = task_type_schema_result(
        task_type,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    return " ".join(
        str(field.get("description", ""))
        for field in result.data["fields"]
        if isinstance(field, dict)
    ).lower()


def single_task_params_edit_plan(
    dag: FakeDag,
    *,
    project: ResolvedProject,
    catalog: TaskAuthoringCatalog,
    document: YamlObject,
    input_mode: str,
) -> WorkflowMutationPlan:
    """Prepare a one-task parameter edit through patch or workflow-file input."""
    if input_mode == "patch":
        task = cast("list[YamlObject]", document["tasks"])[0]
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": task["name"]},
                                "set": {"task_params": task["task_params"]},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
    desired = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )
    return prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )


def single_task_metadata_edit_plan(
    dag: FakeDag,
    *,
    project: ResolvedProject,
    catalog: TaskAuthoringCatalog,
    task_name: str,
    input_mode: str,
) -> WorkflowMutationPlan:
    """Change only the description while retaining the server task baseline."""
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": task_name},
                                "set": {"description": "metadata only"},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [
                current.model_copy(update={"description": "metadata only"}, deep=True)
            ]
        },
        deep=True,
    )
    return prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )
