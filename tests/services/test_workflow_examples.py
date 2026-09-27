from __future__ import annotations

from shlex import quote
from typing import TYPE_CHECKING

import pytest
import yaml
from tests.value_shape_assertions import assert_mapping

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result, workflow_template_result
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_lint_graph

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize("example", ["output", "branch", "child", "dependent"])
@pytest.mark.parametrize("with_schedule", [False, True])
def test_complete_examples_compile_for_the_selected_exact_profile(
    version: str, example: str, *, with_schedule: bool
) -> None:
    catalog = get_task_authoring_catalog(version)
    if version == "1.3.9" and example in {"output", "branch"}:
        with pytest.raises(UnsupportedFeatureError):
            workflow_template_result(
                example=example, catalog=catalog, with_schedule=with_schedule
            )
        return
    result = workflow_template_result(
        example=example, catalog=catalog, with_schedule=with_schedule
    )
    data = assert_mapping(result.data)
    text = data["yaml"]
    assert isinstance(text, str)
    document = yaml.safe_load(text)
    assert isinstance(document["workflow"], dict)
    assert ("schedule" in document) is with_schedule
    assert document["workflow"]["release_state"] == (
        "ONLINE" if with_schedule else "OFFLINE"
    )
    if with_schedule:
        assert document["schedule"]["enabled"] is False
    assert result.resolved["example"] == example
    context = workflow_authoring_context(
        catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
    )
    spec = validate_workflow_document(document, authoring_context=context)
    if version == "1.3.9":
        prepare_legacy_workflow_lint_graph(spec).materialize()
    else:
        prepare_workflow_create_compilation(spec, catalog=catalog).preview()
    if example == "output":
        producer, consumer = document["tasks"]
        assert any(p["direct"] == "OUT" for p in producer["task_params"]["localParams"])
        expected = (
            ["IN"]
            if catalog.parameter_semantics.output.downstream_binding
            == "declared-in-only"
            else []
        )
        assert [p["direct"] for p in consumer["task_params"]["localParams"]] == expected
        assert consumer["depends_on"] == [producer["name"]]
        assert "rawScript: |" in text
    elif example == "branch":
        assert document["workflow"]["global_params"]["route"] == "A"
        assert len(document["tasks"][-1]["depends_on"]) == 3
    elif example == "child":
        assert len(document["tasks"]) == 1
        assert "defines only the parent" in text
        assert "template workflow" in text
        assert "--example basic" in text


@pytest.mark.parametrize("version", ["1.3.9", "3.2.1", "3.4.1"])
def test_explicit_basic_is_identical_to_the_existing_default(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    default = workflow_template_result(catalog=catalog)
    basic = workflow_template_result(catalog=catalog, example="basic")
    assert default.data == basic.data
    assert default.resolved == basic.resolved


def test_workflow_example_selector_and_navigation_keep_environment(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "team settings.env"
    env_file.write_text("DS_VERSION=3.4.1\n", encoding="utf-8")
    catalog = get_task_authoring_catalog("3.4.1")
    with pytest.raises(UserInputError, match="Unknown workflow example"):
        workflow_template_result(example="all-definitions", catalog=catalog)
    result = workflow_template_result(
        example="child", catalog=catalog, env_file=str(env_file)
    )
    text = assert_mapping(result.data)["yaml"]
    assert isinstance(text, str)
    assert (
        f"dsctl --env-file {quote(str(env_file))} workflow list --project PROJECT"
        in text
    )
    task = assert_mapping(
        task_template_result("DEPENDENT", catalog=catalog, env_file=str(env_file)).data
    )
    assert f"dsctl --env-file {quote(str(env_file))} template workflow" in str(
        task["yaml"]
    )
    assert "--example dependent" in str(task["yaml"])
