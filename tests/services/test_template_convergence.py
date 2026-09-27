from __future__ import annotations

from collections import Counter
from shlex import quote
from typing import TYPE_CHECKING

import httpx
import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.output import require_json_object
from dsctl.services._task_resource_refs import resolve_task_resource_refs
from dsctl.services._task_templates import task_template_variants
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import task_template_metadata, task_template_result
from dsctl.upstream.resources import bind_task_resource_resolver
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("version", [v for v in TARGET_DS_VERSIONS if v != "1.3.9"])
def test_switch_template_and_schema_share_evaluator_input_boundary(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result("SWITCH", catalog=catalog)
    text = assert_mapping(result.data)["yaml"]
    assert isinstance(text, str)
    task = yaml.safe_load(parameter_example_yaml("SWITCH", version))
    modern = tuple(map(int, version.split("."))) >= (3, 2, 2)
    assert ("localParams" in task["task_params"]) is modern
    schema = assert_mapping(task_type_schema_result("SWITCH", catalog=catalog).data)
    fields = schema["fields"]
    assert isinstance(fields, list)
    condition = next(
        assert_mapping(f)
        for f in fields
        if assert_mapping(f)["path"]
        == "task_params.switchResult.dependTaskList[].condition"
    )
    description = condition["description"]
    assert isinstance(description, str)
    if modern:
        assert "prepared parameter map" in description
        assert task["task_params"]["localParams"][0]["value"] == "A"
        assert "# localParams:\n# - prop: route" in text
        assert "#   value: A" in text
        assert "${route} in switchResult branch conditions" in text
    else:
        assert "not this task's localParams" in description
        assert "workflow.global_params.route: A" in text
        assert "# localParams:" not in text


@pytest.mark.parametrize(
    "version", ["3.1.0", "3.1.3", "3.2.2", "3.3.1", "3.4.1", "3.4.2"]
)
def test_sagemaker_exact_fields_have_one_owner_and_consistent_requiredness(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    data = assert_mapping(task_type_schema_result("SAGEMAKER", catalog=catalog).data)
    fields = [assert_mapping(f) for f in assert_sequence(data["fields"])]
    assert all(n == 1 for n in Counter(f["path"] for f in fields).values())
    schema_data = assert_mapping(
        task_type_schema_result("SAGEMAKER", catalog=catalog, json_schema=True).data
    )
    schema = assert_mapping(schema_data["schema"])
    params = assert_mapping(assert_mapping(schema["$defs"])["task_params"])
    if catalog.authoring_surface.sagemaker.datasource_required:
        datasource = next(f for f in fields if f["path"] == "task_params.datasource")
        assert datasource["required"] is True
        assert "datasource" in assert_sequence(params["required"])
        node = assert_mapping(assert_mapping(params["properties"])["datasource"])
        assert node["anyOf"] == [
            {"minimum": 1, "type": "integer"},
            {"pattern": r"\S", "type": "string"},
        ]
        assert "default" not in node


@pytest.mark.parametrize(
    "task_type",
    ["SHELL", "PYTHON", "DEPENDENT", "SWITCH", "SUB_WORKFLOW", "CONDITIONS"],
)
def test_default_template_avoids_alias_choices_and_duplicates(
    task_type: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    metadata = task_template_metadata(catalog=catalog)[task_type]
    expected = {
        "SHELL": ["output", "resource"],
        "PYTHON": ["output", "resource"],
        "DEPENDENT": ["task-dependency"],
    }.get(task_type, [])
    assert metadata["variants"] == expected
    assert "default_variant" not in metadata
    main = assert_mapping(task_template_result(task_type, catalog=catalog).data)
    assert "rows" not in main
    assert "lines" not in main
    assert "compatibility_variants" not in assert_mapping(main["template"])
    assert "variant" not in assert_mapping(main["template"])
    artifact = assert_mapping(main["artifact"])
    assert "--variant" not in str(artifact["raw_command"])
    text = main["yaml"]
    assert isinstance(text, str)
    assert "dsctl task-type schema " + task_type in text
    if task_type in {"SHELL", "PYTHON"}:
        task = yaml.safe_load(text)
        assert "command" in task
        assert "task_params" not in task
        assert "Move command to task_params.rawScript" in text
        assert "# localParams:" in text
        assert "# resourceList:" not in text
        assert "direct: OUT" not in text
    for alias in ("minimal", "params"):
        with pytest.raises(UserInputError, match="Unsupported task template variant"):
            task_template_result(task_type, variant=alias, catalog=catalog)
    for variant in task_template_variants(task_type, catalog=catalog):
        assert (
            task_template_result(task_type, variant=variant, catalog=catalog).resolved[
                "variant"
            ]
            == variant
        )


@pytest.mark.parametrize(
    ("task_type", "expected"),
    [
        ("DVC", ["upload", "download", "init"]),
        ("EMR", ["add-steps"]),
        ("DATASYNC", ["raw-json"]),
    ],
)
def test_distinct_operations_remain_discoverable(
    task_type: str, expected: list[str]
) -> None:
    assert task_template_metadata()[task_type]["variants"] == expected


def test_procedure_hint_uses_ordered_bindings_without_script_substitution() -> None:
    data = assert_mapping(task_template_result("PROCEDURE").data)
    text = data["yaml"]
    assert isinstance(text, str)

    assert "one ordered binding per ?" in text
    assert "refresh_daily(?)" in text
    assert "Use ${prop} in the task's script/request field" not in text


@pytest.mark.parametrize(
    ("task_type", "alias"),
    [
        ("SHELL", "minimal"),
        ("SHELL", "params"),
        ("SQL", "pre-post-statements"),
        ("REMOTESHELL", "datasource"),
        ("SUB_WORKFLOW", "child-workflow"),
        ("SUB_WORKFLOW", "params"),
        ("DEPENDENT", "workflow-dependency"),
        ("DEPENDENT", "params"),
        ("SWITCH", "branching"),
        ("SWITCH", "params"),
        ("CONDITIONS", "condition-routing"),
        ("CONDITIONS", "params"),
        ("HTTP", "params"),
        ("PROCEDURE", "params"),
        ("JUPYTER", "params"),
        ("ZEPPELIN", "params"),
        ("HIVECLI", "params"),
        ("EMR", "params"),
        ("SAGEMAKER", "params"),
    ],
)
def test_removed_selectors_direct_the_caller_to_default_or_real_scenarios(
    task_type: str, alias: str
) -> None:
    with pytest.raises(UserInputError) as caught:
        task_template_result(task_type, variant=alias)
    assert alias not in assert_sequence(caught.value.details["available_variants"])
    assert "Omit --variant for the default template" in str(caught.value.suggestion)


@pytest.mark.parametrize("version", ["3.2.0", "3.4.1"])
@pytest.mark.parametrize(
    ("task_type", "extension"), [("SHELL", "sh"), ("PYTHON", "py")]
)
def test_resource_scene_resolves_the_file_that_its_script_executes(
    version: str, task_type: str, extension: str
) -> None:
    catalog = get_task_authoring_catalog(version)
    data = assert_mapping(
        task_template_result(task_type, variant="resource", catalog=catalog).data
    )
    text = data["yaml"]
    assert isinstance(text, str)
    task = yaml.safe_load(text)
    params = require_json_object(task["task_params"], label="test task parameters")
    resource_name = f"/scripts/job.{extension}"
    native_name = f"/tenant/resources{resource_name}"
    assert params["resourceList"] == [{"resourceName": resource_name}]
    assert resource_name.lstrip("/") in str(params["rawScript"])
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        payload: object = {
            "totalList": [
                {
                    "id": 71,
                    "fullName": native_name,
                    "directory": False,
                    "type": "FILE",
                    "size": 20,
                }
            ],
            "total": 1,
            "totalPage": 1,
            "currentPage": 1,
        }
        if request.url.path.endswith("/resources/base-dir"):
            payload = (
                "/tenant/resources"
                if request.url.params["type"] == "FILE"
                else "/tenant/udfs"
            )
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": payload})

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        resolver = bind_task_resource_resolver(version, profile, http_client=client)
        resources = resolve_task_resource_refs(
            resolver, [resource_name], boundary_resource="workflow", action="create"
        )
    assert seen[-1].url.params["fullName"] == "/tenant/resources/scripts"
    assert seen[-1].url.params["searchVal"] == f"job.{extension}"
    native = encode_task_parameters(
        version=version,
        task_type=task_type,
        task_params=params,
        refs=TaskRefIndex.from_code_by_name({}),
        source=ProjectionSource.TYPED_AUTHORING,
        resource_refs=resources,
    ).task_params
    attachment = assert_mapping(assert_sequence(native["resourceList"])[0])
    assert attachment["resourceName"] == native_name


def test_legacy_reference_discovery_preserves_names_and_explicit_environment(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "team settings.env"
    env_file.write_text("DS_VERSION=1.3.9\n", encoding="utf-8")
    catalog = get_task_authoring_catalog("1.3.9")
    result = task_template_result("DEPENDENT", catalog=catalog, env_file=str(env_file))
    text = assert_mapping(result.data)["yaml"]
    assert isinstance(text, str)
    assert f"dsctl --env-file {quote(str(env_file))} task-type schema DEPENDENT" in text
    for suffix in ["projectName=name", "workflowName=name", "taskName=name"]:
        assert suffix in text
    for command in ["project list", "workflow list", "task list"]:
        assert command in text


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
@pytest.mark.parametrize("task_type", ["SPARK", "FLINK"])
def test_generic_templates_apply_exact_runtime_projection(
    version: str, task_type: str
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result(task_type, catalog=catalog)
    text = assert_mapping(result.data)["yaml"]
    assert isinstance(text, str)
    for field in ["task_group_id", "task_group_priority", "cpu_quota", "memory_max"]:
        assert field + ":" not in text
    if version == "1.3.9":
        assert "environment_code:" not in text
        assert "delay:" not in text
