from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml
from pydantic import ValidationError
from tests.value_shape_assertions import assert_mapping, assert_sequence

from dsctl.models.task_spec.script import ScriptTaskParamsSpec
from dsctl.services.lint.workflow import lint_workflow_result
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.common import YamlObject, YamlValue


@pytest.mark.parametrize("task_type", ["SHELL", "PYTHON"])
@pytest.mark.parametrize("version", ["3.1.9", "3.2.0", "3.4.1", "3.4.3"])
@pytest.mark.parametrize("datasource", ["analytics", 5])
@pytest.mark.parametrize(
    "attachment",
    [
        {"id": 71},
        {"resourceName": "/scripts/job.sh", "id": 71},
        {"resourceName": "scripts/job.sh"},
    ],
    ids=["native-id", "extra-native-id", "unrooted-name"],
)
def test_script_resource_shape_is_validated_before_parallel_name_resolution(
    tmp_path: Path,
    task_type: str,
    version: str,
    datasource: str | int,
    attachment: YamlObject,
) -> None:
    document: YamlObject = {
        "workflow": {"name": "mixed-sql-script"},
        "tasks": [
            {
                "name": "query",
                "type": "SQL",
                "task_params": {
                    "type": "MYSQL",
                    "sqlType": 0,
                    "datasource": datasource,
                    "sql": "SELECT 1",
                },
            },
            {
                "name": "run",
                "type": task_type,
                "task_params": {
                    "rawScript": "echo ok",
                    "resourceList": [attachment],
                },
            },
        ],
    }
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False))

    result = lint_workflow_result(
        file=path, catalog=get_task_authoring_catalog(version)
    )

    data = assert_mapping(result.data)
    assert data["valid"] is False
    diagnostics = [
        assert_mapping(item) for item in assert_sequence(data["diagnostics"])
    ]
    errors = [item for item in diagnostics if item["severity"] == "error"]
    assert errors
    assert all(
        str(item["path"]).startswith("tasks[1].task_params.resourceList[0]")
        for item in errors
    )
    assert not any(
        item["code"] == "workflow_compilation_deferred" for item in diagnostics
    )


@pytest.mark.parametrize("task_type", ["SHELL", "PYTHON"])
@pytest.mark.parametrize("version", ["3.1.9", "3.4.3"])
def test_script_resource_json_schema_owns_the_canonical_name_only(
    task_type: str, version: str
) -> None:
    result = task_type_schema_result(
        task_type,
        catalog=get_task_authoring_catalog(version),
        json_schema=True,
    )
    schema = assert_mapping(assert_mapping(result.data)["schema"])
    params = assert_mapping(assert_mapping(schema["$defs"])["task_params"])
    resources = assert_mapping(assert_mapping(params["properties"])["resourceList"])
    ref = assert_mapping(resources["items"])["$ref"]
    assert ref == "#/$defs/task_params/$defs/ScriptResourceRefSpec"
    resource = assert_mapping(assert_mapping(params["$defs"])["ScriptResourceRefSpec"])
    assert resource["additionalProperties"] is False
    assert resource["required"] == ["resourceName"]
    properties = assert_mapping(resource["properties"])
    assert list(properties) == ["resourceName"]
    assert assert_mapping(properties["resourceName"])["type"] == "string"


@pytest.mark.parametrize(
    "resource_name", ["/a", "/scripts/nightly job.py", "/scripts/任务.sh"]
)
def test_canonical_script_resource_path_is_retained_literally(
    resource_name: str,
) -> None:
    params = ScriptTaskParamsSpec.model_validate(
        {
            "rawScript": "echo ok",
            "resourceList": [{"resourceName": resource_name}],
        }
    )

    assert params.to_payload()["resourceList"] == [{"resourceName": resource_name}]


@pytest.mark.parametrize("resource_name", [None, 71, "", "/", "//job.sh", "/jobs/"])
def test_canonical_script_resource_name_requires_a_rooted_file(
    resource_name: YamlValue,
) -> None:
    with pytest.raises(ValidationError):
        ScriptTaskParamsSpec.model_validate(
            {
                "rawScript": "echo ok",
                "resourceList": [{"resourceName": resource_name}],
            }
        )


@pytest.mark.parametrize("task_type", ["SHELL", "PYTHON"])
@pytest.mark.parametrize("version", ["1.3.9", "3.1.9", "3.4.3"])
def test_native_script_resource_identity_is_preserved_only_by_explicit_opaque(
    task_type: str, version: str
) -> None:
    native: YamlObject = {
        "rawScript": "echo ok",
        "resourceList": [{"id": 71, "res": "scripts/job.sh"}],
    }
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.normalize_task_params(
            task_type, native, intent=TaskAuthoringIntent.OPAQUE_PRESERVE
        )
        == native
    )
    with pytest.raises(ValueError):
        catalog.normalize_task_params(
            task_type, native, intent=TaskAuthoringIntent.TYPED_CREATE
        )
