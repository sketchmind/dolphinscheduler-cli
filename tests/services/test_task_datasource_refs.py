from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml
from pydantic import TypeAdapter
from tests.fakes import FakeDataSource, FakeDataSourceAdapter, FakeEnumValue

from dsctl.errors import ResolutionError, UserInputError
from dsctl.models.common import ModelValidationError
from dsctl.models.task_spec.datasource_ref import DatasourceReference
from dsctl.models.workflow_spec import load_workflow_spec, validate_workflow_document
from dsctl.services import _task_templates
from dsctl.services._task_datasource_refs import (
    resolve_workflow_spec_task_datasources,
)
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.support.json_types import require_json_object
from dsctl.upstream.task_profiles import task_profile_versions

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_spec import WorkflowSpec


def _sql_spec(datasource: int | str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog("3.4.1")
    return validate_workflow_document(
        {
            "workflow": {"name": "daily-query"},
            "tasks": [
                {
                    "name": "query-orders",
                    "type": "SQL",
                    "task_params": {
                        "type": "MYSQL",
                        "datasource": datasource,
                        "sql": "select 1",
                        "sqlType": 0,
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def test_datasource_reference_schema_exposes_runtime_constraints() -> None:
    schema = TypeAdapter(DatasourceReference).json_schema()

    assert schema["anyOf"] == [
        {"minimum": 1, "type": "integer"},
        {"pattern": r"\S", "type": "string"},
    ]


@pytest.mark.parametrize("version", task_profile_versions())
def test_exact_task_datasource_schemas_match_valid_canonical_references(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    canonical_schema = TypeAdapter(DatasourceReference).json_schema()
    datasource_families = {
        "ALIYUN_SERVERLESS_SPARK",
        "DATA_QUALITY",
        "K8S",
        "PROCEDURE",
        "REMOTESHELL",
        "SAGEMAKER",
        "SQL",
        "ZEPPELIN",
    }
    for task_type in sorted(catalog.reviewed_typed_task_types & datasource_families):
        fields_data = require_json_object(
            task_type_schema_result(task_type, catalog=catalog).data,
            label="task fields",
        )
        fields = fields_data["fields"]
        assert isinstance(fields, list)
        datasource_field = next(
            (
                field
                for field in fields
                if isinstance(field, dict)
                and field.get("path") == "task_params.datasource"
            ),
            None,
        )
        if datasource_field is None:
            continue
        assert datasource_field["type"] == "integer|string"
        schema_data = require_json_object(
            task_type_schema_result(task_type, catalog=catalog, json_schema=True).data,
            label="task schema",
        )
        schema = schema_data["schema"]
        assert isinstance(schema, dict)
        definitions = schema["$defs"]
        assert isinstance(definitions, dict)
        params_schema = definitions["task_params"]
        assert isinstance(params_schema, dict)
        properties = params_schema["properties"]
        assert isinstance(properties, dict)
        datasource_schema = properties["datasource"]
        assert isinstance(datasource_schema, dict)
        assert datasource_schema["anyOf"] == canonical_schema["anyOf"]
        assert "default" not in datasource_schema
        assert "type" not in datasource_schema
        required = params_schema["required"]
        assert isinstance(required, list)
        assert "datasource" in required

        task = yaml.safe_load(
            _task_templates.task_template_yaml(task_type, catalog=catalog)
        )
        params = task["task_params"]
        for reference in (17, "123"):
            normalized = catalog.normalize_task_params(
                task_type,
                {**params, "datasource": reference},
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )
            assert normalized["datasource"] == reference
        for invalid in (True, 0, -1, 1.5, "", "  "):
            with pytest.raises(ModelValidationError):
                catalog.normalize_task_params(
                    task_type,
                    {**params, "datasource": invalid},
                    intent=TaskAuthoringIntent.TYPED_CREATE,
                )


def test_numeric_looking_string_is_resolved_as_exact_datasource_name() -> None:
    adapter = FakeDataSourceAdapter(
        [
            FakeDataSource(
                id=17,
                name="123",
                type_value=FakeEnumValue("MYSQL"),
            )
        ]
    )

    resolved_spec, resolutions = resolve_workflow_spec_task_datasources(
        adapter,
        _sql_spec("123"),
        catalog=get_task_authoring_catalog("3.4.1"),
        action="workflow.create",
    )

    assert resolved_spec.tasks[0].task_params is not None
    assert resolved_spec.tasks[0].task_params["datasource"] == 17
    assert resolutions == [
        {
            "task": "query-orders",
            "task_type": "SQL",
            "requested": "123",
            "id": 17,
            "name": "123",
            "type": "MYSQL",
        }
    ]


def test_unchanged_baseline_id_requires_no_datasource_lookup() -> None:
    original = _sql_spec(17)

    resolved_spec, resolutions = resolve_workflow_spec_task_datasources(
        None,
        original,
        catalog=get_task_authoring_catalog("3.4.1"),
        action="workflow.edit",
        baseline={"query-orders": ("SQL", 17, "MYSQL")},
    )

    assert resolved_spec.tasks[0].task_params == original.tasks[0].task_params
    assert resolutions == []


def test_same_id_is_rechecked_when_sql_datasource_type_changes() -> None:
    adapter = FakeDataSourceAdapter(
        [
            FakeDataSource(
                id=17,
                name="analytics-mysql",
                type_value=FakeEnumValue("MYSQL"),
            )
        ]
    )
    spec = _sql_spec(17)
    params = dict(spec.tasks[0].task_params or {})
    params["type"] = "POSTGRESQL"
    spec = spec.model_copy(
        update={"tasks": [spec.tasks[0].model_copy(update={"task_params": params})]}
    )

    with pytest.raises(UserInputError, match="requires POSTGRESQL") as exc_info:
        resolve_workflow_spec_task_datasources(
            adapter,
            spec,
            catalog=get_task_authoring_catalog("3.4.1"),
            action="workflow.edit",
            baseline={"query-orders": ("SQL", 17, "MYSQL")},
        )

    assert exc_info.value.details["reason"] == "datasource_type_mismatch"
    assert exc_info.value.suggestion == (
        "Run `dsctl datasource list` and choose a visible datasource whose type "
        "is POSTGRESQL; use its exact name or positive id."
    )


@pytest.mark.parametrize("invalid_datasource", [True, 1.5, {"name": "mysql"}])
def test_invalid_datasource_reference_is_a_model_validation_issue(
    tmp_path: Path,
    invalid_datasource: object,
) -> None:
    source = tmp_path / "invalid-datasource.yaml"
    source.write_text(
        "workflow: {name: daily-query}\n"
        "tasks:\n"
        "  - name: query-orders\n"
        "    type: SQL\n"
        "    task_params:\n"
        "      type: MYSQL\n"
        f"      datasource: {invalid_datasource!r}\n"
        "      sql: select 1\n"
        "      sqlType: 0\n",
        encoding="utf-8",
    )

    with pytest.raises(ModelValidationError) as exc_info:
        load_workflow_spec(
            source,
            authoring_context=workflow_authoring_context(
                catalog=get_task_authoring_catalog("3.4.1"),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )

    assert any(
        issue.path == "tasks[0]"
        and "task_params.datasource" in issue.message
        and "positive integer id or nonblank exact name" in issue.message
        for issue in exc_info.value.issues
    )


def test_datasource_type_mismatch_is_actionable_user_input() -> None:
    adapter = FakeDataSourceAdapter(
        [
            FakeDataSource(
                id=18,
                name="analytics-postgres",
                type_value=FakeEnumValue("POSTGRESQL"),
            )
        ]
    )

    with pytest.raises(UserInputError, match="requires MYSQL") as exc_info:
        resolve_workflow_spec_task_datasources(
            adapter,
            _sql_spec("analytics-postgres"),
            catalog=get_task_authoring_catalog("3.4.1"),
            action="workflow.create",
        )

    assert exc_info.value.details["reason"] == "datasource_type_mismatch"
    assert exc_info.value.details["expected_type"] == "MYSQL"
    assert exc_info.value.details["actual_type"] == "POSTGRESQL"


def test_ambiguous_exact_datasource_name_reports_sorted_ids() -> None:
    adapter = FakeDataSourceAdapter(
        [
            FakeDataSource(
                id=19,
                name="analytics-mysql",
                type_value=FakeEnumValue("MYSQL"),
            ),
            FakeDataSource(
                id=17,
                name="analytics-mysql",
                type_value=FakeEnumValue("MYSQL"),
            ),
        ]
    )

    with pytest.raises(ResolutionError, match="ambiguous") as exc_info:
        resolve_workflow_spec_task_datasources(
            adapter,
            _sql_spec("analytics-mysql"),
            catalog=get_task_authoring_catalog("3.4.1"),
            action="workflow.create",
        )

    assert exc_info.value.details["ids"] == [17, 19]
    assert exc_info.value.details["task"] == "query-orders"


@pytest.mark.parametrize(
    ("task_type", "version"),
    [
        ("SQL", "3.4.1"),
        ("PROCEDURE", "3.4.1"),
        ("DATA_QUALITY", "3.2.2"),
        ("K8S", "3.4.1"),
        ("ALIYUN_SERVERLESS_SPARK", "3.4.1"),
        ("REMOTESHELL", "3.4.1"),
        ("SAGEMAKER", "3.4.1"),
        ("ZEPPELIN", "3.4.1"),
    ],
)
def test_datasource_authoring_schema_exposes_name_or_id(
    task_type: str,
    version: str,
) -> None:
    result = task_type_schema_result(
        task_type,
        full=True,
        catalog=get_task_authoring_catalog(version),
    )
    data = require_json_object(result.data, label="task schema")
    fields = data["fields"]
    assert isinstance(fields, list)
    datasource_field = next(
        field
        for field in fields
        if isinstance(field, dict) and field.get("path") == "task_params.datasource"
    )
    assert datasource_field["type"] == "integer|string"
    choice_sources = data["choice_sources"]
    assert isinstance(choice_sources, list)
    datasource_source = next(
        source
        for source in choice_sources
        if isinstance(source, dict) and source.get("path") == "task_params.datasource"
    )
    assert datasource_source["value"] == "name or id"
