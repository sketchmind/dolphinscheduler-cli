"""Exact data-quality preflight before workflow authoring mutations."""

import json
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflowAdapter,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiTransportError,
    DsctlError,
    InvalidStateError,
    UserInputError,
)
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services.task_authoring_catalog import (
    get_task_authoring_catalog,
)
from dsctl.upstream.data_quality import DataQualityAuthoringInspectionError


class _DataQualityInspectorSpy:
    def __init__(
        self,
        *,
        error: DataQualityAuthoringInspectionError | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.error = error

    def inspect(self, *, datasource_id: int, database: str | None) -> None:
        self.calls.append({"datasource_id": datasource_id, "database": database})
        if self.error is not None:
            raise self.error


def _data_quality_native_322(*, richer: bool = False) -> dict[str, object]:
    native: dict[str, object] = {
        "localParams": [],
        "ruleId": 10,
        "ruleInputParameter": {
            "src_connector_type": "0",
            "src_datasource_id": "41",
            "src_database": "warehouse",
            "src_table": "orders",
            "src_filter": "",
            "statistics_name": "table_count.total",
            "comparison_type": "1",
            "comparison_name": "12",
            "check_type": "0",
            "threshold": "0",
            "failure_strategy": "1",
            "operator": "0",
        },
        "sparkParameters": {
            "deployMode": "local",
            "programType": "JAVA",
            "mainClass": (
                "org.apache.dolphinscheduler.data.quality.DataQualityApplication"
            ),
        },
    }
    if richer:
        native["futureField"] = {"preserve": True}
    return native


def _mysql_datasources() -> FakeDataSourceAdapter:
    return FakeDataSourceAdapter(
        [
            FakeDataSource(
                id=41,
                name="analytics-mysql",
                type_value=FakeEnumValue("MYSQL"),
            )
        ]
    )


@pytest.mark.parametrize(
    ("version", "task_params", "expected_database"),
    [
        (
            "3.0.6",
            {"datasource": 41, "table": "orders", "expectedRowCount": 12},
            None,
        ),
        (
            "3.2.2",
            {
                "datasource": 41,
                "database": "warehouse",
                "table": "orders",
                "expectedRowCount": 12,
            },
            "warehouse",
        ),
    ],
)
def test_data_quality_create_dry_run_runs_exact_live_preflight(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_task_adapter: FakeTaskAdapter,
    version: str,
    task_params: dict[str, object],
    expected_database: str | None,
) -> None:
    inspector = _DataQualityInspectorSpy()
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        data_quality_authoring_inspector=inspector,
        datasource_adapter=_mysql_datasources(),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / "data-quality.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "workflow": {"name": "guarded-row-count", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "check-orders",
                        "type": "DATA_QUALITY",
                        "task_params": task_params,
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert _mapping(result.data)["dry_run"] is True
    assert inspector.calls == [{"datasource_id": 41, "database": expected_database}]
    assert fake_task_adapter.generate_code_calls == []


@pytest.mark.parametrize(
    ("field", "reason", "expected_error"),
    [
        ("ruleJson.executeSqlList", "the stock SQL has drifted", InvalidStateError),
        (
            "datasource.database",
            "the datasource detail does not match the canonical database",
            UserInputError,
        ),
        (
            "remote",
            "the required live inspection could not be completed",
            ApiTransportError,
        ),
    ],
)
def test_data_quality_preflight_failure_translates_before_task_code_allocation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_task_adapter: FakeTaskAdapter,
    field: str,
    reason: str,
    expected_error: type[DsctlError],
) -> None:
    version = "3.2.2"
    inspection_error = DataQualityAuthoringInspectionError(
        ds_version=version,
        field=field,
        reason=reason,
    )
    inspector = _DataQualityInspectorSpy(error=inspection_error)
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        data_quality_authoring_inspector=inspector,
        datasource_adapter=_mysql_datasources(),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    spec_path = tmp_path / "data-quality.yaml"
    spec_path.write_text(
        """
workflow:
  name: guarded-row-count
  project: etl-prod
tasks:
  - name: check-orders
    type: DATA_QUALITY
    task_params:
      datasource: 41
      database: warehouse
      table: orders
      expectedRowCount: 12
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(expected_error, match="live authoring preflight") as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.details == {
        "action": "workflow.create",
        "ds_version": version,
        "field": field,
        "reason": reason,
        "task": "check-orders",
        "task_type": "DATA_QUALITY",
    }
    assert fake_task_adapter.generate_code_calls == []


@pytest.mark.parametrize(
    ("native_kind", "expected_calls"),
    [("fixed-point", 1), ("richer", 0)],
)
def test_data_quality_edit_preflights_only_typed_projection_sources(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    native_kind: str,
    expected_calls: int,
) -> None:
    version = "3.2.2"
    richer = native_kind == "richer"
    live_workflow = fake_workflow_adapter.workflows[0]
    fake_workflow_adapter.dags[101] = FakeDag(
        workflow_definition_value=live_workflow,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=201,
                name="check-orders",
                project_code_value=7,
                project_name_value="etl-prod",
                task_type_value="DATA_QUALITY",
                task_params_value=json.dumps(_data_quality_native_322(richer=richer)),
                worker_group_value="default",
                is_cache_value=FakeEnumValue("NO"),
            )
        ],
        workflow_task_relation_list_value=[],
    )
    inspector = _DataQualityInspectorSpy()
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        data_quality_authoring_inspector=inspector,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(version),
    )
    patch_path = tmp_path / "data-quality.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Guarded DATA_QUALITY workflow
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )

    assert _mapping(result.data)["dry_run"] is True
    assert inspector.calls == (
        [{"datasource_id": 41, "database": "warehouse"}] if expected_calls else []
    )
