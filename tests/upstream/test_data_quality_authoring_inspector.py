from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.data_quality import (
    DATA_QUALITY_AUTHORING_VERSIONS,
    DataQualityAuthoringInspectionError,
    DataQualityAuthoringInspector,
    bind_data_quality_authoring_inspector,
)
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.client import DolphinSchedulerClient
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.protocol import DataSourceOperations


_LEGACY_VERSIONS = (
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
)
_MODERN_VERSIONS = ("3.2.0", "3.2.1", "3.2.2")
_ALL_VERSIONS = _LEGACY_VERSIONS + _MODERN_VERSIONS


@dataclass(frozen=True)
class _Enum:
    value: str


@dataclass(frozen=True)
class _DataSource:
    id: int
    type: str | _Enum


@dataclass
class _DataSources:
    visible: list[_DataSource] = field(
        default_factory=lambda: [_DataSource(id=7, type=_Enum("MYSQL"))]
    )
    detail: JsonObject = field(
        default_factory=lambda: {
            "id": 7,
            "type": "MYSQL",
            "database": "warehouse",
        }
    )
    calls: list[tuple[str, object]] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ReadPage[_DataSource]:
        self.calls.append(("list", (page_no, page_size, search)))
        return ReadPage(
            totalList=list(self.visible),
            total=len(self.visible),
            totalPage=1,
            pageSize=page_size,
            currentPage=page_no,
            pageNo=page_no,
        )

    def get(self, *, datasource_id: int) -> JsonObject:
        self.calls.append(("get", datasource_id))
        return dict(self.detail)


@dataclass
class _HttpClient:
    rule_page: JsonValue
    rule_form: JsonValue
    profile: object | None = None
    calls: list[tuple[str, dict[str, object]]] = field(default_factory=list)

    def get_result(
        self,
        path: str,
        *,
        params: dict[str, object] | None = None,
        **kwargs: object,
    ) -> JsonValue:
        del kwargs
        self.calls.append((path, dict(params or {})))
        if path == "data-quality/rule/page":
            return copy.deepcopy(self.rule_page)
        if path == "data-quality/getRuleFormCreateJson":
            return copy.deepcopy(self.rule_form)
        message = f"unexpected GET {path}"
        raise AssertionError(message)


@pytest.mark.parametrize("ds_version", _ALL_VERSIONS)
def test_inspector_proves_visible_mysql_seed_rule_and_form(ds_version: str) -> None:
    data_sources = _DataSources()
    http_client = _HttpClient(
        rule_page=_rule_page(ds_version),
        rule_form=json.dumps(_rule_form(ds_version), separators=(",", ":")),
    )
    inspector = DataQualityAuthoringInspector(
        ds_version,
        cast("DataSourceOperations", data_sources),
        cast("DolphinSchedulerClient", http_client),
    )

    inspector.inspect(
        datasource_id=7,
        database="warehouse" if ds_version in _MODERN_VERSIONS else None,
    )
    inspector.inspect(
        datasource_id=7,
        database="warehouse" if ds_version in _MODERN_VERSIONS else None,
    )

    assert data_sources.calls == [
        ("list", (1, 100, None)),
        ("get", 7),
    ]
    assert http_client.calls == [
        (
            "data-quality/rule/page",
            {"ruleType": 0, "pageNo": 1, "pageSize": 1000},
        ),
        ("data-quality/getRuleFormCreateJson", {"ruleId": 10}),
    ]


def test_inspector_requires_datasource_id_to_be_permission_visible() -> None:
    data_sources = _DataSources(visible=[])
    inspector = _inspector(data_sources=data_sources)

    with pytest.raises(
        DataQualityAuthoringInspectionError,
        match="permission-visible",
    ) as raised:
        inspector.inspect(datasource_id=7, database=None)

    assert raised.value.field == "datasource"
    assert data_sources.calls == [("list", (1, 100, None))]


@pytest.mark.parametrize(
    ("summary_type", "detail", "reason"),
    [
        (
            "POSTGRESQL",
            {"id": 7, "type": "POSTGRESQL", "database": "warehouse"},
            "MYSQL",
        ),
        ("MYSQL", {"id": 7, "type": "POSTGRESQL", "database": "warehouse"}, "MYSQL"),
        ("MYSQL", {"id": 8, "type": "MYSQL", "database": "warehouse"}, "identity"),
        ("MYSQL", {"id": 7, "type": "MYSQL", "database": ""}, "database"),
        ("MYSQL", {"id": 7, "type": "MYSQL", "database": "   "}, "database"),
    ],
)
def test_inspector_rejects_unproved_datasource_detail(
    summary_type: str,
    detail: JsonObject,
    reason: str,
) -> None:
    data_sources = _DataSources(
        visible=[_DataSource(id=7, type=summary_type)],
        detail=detail,
    )

    with pytest.raises(DataQualityAuthoringInspectionError, match=reason):
        _inspector(data_sources=data_sources).inspect(
            datasource_id=7,
            database=None,
        )


@pytest.mark.parametrize("ds_version", _MODERN_VERSIONS)
@pytest.mark.parametrize("database", [None, "analytics"])
def test_modern_inspector_requires_exact_canonical_database(
    ds_version: str,
    database: str | None,
) -> None:
    with pytest.raises(
        DataQualityAuthoringInspectionError,
        match="canonical database",
    ):
        _inspector(ds_version=ds_version).inspect(
            datasource_id=7,
            database=database,
        )


@pytest.mark.parametrize(
    ("mutation", "field", "reason"),
    [
        (lambda page: page["totalList"].clear(), "rule", "id 10"),
        (lambda page: page["totalList"][0].update(name="changed"), "rule", "name"),
        (lambda page: page["totalList"][0].update(type=1), "rule", "type"),
        (lambda page: page["totalList"][0].update(ruleJson="{"), "ruleJson", "JSON"),
        (
            lambda page: _execute_sqls(page).append(dict(_execute_sql(page))),
            "ruleJson.executeSqlList",
            "exactly one",
        ),
        (
            lambda page: _execute_sql(page).update(sql="SELECT 1"),
            "ruleJson.executeSqlList",
            "SQL",
        ),
        (
            lambda page: _execute_sql(page).update(tableAlias="total_count"),
            "ruleJson.executeSqlList",
            "alias",
        ),
        (
            lambda page: _execute_sql(page).update(type=2),
            "ruleJson.executeSqlList",
            "type",
        ),
        (
            lambda page: _execute_sql(page).update(isErrorOutputSql=True),
            "ruleJson.executeSqlList",
            "error-output",
        ),
        (
            lambda page: _input_entries(page).pop(),
            "ruleJson.ruleInputEntryList",
            "field relation",
        ),
        (
            lambda page: _statistics_entry(page).update(
                valuesMap='{"statistics_name":"other"}'
            ),
            "ruleJson.ruleInputEntryList",
            "statistics_name",
        ),
    ],
)
def test_inspector_fails_closed_when_seed_rule_semantics_drift(
    mutation: Callable[[JsonObject], object],
    field: str,
    reason: str,
) -> None:
    page = _rule_page("3.0.0")
    mutation(page)
    _store_rule_json(page)

    with pytest.raises(DataQualityAuthoringInspectionError, match=reason) as raised:
        _inspector(rule_page=page).inspect(datasource_id=7, database=None)

    assert raised.value.field == field


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.2.2"])
@pytest.mark.parametrize(
    "form",
    [
        [],
        [{"field": "comparison_type", "options": []}],
        [
            {
                "field": "comparison_type",
                "options": [{"label": "Other", "value": 1, "disabled": False}],
            }
        ],
        [
            {
                "field": "comparison_type",
                "options": [{"label": "FixValue", "value": "1", "disabled": False}],
            }
        ],
    ],
)
def test_inspector_fails_closed_when_fixed_value_form_mapping_drifts(
    ds_version: str,
    form: list[JsonObject],
) -> None:
    with pytest.raises(
        DataQualityAuthoringInspectionError,
        match="FixValue",
    ) as raised:
        _inspector(ds_version=ds_version, rule_form=json.dumps(form)).inspect(
            datasource_id=7,
            database="warehouse" if ds_version in _MODERN_VERSIONS else None,
        )

    assert raised.value.field == "ruleForm.comparison_type"


def test_binder_is_exactly_limited_to_reviewed_data_quality_versions() -> None:
    assert frozenset(_ALL_VERSIONS) == DATA_QUALITY_AUTHORING_VERSIONS
    assert (
        frozenset(
            version
            for version in TARGET_DS_VERSIONS
            if get_task_authoring_surface(version).data_quality.available
        )
        == DATA_QUALITY_AUTHORING_VERSIONS
    )

    for ds_version in _ALL_VERSIONS:
        profile = make_profile(ds_version=ds_version)
        inspector = bind_data_quality_authoring_inspector(
            ds_version,
            profile,
            http_client=cast(
                "DolphinSchedulerClient",
                _HttpClient({}, [], profile=profile),
            ),
        )
        assert inspector is not None

    assert (
        bind_data_quality_authoring_inspector(
            "3.4.1",
            make_profile(ds_version="3.4.1"),
            http_client=cast("DolphinSchedulerClient", _HttpClient({}, [])),
        )
        is None
    )


def _inspector(
    *,
    ds_version: str = "3.0.0",
    data_sources: _DataSources | None = None,
    rule_page: JsonValue | None = None,
    rule_form: JsonValue | None = None,
) -> DataQualityAuthoringInspector:
    return DataQualityAuthoringInspector(
        ds_version,
        cast("DataSourceOperations", data_sources or _DataSources()),
        cast(
            "DolphinSchedulerClient",
            _HttpClient(
                rule_page=rule_page or _rule_page(ds_version),
                rule_form=(
                    rule_form
                    if rule_form is not None
                    else json.dumps(_rule_form(ds_version), separators=(",", ":"))
                ),
            ),
        ),
    )


def _rule_page(ds_version: str) -> JsonObject:
    rule_json: JsonObject = {
        "ruleInputEntryList": [
            {"field": field, "valuesMap": values_map}
            for field, values_map in _input_fields(ds_version)
        ],
        "executeSqlList": [
            {
                "index": 1,
                "sql": (
                    "SELECT COUNT(*) AS total FROM ${src_table} WHERE (${src_filter})"
                ),
                "tableAlias": "table_count",
                "type": 1,
                "isErrorOutputSql": False,
            }
        ],
    }
    return {
        "totalList": [
            {
                "id": 10,
                "name": "$t(table_count_check)",
                "type": 0,
                "ruleJson": json.dumps(rule_json, separators=(",", ":")),
            }
        ],
        "total": 1,
        "totalPage": 1,
        "pageSize": 1000,
        "currentPage": 1,
    }


def _input_fields(ds_version: str) -> list[tuple[str, str | None]]:
    fields = [
        ("src_connector_type", None),
        ("src_datasource_id", None),
        ("src_table", None),
        ("src_filter", None),
        ("statistics_name", '{"statistics_name":"table_count.total"}'),
        ("check_type", None),
        ("operator", None),
        ("threshold", None),
        ("failure_strategy", None),
        ("comparison_name", None),
        ("comparison_type", None),
    ]
    if ds_version in _MODERN_VERSIONS:
        fields.insert(2, ("src_database", None))
    return fields


def _rule_form(ds_version: str) -> list[JsonObject]:
    del ds_version
    return [
        {"field": "src_table", "options": None},
        {
            "field": "comparison_type",
            "options": [
                {"label": "FixValue", "value": 1, "disabled": False},
                {"label": "TableValue", "value": 2, "disabled": False},
            ],
        },
    ]


def _rule_json(page: JsonObject) -> JsonObject:
    row = cast("list[JsonObject]", page["totalList"])[0]
    cached = row.get("_decodedRuleJson")
    if isinstance(cached, dict):
        return cast("JsonObject", cached)
    value = json.loads(cast("str", row["ruleJson"]))
    row["_decodedRuleJson"] = value
    return cast("JsonObject", value)


def _store_rule_json(page: JsonObject) -> None:
    rows = cast("list[JsonObject]", page["totalList"])
    if not rows:
        return
    row = rows[0]
    decoded = row.pop("_decodedRuleJson", None)
    if decoded is not None:
        row["ruleJson"] = json.dumps(decoded, separators=(",", ":"))


def _statistics_entry(page: JsonObject) -> JsonObject:
    entries = _input_entries(page)
    return next(entry for entry in entries if entry["field"] == "statistics_name")


def _execute_sqls(page: JsonObject) -> list[JsonObject]:
    return cast("list[JsonObject]", _rule_json(page)["executeSqlList"])


def _execute_sql(page: JsonObject) -> JsonObject:
    return _execute_sqls(page)[0]


def _input_entries(page: JsonObject) -> list[JsonObject]:
    return cast("list[JsonObject]", _rule_json(page)["ruleInputEntryList"])
