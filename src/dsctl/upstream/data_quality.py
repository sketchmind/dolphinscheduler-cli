from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, NoReturn, cast

from dsctl.upstream.datasources import DataSourceAdapter
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.protocol import (
        DataQualityAuthoringInspector as DataQualityInspectorPort,
    )
    from dsctl.upstream.protocol import (
        DataSourceOperations,
        DataSourceRecord,
    )


DATA_QUALITY_AUTHORING_VERSIONS = frozenset(
    {
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
        "3.2.0",
        "3.2.1",
        "3.2.2",
    }
)

_RULE_ID = 10
_RULE_PAGE_SIZE = 1_000
_RULE_NAME = "$t(table_count_check)"
_RULE_TYPE = 0
_RULE_SQL = "SELECT COUNT(*) AS total FROM ${src_table} WHERE (${src_filter})"
_RULE_TABLE_ALIAS = "table_count"
_STATISTICS_NAME = "table_count.total"
_LEGACY_INPUT_FIELDS = frozenset(
    {
        "src_connector_type",
        "src_datasource_id",
        "src_table",
        "src_filter",
        "statistics_name",
        "check_type",
        "operator",
        "threshold",
        "failure_strategy",
        "comparison_name",
        "comparison_type",
    }
)
_MODERN_INPUT_FIELDS = _LEGACY_INPUT_FIELDS | {"src_database"}


class DataQualityAuthoringInspectionError(Exception):
    """Live DATA_QUALITY prerequisites do not match the reviewed fixed wire."""

    def __init__(
        self,
        *,
        ds_version: str,
        field: str,
        reason: str,
        cause: BaseException | None = None,
    ) -> None:
        """Retain a narrow failure coordinate for service-layer translation."""
        super().__init__(f"DATA_QUALITY inspection failed for {field}: {reason}")
        self.ds_version = ds_version
        self.field = field
        self.reason = reason
        self.cause = cause


class DataQualityAuthoringInspector:
    """Prove the mutable upstream facts used by one closed DQ authoring facet."""

    def __init__(
        self,
        ds_version: str,
        datasources: DataSourceOperations,
        http_client: DolphinSchedulerClient,
    ) -> None:
        """Bind exact-version datasource reads and the two stable DQ GETs."""
        surface = get_task_authoring_surface(ds_version).data_quality
        if not surface.available:
            message = (
                f"DS {ds_version} has no reviewed DATA_QUALITY authoring inspector"
            )
            raise ValueError(message)
        self.ds_version = ds_version
        self._surface = surface
        self._datasources = datasources
        self._http_client = http_client
        self._inspected_datasources: set[tuple[int, str | None]] = set()
        self._rule_attested = False

    def inspect(self, *, datasource_id: int, database: str | None) -> None:
        """Fail closed unless the datasource and stock rule still match review."""
        try:
            datasource_key = (datasource_id, database)
            if datasource_key not in self._inspected_datasources:
                self._inspect_datasource(
                    datasource_id=datasource_id,
                    database=database,
                )
                self._inspected_datasources.add(datasource_key)
            if not self._rule_attested:
                self._inspect_rule()
                self._inspect_rule_form()
                self._rule_attested = True
        except DataQualityAuthoringInspectionError:
            raise
        except Exception as exc:
            raise DataQualityAuthoringInspectionError(
                ds_version=self.ds_version,
                field="remote",
                reason="the required live inspection could not be completed",
                cause=exc,
            ) from exc

    def _inspect_datasource(
        self,
        *,
        datasource_id: int,
        database: str | None,
    ) -> None:
        visible = collect_pages(
            lambda *, page_no, page_size: self._datasources.list(
                page_no=page_no,
                page_size=page_size,
            ),
            resource="datasource",
        )
        matches = [item for item in visible if item.id == datasource_id]
        if len(matches) != 1:
            self._fail(
                "datasource",
                "the selected datasource id is not uniquely permission-visible",
            )
        summary = matches[0]
        if _datasource_type(summary) != "MYSQL":
            self._fail("datasource.type", "the selected datasource is not MYSQL")

        detail = self._datasources.get(datasource_id=datasource_id)
        if _strict_int(detail.get("id")) != datasource_id:
            self._fail("datasource.id", "the detail identity does not match")
        if _json_enum_value(detail.get("type")) != "MYSQL":
            self._fail("datasource.type", "the datasource detail is not MYSQL")
        detail_database = detail.get("database")
        if not isinstance(detail_database, str) or not detail_database.strip():
            self._fail("datasource.database", "the datasource database is blank")

        if self._surface.database_wire_required:
            if not isinstance(database, str) or not database:
                self._fail(
                    "datasource.database",
                    "the modern facet requires a canonical database",
                )
            if detail_database != database:
                self._fail(
                    "datasource.database",
                    "the datasource detail does not match the canonical database",
                )
        elif database is not None:
            self._fail(
                "datasource.database",
                "the legacy canonical facet must omit database",
            )

    def _inspect_rule(self) -> None:
        payload = self._http_client.get_result(
            "data-quality/rule/page",
            params={
                "ruleType": _RULE_TYPE,
                "pageNo": 1,
                "pageSize": _RULE_PAGE_SIZE,
            },
        )
        page = self._mapping(payload, field="rulePage")
        rows = self._sequence(page.get("totalList"), field="rulePage.totalList")
        matches = [
            self._mapping(row, field="rulePage.totalList[]")
            for row in rows
            if isinstance(row, Mapping) and _strict_int(row.get("id")) == _RULE_ID
        ]
        if len(matches) != 1:
            self._fail("rule", "stock rule id 10 is not uniquely visible")
        rule = matches[0]
        if rule.get("name") != _RULE_NAME:
            self._fail("rule", "stock rule id 10 has an unexpected name")
        if _strict_int(rule.get("type")) != _RULE_TYPE:
            self._fail("rule", "stock rule id 10 has an unexpected type")
        rule_json = self._decode_json_text(rule.get("ruleJson"), field="ruleJson")
        self._inspect_rule_json(rule_json)

    def _inspect_rule_json(self, payload: JsonValue) -> None:
        rule = self._mapping(payload, field="ruleJson")
        if set(rule) != {"ruleInputEntryList", "executeSqlList"}:
            self._fail("ruleJson", "the rule JSON semantic root has drifted")

        execute_sqls = self._sequence(
            rule.get("executeSqlList"),
            field="ruleJson.executeSqlList",
        )
        if len(execute_sqls) != 1:
            self._fail(
                "ruleJson.executeSqlList",
                "the rule must contain exactly one execute SQL",
            )
        execute_sql = self._mapping(
            execute_sqls[0],
            field="ruleJson.executeSqlList",
        )
        if execute_sql.get("sql") != _RULE_SQL:
            self._fail("ruleJson.executeSqlList", "the stock SQL has drifted")
        if execute_sql.get("tableAlias") != _RULE_TABLE_ALIAS:
            self._fail("ruleJson.executeSqlList", "the stock table alias has drifted")
        if _strict_int(execute_sql.get("index")) != 1:
            self._fail("ruleJson.executeSqlList", "the stock SQL index has drifted")
        if _strict_int(execute_sql.get("type")) != 1:
            self._fail("ruleJson.executeSqlList", "the stock SQL type has drifted")
        if execute_sql.get("isErrorOutputSql") is not False:
            self._fail(
                "ruleJson.executeSqlList",
                "the stock SQL error-output flag has drifted",
            )

        inputs = self._sequence(
            rule.get("ruleInputEntryList"),
            field="ruleJson.ruleInputEntryList",
        )
        input_rows = [
            self._mapping(item, field="ruleJson.ruleInputEntryList") for item in inputs
        ]
        fields = [item.get("field") for item in input_rows]
        expected_fields = (
            _MODERN_INPUT_FIELDS
            if self._surface.database_wire_required
            else _LEGACY_INPUT_FIELDS
        )
        if len(fields) != len(expected_fields) or set(fields) != expected_fields:
            self._fail(
                "ruleJson.ruleInputEntryList",
                "the stock input field relation has drifted",
            )
        statistics_entry = next(
            item for item in input_rows if item.get("field") == "statistics_name"
        )
        values_map = self._decode_json_text(
            statistics_entry.get("valuesMap"),
            field="ruleJson.ruleInputEntryList",
        )
        if values_map != {"statistics_name": _STATISTICS_NAME}:
            self._fail(
                "ruleJson.ruleInputEntryList",
                "the stock statistics_name mapping has drifted",
            )

    def _inspect_rule_form(self) -> None:
        payload = self._http_client.get_result(
            "data-quality/getRuleFormCreateJson",
            params={"ruleId": _RULE_ID},
        )
        form_payload = self._decode_json_text(payload, field="ruleForm")
        entries = self._sequence(form_payload, field="ruleForm")
        comparison_entries = [
            self._mapping(entry, field="ruleForm.comparison_type")
            for entry in entries
            if isinstance(entry, Mapping) and entry.get("field") == "comparison_type"
        ]
        if len(comparison_entries) != 1:
            self._fail(
                "ruleForm.comparison_type",
                "the form does not expose one comparison_type FixValue mapping",
            )
        options = self._sequence(
            comparison_entries[0].get("options"),
            field="ruleForm.comparison_type",
        )
        fixed_value_options = [
            self._mapping(option, field="ruleForm.comparison_type")
            for option in options
            if isinstance(option, Mapping) and _strict_int(option.get("value")) == 1
        ]
        if len(fixed_value_options) != 1:
            self._fail(
                "ruleForm.comparison_type",
                "comparison_type value 1 does not map uniquely to FixValue",
            )
        fixed_value = fixed_value_options[0]
        if (
            fixed_value.get("label") != "FixValue"
            or fixed_value.get("disabled") is not False
        ):
            self._fail(
                "ruleForm.comparison_type",
                "comparison_type value 1 is not an enabled FixValue option",
            )

    def _decode_json_text(self, value: JsonValue, *, field: str) -> JsonValue:
        if not isinstance(value, str):
            self._fail(field, "the response is not JSON text")
        try:
            return cast(
                "JsonValue",
                json.loads(
                    value,
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_json_constant,
                ),
            )
        except (json.JSONDecodeError, ValueError) as exc:
            raise DataQualityAuthoringInspectionError(
                ds_version=self.ds_version,
                field=field,
                reason="the response contains invalid or ambiguous JSON",
                cause=exc,
            ) from exc

    def _mapping(self, value: JsonValue, *, field: str) -> JsonObject:
        if not isinstance(value, Mapping) or not all(
            isinstance(key, str) for key in value
        ):
            self._fail(field, "the response is not an object")
        return cast("JsonObject", dict(value))

    def _sequence(self, value: JsonValue, *, field: str) -> list[JsonValue]:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            self._fail(field, "the response is not a list")
        return list(value)

    def _fail(self, field: str, reason: str) -> NoReturn:
        raise DataQualityAuthoringInspectionError(
            ds_version=self.ds_version,
            field=field,
            reason=reason,
        )


def bind_data_quality_authoring_inspector(
    ds_version: str,
    profile: ClusterProfile,
    *,
    http_client: DolphinSchedulerClient,
) -> DataQualityInspectorPort | None:
    """Bind only the seven exact DQ profiles without adding a CRUD surface."""
    if ds_version not in DATA_QUALITY_AUTHORING_VERSIONS:
        return None
    datasources = (
        DataSourceAdapter.for_version(ds_version)
        .bind(
            profile,
            http_client=http_client,
        )
        .datasources
    )
    return cast(
        "DataQualityInspectorPort",
        DataQualityAuthoringInspector(
            ds_version,
            datasources,
            http_client,
        ),
    )


def _datasource_type(datasource: DataSourceRecord) -> str | None:
    return enum_value(datasource.type)


def _json_enum_value(value: JsonValue) -> str | None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return None if value is None else str(value)
    enum_string = getattr(value, "value", None)
    return enum_string if isinstance(enum_string, str) else None


def _strict_int(value: JsonValue) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            message = f"duplicate JSON key {key!r}"
            raise ValueError(message)
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    message = f"non-standard JSON constant {value!r}"
    raise ValueError(message)


__all__ = [
    "DATA_QUALITY_AUTHORING_VERSIONS",
    "DataQualityAuthoringInspectionError",
    "DataQualityAuthoringInspector",
    "bind_data_quality_authoring_inspector",
]
