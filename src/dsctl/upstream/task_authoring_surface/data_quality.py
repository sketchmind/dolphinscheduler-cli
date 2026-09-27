from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

DataQualityResultOperator = Literal["EQ", "NE"]


@dataclass(frozen=True, slots=True)
class DataQualityAuthoringSurface:
    """Closed MySQL table-count rule and exact result-comparison epoch."""

    available: bool
    rule_id: int | None
    main_class: str | None
    fixed_value_comparison: bool
    blocking_failure_strategy: bool
    result_operator: DataQualityResultOperator | None
    database_field_required: bool
    database_wire_required: bool


_DATA_QUALITY_ABSENT = DataQualityAuthoringSurface(
    available=False,
    rule_id=None,
    main_class=None,
    fixed_value_comparison=False,
    blocking_failure_strategy=False,
    result_operator=None,
    database_field_required=False,
    database_wire_required=False,
)
_DATA_QUALITY_TABLE_COUNT_NE = DataQualityAuthoringSurface(
    available=True,
    rule_id=10,
    main_class="org.apache.dolphinscheduler.data.quality.DataQualityApplication",
    fixed_value_comparison=True,
    blocking_failure_strategy=True,
    result_operator="NE",
    database_field_required=False,
    database_wire_required=False,
)
_DATA_QUALITY_TABLE_COUNT_EQ = replace(
    _DATA_QUALITY_TABLE_COUNT_NE,
    result_operator="EQ",
)
_DATA_QUALITY_TABLE_COUNT_EQ_WITH_DATABASE = replace(
    _DATA_QUALITY_TABLE_COUNT_EQ,
    database_field_required=True,
    database_wire_required=True,
)


def _data_quality_surface(version: str) -> DataQualityAuthoringSurface:
    if version in {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
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
    }:
        return _DATA_QUALITY_TABLE_COUNT_NE
    if version in {"3.0.4", "3.0.5", "3.0.6"}:
        return _DATA_QUALITY_TABLE_COUNT_EQ
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _DATA_QUALITY_TABLE_COUNT_EQ_WITH_DATABASE
    if version in TARGET_DS_VERSIONS:
        return _DATA_QUALITY_ABSENT
    message = f"No exact DATA_QUALITY authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
