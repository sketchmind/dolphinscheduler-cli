from __future__ import annotations

import re

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference

DATA_QUALITY_MYSQL_IDENTIFIER_JSON_SCHEMA_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,63}$"
DATA_QUALITY_EXPECTED_ROW_COUNT_MAX = 9_007_199_254_740_991


def _validate_data_quality_mysql_identifier(value: str, *, field: str) -> str:
    """Keep interpolated DQ identifiers within one conservative literal subset."""
    if not re.fullmatch(DATA_QUALITY_MYSQL_IDENTIFIER_JSON_SCHEMA_PATTERN, value):
        message = (
            f"DATA_QUALITY {field} must be one conservative literal MySQL identifier"
        )
        raise ValueError(message)
    return value


class DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec(TaskParamsSpec):
    """Legacy local-Spark equality check using the datasource database."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
    )

    datasource: DatasourceReference = Field(
        description="Positive MYSQL datasource id or exact datasource name."
    )
    table: str = Field(
        min_length=1,
        max_length=64,
        json_schema_extra={
            "pattern": DATA_QUALITY_MYSQL_IDENTIFIER_JSON_SCHEMA_PATTERN,
        },
    )
    expected_row_count: int = Field(
        alias="expectedRowCount",
        ge=0,
        le=DATA_QUALITY_EXPECTED_ROW_COUNT_MAX,
    )

    @field_validator("table")
    @classmethod
    def validate_mysql_identifier(cls, value: str) -> str:
        """Keep SQL interpolation unambiguous by accepting conservative names."""
        return _validate_data_quality_mysql_identifier(value, field="table")

    @field_validator("datasource")
    @classmethod
    def validate_datasource_range(
        cls,
        value: DatasourceReference,
    ) -> DatasourceReference:
        """Keep numeric ids inside the native Java integer range."""
        if isinstance(value, int) and value > 2_147_483_647:
            message = "datasource id must be at most 2147483647"
            raise ValueError(message)
        return value


class DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec(
    DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec,
):
    """Modern local-Spark equality check with an explicit MySQL database."""

    database: str = Field(
        min_length=1,
        max_length=64,
        json_schema_extra={
            "pattern": DATA_QUALITY_MYSQL_IDENTIFIER_JSON_SCHEMA_PATTERN,
        },
    )

    @field_validator("database")
    @classmethod
    def validate_mysql_database_identifier(cls, value: str) -> str:
        """Keep the exact database wire literal and interpolation-safe."""
        return _validate_data_quality_mysql_identifier(value, field="database")
