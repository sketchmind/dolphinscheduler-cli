from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Literal

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.common import GlobalParamSpec
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference

if TYPE_CHECKING:
    from dsctl.models.common import (
        YamlValue,
    )


class Sql139DatasourceType(StrEnum):
    """Datasource types implemented by the exact DolphinScheduler 1.3.9 SQL task."""

    MYSQL = "MYSQL"
    POSTGRESQL = "POSTGRESQL"
    HIVE = "HIVE"
    SPARK = "SPARK"
    CLICKHOUSE = "CLICKHOUSE"
    ORACLE = "ORACLE"
    SQLSERVER = "SQLSERVER"
    DB2 = "DB2"


_SQL_139_INTEGER_TYPE_ERROR = "sql_139_integer_type"


class SqlTaskParamsSpec(TaskParamsSpec):
    """Typed YAML shape for SQL task params."""

    datasource_type: str = Field(alias="type")
    datasource: DatasourceReference = Field(
        description="Positive datasource id or exact datasource name."
    )
    sql: str
    sql_type: int = Field(alias="sqlType", ge=0, le=1)
    send_email: bool | None = Field(default=None, alias="sendEmail")
    display_rows: int | None = Field(default=None, alias="displayRows", ge=0)
    show_type: str | None = Field(default=None, alias="showType")
    conn_params: str | None = Field(default=None, alias="connParams")
    pre_statements: list[str] = Field(default_factory=list, alias="preStatements")
    post_statements: list[str] = Field(default_factory=list, alias="postStatements")
    group_id: int | None = Field(default=None, alias="groupId", ge=0)
    title: str | None = None
    limit: int | None = Field(default=None, ge=0)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )
    var_pool: list[GlobalParamSpec] = Field(default_factory=list, alias="varPool")

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep DS SQL task list fields non-null even when YAML omits them."""
        return ("pre_statements", "post_statements", "local_params", "var_pool")

    @field_validator("datasource_type", "sql", "show_type")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Reject empty SQL text fields after trimming."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            message = "Task text fields must not be empty"
            raise ValueError(message)
        return normalized


class Sql139InlineTaskParamsSpec(TaskParamsSpec):
    """Closed canonical SQL subset projected onto the exact 1.3.9 wire."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    datasource_type: Sql139DatasourceType = Field(alias="type")
    datasource: DatasourceReference = Field(
        description="Positive datasource id or exact datasource name."
    )
    sql: str
    sql_type: Literal[0, 1] = Field(alias="sqlType")
    display_rows: int = Field(default=10, alias="displayRows", ge=0, le=2_147_483_647)
    conn_params: str = Field(default="", alias="connParams")
    pre_statements: list[str] = Field(default_factory=list, alias="preStatements")
    post_statements: list[str] = Field(default_factory=list, alias="postStatements")
    limit: int = Field(default=0, ge=0, le=2_147_483_647)
    local_params: list[GlobalParamSpec] = Field(
        default_factory=list,
        alias="localParams",
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Materialize every safe legacy default owned by canonical authoring."""
        return (
            "display_rows",
            "conn_params",
            "pre_statements",
            "post_statements",
            "limit",
            "local_params",
        )

    @field_validator(
        "sql_type",
        "display_rows",
        "limit",
        mode="before",
    )
    @classmethod
    def validate_strict_integer(
        cls,
        value: YamlValue,
        info: ValidationInfo,
    ) -> YamlValue:
        """Reject booleans and strings before Pydantic can coerce integer fields."""
        if not isinstance(value, int) or isinstance(value, bool):
            field_name = {
                "sql_type": "sqlType",
                "display_rows": "displayRows",
            }.get(info.field_name or "", info.field_name)
            message = f"{field_name} must be an integer"
            raise PydanticCustomError(_SQL_139_INTEGER_TYPE_ERROR, message)
        return value

    @field_validator("datasource")
    @classmethod
    def validate_datasource_range(
        cls,
        value: DatasourceReference,
    ) -> DatasourceReference:
        """Keep numeric ids inside the exact legacy Java integer range."""
        if isinstance(value, int) and value > 2_147_483_647:
            message = "datasource id must be at most 2147483647"
            raise ValueError(message)
        return value

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, value: str) -> str:
        """Require SQL text while preserving its exact spelling."""
        if not value.strip():
            message = "sql must not be blank"
            raise ValueError(message)
        return value

    @field_validator("pre_statements", "post_statements")
    @classmethod
    def validate_statements(cls, value: list[str]) -> list[str]:
        """Require every optional pre/post statement to contain SQL text."""
        if any(not statement.strip() for statement in value):
            message = "preStatements and postStatements entries must not be blank"
            raise ValueError(message)
        return value

    @field_validator("local_params")
    @classmethod
    def validate_local_params(
        cls,
        value: list[GlobalParamSpec],
    ) -> list[GlobalParamSpec]:
        """Own unique IN-only parameters over the exact nine scalar types."""
        names = [parameter.prop for parameter in value]
        if len(names) != len(set(names)):
            message = "localParams prop names must be unique"
            raise ValueError(message)
        for parameter in value:
            if parameter.direct.value != "IN":
                message = "localParams supports only IN parameters"
                raise ValueError(message)
            if parameter.type.value in {"LIST", "FILE"}:
                message = "localParams supports only the nine scalar data types"
                raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_conn_params(self) -> Sql139InlineTaskParamsSpec:
        """Restrict connection overrides to the reversible HIVE-only grammar."""
        if self.datasource_type is not Sql139DatasourceType.HIVE:
            if self.conn_params:
                message = "connParams is available only when type is HIVE"
                raise ValueError(message)
            return self
        if not self.conn_params:
            return self
        keys: set[str] = set()
        for entry in self.conn_params.split(";"):
            if entry.count("=") != 1:
                message = (
                    "connParams must use key=value entries separated by semicolons"
                )
                raise ValueError(message)
            key, value = entry.split("=", 1)
            if not key.strip() or not value.strip():
                message = "connParams keys and values must not be blank"
                raise ValueError(message)
            if key in keys:
                message = "connParams keys must be unique"
                raise ValueError(message)
            keys.add(key)
        return self
