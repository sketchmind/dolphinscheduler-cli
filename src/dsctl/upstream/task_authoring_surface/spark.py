from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SparkInlineSqlWireEpoch = Literal[
    "legacy-spark2",
    "script",
    "script-master",
]
SparkHomeVariable = Literal["SPARK_HOME2", "SPARK_HOME"]


@dataclass(frozen=True, slots=True)
class SparkInlineSqlAuthoringSurface:
    """Inline local Spark SQL wire and runtime semantics for one release."""

    available: bool
    wire_epoch: SparkInlineSqlWireEpoch | None
    parameter_substitution: bool
    spark_home: SparkHomeVariable | None
    sql_logged: bool
    line_endings_normalized: bool
    failover_supported: bool
    retry_reexecutes: bool


_SPARK_INLINE_SQL_ABSENT = SparkInlineSqlAuthoringSurface(
    available=False,
    wire_epoch=None,
    parameter_substitution=False,
    spark_home=None,
    sql_logged=False,
    line_endings_normalized=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_SPARK_INLINE_SQL_LEGACY_LITERAL = SparkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="legacy-spark2",
    parameter_substitution=False,
    spark_home="SPARK_HOME2",
    sql_logged=True,
    line_endings_normalized=True,
    failover_supported=False,
    retry_reexecutes=True,
)
_SPARK_INLINE_SQL_LEGACY_PARAMETERIZED = SparkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="legacy-spark2",
    parameter_substitution=True,
    spark_home="SPARK_HOME2",
    sql_logged=True,
    line_endings_normalized=True,
    failover_supported=False,
    retry_reexecutes=True,
)
_SPARK_INLINE_SQL_SCRIPT = SparkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="script",
    parameter_substitution=True,
    spark_home="SPARK_HOME",
    sql_logged=True,
    line_endings_normalized=True,
    failover_supported=False,
    retry_reexecutes=True,
)
_SPARK_INLINE_SQL_SCRIPT_MASTER = SparkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="script-master",
    parameter_substitution=True,
    spark_home="SPARK_HOME",
    sql_logged=True,
    line_endings_normalized=True,
    failover_supported=False,
    retry_reexecutes=True,
)


def _spark_inline_sql_surface(version: str) -> SparkInlineSqlAuthoringSurface:
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        return _SPARK_INLINE_SQL_ABSENT
    if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _SPARK_INLINE_SQL_LEGACY_LITERAL
    if version in {
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
        return _SPARK_INLINE_SQL_LEGACY_PARAMETERIZED
    if version in {"3.2.0", "3.2.1"}:
        return _SPARK_INLINE_SQL_SCRIPT
    if version in {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _SPARK_INLINE_SQL_SCRIPT_MASTER
    message = f"No exact SPARK inline-SQL surface for DolphinScheduler {version}"
    raise ValueError(message)
