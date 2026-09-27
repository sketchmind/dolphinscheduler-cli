from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

FlinkInlineSqlWireEpoch = Literal[
    "legacy-default-charset",
    "broken-unconditional-main-jar-no-review",
    "broken-inverted-sql-deploy-mode",
    "utf8-path-command",
    "utf8-flink-home-command",
    "utf8-parameterized",
]
FlinkScriptEncoding = Literal["platform-default", "utf-8"]
FlinkSqlCommand = Literal["PATH", "FLINK_HOME"]
FlinkStreamStopSequence = Literal[
    "none",
    "plugin-cancel-then-pid-tree",
    "pid-tree-then-plugin-cancel",
    "plugin-cancel-only",
]


@dataclass(frozen=True, slots=True)
class FlinkInlineSqlAuthoringSurface:
    """Inline local Flink SQL wire and executor semantics for one release."""

    available: bool
    wire_epoch: FlinkInlineSqlWireEpoch | None
    script_encoding: FlinkScriptEncoding | None
    sql_command: FlinkSqlCommand | None
    parameter_substitution: bool
    task_params_logged: bool
    script_logged: bool
    command_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool
    exclusion_reason: str | None


@dataclass(frozen=True, slots=True)
class FlinkStreamInlineSqlAuthoringSurface(FlinkInlineSqlAuthoringSurface):
    """FLINK_STREAM local-SQL execution plus exact stop-path limitations."""

    local_sql_application_id_expected: bool
    cancel_requires_application_id: bool
    savepoint_requires_application_id: bool
    stop_sequence: FlinkStreamStopSequence
    reliable_stop_supported: bool


_FLINK_INLINE_SQL_ABSENT = FlinkInlineSqlAuthoringSurface(
    available=False,
    wire_epoch=None,
    script_encoding=None,
    sql_command=None,
    parameter_substitution=False,
    task_params_logged=False,
    script_logged=False,
    command_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
    exclusion_reason="sql-program-type-absent",
)
_FLINK_INLINE_SQL_BROKEN_MAIN_JAR = FlinkInlineSqlAuthoringSurface(
    available=False,
    wire_epoch="broken-unconditional-main-jar-no-review",
    script_encoding="utf-8",
    sql_command="PATH",
    parameter_substitution=False,
    task_params_logged=True,
    script_logged=False,
    command_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=False,
    exclusion_reason="broken-unconditional-main-jar-no-review",
)
_FLINK_INLINE_SQL_LEGACY_DEFAULT_CHARSET = FlinkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="legacy-default-charset",
    script_encoding="platform-default",
    sql_command="PATH",
    parameter_substitution=False,
    task_params_logged=True,
    script_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
    exclusion_reason=None,
)
_FLINK_INLINE_SQL_UTF8_PATH = FlinkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="utf8-path-command",
    script_encoding="utf-8",
    sql_command="PATH",
    parameter_substitution=False,
    task_params_logged=True,
    script_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
    exclusion_reason=None,
)
_FLINK_INLINE_SQL_UTF8_FLINK_HOME = FlinkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="utf8-flink-home-command",
    script_encoding="utf-8",
    sql_command="FLINK_HOME",
    parameter_substitution=False,
    task_params_logged=True,
    script_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
    exclusion_reason=None,
)
_FLINK_INLINE_SQL_INVERTED_DEPLOY_MODE = replace(
    _FLINK_INLINE_SQL_UTF8_PATH,
    available=False,
    wire_epoch="broken-inverted-sql-deploy-mode",
    exclusion_reason="local-cluster-sql-execution-target-inverted",
)
_FLINK_INLINE_SQL_UTF8_PARAMETERIZED = FlinkInlineSqlAuthoringSurface(
    available=True,
    wire_epoch="utf8-parameterized",
    script_encoding="utf-8",
    sql_command="FLINK_HOME",
    parameter_substitution=True,
    task_params_logged=True,
    script_logged=True,
    command_logged=True,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_reexecutes=True,
    exclusion_reason=None,
)
_FLINK_STREAM_EXECUTOR_REVIEWS: dict[
    str,
    tuple[FlinkStreamStopSequence, str | None],
] = {
    "3.1.0": ("plugin-cancel-then-pid-tree", None),
    "3.1.1": ("plugin-cancel-then-pid-tree", None),
    "3.1.2": ("plugin-cancel-then-pid-tree", None),
    "3.1.3": ("plugin-cancel-then-pid-tree", None),
    "3.1.4": ("plugin-cancel-then-pid-tree", None),
    "3.1.5": ("plugin-cancel-then-pid-tree", None),
    "3.1.6": ("plugin-cancel-then-pid-tree", None),
    "3.1.7": ("plugin-cancel-then-pid-tree", None),
    "3.1.8": ("plugin-cancel-then-pid-tree", None),
    "3.1.9": ("plugin-cancel-then-pid-tree", None),
    "3.2.0": ("pid-tree-then-plugin-cancel", None),
    "3.2.1": ("pid-tree-then-plugin-cancel", None),
    "3.2.2": ("pid-tree-then-plugin-cancel", None),
    "3.3.1": ("plugin-cancel-only", "stream-executor-service-not-supported"),
    "3.3.2": ("plugin-cancel-only", "stream-executor-service-not-supported"),
    "3.4.0": ("plugin-cancel-only", "stream-executor-service-not-supported"),
    "3.4.1": ("plugin-cancel-only", "stream-executor-service-not-supported"),
    "3.4.2": ("plugin-cancel-only", "stream-executor-service-not-supported"),
    "3.4.3": ("plugin-cancel-only", "stream-executor-service-not-supported"),
}


def _flink_inline_sql_surface(version: str) -> FlinkInlineSqlAuthoringSurface:
    if version == "3.1.1":
        return _FLINK_INLINE_SQL_INVERTED_DEPLOY_MODE
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
        return _FLINK_INLINE_SQL_ABSENT
    if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _FLINK_INLINE_SQL_LEGACY_DEFAULT_CHARSET
    if version == "3.1.0":
        return _FLINK_INLINE_SQL_BROKEN_MAIN_JAR
    if version in {
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
    }:
        return _FLINK_INLINE_SQL_UTF8_PATH
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1"}:
        return _FLINK_INLINE_SQL_UTF8_FLINK_HOME
    if version in {"3.4.2", "3.4.3"}:
        return _FLINK_INLINE_SQL_UTF8_PARAMETERIZED
    message = f"No exact FLINK inline-SQL surface for DolphinScheduler {version}"
    raise ValueError(message)


def _flink_stream_surface_from_flink(
    surface: FlinkInlineSqlAuthoringSurface,
    *,
    stop_sequence: FlinkStreamStopSequence,
    available: bool | None = None,
    exclusion_reason: str | None = None,
) -> FlinkStreamInlineSqlAuthoringSurface:
    """Deepen one shared Flink executor epoch with stream stop semantics."""
    registered = surface.wire_epoch is not None
    return FlinkStreamInlineSqlAuthoringSurface(
        available=surface.available if available is None else available,
        wire_epoch=surface.wire_epoch,
        script_encoding=surface.script_encoding,
        sql_command=surface.sql_command,
        parameter_substitution=surface.parameter_substitution,
        task_params_logged=surface.task_params_logged,
        script_logged=surface.script_logged,
        command_logged=surface.command_logged,
        result_output_supported=surface.result_output_supported,
        durable_application_id=surface.durable_application_id,
        failover_supported=surface.failover_supported,
        retry_reexecutes=surface.retry_reexecutes,
        exclusion_reason=(
            surface.exclusion_reason if exclusion_reason is None else exclusion_reason
        ),
        local_sql_application_id_expected=False,
        cancel_requires_application_id=registered,
        savepoint_requires_application_id=registered,
        stop_sequence=stop_sequence,
        reliable_stop_supported=False,
    )


def _flink_stream_inline_sql_surface(
    version: str,
) -> FlinkStreamInlineSqlAuthoringSurface:
    """Select the stream plugin's own main-jar guard before shared SQL rendering."""
    flink_surface = (
        _FLINK_INLINE_SQL_BROKEN_MAIN_JAR
        if version in {"3.1.0", "3.1.1", "3.1.2", "3.1.3", "3.1.4"}
        else _flink_inline_sql_surface(version)
    )
    if flink_surface.wire_epoch in {None, "legacy-default-charset"}:
        return _flink_stream_surface_from_flink(
            _FLINK_INLINE_SQL_ABSENT,
            stop_sequence="none",
            exclusion_reason="task-type-absent",
        )
    review = _FLINK_STREAM_EXECUTOR_REVIEWS.get(version)
    if review is None:
        message = f"No FLINK_STREAM executor review for DolphinScheduler {version}"
        raise ValueError(message)
    stop_sequence, exclusion_reason = review
    return _flink_stream_surface_from_flink(
        flink_surface,
        stop_sequence=stop_sequence,
        available=False if exclusion_reason is not None else None,
        exclusion_reason=exclusion_reason,
    )
