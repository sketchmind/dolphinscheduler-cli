from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

OpenmldbPythonLauncher = Literal["PYTHON_HOME", "PYTHON_LAUNCHER"]


@dataclass(frozen=True, slots=True)
class OpenmldbAuthoringSurface:
    """OpenMLDB Python launcher, logging, and recovery semantics."""

    available: bool
    python_launcher: OpenmldbPythonLauncher | None
    task_params_logged: bool
    sql_logged: bool
    rendered_script_logged: bool
    generated_script_logged: bool
    line_endings_normalized: bool
    result_output_supported: bool
    failover_supported: bool
    retry_reexecutes: bool
    exclusion_reason: str | None = None


_OPENMLDB_ABSENT = OpenmldbAuthoringSurface(
    available=False,
    python_launcher=None,
    task_params_logged=False,
    sql_logged=False,
    rendered_script_logged=False,
    generated_script_logged=False,
    line_endings_normalized=False,
    result_output_supported=False,
    failover_supported=False,
    retry_reexecutes=False,
)
_OPENMLDB_PYTHON_HOME = OpenmldbAuthoringSurface(
    available=True,
    python_launcher="PYTHON_HOME",
    task_params_logged=True,
    sql_logged=True,
    rendered_script_logged=True,
    generated_script_logged=True,
    line_endings_normalized=True,
    result_output_supported=False,
    failover_supported=False,
    retry_reexecutes=True,
)
_OPENMLDB_PYTHON_LAUNCHER = OpenmldbAuthoringSurface(
    available=True,
    python_launcher="PYTHON_LAUNCHER",
    task_params_logged=True,
    sql_logged=True,
    rendered_script_logged=True,
    generated_script_logged=True,
    line_endings_normalized=True,
    result_output_supported=False,
    failover_supported=False,
    retry_reexecutes=True,
)


def _openmldb_surface(version: str) -> OpenmldbAuthoringSurface:
    if version == "3.1.2":
        return replace(
            _OPENMLDB_PYTHON_HOME,
            available=False,
            exclusion_reason="python-parent-parameters-uninitialized-after-execution",
        )
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return _OPENMLDB_ABSENT
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
        return _OPENMLDB_PYTHON_HOME
    if version in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }:
        return _OPENMLDB_PYTHON_LAUNCHER
    message = f"No exact OPENMLDB authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
