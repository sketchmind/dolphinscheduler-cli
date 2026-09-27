from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

JupyterEnvironmentMode = Literal["preinstalled-conda"]


@dataclass(frozen=True, slots=True)
class JupyterAuthoringSurface:
    """Reviewed portable notebook subset and exact runtime hazards."""

    available: bool
    environment_mode: JupyterEnvironmentMode | None
    literal_parameters: bool
    timeouts_as_decimal_strings: bool
    authored_values_logged: bool
    failover_supported: bool

    @property
    def supports_preinstalled_notebook(self) -> bool:
        """Return whether all facts required by the reviewed facet are present."""
        return (
            self.available
            and self.environment_mode == "preinstalled-conda"
            and self.literal_parameters
            and self.timeouts_as_decimal_strings
        )


_JUPYTER_ABSENT = JupyterAuthoringSurface(
    available=False,
    environment_mode=None,
    literal_parameters=False,
    timeouts_as_decimal_strings=False,
    authored_values_logged=False,
    failover_supported=False,
)
_JUPYTER_PREINSTALLED_NOTEBOOK = JupyterAuthoringSurface(
    available=True,
    environment_mode="preinstalled-conda",
    literal_parameters=True,
    timeouts_as_decimal_strings=True,
    authored_values_logged=True,
    failover_supported=False,
)


def _jupyter_surface(version: str) -> JupyterAuthoringSurface:
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
        return _JUPYTER_ABSENT
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
        return _JUPYTER_PREINSTALLED_NOTEBOOK
    message = f"No exact JUPYTER authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
